"""Scope-bound hybrid retrieval for active Knowledge Base content."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import Embedder, rrf_fuse

from .models import (
    KnowledgeCitation,
    KnowledgeHit,
    KnowledgePublicCode,
    KnowledgeSearchMode,
    KnowledgeSearchStatus,
    KnowledgeValidationError,
    validate_knowledge_base_id,
    validate_scope_id,
)
from .store import _PG_SET_SCOPE, _pg_error_boundary, _pg_harden_engine_logging

_DEFAULT_QUERY_MAX_CHARS = 2_000
_DEFAULT_K_MAX = 10
_DEFAULT_CANDIDATE_MULTIPLIER = 3
_MIN_CANDIDATES = 20
_SNIPPET_MAX_CHARS = 2_000

_EMBEDDING_UNAVAILABLE = "embedding_unavailable"
_EMBEDDING_CONFIGURATION_MISMATCH = KnowledgePublicCode.embedding_configuration_mismatch.value

_ACTIVE_KB = text(
    "SELECT embedding_model, embedding_dim "
    "FROM knowledge_bases "
    "WHERE scope_id = :scope AND id = :kb AND status = 'active'"
)

_LEXICAL_CHUNKS = text(
    """
    SELECT c.id
    FROM kb_chunks AS c
    JOIN kb_document_versions AS v
      ON v.scope_id = c.scope_id
     AND v.kb_id = c.kb_id
     AND v.document_id = c.document_id
     AND v.id = c.document_version_id
    JOIN kb_documents AS d
      ON d.scope_id = c.scope_id
     AND d.kb_id = c.kb_id
     AND d.id = c.document_id
    JOIN knowledge_bases AS b
      ON b.scope_id = c.scope_id
     AND b.id = c.kb_id
    WHERE c.scope_id = :scope
      AND c.kb_id = :kb
      AND v.scope_id = :scope
      AND v.kb_id = :kb
      AND v.status = 'active'
      AND d.scope_id = :scope
      AND d.kb_id = :kb
      AND d.status = 'active'
      AND d.active_version_id = c.document_version_id
      AND b.scope_id = :scope
      AND b.id = :kb
      AND b.status = 'active'
      AND c.model = b.embedding_model
      AND c.dim = b.embedding_dim
      AND (
          similarity(c.text, :query) > 0.1
          OR word_similarity(:query, c.text) > 0.2
          OR c.fts @@ plainto_tsquery('simple', :query)
      )
    ORDER BY GREATEST(
        similarity(c.text, :query),
        ts_rank(c.fts, plainto_tsquery('simple', :query))
      ) DESC,
      c.document_id ASC,
      c.ordinal ASC,
      c.id ASC
    LIMIT :limit
    """
)

_SEMANTIC_CHUNKS = text(
    """
    SELECT c.id
    FROM kb_chunks AS c
    JOIN kb_document_versions AS v
      ON v.scope_id = c.scope_id
     AND v.kb_id = c.kb_id
     AND v.document_id = c.document_id
     AND v.id = c.document_version_id
    JOIN kb_documents AS d
      ON d.scope_id = c.scope_id
     AND d.kb_id = c.kb_id
     AND d.id = c.document_id
    JOIN knowledge_bases AS b
      ON b.scope_id = c.scope_id
     AND b.id = c.kb_id
    WHERE c.scope_id = :scope
      AND c.kb_id = :kb
      AND v.scope_id = :scope
      AND v.kb_id = :kb
      AND v.status = 'active'
      AND d.scope_id = :scope
      AND d.kb_id = :kb
      AND d.status = 'active'
      AND d.active_version_id = c.document_version_id
      AND b.scope_id = :scope
      AND b.id = :kb
      AND b.status = 'active'
      AND b.embedding_model = :model
      AND b.embedding_dim = :dim
      AND c.model = b.embedding_model
      AND c.dim = b.embedding_dim
    ORDER BY c.embedding <=> CAST(:query_vector AS vector) ASC,
      c.document_id ASC,
      c.ordinal ASC,
      c.id ASC
    LIMIT :limit
    """
)

_FINAL_CHUNKS = text(
    """
    WITH ranked(chunk_id, position) AS (
        SELECT chunk_id, position
        FROM unnest(CAST(:chunk_ids AS text[]))
             WITH ORDINALITY AS ids(chunk_id, position)
    )
    SELECT c.id,
           c.kb_id,
           c.document_id,
           c.document_version_id,
           c.ordinal,
           c.text,
           c.char_start,
           c.char_end,
           c.heading_path,
           d.title,
           d.source_uri
    FROM ranked
    JOIN kb_chunks AS c
      ON c.id = ranked.chunk_id
    JOIN kb_document_versions AS v
      ON v.scope_id = c.scope_id
     AND v.kb_id = c.kb_id
     AND v.document_id = c.document_id
     AND v.id = c.document_version_id
    JOIN kb_documents AS d
      ON d.scope_id = c.scope_id
     AND d.kb_id = c.kb_id
     AND d.id = c.document_id
    JOIN knowledge_bases AS b
      ON b.scope_id = c.scope_id
     AND b.id = c.kb_id
    WHERE c.scope_id = :scope
      AND c.kb_id = :kb
      AND v.scope_id = :scope
      AND v.kb_id = :kb
      AND v.status = 'active'
      AND d.scope_id = :scope
      AND d.kb_id = :kb
      AND d.status = 'active'
      AND d.active_version_id = c.document_version_id
      AND b.scope_id = :scope
      AND b.id = :kb
      AND b.status = 'active'
      AND c.model = b.embedding_model
      AND c.dim = b.embedding_dim
    ORDER BY ranked.position
    """
)


@dataclass(frozen=True, slots=True)
class _KnowledgePin:
    model: str
    dim: int


@dataclass(frozen=True, slots=True)
class _SearchRow:
    chunk_id: str
    kb_id: str
    document_id: str
    document_version_id: str
    ordinal: int
    text: str
    char_start: int
    char_end: int
    heading_path: tuple[str, ...]
    title: str
    source_uri: str | None


def _positive_int(value: object, *, label: str, maximum: int | None = None) -> int:
    if type(value) is not int or value < 1 or (maximum is not None and value > maximum):
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_input,
            f"{label} must be a positive integer.",
        )
    return value


def _search_query(value: object, *, max_chars: int) -> str:
    if not isinstance(value, str) or "\x00" in value or len(value) > max_chars:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_input,
            "Knowledge search query is invalid.",
        )
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_input,
            "Knowledge search query is invalid.",
        ) from exc
    normalized = value.strip()
    if not normalized:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_input,
            "Knowledge search query is invalid.",
        )
    return normalized


def _search_k(value: object, *, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise KnowledgeValidationError(
            KnowledgePublicCode.invalid_input,
            f"Knowledge search k must be an integer from 1 to {maximum}.",
        )
    return value


def _vector_literal(vector: Sequence[float]) -> str:
    return "[" + ",".join(repr(value) for value in vector) + "]"


def _validated_vector(value: object, *, dim: int) -> list[float] | None:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes) or len(value) != dim:
        return None
    vector: list[float] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int | float):
            return None
        number = float(item)
        if not math.isfinite(number):
            return None
        vector.append(number)
    if not any(vector):
        return None
    return vector


def _fuse_chunk_ids(ranked_lists: Sequence[Sequence[str]]) -> list[str]:
    """Fuse text IDs through a stable, request-local integer namespace.

    Local IDs follow first appearance across lexical and then semantic candidates.
    Python's stable sort in ``rrf_fuse`` therefore makes exact score ties resolve by
    that deterministic first-appearance order.
    """

    local_by_chunk: dict[str, int] = {}
    chunk_by_local: dict[int, str] = {}
    integer_lists: list[list[int]] = []
    for ranked in ranked_lists:
        integer_ranked: list[int] = []
        for chunk_id in ranked:
            local_id = local_by_chunk.get(chunk_id)
            if local_id is None:
                local_id = len(local_by_chunk) + 1
                local_by_chunk[chunk_id] = local_id
                chunk_by_local[local_id] = chunk_id
            integer_ranked.append(local_id)
        integer_lists.append(integer_ranked)
    return [chunk_by_local[local_id] for local_id in rrf_fuse(integer_lists)]


class KnowledgeSearcher:
    """Search one scope's active Knowledge Base versions."""

    def __init__(
        self,
        engine: AsyncEngine,
        scope_id: str,
        embedder: Embedder | None = None,
        *,
        query_max_chars: int = _DEFAULT_QUERY_MAX_CHARS,
        k_max: int = _DEFAULT_K_MAX,
        candidate_multiplier: int = _DEFAULT_CANDIDATE_MULTIPLIER,
    ) -> None:
        if not isinstance(engine, AsyncEngine):
            raise KnowledgeValidationError(
                KnowledgePublicCode.invalid_input,
                "Knowledge database engine is invalid.",
            )
        _pg_harden_engine_logging(engine)
        self._engine = engine
        self._scope_id = validate_scope_id(scope_id)
        self._embedder = embedder
        self._query_max_chars = _positive_int(
            query_max_chars,
            label="Knowledge search query limit",
        )
        self._k_max = _positive_int(
            k_max,
            label="Knowledge search result limit",
            maximum=_DEFAULT_K_MAX,
        )
        self._candidate_multiplier = _positive_int(
            candidate_multiplier,
            label="Knowledge search candidate multiplier",
        )

    @property
    def scope_id(self) -> str:
        return self._scope_id

    @property
    def query_max_chars(self) -> int:
        return self._query_max_chars

    @property
    def k_max(self) -> int:
        return self._k_max

    @property
    def candidate_multiplier(self) -> int:
        return self._candidate_multiplier

    async def _pin_and_lexical(
        self,
        kb_id: str,
        query: str,
        *,
        candidates: int,
    ) -> tuple[_KnowledgePin | None, list[str]]:
        async with self._engine.begin() as conn:
            await conn.execute(_PG_SET_SCOPE, {"scope": self._scope_id})
            pin_row = (
                (
                    await conn.execute(
                        _ACTIVE_KB,
                        {"scope": self._scope_id, "kb": kb_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if pin_row is None:
                return None, []
            pin = _KnowledgePin(
                model=str(pin_row["embedding_model"]),
                dim=int(pin_row["embedding_dim"]),
            )
            chunk_ids = (
                await conn.execute(
                    _LEXICAL_CHUNKS,
                    {
                        "scope": self._scope_id,
                        "kb": kb_id,
                        "query": query,
                        "limit": candidates,
                    },
                )
            ).scalars()
            return pin, [str(chunk_id) for chunk_id in chunk_ids]

    def _embedder_matches(self, pin: _KnowledgePin) -> bool:
        if self._embedder is None:
            return False
        try:
            model = self._embedder.model
            dim = self._embedder.dim
        except Exception:  # noqa: BLE001 - malformed optional adapter is degraded safely
            return False
        return isinstance(model, str) and type(dim) is int and model == pin.model and dim == pin.dim

    async def _query_vector(self, query: str, *, dim: int) -> list[float] | None:
        assert self._embedder is not None
        try:
            vectors = await self._embedder.embed([query])
            if (
                not isinstance(vectors, Sequence)
                or isinstance(vectors, str | bytes)
                or len(vectors) != 1
            ):
                return None
            return _validated_vector(vectors[0], dim=dim)
        except Exception:  # noqa: BLE001 - provider failures intentionally degrade to lexical
            return None

    async def _semantic(
        self,
        kb_id: str,
        pin: _KnowledgePin,
        vector: Sequence[float],
        *,
        candidates: int,
    ) -> list[str]:
        async with self._engine.begin() as conn:
            await conn.execute(_PG_SET_SCOPE, {"scope": self._scope_id})
            chunk_ids = (
                await conn.execute(
                    _SEMANTIC_CHUNKS,
                    {
                        "scope": self._scope_id,
                        "kb": kb_id,
                        "model": pin.model,
                        "dim": pin.dim,
                        "query_vector": _vector_literal(vector),
                        "limit": candidates,
                    },
                )
            ).scalars()
            return [str(chunk_id) for chunk_id in chunk_ids]

    async def _final_rows(self, kb_id: str, chunk_ids: Sequence[str]) -> list[_SearchRow]:
        if not chunk_ids:
            return []
        async with self._engine.begin() as conn:
            await conn.execute(_PG_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                (
                    await conn.execute(
                        _FINAL_CHUNKS,
                        {
                            "scope": self._scope_id,
                            "kb": kb_id,
                            "chunk_ids": list(chunk_ids),
                        },
                    )
                )
                .mappings()
                .all()
            )

        results: list[_SearchRow] = []
        for row in rows:
            raw_heading_path: Any = row["heading_path"]
            heading_path = (
                tuple(str(item) for item in raw_heading_path)
                if isinstance(raw_heading_path, list | tuple)
                else ()
            )
            results.append(
                _SearchRow(
                    chunk_id=str(row["id"]),
                    kb_id=str(row["kb_id"]),
                    document_id=str(row["document_id"]),
                    document_version_id=str(row["document_version_id"]),
                    ordinal=int(row["ordinal"]),
                    text=str(row["text"]),
                    char_start=int(row["char_start"]),
                    char_end=int(row["char_end"]),
                    heading_path=heading_path,
                    title=str(row["title"]),
                    source_uri=(None if row["source_uri"] is None else str(row["source_uri"])),
                )
            )
        return results

    @_pg_error_boundary
    async def search(
        self,
        kb_id: str,
        query: str,
        *,
        k: int = 5,
    ) -> tuple[list[KnowledgeHit], KnowledgeSearchStatus]:
        """Return active, scope-bound chunks ranked by lexical/semantic RRF."""

        base_id = validate_knowledge_base_id(kb_id)
        normalized_query = _search_query(query, max_chars=self._query_max_chars)
        result_limit = _search_k(k, maximum=self._k_max)
        candidates = max(result_limit * self._candidate_multiplier, _MIN_CANDIDATES)

        pin, lexical_ids = await self._pin_and_lexical(
            base_id,
            normalized_query,
            candidates=candidates,
        )
        if pin is None:
            mode = (
                KnowledgeSearchMode.hybrid
                if self._embedder is not None
                else KnowledgeSearchMode.lexical
            )
            return [], KnowledgeSearchStatus(mode=mode)

        semantic_ids: list[str] = []
        semantic_error: str | None = None
        if self._embedder is None:
            mode = KnowledgeSearchMode.lexical
        elif not self._embedder_matches(pin):
            mode = KnowledgeSearchMode.lexical_degraded
            semantic_error = _EMBEDDING_CONFIGURATION_MISMATCH
        else:
            query_vector = await self._query_vector(normalized_query, dim=pin.dim)
            if query_vector is None:
                mode = KnowledgeSearchMode.lexical_degraded
                semantic_error = _EMBEDDING_UNAVAILABLE
            else:
                semantic_ids = await self._semantic(
                    base_id,
                    pin,
                    query_vector,
                    candidates=candidates,
                )
                mode = KnowledgeSearchMode.hybrid

        ranked_lists = (
            [lexical_ids, semantic_ids] if mode is KnowledgeSearchMode.hybrid else [lexical_ids]
        )
        fused_ids = _fuse_chunk_ids(ranked_lists)
        final_rows = await self._final_rows(base_id, fused_ids)

        hits: list[KnowledgeHit] = []
        for row in final_rows[:result_limit]:
            rank = len(hits) + 1
            hits.append(
                KnowledgeHit(
                    snippet=row.text[:_SNIPPET_MAX_CHARS],
                    rank=rank,
                    heading_path=list(row.heading_path),
                    citation=KnowledgeCitation(
                        id=f"cite_{rank}",
                        kb_id=row.kb_id,
                        document_id=row.document_id,
                        document_version_id=row.document_version_id,
                        chunk_id=row.chunk_id,
                        title=row.title,
                        source_uri=row.source_uri,
                        ordinal=row.ordinal,
                        char_start=row.char_start,
                        char_end=row.char_end,
                        label=f"{row.title}#chunk-{row.ordinal + 1}",
                    ),
                )
            )
        return hits, KnowledgeSearchStatus(mode=mode, semantic_error=semantic_error)


__all__ = ["KnowledgeSearcher"]
