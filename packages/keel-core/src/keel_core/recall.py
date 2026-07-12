"""Semantic recall projection: message indexing + hybrid message ranking."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import Embedder, rrf_fuse
from keel_core.types import ScopeId, SessionId

logger = logging.getLogger("keel.core.recall")

RecallMode = Literal["hybrid", "lexical", "lexical-degraded"]
_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")
_INSERT_EMBEDDING = text(
    "INSERT INTO message_embeddings "
    "(event_id, scope_id, session_id, seq, role, content, model, dim, embedding) "
    "VALUES (:event_id, :scope_id, :session_id, :seq, :role, :content, "
    ":model, :dim, CAST(:embedding AS vector)) "
    "ON CONFLICT (event_id, model, dim) DO NOTHING "
    "RETURNING event_id"
)


@dataclass(frozen=True)
class BackfillResult:
    indexed: int
    remaining: bool


@dataclass(frozen=True)
class RankedMessage:
    event_id: int
    session_id: str
    seq: int
    role: str
    content: str


@dataclass(frozen=True)
class RecallStatus:
    mode: RecallMode
    indexed: int = 0
    remaining: bool = False
    error: str | None = None


@dataclass(frozen=True)
class _MessageRow:
    event_id: int
    session_id: str
    seq: int
    role: str
    content: str


def _vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(repr(float(value)) for value in vector) + "]"


class MessageEmbeddingIndexer:
    """Build the rebuildable semantic projection for one scope."""

    def __init__(
        self,
        engine: AsyncEngine,
        scope_id: ScopeId,
        embedder: Embedder,
        *,
        batch_size: int = 64,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self._engine = engine
        self._scope_id = scope_id
        self._embedder = embedder
        self._batch_size = batch_size

    async def index_session(self, session_id: SessionId) -> int:
        """Embed and persist any un-projected messages for *session_id*; return count inserted."""
        rows = await self._missing_rows(session_id=session_id)
        return await self._index_rows(rows)

    async def backfill_scope(self, *, limit: int = 500) -> BackfillResult:
        """Embed up to *limit* un-projected scope messages; report whether more remain."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        rows = await self._missing_rows(limit=limit + 1)
        selected = rows[:limit]
        indexed = await self._index_rows(selected)
        return BackfillResult(indexed=indexed, remaining=len(rows) > limit)

    async def _missing_rows(
        self,
        *,
        session_id: SessionId | None = None,
        limit: int | None = None,
    ) -> list[_MessageRow]:
        sql = (
            "SELECT e.id AS event_id, e.session_id, e.seq, "
            "e.payload->>'role' AS role, e.payload->>'text' AS content "
            "FROM events e "
            "LEFT JOIN message_embeddings m "
            "ON m.event_id = e.id AND m.model = :model AND m.dim = :dim "
            "WHERE e.scope_id = :scope "
            "AND e.type = 'message.token' "
            "AND e.payload->>'role' IN ('user', 'assistant') "
            "AND COALESCE((e.payload->>'partial')::boolean, false) = false "
            "AND NULLIF(BTRIM(e.payload->>'text'), '') IS NOT NULL "
            "AND m.event_id IS NULL "
        )
        params: dict[str, object] = {
            "scope": self._scope_id,
            "model": self._embedder.model,
            "dim": self._embedder.dim,
        }
        if session_id is not None:
            sql += "AND e.session_id = :session_id "
            params["session_id"] = session_id
        sql += "ORDER BY e.id "
        if limit is not None:
            sql += "LIMIT :limit"
            params["limit"] = limit

        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (await conn.execute(text(sql), params)).all()
        return [
            _MessageRow(
                event_id=int(row.event_id),
                session_id=str(row.session_id),
                seq=int(row.seq),
                role=str(row.role),
                content=str(row.content),
            )
            for row in rows
        ]

    async def _index_rows(self, rows: list[_MessageRow]) -> int:
        inserted = 0
        for start in range(0, len(rows), self._batch_size):
            batch = rows[start : start + self._batch_size]
            vectors = await self._embedder.embed([row.content for row in batch])
            params = [
                {
                    "event_id": row.event_id,
                    "scope_id": self._scope_id,
                    "session_id": row.session_id,
                    "seq": row.seq,
                    "role": row.role,
                    "content": row.content,
                    "model": self._embedder.model,
                    "dim": self._embedder.dim,
                    "embedding": _vector_literal(vector),
                }
                for row, vector in zip(batch, vectors, strict=True)
            ]
            async with self._engine.begin() as conn:
                await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
                for item in params:
                    result = await conn.execute(_INSERT_EMBEDDING, item)
                    if result.scalar_one_or_none() is not None:
                        inserted += 1
        return inserted


async def _lexical_event_ids(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    limit: int,
) -> list[int]:
    sql = text(
        "SELECT e.id, GREATEST("
        "similarity(e.payload->>'text', :query), "
        "ts_rank(to_tsvector('simple', e.payload->>'text'), "
        "plainto_tsquery('simple', :query))) AS score "
        "FROM events e "
        "WHERE e.scope_id = :scope "
        "AND e.type = 'message.token' "
        "AND e.payload->>'role' IN ('user', 'assistant') "
        "AND COALESCE((e.payload->>'partial')::boolean, false) = false "
        "AND NULLIF(BTRIM(e.payload->>'text'), '') IS NOT NULL "
        "AND (similarity(e.payload->>'text', :query) > 0.1 "
        "OR to_tsvector('simple', e.payload->>'text') "
        "@@ plainto_tsquery('simple', :query)) "
        "ORDER BY score DESC, e.id DESC LIMIT :limit"
    )
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        rows = await conn.execute(
            sql,
            {"scope": scope_id, "query": query, "limit": limit},
        )
        return [int(row.id) for row in rows]


async def _semantic_event_ids(
    engine: AsyncEngine,
    scope_id: ScopeId,
    embedder: Embedder,
    query: str,
    *,
    limit: int,
) -> list[int]:
    query_vector = (await embedder.embed([query]))[0]
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        rows = await conn.execute(
            text(
                "SELECT event_id FROM message_embeddings "
                "WHERE scope_id = :scope AND model = :model AND dim = :dim "
                "ORDER BY embedding <=> CAST(:query_vector AS vector) "
                "LIMIT :limit"
            ),
            {
                "scope": scope_id,
                "model": embedder.model,
                "dim": embedder.dim,
                "query_vector": _vector_literal(query_vector),
                "limit": limit,
            },
        )
        return [int(row.event_id) for row in rows]


async def _messages_by_id(
    engine: AsyncEngine,
    scope_id: ScopeId,
    event_ids: list[int],
) -> dict[int, RankedMessage]:
    if not event_ids:
        return {}
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        rows = await conn.execute(
            text(
                "SELECT id, session_id, seq, payload->>'role' AS role, "
                "payload->>'text' AS content "
                "FROM events WHERE scope_id = :scope AND id = ANY(:event_ids)"
            ),
            {"scope": scope_id, "event_ids": event_ids},
        )
        return {
            int(row.id): RankedMessage(
                event_id=int(row.id),
                session_id=str(row.session_id),
                seq=int(row.seq),
                role=str(row.role),
                content=str(row.content),
            )
            for row in rows
        }


async def rank_session_messages(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    k: int,
    embedder: Embedder | None,
    candidate_limit: int | None = None,
    batch_size: int = 64,
    catchup_limit: int = 500,
) -> tuple[list[RankedMessage], RecallStatus]:
    """Rank complete session messages by lexical + semantic RRF."""
    if not query.strip():
        mode: RecallMode = "hybrid" if embedder is not None else "lexical"
        return [], RecallStatus(mode=mode)

    _climit = candidate_limit if candidate_limit is not None else max(k * 8, 80)
    lexical_ids = await _lexical_event_ids(engine, scope_id, query, limit=_climit)
    semantic_ids: list[int] = []
    indexed = 0
    remaining = False
    catchup_error: str | None = None

    if embedder is None:
        mode = "lexical"
    else:
        indexer = MessageEmbeddingIndexer(engine, scope_id, embedder, batch_size=batch_size)
        try:
            backfill = await indexer.backfill_scope(limit=catchup_limit)
            indexed = backfill.indexed
            remaining = backfill.remaining
            if indexed:
                logger.info(
                    "session embedding catch-up complete scope=%s model=%s "
                    "dim=%s indexed=%d remaining=%s",
                    scope_id,
                    embedder.model,
                    embedder.dim,
                    indexed,
                    remaining,
                )
            if remaining:
                logger.info(
                    "session embedding catch-up reached limit scope=%s model=%s dim=%s",
                    scope_id,
                    embedder.model,
                    embedder.dim,
                )
        except Exception as exc:  # noqa: BLE001 - visible best-effort catch-up
            catchup_error = f"{exc.__class__.__name__}: {exc}"
            logger.exception(
                "session embedding catch-up failed scope=%s model=%s dim=%s",
                scope_id,
                embedder.model,
                embedder.dim,
            )
        try:
            semantic_ids = await _semantic_event_ids(
                engine,
                scope_id,
                embedder,
                query,
                limit=_climit,
            )
            mode = "hybrid"
        except Exception as exc:  # noqa: BLE001 - explicit lexical degradation
            error = f"{exc.__class__.__name__}: {exc}"
            logger.exception(
                "semantic session search failed; using lexical only scope=%s model=%s dim=%s",
                scope_id,
                embedder.model,
                embedder.dim,
            )
            # Combine both errors deterministically if both are present
            combined_error = error
            if catchup_error:
                combined_error = "; ".join(filter(None, [catchup_error, error]))
            fused = rrf_fuse([lexical_ids, semantic_ids])[:k]
            rows = await _messages_by_id(engine, scope_id, fused)
            return (
                [rows[event_id] for event_id in fused if event_id in rows],
                RecallStatus(
                    mode="lexical-degraded",
                    indexed=indexed,
                    remaining=remaining,
                    error=combined_error,
                ),
            )

    ranked_lists = [lexical_ids, semantic_ids] if embedder is not None else [lexical_ids]
    fused = rrf_fuse(ranked_lists)[:k]
    rows = await _messages_by_id(engine, scope_id, fused)
    return (
        [rows[event_id] for event_id in fused if event_id in rows],
        RecallStatus(
            mode=mode,
            indexed=indexed,
            remaining=remaining,
            error=catchup_error,
        ),
    )
