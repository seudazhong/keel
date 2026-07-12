"""Hybrid search + archival memory (WS-D).

``archival_search`` fuses a **lexical** arm (``pg_trgm`` similarity + ``tsvector``
FTS, CJK-safe) with a **semantic** arm (pgvector KNN over ``(model, dim)``-pinned
embeddings) using Reciprocal Rank Fusion. ``session_search`` is a lexical search
over a scope's past messages. Both stores are scope-bound (ADR-0009): every query
filters ``scope_id`` and sets the RLS GUC.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import Embedder, rrf_fuse
from keel_core.protocols import ToolContext, ToolResult
from keel_core.recall import RecallStatus, rank_session_messages
from keel_core.types import ScopeId

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


@dataclass
class SearchHit:
    """One retrieval result."""

    content: str
    source: str = ""


@dataclass
class SessionSearchHit:
    """A session matched by :func:`search_sessions` (ranked, with a snippet)."""

    id: str
    title: str | None
    snippet: str
    messages: int
    updated_at: Any


def _vector_literal(vec: list[float]) -> str:
    return "[" + ",".join(repr(float(x)) for x in vec) + "]"


class ArchivalStore:
    """Scope-bound archival memory with hybrid (lexical ⊕ semantic) retrieval."""

    def __init__(self, engine: AsyncEngine, scope_id: ScopeId, embedder: Embedder) -> None:
        self._engine = engine
        self._scope_id = scope_id
        self._embedder = embedder

    async def add(self, content: str) -> int:
        embedding = (await self._embedder.embed([content]))[0]
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "INSERT INTO archival (scope_id, content, model, dim, embedding) "
                        "VALUES (:scope, :content, :model, :dim, CAST(:emb AS vector)) "
                        "RETURNING id"
                    ),
                    {
                        "scope": self._scope_id,
                        "content": content,
                        "model": self._embedder.model,
                        "dim": self._embedder.dim,
                        "emb": _vector_literal(embedding),
                    },
                )
            ).one()
        return int(row.id)

    async def search(self, query: str, *, k: int = 5) -> list[SearchHit]:
        if not query.strip():
            return []
        candidates = max(k * 2, 10)
        qvec = (await self._embedder.embed([query]))[0]

        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            lexical = [
                int(r.id)
                for r in await conn.execute(
                    text(
                        "SELECT id, GREATEST("
                        "  similarity(content, :q), "
                        "  ts_rank(fts, plainto_tsquery('simple', :q))"
                        ") AS lex "
                        "FROM archival WHERE scope_id = :scope "
                        "ORDER BY lex DESC LIMIT :n"
                    ),
                    {"q": query, "scope": self._scope_id, "n": candidates},
                )
            ]
            semantic = [
                int(r.id)
                for r in await conn.execute(
                    text(
                        "SELECT id FROM archival "
                        "WHERE scope_id = :scope AND model = :model AND dim = :dim "
                        "ORDER BY embedding <=> CAST(:qvec AS vector) LIMIT :n"
                    ),
                    {
                        "scope": self._scope_id,
                        "model": self._embedder.model,
                        "dim": self._embedder.dim,
                        "qvec": _vector_literal(qvec),
                        "n": candidates,
                    },
                )
            ]
            fused = rrf_fuse([lexical, semantic])[:k]
            if not fused:
                return []
            rows = {
                int(r.id): r.content
                for r in await conn.execute(
                    text(
                        "SELECT id, content FROM archival "
                        "WHERE scope_id = :scope AND id = ANY(:ids)"
                    ),
                    {"scope": self._scope_id, "ids": fused},
                )
            }
        return [SearchHit(content=rows[i], source="archival") for i in fused if i in rows]


async def hybrid_session_search(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    k: int = 5,
    embedder: Embedder | None = None,
    batch_size: int = 64,
    catchup_limit: int = 500,
) -> tuple[list[SearchHit], RecallStatus]:
    """Hybrid (lexical + semantic) search over a scope's past messages."""
    ranked, status = await rank_session_messages(
        engine,
        scope_id,
        query,
        k=k,
        embedder=embedder,
        batch_size=batch_size,
        catchup_limit=catchup_limit,
    )
    return (
        [
            SearchHit(content=message.content, source=f"session:{message.session_id}")
            for message in ranked
        ],
        status,
    )


async def session_search(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    k: int = 5,
    embedder: Embedder | None = None,
    batch_size: int = 64,
    catchup_limit: int = 500,
) -> list[SearchHit]:
    """Backward-compatible wrapper: lexical-or-hybrid search over a scope's past messages."""
    hits, _ = await hybrid_session_search(
        engine,
        scope_id,
        query,
        k=k,
        embedder=embedder,
        batch_size=batch_size,
        catchup_limit=catchup_limit,
    )
    return hits


async def hybrid_search_sessions(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    k: int = 20,
    embedder: Embedder | None = None,
    batch_size: int = 64,
    catchup_limit: int = 500,
) -> tuple[list[SessionSearchHit], RecallStatus]:
    """Hybrid (lexical + semantic) session search; best-message per session grouping."""
    message_limit = max(k * 8, 80)
    ranked, status = await rank_session_messages(
        engine,
        scope_id,
        query,
        k=message_limit,
        candidate_limit=message_limit,
        embedder=embedder,
        batch_size=batch_size,
        catchup_limit=catchup_limit,
    )
    best_messages = []
    seen_sessions: set[str] = set()
    for message in ranked:
        if message.session_id not in seen_sessions:
            seen_sessions.add(message.session_id)
            best_messages.append(message)
        if len(best_messages) >= k:
            break
    if not best_messages:
        return [], status

    session_ids = [message.session_id for message in best_messages]
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        rows = (
            await conn.execute(
                text(
                    "SELECT s.id, s.title, s.updated_at, "
                    "(SELECT count(*) FROM events e "
                    "WHERE e.session_id = s.id AND e.scope_id = s.scope_id "
                    "AND e.type = 'message.token') AS messages "
                    "FROM sessions s "
                    "WHERE s.scope_id = :scope AND s.id = ANY(:session_ids)"
                ),
                {"scope": scope_id, "session_ids": session_ids},
            )
        ).all()
    summaries = {str(row.id): row for row in rows}
    hits = [
        SessionSearchHit(
            id=message.session_id,
            title=summaries[message.session_id].title,
            snippet=message.content,
            messages=int(summaries[message.session_id].messages),
            updated_at=summaries[message.session_id].updated_at,
        )
        for message in best_messages
        if message.session_id in summaries
    ]
    return hits, status


async def search_sessions(
    engine: AsyncEngine,
    scope_id: ScopeId,
    query: str,
    *,
    k: int = 20,
    embedder: Embedder | None = None,
    batch_size: int = 64,
    catchup_limit: int = 500,
) -> list[SessionSearchHit]:
    """Backward-compatible wrapper: rank a scope's sessions by hybrid message match."""
    hits, _ = await hybrid_search_sessions(
        engine,
        scope_id,
        query,
        k=k,
        embedder=embedder,
        batch_size=batch_size,
        catchup_limit=catchup_limit,
    )
    return hits


class ArchivalInsertTool:
    """``archival_insert`` (P3): save a passage to scope-bound archival memory."""

    name = "archival_insert"
    description = "Save a passage to your archival memory for later semantic recall."
    writes = True

    def __init__(self, engine: AsyncEngine, embedder: Embedder) -> None:
        self._engine = engine
        self._embedder = embedder

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"content": {"type": "string"}},
            "required": ["content"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        store = ArchivalStore(self._engine, ctx.scope_id, self._embedder)
        row_id = await store.add(str(args.get("content", "")))
        return ToolResult(ok=True, output=f"saved to archival memory (id={row_id})")


class ArchivalSearchTool:
    """``archival_search`` tool (P3): scope-bound hybrid retrieval."""

    name = "archival_search"
    description = "Search archival memory for passages relevant to a query."
    writes = False

    def __init__(self, engine: AsyncEngine, embedder: Embedder) -> None:
        self._engine = engine
        self._embedder = embedder

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"query": {"type": "string"}, "k": {"type": "integer"}},
            "required": ["query"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        store = ArchivalStore(self._engine, ctx.scope_id, self._embedder)
        hits = await store.search(str(args.get("query", "")), k=int(args.get("k", 5)))
        return ToolResult(ok=True, output="\n".join(h.content for h in hits))


class SessionSearchTool:
    """``session_search``: scope-bound hybrid recall over past messages."""

    name = "session_search"
    description = "Search this scope's past messages by lexical and semantic relevance."
    writes = False

    def __init__(
        self,
        engine: AsyncEngine,
        embedder: Embedder | None = None,
        *,
        batch_size: int = 64,
        catchup_limit: int = 500,
    ) -> None:
        self._engine = engine
        self._embedder = embedder
        self._batch_size = batch_size
        self._catchup_limit = catchup_limit

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"query": {"type": "string"}, "k": {"type": "integer"}},
            "required": ["query"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        hits, status = await hybrid_session_search(
            self._engine,
            ctx.scope_id,
            str(args.get("query", "")),
            k=int(args.get("k", 5)),
            embedder=self._embedder,
            batch_size=self._batch_size,
            catchup_limit=self._catchup_limit,
        )
        output = "\n".join(hit.content for hit in hits)
        if status.mode == "lexical-degraded":
            output = "[semantic unavailable; lexical results only]\n" + output
        return ToolResult(ok=True, output=output)
