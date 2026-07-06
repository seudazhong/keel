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
from keel_core.types import ScopeId

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


@dataclass
class SearchHit:
    """One retrieval result."""

    content: str
    source: str = ""


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


async def session_search(
    engine: AsyncEngine, scope_id: ScopeId, query: str, *, k: int = 5
) -> list[SearchHit]:
    """Lexical search over a scope's past messages (CJK-safe trigram)."""
    if not query.strip():
        return []
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        rows = await conn.execute(
            text(
                "SELECT session_id, payload->>'text' AS body "
                "FROM events "
                "WHERE scope_id = :scope AND type = 'message.token' "
                "AND payload->>'text' IS NOT NULL "
                "ORDER BY similarity(payload->>'text', :q) DESC LIMIT :k"
            ),
            {"scope": scope_id, "q": query, "k": k},
        )
        return [SearchHit(content=r.body, source=f"session:{r.session_id}") for r in rows]


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
    """``session_search`` tool (P3): scope-bound lexical search over past messages."""

    name = "session_search"
    description = "Search this scope's past messages for a query."
    writes = False

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"query": {"type": "string"}, "k": {"type": "integer"}},
            "required": ["query"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        hits = await session_search(
            self._engine, ctx.scope_id, str(args.get("query", "")), k=int(args.get("k", 5))
        )
        return ToolResult(ok=True, output="\n".join(h.content for h in hits))
