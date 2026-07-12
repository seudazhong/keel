"""Semantic recall projection: message indexing + hybrid message ranking."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import Embedder
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
        rows = await self._missing_rows(session_id=session_id)
        return await self._index_rows(rows)

    async def backfill_scope(self, *, limit: int = 500) -> BackfillResult:
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
