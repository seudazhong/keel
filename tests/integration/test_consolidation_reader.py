"""Integration: bounded consolidation batch reader (filters, truncation, budget)."""

from __future__ import annotations

import json
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.reader import ConsolidationBatchReader

pytestmark = pytest.mark.integration


async def _emit(
    conn: Any,
    scope: str,
    session_id: str,
    seq: int,
    role: str,
    content: str,
    *,
    etype: str = "message.token",
    partial: bool = False,
) -> int:
    row = (
        await conn.execute(
            text(
                "INSERT INTO events (session_id, scope_id, seq, type, ts, payload) "
                "VALUES (:session, :scope, :seq, :type, now(), CAST(:payload AS jsonb)) "
                "RETURNING id"
            ),
            {
                "session": session_id,
                "scope": scope,
                "seq": seq,
                "type": etype,
                "payload": json.dumps({"role": role, "text": content, "partial": partial}),
            },
        )
    ).one()
    return int(row.id)


async def test_reader_filters_and_orders(migrated_db: AsyncEngine) -> None:
    scope = "rdr:filter"
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        u1 = await _emit(conn, scope, "chat:a", 1, "user", "hello")
        a1 = await _emit(conn, scope, "chat:a", 2, "assistant", "hi there")
        await _emit(conn, scope, "chat:a", 3, "assistant", "streaming", partial=True)
        await _emit(conn, scope, "digest:x", 1, "user", "digest noise")
        await _emit(conn, scope, "consolidation:x:run", 1, "user", "self noise")
        await _emit(conn, scope, "chat:a", 4, "system", "system noise")

    batch = await ConsolidationBatchReader(migrated_db, scope).read(0)
    assert [m.event_id for m in batch.messages] == [u1, a1]
    assert batch.user_event_ids == frozenset({u1})
    assert batch.max_event_id == a1
    assert batch.eligible_count == 2


async def test_reader_truncates_and_respects_budget(migrated_db: AsyncEngine) -> None:
    scope = "rdr:budget"
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        first = await _emit(conn, scope, "chat:b", 1, "user", "x" * 100)
        await _emit(conn, scope, "chat:b", 2, "user", "y" * 100)

    reader = ConsolidationBatchReader(migrated_db, scope)
    truncated = await reader.read(0, message_max_chars=10)
    assert all(len(m.content) == 10 for m in truncated.messages)

    budgeted = await reader.read(0, message_max_chars=100, input_max_chars=100)
    assert [m.event_id for m in budgeted.messages] == [first]
    assert budgeted.max_event_id == first
    assert budgeted.eligible_count == 2


async def test_reader_after_cursor_skips_consumed(migrated_db: AsyncEngine) -> None:
    scope = "rdr:cursor"
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        first = await _emit(conn, scope, "chat:c", 1, "user", "one")
        second = await _emit(conn, scope, "chat:c", 2, "user", "two")

    batch = await ConsolidationBatchReader(migrated_db, scope).read(first)
    assert [m.event_id for m in batch.messages] == [second]
    assert batch.max_event_id == second
