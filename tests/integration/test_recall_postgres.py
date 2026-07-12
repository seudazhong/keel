"""Integration: semantic session-recall projection and ranking."""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import FakeEmbedder
from keel_core.recall import MessageEmbeddingIndexer

pytestmark = pytest.mark.integration


async def test_message_embeddings_schema_and_rls(migrated_db: AsyncEngine) -> None:
    async with migrated_db.connect() as conn:
        table_count = await conn.scalar(
            text(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'message_embeddings'"
            )
        )
        rls = await conn.scalar(
            text("SELECT relrowsecurity FROM pg_class WHERE relname = 'message_embeddings'")
        )
        columns = {
            str(row.column_name)
            for row in (
                await conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = 'public' "
                        "AND table_name = 'message_embeddings'"
                    )
                )
            )
        }

    assert table_count == 1
    assert rls is True
    assert columns == {
        "event_id",
        "scope_id",
        "session_id",
        "seq",
        "role",
        "content",
        "model",
        "dim",
        "embedding",
        "created_at",
    }


async def _seed_event(
    engine: AsyncEngine,
    scope: str,
    session_id: str,
    seq: int,
    *,
    role: str,
    content: str,
    partial: bool = False,
    event_type: str = "message.token",
) -> int:
    payload = {"role": role, "text": content}
    if partial:
        payload["partial"] = True
    async with engine.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"),
            {"scope": scope},
        )
        await conn.execute(
            text(
                "INSERT INTO sessions (id, scope_id, next_seq) "
                "VALUES (:sid, :scope, :next_seq) "
                "ON CONFLICT (id) DO UPDATE "
                "SET next_seq = GREATEST(sessions.next_seq, :next_seq)"
            ),
            {"sid": session_id, "scope": scope, "next_seq": seq + 1},
        )
        event_id = await conn.scalar(
            text(
                "INSERT INTO events "
                "(session_id, scope_id, seq, type, version, ts, payload) "
                "VALUES (:sid, :scope, :seq, :type, 1, now(), CAST(:payload AS jsonb)) "
                "RETURNING id"
            ),
            {
                "sid": session_id,
                "scope": scope,
                "seq": seq,
                "type": event_type,
                "payload": json.dumps(payload),
            },
        )
    assert event_id is not None
    return int(event_id)


async def _projection_rows(engine: AsyncEngine, scope: str) -> list[object]:
    async with engine.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"),
            {"scope": scope},
        )
        return list(
            (
                await conn.execute(
                    text(
                        "SELECT event_id, session_id, seq, role, content, model, dim "
                        "FROM message_embeddings "
                        "WHERE scope_id = :scope ORDER BY seq"
                    ),
                    {"scope": scope},
                )
            ).all()
        )


async def test_index_session_filters_and_persists_complete_messages(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    await _seed_event(migrated_db, scope, session_id, 1, role="user", content="hello")
    await _seed_event(migrated_db, scope, session_id, 2, role="assistant", content="hi")
    await _seed_event(
        migrated_db, scope, session_id, 3, role="assistant", content="partial", partial=True
    )
    await _seed_event(migrated_db, scope, session_id, 4, role="system", content="hidden")
    await _seed_event(migrated_db, scope, session_id, 5, role="user", content="   ")
    await _seed_event(
        migrated_db,
        scope,
        session_id,
        6,
        role="tool",
        content="tool output",
        event_type="tool.result",
    )

    indexed = await MessageEmbeddingIndexer(
        migrated_db, scope, FakeEmbedder(dim=16, model="fake/recall")
    ).index_session(session_id)

    rows = await _projection_rows(migrated_db, scope)
    assert indexed == 2
    assert [(r.role, r.content) for r in rows] == [("user", "hello"), ("assistant", "hi")]
    assert all(r.model == "fake/recall" and r.dim == 16 for r in rows)
