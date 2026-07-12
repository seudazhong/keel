"""Integration: semantic session-recall projection and ranking."""

from __future__ import annotations

import asyncio
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


async def test_backfill_scope_is_bounded_and_resumable(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    for seq in range(1, 4):
        await _seed_event(
            migrated_db,
            scope,
            session_id,
            seq,
            role="user",
            content=f"message {seq}",
        )
    indexer = MessageEmbeddingIndexer(migrated_db, scope, FakeEmbedder(dim=8))

    first = await indexer.backfill_scope(limit=2)
    second = await indexer.backfill_scope(limit=2)

    assert first.indexed == 2 and first.remaining is True
    assert second.indexed == 1 and second.remaining is False
    assert len(await _projection_rows(migrated_db, scope)) == 3


async def test_index_session_is_repeat_and_concurrency_safe(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    await _seed_event(migrated_db, scope, session_id, 1, role="user", content="one")
    indexer = MessageEmbeddingIndexer(migrated_db, scope, FakeEmbedder(dim=8))

    await asyncio.gather(indexer.index_session(session_id), indexer.index_session(session_id))
    assert len(await _projection_rows(migrated_db, scope)) == 1
    assert await indexer.index_session(session_id) == 0


async def test_projection_is_model_pinned_and_event_cascades(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    event_id = await _seed_event(migrated_db, scope, session_id, 1, role="user", content="one")
    await MessageEmbeddingIndexer(
        migrated_db, scope, FakeEmbedder(dim=8, model="fake/a")
    ).index_session(session_id)
    await MessageEmbeddingIndexer(
        migrated_db, scope, FakeEmbedder(dim=16, model="fake/b")
    ).index_session(session_id)

    async with migrated_db.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"),
            {"scope": scope},
        )
        pin_rows = (
            await conn.execute(
                text("SELECT model, dim FROM message_embeddings WHERE event_id = :event_id"),
                {"event_id": event_id},
            )
        ).all()
        await conn.execute(
            text("DELETE FROM events WHERE id = :event_id AND scope_id = :scope"),
            {"event_id": event_id, "scope": scope},
        )
        remaining = await conn.scalar(
            text("SELECT count(*) FROM message_embeddings WHERE event_id = :event_id"),
            {"event_id": event_id},
        )

    assert {(str(row.model), int(row.dim)) for row in pin_rows} == {
        ("fake/a", 8),
        ("fake/b", 16),
    }
    assert remaining == 0
