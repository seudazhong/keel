"""Integration: semantic session-recall projection and ranking."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Sequence

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import FakeEmbedder
from keel_core.recall import MessageEmbeddingIndexer, rank_session_messages

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


class _MeaningEmbedder:
    model = "fake/meaning"
    dim = 2

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for value in texts:
            lowered = value.lower()
            if "feline" in lowered or "cat nap" in lowered:
                vectors.append([1.0, 0.0])
            else:
                vectors.append([0.0, 1.0])
        return vectors


class _FailingEmbedder:
    model = "fake/failing"
    dim = 2

    def __init__(self) -> None:
        self.call_count = 0

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.call_count += 1
        if self.call_count == 1:
            raise RuntimeError(f"catch-up embedding failed for {len(texts)} texts")
        raise RuntimeError(f"query embedding failed for {len(texts)} texts")


async def test_rank_session_messages_finds_semantic_only_match(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    target = f"s:{uuid.uuid4().hex}"
    other = f"s:{uuid.uuid4().hex}"
    await _seed_event(
        migrated_db, scope, target, 1, role="user", content="The feline sleeps on the sofa"
    )
    await _seed_event(migrated_db, scope, other, 1, role="user", content="Quarterly finance report")

    hits, status = await rank_session_messages(
        migrated_db,
        scope,
        "cat nap",
        k=5,
        embedder=_MeaningEmbedder(),
        batch_size=2,
        catchup_limit=20,
    )

    assert status.mode == "hybrid"
    assert hits and hits[0].session_id == target
    assert "feline" in hits[0].content


async def test_rank_session_messages_explicitly_degrades_to_lexical(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    await _seed_event(migrated_db, scope, session_id, 1, role="user", content="quarterly invoices")

    embedder = _FailingEmbedder()
    hits, status = await rank_session_messages(
        migrated_db,
        scope,
        "invoices",
        k=5,
        embedder=embedder,
    )

    assert status.mode == "lexical-degraded"
    assert status.error is not None
    # Both catch-up and query error messages should be present
    assert "catch-up embedding failed" in status.error
    assert "query embedding failed" in status.error
    assert hits and hits[0].session_id == session_id


async def test_rank_session_messages_without_embedder_is_lexical(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    await _seed_event(migrated_db, scope, session_id, 1, role="assistant", content="Tokyo flight")

    hits, status = await rank_session_messages(migrated_db, scope, "Tokyo", k=5, embedder=None)

    assert status.mode == "lexical"
    assert hits and hits[0].session_id == session_id


async def test_cross_scope_isolation_no_projection_or_message_leak(
    migrated_db: AsyncEngine,
) -> None:
    """Messages indexed in scope A must not surface when ranking or indexing from scope B."""
    scope_a = f"u:{uuid.uuid4().hex}"
    scope_b = f"u:{uuid.uuid4().hex}"
    session_a = f"s:{uuid.uuid4().hex}"
    embedder = FakeEmbedder(dim=16, model="fake/cross-scope")

    # Seed and fully index one message in scope A.
    await _seed_event(migrated_db, scope_a, session_a, 1, role="user", content="secret in scope A")
    indexed_a = await MessageEmbeddingIndexer(migrated_db, scope_a, embedder).index_session(
        session_a
    )
    assert indexed_a == 1

    # Ranking via scope B must return no hits (both lexical and semantic arms are scoped).
    hits_b, status_b = await rank_session_messages(
        migrated_db,
        scope_b,
        "secret in scope A",
        k=5,
        embedder=embedder,
        catchup_limit=20,
    )
    assert hits_b == [], "scope B must not see scope A messages"
    assert status_b.indexed == 0, "scope B backfill must not index scope A rows"

    # Confirm the projection row is absent for scope B (RLS check).
    rows_b = await _projection_rows(migrated_db, scope_b)
    assert rows_b == [], "scope B must have no projection rows"

    # Scope A must still be able to find its own message.
    hits_a, _ = await rank_session_messages(
        migrated_db,
        scope_a,
        "secret in scope A",
        k=5,
        embedder=embedder,
        catchup_limit=20,
    )
    assert any(h.session_id == session_a for h in hits_a), "scope A must see its own message"
