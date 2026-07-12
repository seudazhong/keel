"""Integration: archival hybrid search, (model,dim) pinning, session search, scope."""

from __future__ import annotations

import uuid
from collections.abc import Sequence

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ToolContext
from keel_core.search import (
    ArchivalStore,
    SessionSearchTool,
    hybrid_search_sessions,
    hybrid_session_search,
    session_search,
)

pytestmark = pytest.mark.integration


class _RecallEmbedder:
    model = "fake/search-meaning"
    dim = 2

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        return [
            [1.0, 0.0] if ("feline" in text.lower() or "cat nap" in text.lower()) else [0.0, 1.0]
            for text in texts
        ]


class _BrokenRecallEmbedder:
    model = "fake/search-broken"
    dim = 2

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        raise RuntimeError(f"offline for {len(texts)} texts")


async def test_archival_hybrid_search_finds_relevant(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    store = ArchivalStore(migrated_db, scope, FakeEmbedder(dim=32))
    await store.add("The mitochondria is the powerhouse of the cell.")
    await store.add("Rome was founded, according to legend, in 753 BC.")
    await store.add("Photosynthesis converts sunlight into chemical energy in plants.")

    hits = await store.search("what powers a cell", k=2)
    joined = " ".join(h.content for h in hits)
    assert "mitochondria" in joined  # retrieved by lexical+semantic fusion


async def test_archival_search_is_scope_isolated(migrated_db: AsyncEngine) -> None:
    embedder = FakeEmbedder(dim=16)
    mine = f"u:{uuid.uuid4().hex}"
    other = f"g:{uuid.uuid4().hex}"
    await ArchivalStore(migrated_db, mine, embedder).add("secret personal note about project X")

    # A store bound to another scope cannot retrieve it.
    hits = await ArchivalStore(migrated_db, other, embedder).search("project X", k=5)
    assert hits == []


async def test_model_dim_pinning_excludes_mismatched_embeddings(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    # Two rows embedded by different (model, dim) pins.
    await ArchivalStore(migrated_db, scope, FakeEmbedder(dim=8, model="m-a")).add(
        "alpha content one"
    )
    await ArchivalStore(migrated_db, scope, FakeEmbedder(dim=16, model="m-b")).add(
        "alpha content two"
    )

    # Searching with model m-b/dim16 only compares vectors from that pin in the
    # semantic arm (a cross-dim KNN would error); results are still returned.
    hits = await ArchivalStore(migrated_db, scope, FakeEmbedder(dim=16, model="m-b")).search(
        "alpha content", k=5
    )
    contents = {h.content for h in hits}
    assert "alpha content two" in contents


async def test_session_search_finds_past_messages(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s-{uuid.uuid4().hex}"
    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": scope})
        await conn.execute(
            text("INSERT INTO sessions (id, scope_id, next_seq) VALUES (:sid, :s, 2)"),
            {"sid": session_id, "s": scope},
        )
        await conn.execute(
            text(
                "INSERT INTO events (session_id, scope_id, seq, type, ts, payload) "
                "VALUES (:sid, :s, 1, 'message.token', now(), "
                "CAST(:p AS jsonb))"
            ),
            {
                "sid": session_id,
                "s": scope,
                "p": '{"role":"user","text":"remember my flight to Tokyo"}',
            },
        )

    hits = await session_search(migrated_db, scope, "Tokyo flight", k=5)
    assert any("Tokyo" in h.content for h in hits)
    assert hits[0].source.startswith("session:")


async def test_archival_insert_then_search(migrated_db: AsyncEngine) -> None:
    from keel_core.embeddings import FakeEmbedder
    from keel_core.protocols import ToolContext
    from keel_core.search import ArchivalInsertTool, ArchivalSearchTool

    embedder = FakeEmbedder(dim=16)
    ctx = ToolContext(scope_id="u:arch", session_id="s")
    inserted = await ArchivalInsertTool(migrated_db, embedder).run(
        {"content": "the capital of France is Paris"}, ctx
    )
    assert inserted.ok
    found = await ArchivalSearchTool(migrated_db, embedder).run(
        {"query": "France capital", "k": 3}, ctx
    )
    assert found.ok and "Paris" in found.output


async def test_message_and_session_wrappers_share_semantic_ranking(
    migrated_db: AsyncEngine,
) -> None:
    from keel_core.loop import admit
    from keel_core.state import PostgresEventStore

    scope = f"u:{uuid.uuid4().hex}"
    target = f"s:{uuid.uuid4().hex}"
    other = f"s:{uuid.uuid4().hex}"
    await admit(
        PostgresEventStore(migrated_db, scope),
        target,
        scope,
        "The feline sleeps on the sofa",
    )
    await admit(
        PostgresEventStore(migrated_db, scope),
        other,
        scope,
        "Quarterly finance report",
    )

    message_hits, message_status = await hybrid_session_search(
        migrated_db, scope, "cat nap", k=5, embedder=_RecallEmbedder()
    )
    session_hits, session_status = await hybrid_search_sessions(
        migrated_db, scope, "cat nap", k=5, embedder=_RecallEmbedder()
    )

    assert message_status.mode == session_status.mode == "hybrid"
    assert message_hits and message_hits[0].source == f"session:{target}"
    assert session_hits and session_hits[0].id == target
    assert "feline" in session_hits[0].snippet


async def test_session_search_tool_marks_degraded_results(
    migrated_db: AsyncEngine,
) -> None:
    from keel_core.loop import admit
    from keel_core.state import PostgresEventStore

    scope = f"u:{uuid.uuid4().hex}"
    session_id = f"s:{uuid.uuid4().hex}"
    await admit(
        PostgresEventStore(migrated_db, scope),
        session_id,
        scope,
        "quarterly invoices",
    )

    result = await SessionSearchTool(migrated_db, _BrokenRecallEmbedder()).run(
        {"query": "invoices", "k": 5},
        ToolContext(scope_id=scope, session_id="current"),
    )

    assert result.ok
    assert result.output.startswith("[semantic unavailable; lexical results only]")
    assert "invoices" in result.output
