"""Integration: archival add_consolidated dedupe/merge + tool success path."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.context import ConsolidationRunContext
from keel_core.consolidation.proposals import MemoryProposalStore
from keel_core.consolidation.tools import ArchivalConsolidateInsertTool, ProposeRewriteTool
from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ToolContext
from keel_core.search import ArchivalStore

pytestmark = pytest.mark.integration


class _SemanticRetryEmbedder:
    model = "fake/semantic-retry"
    dim = 2

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for value in texts:
            lowered = value.lower()
            if "backup" in lowered or "back up" in lowered:
                vectors.append([1.0, 0.0])
            else:
                vectors.append([0.0, 1.0])
        return vectors


async def _archival_rows(engine: AsyncEngine, scope: str) -> list[tuple[str, list[int]]]:
    async with engine.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        rows = (
            await conn.execute(
                text(
                    "SELECT origin, source_event_ids FROM archival WHERE scope_id = :s ORDER BY id"
                ),
                {"s": scope},
            )
        ).all()
    return [(str(r.origin), [int(i) for i in r.source_event_ids]) for r in rows]


async def test_add_consolidated_dedupes_and_merges(migrated_db: AsyncEngine) -> None:
    scope = "tool:dedupe"
    store = ArchivalStore(migrated_db, scope, FakeEmbedder())

    id_a, created_a = await store.add_consolidated("Likes  tea", source_event_ids=[1])
    id_dup, created_dup = await store.add_consolidated("Likes tea", source_event_ids=[2])
    id_b, created_b = await store.add_consolidated("likes tea", source_event_ids=[3])

    assert created_a is True
    assert created_dup is False  # whitespace-only difference -> same content hash
    assert id_dup == id_a
    assert created_b is False  # case-only difference -> same normalized content hash
    assert id_b == id_a

    rows = await _archival_rows(migrated_db, scope)
    assert len(rows) == 1
    assert all(origin == "consolidation" for origin, _ in rows)
    assert [ids for _, ids in rows] == [[1, 2, 3]]


async def test_archival_tool_success_path(migrated_db: AsyncEngine) -> None:
    scope = "tool:ok"
    run_context = ConsolidationRunContext(
        allowed_event_ids=frozenset({10, 11}),
        allowed_user_event_ids=frozenset({10}),
    )
    tool = ArchivalConsolidateInsertTool(
        migrated_db, FakeEmbedder(), run_context, min_confidence=0.8
    )
    result = await tool.run(
        {"content": "user prefers dark mode", "confidence": 0.95, "source_event_ids": [10, 11]},
        ToolContext(scope_id=scope, session_id=f"consolidation:{scope}:r"),
    )
    assert result.ok is True
    assert run_context.successful_actions == 1
    assert run_context.validation_errors == 0
    rows = await _archival_rows(migrated_db, scope)
    assert rows == [("consolidation", [10, 11])]


async def test_add_consolidated_concurrent_merge(migrated_db: AsyncEngine) -> None:
    """FOR UPDATE serializes concurrent source_event_ids merges on the same content hash."""
    scope = "tool:concurrent"
    store = ArchivalStore(migrated_db, scope, FakeEmbedder())

    # Establish the base row with source [1].
    id_base, created_base = await store.add_consolidated("concurrent fact", source_event_ids=[1])
    assert created_base is True

    # Concurrently add [2] and [3] – FOR UPDATE serializes the two UPDATE paths.
    results = await asyncio.gather(
        store.add_consolidated("concurrent fact", source_event_ids=[2]),
        store.add_consolidated("concurrent fact", source_event_ids=[3]),
    )

    for row_id, created in results:
        assert row_id == id_base
        assert created is False

    rows = await _archival_rows(migrated_db, scope)
    assert len(rows) == 1
    assert [ids for _, ids in rows] == [[1, 2, 3]]


async def test_add_consolidated_semantically_dedupes_retry_for_same_source(
    migrated_db: AsyncEngine,
) -> None:
    scope = "tool:semantic-retry"
    store = ArchivalStore(migrated_db, scope, _SemanticRetryEmbedder())

    base_id, base_created = await store.add_consolidated(
        "Create a database backup before release",
        source_event_ids=[100],
    )
    replay_id, replay_created = await store.add_consolidated(
        "Back up the database prior to deployment",
        source_event_ids=[100],
    )
    distinct_id, distinct_created = await store.add_consolidated(
        "The API listens on port 8000",
        source_event_ids=[100],
    )

    assert base_created is True
    assert replay_created is False
    assert replay_id == base_id
    assert distinct_created is True
    assert distinct_id != base_id


async def test_add_consolidated_does_not_semantically_merge_unrelated_sources(
    migrated_db: AsyncEngine,
) -> None:
    scope = "tool:semantic-sources"
    store = ArchivalStore(migrated_db, scope, _SemanticRetryEmbedder())

    first_id, first_created = await store.add_consolidated(
        "Create a database backup before release",
        source_event_ids=[200],
    )
    second_id, second_created = await store.add_consolidated(
        "Back up the database prior to deployment",
        source_event_ids=[201],
    )

    assert first_created is True
    assert second_created is True
    assert second_id != first_id


async def test_propose_tool_happy_path_and_idempotency(migrated_db: AsyncEngine) -> None:
    """ProposeRewriteTool: first call creates a pending proposal; second is idempotent."""
    scope = "tool:propose"
    run_context = ConsolidationRunContext(
        allowed_event_ids=frozenset({20, 21}),
        allowed_user_event_ids=frozenset({20}),
    )
    tool = ProposeRewriteTool(migrated_db, run_context)
    args: dict = {
        "block": "human",
        "proposed_value": "prefers dark mode",
        "reason": "explicitly stated in session",
        "source_event_ids": [20, 21],
    }
    tool_ctx = ToolContext(scope_id=scope, session_id=f"consolidation:{scope}:r")

    # First call: creates proposal.
    result1 = await tool.run(args, tool_ctx)
    assert result1.ok is True
    assert "created" in result1.output
    assert run_context.successful_actions == 1
    assert run_context.validation_errors == 0

    proposal_store = MemoryProposalStore(migrated_db, scope)
    pending = await proposal_store.list_proposals(status="pending")
    assert len(pending) == 1
    assert pending[0].block == "human"
    assert pending[0].proposed_value == "prefers dark mode"
    first_id = pending[0].id

    # Second call with identical args: idempotent, returns the existing proposal.
    result2 = await tool.run(args, tool_ctx)
    assert result2.ok is True
    assert "already proposed" in result2.output
    assert run_context.successful_actions == 2  # incremented on both paths

    pending_after = await proposal_store.list_proposals(status="pending")
    assert len(pending_after) == 1
    assert pending_after[0].id == first_id
