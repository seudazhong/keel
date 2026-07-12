"""Integration: archival add_consolidated dedupe/merge + tool success path."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Sequence

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings
from keel_core.consolidation.agent import consolidation_registry
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


# Second component of a unit vector whose cosine distance from [1, 0] is exactly 0.06:
# just past the 0.05 default but under a configured 0.08 (and the retired 0.1).
_ORTHO = math.sqrt(1.0 - 0.94**2)


class _AngleEmbedder:
    """Maps 'two'-tagged text to a vector 0.06 (cosine) away from every other text."""

    model = "fake/angle"
    dim = 2

    async def embed(self, texts: Sequence[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for value in texts:
            if "two" in value.lower():
                vectors.append([0.94, _ORTHO])
            else:
                vectors.append([1.0, 0.0])
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


async def test_add_consolidated_keeps_distinct_when_sources_overlap_but_differ(
    migrated_db: AsyncEngine,
) -> None:
    """A distance-0 rephrase must NOT merge when its source set merely overlaps.

    Regression guard: overlap + distance was the false-merge bug; exact-source
    equality now keeps two related-but-distinct facts apart.
    """
    scope = "tool:semantic-overlap"
    store = ArchivalStore(migrated_db, scope, _SemanticRetryEmbedder())

    base_id, base_created = await store.add_consolidated(
        "Create a database backup before release",
        source_event_ids=[100],
    )
    overlap_id, overlap_created = await store.add_consolidated(
        "Back up the database prior to deployment",
        source_event_ids=[100, 200],  # overlaps [100] but is not the same set
    )

    assert base_created is True
    assert overlap_created is True  # distance 0, yet the source set differs -> distinct
    assert overlap_id != base_id
    rows = await _archival_rows(migrated_db, scope)
    assert [ids for _, ids in rows] == [[100], [100, 200]]


async def test_add_consolidated_keeps_distinct_when_distance_above_threshold(
    migrated_db: AsyncEngine,
) -> None:
    """Exact same source but an embedding just past the 0.05 default stays distinct."""
    scope = "tool:semantic-distance"
    store = ArchivalStore(migrated_db, scope, _AngleEmbedder())

    base_id, base_created = await store.add_consolidated("fact one", source_event_ids=[300])
    far_id, far_created = await store.add_consolidated("fact two", source_event_ids=[300])

    assert base_created is True
    assert far_created is True  # cosine distance 0.06 > 0.05 default -> distinct
    assert far_id != base_id


async def test_add_consolidated_semantic_dedupe_disabled_when_threshold_zero(
    migrated_db: AsyncEngine,
) -> None:
    """Distance 0 disables the semantic pass; exact content-hash dedupe still merges."""
    scope = "tool:semantic-off"
    store = ArchivalStore(migrated_db, scope, _SemanticRetryEmbedder())

    base_id, base_created = await store.add_consolidated(
        "Create a database backup before release",
        source_event_ids=[400],
        semantic_dedupe_distance=0.0,
    )
    # Distance-0 rephrase over the same source must NOT merge when the pass is off.
    rephrase_id, rephrase_created = await store.add_consolidated(
        "Back up the database prior to deployment",
        source_event_ids=[400],
        semantic_dedupe_distance=0.0,
    )
    # Byte/format-equivalent content still merges by content hash regardless.
    hash_id, hash_created = await store.add_consolidated(
        "Create a database backup before release",
        source_event_ids=[401],
        semantic_dedupe_distance=0.0,
    )

    assert base_created is True
    assert rephrase_created is True
    assert rephrase_id != base_id
    assert hash_created is False
    assert hash_id == base_id


async def test_registry_threads_non_default_semantic_dedupe_distance(
    migrated_db: AsyncEngine,
) -> None:
    """A non-default Settings threshold flows registry -> tool -> store and merges at 0.06."""
    scope = "tool:registry-threshold"
    run_context = ConsolidationRunContext(
        allowed_event_ids=frozenset({500}),
        allowed_user_event_ids=frozenset({500}),
    )
    settings = Settings(consolidation_semantic_dedupe_distance=0.08)
    registry = consolidation_registry(migrated_db, _AngleEmbedder(), run_context, settings)
    tool = registry.get("archival_consolidate_insert")
    assert tool is not None
    ctx = ToolContext(scope_id=scope, session_id=f"consolidation:{scope}:r")

    first = await tool.run(
        {"content": "fact one", "confidence": 0.9, "source_event_ids": [500]}, ctx
    )
    second = await tool.run(
        {"content": "fact two", "confidence": 0.9, "source_event_ids": [500]}, ctx
    )

    assert "inserted" in first.output
    # 0.06 distance exceeds the 0.05 default but is under the configured 0.08 -> merges.
    assert "merged" in second.output
    rows = await _archival_rows(migrated_db, scope)
    assert len(rows) == 1


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
