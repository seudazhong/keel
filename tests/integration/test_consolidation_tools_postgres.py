"""Integration: archival add_consolidated dedupe/merge + tool success path."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation.context import ConsolidationRunContext
from keel_core.consolidation.tools import ArchivalConsolidateInsertTool
from keel_core.embeddings import FakeEmbedder
from keel_core.protocols import ToolContext
from keel_core.search import ArchivalStore

pytestmark = pytest.mark.integration


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
