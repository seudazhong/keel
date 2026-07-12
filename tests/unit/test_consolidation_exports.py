"""Smoke test: the consolidation package surface imports and wires a 2-tool registry."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import create_async_engine

from keel_core import consolidation
from keel_core.config import Settings
from keel_core.consolidation import ConsolidationRunContext, consolidation_registry
from keel_core.embeddings import FakeEmbedder

_DUMMY_URL = "postgresql+psycopg://localhost:5432/keel"

_EXPECTED = (
    "normalize_whitespace",
    "archival_content_hash",
    "consolidation_idempotency_key",
    "ConsolidationRunContext",
    "should_advance_cursor",
    "ConsolidationLease",
    "ConsolidationCursorState",
    "ConsolidationCursorStore",
    "ConsolidationMessage",
    "ConsolidationBatch",
    "ConsolidationBatchReader",
    "MemoryProposal",
    "ProposalOutcome",
    "ProposalResolution",
    "MemoryProposalStore",
    "validate_propose_rewrite",
    "validate_archival_insert",
    "ProposeRewriteTool",
    "ArchivalConsolidateInsertTool",
    "MEMORY_CONSOLIDATOR_AGENT_ID",
    "CONSOLIDATION_SYSTEM_INSTRUCTION",
    "consolidation_schedule_id",
    "consolidation_session_id",
    "build_consolidation_agent",
    "consolidation_permissions",
    "consolidation_registry",
    "consolidation_system_context",
    "format_consolidation_prompt",
)


def test_public_surface_is_exported() -> None:
    for name in _EXPECTED:
        assert name in consolidation.__all__, name
        assert hasattr(consolidation, name), name


async def test_registry_wires_two_tools() -> None:
    engine = create_async_engine(_DUMMY_URL)
    try:
        run_context = ConsolidationRunContext(
            allowed_event_ids=frozenset(), allowed_user_event_ids=frozenset()
        )
        registry = consolidation_registry(engine, FakeEmbedder(), run_context, Settings())
        assert len(registry.schemas()) == 2
    finally:
        await engine.dispose()
