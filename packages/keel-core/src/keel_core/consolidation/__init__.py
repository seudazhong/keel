"""Memory consolidation subsystem (spec 2026-07-12-memory-consolidation).

A scheduled, unattended agent reviews recent conversation and durably records
lasting facts: it *proposes* core-memory rewrites (human-reviewed) and inserts
deduplicated, provenance-tagged archival passages. This package's public surface
is re-exported below; the submodules hold the implementation.
"""

from __future__ import annotations

from keel_core.consolidation.agent import (
    CONSOLIDATION_SYSTEM_INSTRUCTION,
    MEMORY_CONSOLIDATOR_AGENT_ID,
    build_consolidation_agent,
    consolidation_permissions,
    consolidation_registry,
    consolidation_schedule_id,
    consolidation_session_id,
    consolidation_system_context,
    format_consolidation_prompt,
)
from keel_core.consolidation.context import ConsolidationRunContext, should_advance_cursor
from keel_core.consolidation.cursor import (
    ConsolidationCursorState,
    ConsolidationCursorStore,
    ConsolidationLease,
)
from keel_core.consolidation.hashing import (
    archival_content_hash,
    consolidation_idempotency_key,
    normalize_whitespace,
)
from keel_core.consolidation.proposals import (
    MemoryProposal,
    MemoryProposalStore,
    ProposalOutcome,
    ProposalResolution,
)
from keel_core.consolidation.reader import (
    ConsolidationBatch,
    ConsolidationBatchReader,
    ConsolidationMessage,
)
from keel_core.consolidation.tools import (
    ArchivalConsolidateInsertTool,
    ProposeRewriteTool,
    validate_archival_insert,
    validate_propose_rewrite,
)

__all__ = [
    "CONSOLIDATION_SYSTEM_INSTRUCTION",
    "MEMORY_CONSOLIDATOR_AGENT_ID",
    "ArchivalConsolidateInsertTool",
    "ConsolidationBatch",
    "ConsolidationBatchReader",
    "ConsolidationCursorState",
    "ConsolidationCursorStore",
    "ConsolidationLease",
    "ConsolidationMessage",
    "ConsolidationRunContext",
    "MemoryProposal",
    "MemoryProposalStore",
    "ProposalOutcome",
    "ProposalResolution",
    "ProposeRewriteTool",
    "archival_content_hash",
    "build_consolidation_agent",
    "consolidation_idempotency_key",
    "consolidation_permissions",
    "consolidation_registry",
    "consolidation_schedule_id",
    "consolidation_session_id",
    "consolidation_system_context",
    "format_consolidation_prompt",
    "normalize_whitespace",
    "should_advance_cursor",
    "validate_archival_insert",
    "validate_propose_rewrite",
]
