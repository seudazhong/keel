"""Per-run consolidation state + the cursor-advance safety predicate.

``ConsolidationRunContext`` carries the batch's allowed event ids (grounding the
user-evidence rule) and mutable success/validation counters the tools bump. The
executor swallows tool exceptions into failed ``ToolResult``s, so a run can reach
``completed`` even after a tool failed; ``should_advance_cursor`` therefore refuses
to advance unless the run completed cleanly AND no validation/infra error was
recorded — otherwise the same batch is safely retried on the next run.
"""

from __future__ import annotations

from dataclasses import dataclass

from keel_core.types import StopReason


@dataclass
class ConsolidationRunContext:
    """Mutable per-run state shared with the consolidation tools."""

    allowed_event_ids: frozenset[int]
    allowed_user_event_ids: frozenset[int]
    successful_actions: int = 0
    validation_errors: int = 0


def should_advance_cursor(reason: StopReason, validation_errors: int) -> bool:
    """Advance the cursor only on a clean completion with zero recorded errors."""
    return reason is StopReason.completed and validation_errors == 0
