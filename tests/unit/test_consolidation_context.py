"""Unit tests for the consolidation run context + cursor-advance predicate."""

from __future__ import annotations

from keel_core.consolidation.context import ConsolidationRunContext, should_advance_cursor
from keel_core.types import StopReason


def test_run_context_defaults() -> None:
    ctx = ConsolidationRunContext(
        allowed_event_ids=frozenset({1, 2}),
        allowed_user_event_ids=frozenset({1}),
    )
    assert ctx.successful_actions == 0
    assert ctx.validation_errors == 0
    assert 1 in ctx.allowed_user_event_ids
    assert ctx.allowed_user_event_ids <= ctx.allowed_event_ids


def test_counters_are_mutable() -> None:
    ctx = ConsolidationRunContext(allowed_event_ids=frozenset(), allowed_user_event_ids=frozenset())
    ctx.validation_errors += 1
    ctx.successful_actions += 2
    assert ctx.validation_errors == 1
    assert ctx.successful_actions == 2


def test_should_advance_only_on_clean_completion() -> None:
    assert should_advance_cursor(StopReason.completed, 0) is True
    assert should_advance_cursor(StopReason.completed, 1) is False
    assert should_advance_cursor(StopReason.error, 0) is False
    assert should_advance_cursor(StopReason.max_iterations, 0) is False
    assert should_advance_cursor(StopReason.suspended, 0) is False
