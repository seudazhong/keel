"""Permission engine tests: deny > ask > allow, default ask."""

from __future__ import annotations

from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import ToolContext
from keel_core.types import PermissionDecision


def _ctx() -> ToolContext:
    return ToolContext(scope_id="s", session_id="sess")


def test_default_is_ask() -> None:
    assert RuleBasedPermissionEngine().evaluate("read", {}, _ctx()) is PermissionDecision.ask


def test_allow_rule() -> None:
    engine = RuleBasedPermissionEngine([Rule("read", PermissionDecision.allow)])
    assert engine.evaluate("read", {}, _ctx()) is PermissionDecision.allow


def test_deny_beats_allow_among_matches() -> None:
    engine = RuleBasedPermissionEngine(
        [Rule("*", PermissionDecision.allow), Rule("shell", PermissionDecision.deny)]
    )
    assert engine.evaluate("shell", {}, _ctx()) is PermissionDecision.deny
    assert engine.evaluate("read", {}, _ctx()) is PermissionDecision.allow


def test_wildcard_matches_any_tool() -> None:
    engine = RuleBasedPermissionEngine([Rule("*", PermissionDecision.allow)])
    assert engine.evaluate("anything", {}, _ctx()) is PermissionDecision.allow
