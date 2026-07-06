"""Permission engine (WS-J) — fail-closed tool authorization.

Rule-based decisions with **deny > ask > allow** precedence among matching rules
and a **default of ask** (FR-X1). Choosing the most-restrictive matching decision
(rather than last-match-wins) keeps the gate fail-closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from keel_core.protocols import ToolContext
from keel_core.types import PermissionDecision

_PRECEDENCE: dict[PermissionDecision, int] = {
    PermissionDecision.allow: 0,
    PermissionDecision.ask: 1,
    PermissionDecision.deny: 2,
}


@dataclass(frozen=True)
class Rule:
    """A permission rule. ``tool`` is an exact tool name or ``"*"`` (any)."""

    tool: str
    decision: PermissionDecision


class RuleBasedPermissionEngine:
    """Evaluate tool calls against rules; most-restrictive match wins."""

    def __init__(
        self,
        rules: list[Rule] | None = None,
        default: PermissionDecision = PermissionDecision.ask,
    ) -> None:
        self._rules = rules or []
        self._default = default

    def evaluate(self, tool: str, args: dict[str, Any], ctx: ToolContext) -> PermissionDecision:
        matches = [rule.decision for rule in self._rules if rule.tool in ("*", tool)]
        if not matches:
            return self._default
        return max(matches, key=lambda decision: _PRECEDENCE[decision])
