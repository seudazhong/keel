"""Shared interactive-run wiring: the file/shell toolset + web-style permissions (M3.6).

Extracted so the **server** (local-preview in-process runtime) and the **worker**
(durable, worker-owned execution) build the *same* interactive toolset + permission policy
from one definition — there is exactly one agent loop and one toolset contract, differing
only in where execution runs. Read-only tools are allowed; mutating tools (write/edit/shell)
require an approval (fail-closed ``ask`` default), matching the server's web policy.
"""

from __future__ import annotations

from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import Tool
from keel_core.tools import (
    EditTool,
    ExecutionEnvironment,
    GlobTool,
    GrepTool,
    LsTool,
    ReadTool,
    ShellTool,
    WriteTool,
)
from keel_core.types import PermissionDecision

READ_ONLY_TOOLS: tuple[str, ...] = ("read", "ls", "glob", "grep")
MUTATING_TOOLS: tuple[str, ...] = ("write", "edit", "shell")


def build_interactive_tools(environment: ExecutionEnvironment) -> list[Tool]:
    """The full interactive file/shell toolset over a fail-closed ExecutionEnvironment."""
    return [
        ReadTool(environment),
        WriteTool(environment),
        EditTool(environment),
        LsTool(environment),
        GlobTool(environment),
        GrepTool(environment),
        ShellTool(environment),
    ]


def interactive_permissions(
    read_only_allow: tuple[str, ...] = (),
) -> RuleBasedPermissionEngine:
    """Read-only + own-scope tools allowed; mutating tools require an approval (ask)."""
    rules = [Rule(name, PermissionDecision.allow) for name in READ_ONLY_TOOLS]
    rules += [Rule(name, PermissionDecision.allow) for name in read_only_allow]
    rules += [Rule(name, PermissionDecision.ask) for name in MUTATING_TOOLS]
    return RuleBasedPermissionEngine(rules, default=PermissionDecision.ask)


__all__ = [
    "MUTATING_TOOLS",
    "READ_ONLY_TOOLS",
    "build_interactive_tools",
    "interactive_permissions",
]
