"""Parallel-safe executor tests (invariant I5).

Independent reads run concurrently; writes to an overlapping resource serialize;
results always come back in source order; the permission gate is applied first.
"""

from __future__ import annotations

import asyncio
from typing import Any

from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import ToolCall, ToolContext, ToolResult
from keel_core.tools.executor import ExecRequest, execute
from keel_core.types import PermissionDecision


def _ctx() -> ToolContext:
    return ToolContext(scope_id="s", session_id="sess")


def _allow_all() -> RuleBasedPermissionEngine:
    return RuleBasedPermissionEngine([Rule("*", PermissionDecision.allow)])


class _ProbeTool:
    def __init__(self, name: str, state: dict[str, Any]) -> None:
        self.name = name
        self.description = name
        self._state = state

    def input_schema(self) -> dict[str, Any]:
        return {}

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        state = self._state
        state["active"] += 1
        state["max_active"] = max(state["max_active"], state["active"])
        state["order"].append(f"start:{self.name}")
        await asyncio.sleep(0.02)
        state["order"].append(f"end:{self.name}")
        state["active"] -= 1
        return ToolResult(ok=True, output=self.name)


def _state() -> dict[str, Any]:
    return {"active": 0, "max_active": 0, "order": []}


async def test_independent_reads_run_concurrently() -> None:
    state = _state()
    requests = [
        ExecRequest(
            ToolCall(id="1", name="r1"), _ProbeTool("r1", state), resources=frozenset({"a"})
        ),
        ExecRequest(
            ToolCall(id="2", name="r2"), _ProbeTool("r2", state), resources=frozenset({"b"})
        ),
    ]
    results = await execute(requests, _ctx(), _allow_all())
    assert [r.output for r in results] == ["r1", "r2"]
    assert state["max_active"] == 2


async def test_writes_to_same_resource_serialize() -> None:
    state = _state()
    requests = [
        ExecRequest(
            ToolCall(id="1", name="w1"),
            _ProbeTool("w1", state),
            write=True,
            resources=frozenset({"a"}),
        ),
        ExecRequest(
            ToolCall(id="2", name="w2"),
            _ProbeTool("w2", state),
            write=True,
            resources=frozenset({"a"}),
        ),
    ]
    results = await execute(requests, _ctx(), _allow_all())
    assert state["max_active"] == 1  # serialized (no overlap)
    assert state["order"] == ["start:w1", "end:w1", "start:w2", "end:w2"]
    assert [r.output for r in results] == ["w1", "w2"]


async def test_results_stay_in_source_order() -> None:
    state = _state()

    class _Slow(_ProbeTool):
        async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
            await asyncio.sleep(0.05)
            return ToolResult(ok=True, output="slow")

    class _Fast(_ProbeTool):
        async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
            return ToolResult(ok=True, output="fast")

    requests = [
        ExecRequest(
            ToolCall(id="1", name="slow"), _Slow("slow", state), resources=frozenset({"a"})
        ),
        ExecRequest(
            ToolCall(id="2", name="fast"), _Fast("fast", state), resources=frozenset({"b"})
        ),
    ]
    results = await execute(requests, _ctx(), _allow_all())
    assert [r.output for r in results] == ["slow", "fast"]  # despite fast finishing first


async def test_permission_deny_blocks_execution() -> None:
    state = _state()
    engine = RuleBasedPermissionEngine([Rule("*", PermissionDecision.deny)])
    requests = [ExecRequest(ToolCall(id="1", name="w"), _ProbeTool("w", state), write=True)]
    results = await execute(requests, _ctx(), engine)
    assert results[0].ok is False
    assert state["order"] == []  # the tool never ran


async def test_ask_requires_approval() -> None:
    state = _state()
    engine = RuleBasedPermissionEngine(default=PermissionDecision.ask)
    requests = [ExecRequest(ToolCall(id="1", name="w"), _ProbeTool("w", state))]

    denied = await execute(requests, _ctx(), engine)  # no approver -> fail closed
    assert denied[0].ok is False

    approved = await execute(requests, _ctx(), engine, approve=lambda call, ctx: True)
    assert approved[0].ok is True
