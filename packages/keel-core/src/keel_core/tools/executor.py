"""Parallel-safe tool executor (WS-C, invariant I5).

Independent, read-only tool calls run concurrently. Calls that **conflict** — they
touch an overlapping resource and at least one is a **write** — serialize in source
order (a write with unknown resources conflicts with everything). Results are always
returned in **source order**, regardless of completion order.

Each call passes the permission gate first (deny -> denied result; ask -> requires an
approver, else fail closed).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

from keel_core.protocols import PermissionEngine, Tool, ToolCall, ToolContext, ToolResult
from keel_core.types import PermissionDecision

ApproveFn = Callable[[ToolCall, ToolContext], bool]


@dataclass
class ExecRequest:
    """A tool call plus the access metadata the executor schedules on."""

    call: ToolCall
    tool: Tool
    write: bool = False
    resources: frozenset[str] = field(default_factory=frozenset)


def _conflicts(a: ExecRequest, b: ExecRequest) -> bool:
    if not (a.write or b.write):
        return False  # two reads never conflict
    if (a.write and not a.resources) or (b.write and not b.resources):
        return True  # a write on unknown resources conflicts with everything
    return bool(a.resources & b.resources)


async def _run_one(
    request: ExecRequest,
    ctx: ToolContext,
    permissions: PermissionEngine,
    approve: ApproveFn | None,
) -> ToolResult:
    decision = permissions.evaluate(request.call.name, request.call.arguments, ctx)
    if decision is PermissionDecision.deny:
        return ToolResult(ok=False, output="permission denied")
    if decision is PermissionDecision.ask and (approve is None or not approve(request.call, ctx)):
        return ToolResult(ok=False, output="approval required")
    try:
        return await request.tool.run(request.call.arguments, ctx)
    except Exception as exc:  # noqa: BLE001 - a tool failure must not crash the run
        return ToolResult(ok=False, output=f"tool error: {exc.__class__.__name__}")


async def execute(
    requests: list[ExecRequest],
    ctx: ToolContext,
    permissions: PermissionEngine,
    approve: ApproveFn | None = None,
) -> list[ToolResult]:
    """Run tool calls with parallel-safety, returning results in source order."""
    count = len(requests)
    done = [asyncio.Event() for _ in range(count)]
    results: list[ToolResult] = [ToolResult(ok=False) for _ in range(count)]

    async def run_unit(index: int) -> None:
        for earlier in range(index):
            if _conflicts(requests[index], requests[earlier]):
                await done[earlier].wait()
        results[index] = await _run_one(requests[index], ctx, permissions, approve)
        done[index].set()

    await asyncio.gather(*(run_unit(i) for i in range(count)))
    return results
