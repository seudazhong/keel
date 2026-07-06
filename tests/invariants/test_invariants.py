"""Executable invariant gate registry (DESIGN-REVIEW §5).

Ten non-negotiable invariants; each is a merge-blocking gate for the milestone
that introduces it. **Proven** invariants run a canonical acceptance assertion
here; **pending** ones are frozen specs that skip until they land in M1.

Full specs: docs/INVARIANTS.md. (Spike tests hold the detailed proofs; this
module is the single canonical checklist.)
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta

import pytest

from keel_core.agents import AgentSpec, Scope
from keel_core.context import StablePromptAssembler
from keel_core.errors import CrossScopeError
from keel_core.loop import RunBudget, ToolRegistry, admit, run
from keel_core.protocols import ProviderChunk, ProviderRequest, ToolCall, ToolContext, ToolResult
from keel_core.scope import DefaultScopeGuard
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, ScopeKind, StopReason
from keel_sandbox.policy import EgressPolicy, PathPolicy
from keel_scheduler.atmostonce import AtMostOnceScheduler, InMemoryClaimStore, Schedule


def _gate_byte_stable_prefix() -> None:
    agent = AgentSpec(id="a", name="n", model="m", scope=Scope(id="s", kind=ScopeKind.personal))
    assembler = StablePromptAssembler()
    assert assembler.assemble(agent, []).cache_key == assembler.assemble(agent, []).cache_key


def _gate_two_level_sandbox() -> None:
    assert not EgressPolicy().is_allowed("example.com")  # network off by default
    assert not PathPolicy("/work").is_allowed("/work/../etc/passwd")  # no escape


def _gate_at_most_once() -> None:
    now = datetime(2026, 1, 1, 9, 0, 0)
    store = InMemoryClaimStore({"j": now})
    runs: list[str] = []
    expected = store.snapshot()["j"]
    schedule = [Schedule("j", expected, timedelta(hours=1))]
    AtMostOnceScheduler(store, runs.append).tick(schedule, now)
    AtMostOnceScheduler(store, runs.append).tick(list(schedule), now)  # second leader
    assert runs == ["j"]


def _gate_per_scope_isolation() -> None:
    with pytest.raises(CrossScopeError):
        DefaultScopeGuard().enforce("group:1", "u:1")


def _loop_agent() -> AgentSpec:
    return AgentSpec(id="a", name="n", model="m", scope=Scope(id="u:1", kind=ScopeKind.personal))


async def _end_turn() -> AsyncIterator[ProviderChunk]:
    yield ProviderChunk(delta="ok", finish_reason=FinishReason.end_turn)


def _gate_bounded_loop_named_termination() -> None:
    async def scenario() -> StopReason:
        store = InMemoryEventStore()
        provider = ScriptedProviderGateway(
            [
                [
                    ProviderChunk(
                        tool_call=ToolCall(id="c", name="x", arguments={}),
                        finish_reason=FinishReason.tool_use,
                    )
                ]
            ]
        )
        await admit(store, "s", "u:1", "go")
        result = await run(
            agent=_loop_agent(),
            session_id="s",
            store=store,
            provider=provider,
            budget=RunBudget(max_iterations=2),
        )
        return result.reason

    assert asyncio.run(scenario()) is StopReason.max_iterations


def _gate_persist_before_first_model_call() -> None:
    async def scenario() -> bool:
        store = InMemoryEventStore()
        seen: dict[str, list[dict[str, object]]] = {}

        class _Probe:
            def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
                seen.setdefault("messages", request.messages)
                return _end_turn()

        await admit(store, "s", "u:1", "hello")
        await run(agent=_loop_agent(), session_id="s", store=store, provider=_Probe())
        return any(m["role"] == "user" and m["content"] == "hello" for m in seen["messages"])

    assert asyncio.run(scenario())


def _gate_stop_reason_gated_tools() -> None:
    calls: list[dict[str, object]] = []

    class _SpyTool:
        name = "t"
        description = "t"

        def input_schema(self) -> dict[str, object]:
            return {}

        async def run(self, args: dict[str, object], ctx: ToolContext) -> ToolResult:
            calls.append(args)
            return ToolResult(ok=True)

    async def scenario() -> None:
        store = InMemoryEventStore()
        provider = ScriptedProviderGateway(
            [
                [
                    ProviderChunk(
                        tool_call=ToolCall(id="c", name="t", arguments={}),
                        finish_reason=FinishReason.end_turn,
                    )
                ]
            ]
        )
        await admit(store, "s", "u:1", "hi")
        await run(
            agent=_loop_agent(),
            session_id="s",
            store=store,
            provider=provider,
            registry=ToolRegistry([_SpyTool()]),
        )

    asyncio.run(scenario())
    assert calls == []


# invariant id -> (enforced-in component, gate | None). None => spec pending M1.
INVARIANTS: dict[str, tuple[str, Callable[[], None] | None]] = {
    "I1-bounded-loop-named-termination": ("keel_core/loop", _gate_bounded_loop_named_termination),
    "I2-persist-before-first-model-call": (
        "keel_core/state",
        _gate_persist_before_first_model_call,
    ),
    "I3-stop-reason-gated-tools": ("keel_core/loop", _gate_stop_reason_gated_tools),
    "I4-byte-stable-prompt-prefix": ("keel_core/context", _gate_byte_stable_prefix),
    "I5-parallel-safe-deterministic-order": ("keel_core/tools", None),
    "I6-two-level-sandbox": ("keel_sandbox+permissions", _gate_two_level_sandbox),
    "I7-shared-budget-delegation-tree": ("keel_core/agents", None),
    "I8-import-not-trust": ("mcp+skills+discovery", None),
    "I9-at-most-once-schedule": ("keel_scheduler", _gate_at_most_once),
    "I10-per-scope-data-isolation": ("keel_core/scope+RLS", _gate_per_scope_isolation),
}


@pytest.mark.parametrize("invariant", list(INVARIANTS), ids=list(INVARIANTS))
def test_invariant_gate(invariant: str) -> None:
    enforced_in, gate = INVARIANTS[invariant]
    if gate is None:
        pytest.skip(f"{invariant}: acceptance spec frozen; implemented in M1 ({enforced_in})")
    gate()


def test_all_ten_invariants_registered() -> None:
    assert len(INVARIANTS) == 10
