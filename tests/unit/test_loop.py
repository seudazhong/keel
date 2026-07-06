"""Loop α tests: named termination, stop-reason gate, durable admission."""

from __future__ import annotations

from collections.abc import AsyncIterator

from keel_core.agents import AgentSpec, Scope
from keel_core.events import EventType
from keel_core.loop import RunBudget, ToolRegistry, admit, run
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import ProviderChunk, ProviderRequest, ToolCall, ToolContext, ToolResult
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, PermissionDecision, ScopeKind, StopReason, TrustLevel


def _agent() -> AgentSpec:
    return AgentSpec(
        id="a1",
        name="A",
        model="test/model",
        scope=Scope(id="u:1", kind=ScopeKind.personal, trust=TrustLevel.trusted),
    )


class _SpyTool:
    name = "echo"
    description = "Echo the args."

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def input_schema(self) -> dict[str, object]:
        return {"type": "object"}

    async def run(self, args: dict[str, object], ctx: ToolContext) -> ToolResult:
        self.calls.append(args)
        return ToolResult(ok=True, output="echoed")


async def _one_end_turn() -> AsyncIterator[ProviderChunk]:
    yield ProviderChunk(delta="ok", finish_reason=FinishReason.end_turn)


def _event_types(store: InMemoryEventStore, session: str) -> list[EventType]:
    return [event.type for event in store.snapshot(session)]


async def test_run_completes_with_named_termination() -> None:
    store = InMemoryEventStore()
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="hi", finish_reason=FinishReason.end_turn)]]
    )
    await admit(store, "s1", "u:1", "hello")
    result = await run(agent=_agent(), session_id="s1", store=store, provider=provider)

    assert result.reason is StopReason.completed
    types = _event_types(store, "s1")
    assert types[0] == EventType.message_token  # admitted user input
    assert EventType.run_started in types
    assert types[-1] == EventType.run_ended


async def test_tool_use_executes_then_completes() -> None:
    store = InMemoryEventStore()
    tool = _SpyTool()
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c1", name="echo", arguments={"x": 1}),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
        ]
    )
    await admit(store, "s1", "u:1", "use the tool")
    result = await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=provider,
        registry=ToolRegistry([tool]),
    )

    assert result.reason is StopReason.completed
    assert tool.calls == [{"x": 1}]
    types = _event_types(store, "s1")
    assert EventType.tool_call in types
    assert EventType.tool_result in types


async def test_stop_reason_gate_blocks_tools_without_tool_use() -> None:
    store = InMemoryEventStore()
    tool = _SpyTool()
    # A tool_call is present but the finish reason is end_turn -> gate stays closed.
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c1", name="echo", arguments={}),
                    finish_reason=FinishReason.end_turn,
                )
            ]
        ]
    )
    await admit(store, "s1", "u:1", "hi")
    result = await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=provider,
        registry=ToolRegistry([tool]),
    )

    assert result.reason is StopReason.completed
    assert tool.calls == []  # the gate held: no tool ran
    assert EventType.tool_call not in _event_types(store, "s1")


async def test_bounded_loop_hits_max_iterations() -> None:
    store = InMemoryEventStore()
    tool = _SpyTool()
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c", name="echo", arguments={}),
                    finish_reason=FinishReason.tool_use,
                )
            ]
        ]
    )
    await admit(store, "s1", "u:1", "loop forever")
    result = await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=provider,
        registry=ToolRegistry([tool]),
        budget=RunBudget(max_iterations=3),
    )

    assert result.reason is StopReason.max_iterations
    assert result.iterations == 3
    assert _event_types(store, "s1")[-1] == EventType.run_ended


async def test_interrupt_terminates() -> None:
    store = InMemoryEventStore()
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="x", finish_reason=FinishReason.end_turn)]]
    )
    await admit(store, "s1", "u:1", "hi")
    result = await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=provider,
        interrupt=lambda: True,
    )
    assert result.reason is StopReason.interrupted


async def test_budget_exhausted() -> None:
    store = InMemoryEventStore()
    tool = _SpyTool()
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    delta="a fairly long assistant response worth many tokens",
                    tool_call=ToolCall(id="c", name="echo", arguments={}),
                    finish_reason=FinishReason.tool_use,
                )
            ]
        ]
    )
    await admit(store, "s1", "u:1", "go")
    result = await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=provider,
        registry=ToolRegistry([tool]),
        budget=RunBudget(max_iterations=100, token_budget=5),
    )
    assert result.reason is StopReason.budget_exhausted


async def test_halted_on_empty_turn() -> None:
    store = InMemoryEventStore()
    provider = ScriptedProviderGateway([[ProviderChunk()]])  # no text, tool, or finish
    await admit(store, "s1", "u:1", "hi")
    result = await run(agent=_agent(), session_id="s1", store=store, provider=provider)
    assert result.reason is StopReason.halted


async def test_provider_failure_yields_error() -> None:
    store = InMemoryEventStore()

    class _BoomGateway:
        def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
            raise RuntimeError("boom")

    await admit(store, "s1", "u:1", "hi")
    result = await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=_BoomGateway(),
        budget=RunBudget(max_retries=1),
    )
    assert result.reason is StopReason.error


async def test_persist_before_first_model_call() -> None:
    store = InMemoryEventStore()
    seen: dict[str, list[dict[str, object]]] = {}

    class _ProbeGateway:
        def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
            seen.setdefault("first_messages", request.messages)
            return _one_end_turn()

    await admit(store, "s1", "u:1", "hello world")
    await run(agent=_agent(), session_id="s1", store=store, provider=_ProbeGateway())

    first_messages = seen["first_messages"]
    assert any(m["role"] == "user" and m["content"] == "hello world" for m in first_messages)


async def test_tool_error_still_reaches_named_termination() -> None:
    store = InMemoryEventStore()

    class _BoomTool:
        name = "boom"
        description = "always raises"

        def input_schema(self) -> dict[str, object]:
            return {}

        async def run(self, args: dict[str, object], ctx: ToolContext) -> ToolResult:
            raise RuntimeError("kaboom")

    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c", name="boom", arguments={}),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
        ]
    )
    await admit(store, "s1", "u:1", "go")
    result = await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=provider,
        registry=ToolRegistry([_BoomTool()]),
    )
    assert result.reason is StopReason.completed  # the run still terminated, named
    types = _event_types(store, "s1")
    assert types[-1] == EventType.run_ended
    assert EventType.tool_result in types  # the failing tool produced a result, not a crash


async def test_agent_budget_is_respected() -> None:
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
    agent = AgentSpec(
        id="a",
        name="A",
        model="test/model",
        scope=Scope(id="u:1", kind=ScopeKind.personal),
        max_iterations=2,
    )
    await admit(store, "s1", "u:1", "loop")
    result = await run(agent=agent, session_id="s1", store=store, provider=provider)
    assert result.reason is StopReason.max_iterations
    assert result.iterations == 2  # honored the agent's cap, not RunBudget's default


async def test_loop_permission_gate_blocks_tool() -> None:
    store = InMemoryEventStore()
    tool = _SpyTool()
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c", name="echo", arguments={}),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
        ]
    )
    engine = RuleBasedPermissionEngine([Rule("*", PermissionDecision.deny)])
    await admit(store, "s1", "u:1", "go")
    result = await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=provider,
        registry=ToolRegistry([tool]),
        permissions=engine,
    )
    assert result.reason is StopReason.completed
    assert tool.calls == []  # the permission gate blocked execution inside the loop


async def test_on_delta_streams_text_deltas() -> None:
    store = InMemoryEventStore()
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(delta="Hel"),
                ProviderChunk(delta="lo", finish_reason=FinishReason.end_turn),
            ]
        ]
    )
    deltas: list[str] = []
    await admit(store, "s1", "u:1", "hi")
    await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=provider,
        on_delta=deltas.append,
    )
    assert deltas == ["Hel", "lo"]  # tokens surfaced live, in order


async def test_on_event_observes_persisted_events_in_order() -> None:
    store = InMemoryEventStore()
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="hi", finish_reason=FinishReason.end_turn)]]
    )
    seen: list[EventType] = []
    await admit(store, "s1", "u:1", "hi")  # before run: observer not yet attached
    await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=provider,
        on_event=lambda event: seen.append(event.type),
    )
    # The observer sees exactly the run's events (not the pre-run admit), in seq order.
    assert seen[0] is EventType.run_started
    assert seen[-1] is EventType.run_ended
    assert EventType.message_token in seen
    # Every observed event was durably appended (same set the store holds for the run).
    stored = [event.type for event in store.snapshot("s1")]
    assert seen == stored[1:]  # stored[0] is the admitted user message


async def test_observer_errors_never_abort_the_run() -> None:
    store = InMemoryEventStore()
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="hi", finish_reason=FinishReason.end_turn)]]
    )

    def boom_event(event: object) -> None:
        raise RuntimeError("render boom")

    def boom_delta(text: str) -> None:
        raise RuntimeError("delta boom")

    await admit(store, "s1", "u:1", "hi")
    result = await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=provider,
        on_event=boom_event,
        on_delta=boom_delta,
    )
    # Live observation is best-effort: failing observers don't crash or mislabel the run.
    assert result.reason is StopReason.completed
    assert _event_types(store, "s1")[-1] == EventType.run_ended  # events still persisted
