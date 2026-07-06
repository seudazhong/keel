"""Agent runtime — Loop α (WS-A).

The two-loop core:

- **Outer loop** drives turns: build request -> call provider -> (stop-reason gate)
  execute tools -> repeat until a **named** :class:`~keel_core.types.StopReason`.
- **Inner loop** retries a provider call a bounded number of times (failover hook
  lands in M1 step 2).

Load-bearing invariants proven here:

- **I1 bounded loop / named termination** — every exit sets exactly one StopReason,
  emitted as ``run.ended{reason}``.
- **I2 persist-before-first-model-call** — :func:`admit` appends the user event
  *before* :func:`run` ever calls the provider.
- **I3 stop-reason-gated tools** — tools run only when ``finish_reason == tool_use``.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from keel_core.agents import AgentSpec
from keel_core.errors import KeelError
from keel_core.events import Event, EventType
from keel_core.protocols import (
    EventStore,
    ProviderGateway,
    ProviderRequest,
    Tool,
    ToolCall,
    ToolContext,
)
from keel_core.types import (
    FinishReason,
    RunId,
    ScopeId,
    SessionId,
    StopReason,
    TrustLevel,
)


def _now() -> datetime:
    return datetime.now(UTC)


class ToolRegistry:
    """Name -> tool lookup (one tool interface, P3)."""

    def __init__(self, tools: Sequence[Tool] = ()) -> None:
        self._tools: dict[str, Tool] = {tool.name: tool for tool in tools}

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)


@dataclass
class RunBudget:
    """Caps that bound a run."""

    max_iterations: int = 20
    token_budget: int | None = None
    max_retries: int = 2


@dataclass
class RunResult:
    """The outcome of a run."""

    run_id: RunId
    reason: StopReason
    iterations: int = 0
    tokens: int = 0
    events: list[Event] = field(default_factory=list)


@dataclass
class _TurnOutput:
    text: str
    tool_calls: list[ToolCall]
    finish_reason: FinishReason | None
    tokens: int


async def _emit(
    store: EventStore,
    event_type: EventType,
    session_id: SessionId,
    scope_id: ScopeId,
    run_id: RunId | None,
    payload: dict[str, object],
) -> None:
    await store.append(
        Event(
            type=event_type,
            seq=0,  # assigned by the store
            session_id=session_id,
            scope_id=scope_id,
            run_id=run_id,
            ts=_now(),
            payload=payload,
        )
    )


async def admit(store: EventStore, session_id: SessionId, scope_id: ScopeId, content: str) -> None:
    """Durably persist user input BEFORE any model call (invariant I2)."""
    await _emit(
        store,
        EventType.message_token,
        session_id,
        scope_id,
        None,
        {"role": "user", "text": content},
    )


async def _build_request(
    agent: AgentSpec, store: EventStore, session_id: SessionId
) -> ProviderRequest:
    messages: list[dict[str, object]] = []
    async for event in store.read(session_id):
        role = event.payload.get("role")
        if role in ("user", "assistant"):
            messages.append({"role": role, "content": event.payload.get("text", "")})
        elif event.type == EventType.tool_result:
            messages.append({"role": "tool", "content": str(event.payload)})
    return ProviderRequest(model=agent.model, messages=messages)


async def _call_provider(
    provider: ProviderGateway, request: ProviderRequest, max_retries: int
) -> _TurnOutput:
    attempt = 0
    while True:
        try:
            text_parts: list[str] = []
            tool_calls: list[ToolCall] = []
            finish: FinishReason | None = None
            async for chunk in provider.stream(request):
                if chunk.delta:
                    text_parts.append(chunk.delta)
                if chunk.tool_call is not None:
                    tool_calls.append(chunk.tool_call)
                if chunk.finish_reason is not None:
                    finish = chunk.finish_reason
            text = "".join(text_parts)
            return _TurnOutput(
                text=text, tool_calls=tool_calls, finish_reason=finish, tokens=len(text)
            )
        except Exception as exc:  # noqa: BLE001 - bounded retry then fail closed
            attempt += 1
            if attempt > max_retries:
                raise KeelError("provider call failed after retries") from exc


async def _execute_tool(
    store: EventStore,
    registry: ToolRegistry,
    session_id: SessionId,
    scope_id: ScopeId,
    run_id: RunId,
    trust: TrustLevel,
    call: ToolCall,
) -> None:
    await _emit(
        store,
        EventType.tool_call,
        session_id,
        scope_id,
        run_id,
        {"tool": call.name, "call_id": call.id, "args": call.arguments},
    )
    tool = registry.get(call.name)
    if tool is None:
        await _emit(
            store,
            EventType.tool_result,
            session_id,
            scope_id,
            run_id,
            {"call_id": call.id, "ok": False, "error": "unknown tool"},
        )
        return
    ctx = ToolContext(scope_id=scope_id, session_id=session_id, trust=trust)
    result = await tool.run(call.arguments, ctx)
    await _emit(
        store,
        EventType.tool_result,
        session_id,
        scope_id,
        run_id,
        {"call_id": call.id, "ok": result.ok, "output": result.output},
    )


async def run(
    *,
    agent: AgentSpec,
    session_id: SessionId,
    store: EventStore,
    provider: ProviderGateway,
    registry: ToolRegistry | None = None,
    budget: RunBudget | None = None,
    interrupt: Callable[[], bool] | None = None,
) -> RunResult:
    """Execute the agent loop until a named termination and return the result."""
    registry = registry or ToolRegistry()
    budget = budget or RunBudget()
    scope_id = agent.scope.id
    trust = agent.scope.trust
    run_id: RunId = uuid.uuid4().hex

    await _emit(store, EventType.run_started, session_id, scope_id, run_id, {"agent": agent.id})

    iterations = 0
    tokens = 0
    reason = StopReason.completed

    while True:
        if interrupt is not None and interrupt():
            reason = StopReason.interrupted
            break
        if iterations >= budget.max_iterations:
            reason = StopReason.max_iterations
            break
        if budget.token_budget is not None and tokens >= budget.token_budget:
            reason = StopReason.budget_exhausted
            break

        await _emit(
            store, EventType.turn_started, session_id, scope_id, run_id, {"turn": iterations}
        )
        request = await _build_request(agent, store, session_id)

        try:
            turn = await _call_provider(provider, request, budget.max_retries)
        except KeelError:
            reason = StopReason.error
            break

        tokens += turn.tokens
        if turn.text:
            await _emit(
                store,
                EventType.message_token,
                session_id,
                scope_id,
                run_id,
                {"role": "assistant", "text": turn.text},
            )

        # Stop-reason gate (I3): tools run ONLY on an explicit tool_use finish.
        if turn.finish_reason == FinishReason.tool_use and turn.tool_calls:
            for call in turn.tool_calls:
                await _execute_tool(store, registry, session_id, scope_id, run_id, trust, call)
            iterations += 1
            await _emit(
                store, EventType.turn_ended, session_id, scope_id, run_id, {"turn": iterations}
            )
            continue

        await _emit(store, EventType.turn_ended, session_id, scope_id, run_id, {"turn": iterations})
        if turn.finish_reason in (FinishReason.end_turn, FinishReason.length):
            reason = StopReason.completed
        elif turn.finish_reason == FinishReason.error:
            reason = StopReason.error
        else:
            # Guardrail: no clear finish and no tool call -> stuck; halt.
            reason = StopReason.halted
        break

    await _emit(store, EventType.run_ended, session_id, scope_id, run_id, {"reason": str(reason)})
    return RunResult(run_id=run_id, reason=reason, iterations=iterations, tokens=tokens)
