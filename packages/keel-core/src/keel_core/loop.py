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
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from keel_core.agents import AgentSpec
from keel_core.connectors import taint_from_events
from keel_core.errors import KeelError
from keel_core.events import Event, EventType
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.projections import project_messages
from keel_core.protocols import (
    EventStore,
    PermissionEngine,
    ProviderGateway,
    ProviderRequest,
    Tool,
    ToolCall,
    ToolContext,
    Usage,
)
from keel_core.tools.executor import ApproveFn, ExecRequest, execute
from keel_core.types import (
    FinishReason,
    PermissionDecision,
    RunId,
    ScopeId,
    SessionId,
    StopReason,
    TrustLevel,
)


def _now() -> datetime:
    return datetime.now(UTC)


# Live-observation seams: an in-process surface (CLI now, IM adapter later) renders
# the canonical event stream without polling. Both are optional and side-effect-only.
EventObserver = Callable[[Event], None]
DeltaObserver = Callable[[str], None]


class _ObservingStore:
    """Decorate an :class:`EventStore` to notify an observer after each append.

    Reads delegate unchanged; the observer fires only on a *successful* append, so
    a surface sees exactly the durable, seq-assigned events (never a lost write).
    Observation is **best-effort**: the event is already durable when the observer
    runs, so an observer error is swallowed rather than aborting the run.
    """

    def __init__(self, inner: EventStore, observer: EventObserver) -> None:
        self._inner = inner
        self._observer = observer

    async def append(self, event: Event) -> None:
        await self._inner.append(event)
        try:
            self._observer(event)
        except Exception:  # noqa: BLE001 - a live observer must never crash a durable run
            pass

    def read(self, session_id: SessionId, after: int | None = None) -> AsyncIterator[Event]:
        return self._inner.read(session_id, after)


# Library default: the caller (server/surface) supplies a real policy engine.
_ALLOW_ALL = RuleBasedPermissionEngine([Rule("*", PermissionDecision.allow)])


class ToolRegistry:
    """Name -> tool lookup (one tool interface, P3)."""

    def __init__(self, tools: Sequence[Tool] = ()) -> None:
        self._tools: dict[str, Tool] = {tool.name: tool for tool in tools}

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)

    def schemas(self) -> list[dict[str, Any]]:
        """Advertise the tools to the provider (OpenAI/LiteLLM function-call format).

        Without this the model never learns the toolset exists and will refuse to
        call anything; the loop then only ever executes tools a caller forces.
        """
        return [
            {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.input_schema(),
                },
            }
            for tool in self._tools.values()
        ]


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
    error: str | None = None
    usage: Usage = field(default_factory=Usage)
    events: list[Event] = field(default_factory=list)


@dataclass
class _TurnOutput:
    text: str
    tool_calls: list[ToolCall]
    finish_reason: FinishReason | None
    tokens: int
    usage: Usage


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
    agent: AgentSpec, store: EventStore, session_id: SessionId, registry: ToolRegistry
) -> ProviderRequest:
    events = [event async for event in store.read(session_id)]
    return ProviderRequest(
        model=agent.model,
        messages=project_messages(events),
        tools=registry.schemas(),
    )


async def _call_provider(
    provider: ProviderGateway,
    request: ProviderRequest,
    max_retries: int,
    on_delta: DeltaObserver | None = None,
    emit_delta: Callable[[str], Awaitable[None]] | None = None,
) -> _TurnOutput:
    attempt = 0
    while True:
        try:
            text_parts: list[str] = []
            tool_calls: list[ToolCall] = []
            finish: FinishReason | None = None
            usage = Usage()
            async for chunk in provider.stream(request):
                if chunk.usage is not None:
                    usage = chunk.usage
                if chunk.delta:
                    text_parts.append(chunk.delta)
                    if on_delta is not None:
                        try:
                            on_delta(chunk.delta)
                        except Exception:  # noqa: BLE001 - live delta observer is best-effort
                            pass
                    if emit_delta is not None:
                        try:
                            await emit_delta(chunk.delta)  # awaited inline -> ordered
                        except Exception:  # noqa: BLE001 - streaming relay is best-effort
                            pass
                if chunk.tool_call is not None:
                    tool_calls.append(chunk.tool_call)
                if chunk.finish_reason is not None:
                    finish = chunk.finish_reason
            text = "".join(text_parts)
            # Real completion tokens drive the budget when the provider reports them.
            tokens = usage.completion_tokens or len(text)
            return _TurnOutput(
                text=text,
                tool_calls=tool_calls,
                finish_reason=finish,
                tokens=tokens,
                usage=usage,
            )
        except Exception as exc:  # noqa: BLE001 - bounded retry then fail closed
            status = getattr(exc, "status_code", None)
            # Non-transient client errors (bad model, auth, invalid request) won't
            # succeed on retry — fail fast, carrying the provider's own message.
            if isinstance(status, int) and 400 <= status < 500 and status != 429:
                raise KeelError(str(exc)) from exc
            attempt += 1
            if attempt > max_retries:
                raise KeelError(f"provider call failed after retries: {exc}") from exc


async def _run_tools(
    store: EventStore,
    registry: ToolRegistry,
    permissions: PermissionEngine,
    approve: ApproveFn | None,
    session_id: SessionId,
    scope_id: ScopeId,
    run_id: RunId,
    trust: TrustLevel,
    calls: list[ToolCall],
) -> None:
    """Emit tool.call events, run the calls through the parallel-safe permission-gated
    executor, then emit tool.result events — all in source order. A failing tool yields
    a failed result (never crashes the run)."""
    # Content taint accumulated by prior tool results this run (G17): outbound
    # actions gate on it via a ConfusedDeputyEngine.
    prior = [event async for event in store.read(session_id)]
    ctx = ToolContext(
        scope_id=scope_id,
        session_id=session_id,
        trust=trust,
        content_taint=taint_from_events(prior),
    )
    for call in calls:
        await _emit(
            store,
            EventType.tool_call,
            session_id,
            scope_id,
            run_id,
            {"tool": call.name, "call_id": call.id, "args": call.arguments},
        )

    requests: list[ExecRequest] = []
    known: list[bool] = []
    for call in calls:
        tool = registry.get(call.name)
        known.append(tool is not None)
        if tool is not None:
            requests.append(
                ExecRequest(call=call, tool=tool, write=bool(getattr(tool, "writes", True)))
            )

    results = iter(await execute(requests, ctx, permissions, approve))
    for call, is_known in zip(calls, known, strict=True):
        if is_known:
            result = next(results)
            payload: dict[str, object] = {
                "call_id": call.id,
                "ok": result.ok,
                "output": result.output,
                "taint": str(result.taint),
            }
        else:
            payload = {"call_id": call.id, "ok": False, "error": "unknown tool"}
        await _emit(store, EventType.tool_result, session_id, scope_id, run_id, payload)


async def run(
    *,
    agent: AgentSpec,
    session_id: SessionId,
    store: EventStore,
    provider: ProviderGateway,
    registry: ToolRegistry | None = None,
    budget: RunBudget | None = None,
    interrupt: Callable[[], bool] | None = None,
    permissions: PermissionEngine | None = None,
    approve: ApproveFn | None = None,
    on_event: EventObserver | None = None,
    on_delta: DeltaObserver | None = None,
    run_id: RunId | None = None,
    stream_deltas: bool = False,
) -> RunResult:
    """Execute the agent loop until a named termination and return the result.

    ``on_event``/``on_delta`` are optional live-observation seams: ``on_event`` fires
    for every persisted event (in seq order) and ``on_delta`` for each streamed text
    delta, letting a surface render the run live without polling the store. A caller
    may pass ``run_id`` (e.g. a server that returned it to a client before the run
    finished); otherwise one is generated. With ``stream_deltas`` the loop also emits
    partial ``message.token`` events per delta (``payload.partial``) so a store's
    fan-out can relay token-by-token; the whole message is still emitted at turn end.
    """
    registry = registry or ToolRegistry()
    budget = budget or RunBudget(
        max_iterations=agent.max_iterations, token_budget=agent.token_budget
    )
    permissions = permissions or _ALLOW_ALL
    if on_event is not None:
        store = _ObservingStore(store, on_event)
    scope_id = agent.scope.id
    trust = agent.scope.trust
    run_id = run_id or uuid.uuid4().hex

    emit_delta: Callable[[str], Awaitable[None]] | None = None
    if stream_deltas:

        async def emit_delta(text: str) -> None:
            await _emit(
                store,
                EventType.message_token,
                session_id,
                scope_id,
                run_id,
                {"role": "assistant", "text": text, "partial": True},
            )

    await _emit(store, EventType.run_started, session_id, scope_id, run_id, {"agent": agent.id})

    iterations = 0
    tokens = 0
    total_usage = Usage()
    error: str | None = None
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
        request = await _build_request(agent, store, session_id, registry)

        try:
            turn = await _call_provider(provider, request, budget.max_retries, on_delta, emit_delta)
        except KeelError as exc:
            reason = StopReason.error
            error = str(exc)
            await _emit(store, EventType.error, session_id, scope_id, run_id, {"message": error})
            break

        tokens += turn.tokens
        total_usage = total_usage + turn.usage
        if turn.text:
            await _emit(
                store,
                EventType.message_token,
                session_id,
                scope_id,
                run_id,
                {"role": "assistant", "text": turn.text, "usage": turn.usage.model_dump()},
            )

        # Stop-reason gate (I3): tools run ONLY on an explicit tool_use finish.
        if turn.finish_reason == FinishReason.tool_use and turn.tool_calls:
            await _run_tools(
                store,
                registry,
                permissions,
                approve,
                session_id,
                scope_id,
                run_id,
                trust,
                turn.tool_calls,
            )
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

    await _emit(
        store,
        EventType.run_ended,
        session_id,
        scope_id,
        run_id,
        {"reason": str(reason), "usage": total_usage.model_dump()},
    )
    return RunResult(
        run_id=run_id,
        reason=reason,
        iterations=iterations,
        tokens=tokens,
        error=error,
        usage=total_usage,
    )
