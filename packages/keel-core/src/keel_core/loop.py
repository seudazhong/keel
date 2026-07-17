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
from keel_core.approvals import ApprovalStore
from keel_core.connectors import taint_from_events
from keel_core.errors import KeelError
from keel_core.events import Event, EventType
from keel_core.evolution import current_event_version
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
    ToolResult,
    Usage,
)
from keel_core.runs import action_hash as _action_hash
from keel_core.tools.executor import ApproveFn, ExecRequest, execute
from keel_core.types import (
    ContentTaint,
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


def _tool_result_payload(call_id: str, result: ToolResult) -> dict[str, object]:
    return {
        "call_id": call_id,
        "ok": result.ok,
        "output": result.output,
        "taint": str(result.taint),
        "citations": [citation.model_dump() for citation in result.citations],
    }


# Live-observation seams: an in-process surface (CLI now, IM adapter later) renders
# the canonical event stream without polling. Both are optional and side-effect-only.
EventObserver = Callable[[Event], None]
DeltaObserver = Callable[[str], None]
SystemContextFn = Callable[[], Awaitable[str]]


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
class ApprovalBinding:
    """Binds durable approvals raised by a run to its org/actor/attempt (M3.6).

    Carried into :func:`_run_tools` so each pending approval records the exact org, actor,
    run attempt, and an ``action_hash`` of (tool, args). A cross-surface decision is then
    verifiable against the exact action + attempt, so a stale/replayed approval cannot be
    reused (see :func:`keel_core.runs.action_hash`)."""

    org_id: str = ""
    actor: str = ""
    run_attempt: int = 0


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
    pending_approvals: list[str] = field(default_factory=list)


@dataclass
class _TurnOutput:
    text: str
    tool_calls: list[ToolCall]
    finish_reason: FinishReason | None
    tokens: int
    usage: Usage


@dataclass
class _LoopOutcome:
    reason: StopReason
    error: str | None
    iterations: int
    tokens: int
    usage: Usage
    pending_approvals: list[str] = field(default_factory=list)


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
            version=current_event_version(event_type),
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


async def admit_run(
    store: EventStore, session_id: SessionId, scope_id: ScopeId, content: str, run_id: RunId
) -> None:
    """Persist a durable run's user turn, tagged with an admission marker for ``run_id``.

    The ``admission_run`` payload marker makes admission **idempotent**: a repair/retry can
    detect an already-persisted prompt (see the durable run service) and never append a
    duplicate user turn or dispatch a prompt-less run (invariant I2, M3.6). The ``dedup_key``
    is enforced by a partial-unique index on ``events`` so two concurrent admitters (or a
    retried request across processes) can never both append the prompt — the loser raises
    :class:`~keel_core.errors.DuplicateEventError` and observes the winner's turn."""
    await _emit(
        store,
        EventType.message_token,
        session_id,
        scope_id,
        run_id,
        {
            "role": "user",
            "text": content,
            "admission_run": run_id,
            "dedup_key": f"admit:{run_id}",
        },
    )


async def admit_steer(
    store: EventStore,
    session_id: SessionId,
    scope_id: ScopeId,
    content: str,
    run_id: RunId,
    control_id: str,
) -> None:
    """Persist a steering user turn, uniquely keyed by its durable control id (M3.6).

    Steering is admitted as a durable user turn before its control row is acked. A crash
    after the append but before the ack leaves the control pending, so a reclaiming worker
    re-drains it — the ``steer_control`` marker + ``dedup_key`` partial-unique index make
    that replay a no-op (:class:`~keel_core.errors.DuplicateEventError`) instead of a
    duplicate steering message."""
    await _emit(
        store,
        EventType.message_token,
        session_id,
        scope_id,
        run_id,
        {
            "role": "user",
            "text": content,
            "steer_control": control_id,
            "dedup_key": f"steer:{control_id}",
        },
    )


async def steer_persisted_in_log(store: EventStore, session_id: SessionId, control_id: str) -> bool:
    """Whether the durable steering turn for ``control_id`` is already in the event log."""
    async for event in store.read(session_id):
        if event.payload.get("steer_control") == control_id:
            return True
    return False


async def admit_system(
    store: EventStore, session_id: SessionId, scope_id: ScopeId, content: str
) -> None:
    """Durably persist a system/standing instruction that starts an unattended run.

    Unlike :func:`admit` (a user turn), this seeds the run with a developer-authored
    instruction — the scheduled digest's "morning triage" prompt — with no human
    present. Persisted before any model call (invariant I2)."""
    await _emit(
        store,
        EventType.message_token,
        session_id,
        scope_id,
        None,
        {"role": "system", "text": content},
    )


async def _build_request(
    agent: AgentSpec,
    store: EventStore,
    session_id: SessionId,
    registry: ToolRegistry,
    system_context: SystemContextFn | None = None,
) -> ProviderRequest:
    events = [event async for event in store.read(session_id)]
    messages = project_messages(events)
    if system_context is not None:
        text = await system_context()
        if text:
            messages.insert(0, {"role": "system", "content": text})
    return ProviderRequest(model=agent.model, messages=messages, tools=registry.schemas())


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
    approvals: ApprovalStore | None = None,
    expires_at: datetime | None = None,
    binding: ApprovalBinding | None = None,
) -> list[str]:
    """Emit tool.call events, run the calls through the parallel-safe permission-gated
    executor, then emit tool.result events — all in source order. A failing tool yields
    a failed result (never crashes the run).

    Durable mode: when ``approvals`` is provided and any call is gated to ``ask``, the
    batch is **suspended** instead of executed — a pending approval is created per ask
    call and the created approval ids are returned (no tool.result yet). The run then
    resumes via :func:`resume` once the approvals resolve. Returns ``[]`` otherwise."""
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

    if approvals is not None:
        asks = [
            call
            for call in calls
            if permissions.evaluate(call.name, call.arguments, ctx) is PermissionDecision.ask
        ]
        if asks:
            reason = "tainted" if ctx.content_taint is ContentTaint.tainted else "first_use"
            # Every approval raised by this one suspended batch shares a batch_id, so the run
            # resumes only once *all* of them are terminal (M3.6 blocker 5).
            batch_id = uuid.uuid4().hex
            created: list[str] = []
            for call in asks:
                key = str(call.arguments.get("idempotency_key") or uuid.uuid4().hex)
                approval_id = await approvals.create_pending(
                    scope_id=scope_id,
                    run_id=run_id,
                    session_id=session_id,
                    tool=call.name,
                    args=call.arguments,
                    call_id=call.id,
                    idempotency_key=key,
                    reason=reason,
                    expires_at=expires_at or _now(),
                    org_id=binding.org_id if binding else "",
                    actor=binding.actor if binding else "",
                    action_hash=_action_hash(call.name, call.arguments),
                    run_attempt=binding.run_attempt if binding else 0,
                    batch_id=batch_id,
                )
                await _emit(
                    store,
                    EventType.approval_requested,
                    session_id,
                    scope_id,
                    run_id,
                    {
                        "approval_id": approval_id,
                        "tool": call.name,
                        "args": call.arguments,
                        "call_id": call.id,
                    },
                )
                created.append(approval_id)
            return created  # SUSPEND: no execute(), no tool.result

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
            payload = _tool_result_payload(call.id, result)
        else:
            payload = {
                "call_id": call.id,
                "ok": False,
                "error": "unknown tool",
                "citations": [],
            }
        await _emit(store, EventType.tool_result, session_id, scope_id, run_id, payload)
    return []


async def _agent_loop(
    *,
    agent: AgentSpec,
    session_id: SessionId,
    store: EventStore,
    provider: ProviderGateway,
    registry: ToolRegistry,
    budget: RunBudget,
    interrupt: Callable[[], bool] | None,
    permissions: PermissionEngine,
    approve: ApproveFn | None,
    run_id: RunId,
    emit_delta: Callable[[str], Awaitable[None]] | None,
    on_delta: DeltaObserver | None,
    approvals: ApprovalStore | None = None,
    expires_at: datetime | None = None,
    start_iteration: int = 0,
    system_context: SystemContextFn | None = None,
    binding: ApprovalBinding | None = None,
) -> _LoopOutcome:
    """The turn loop: build request -> call provider -> (gate) run tools -> repeat.

    Extracted from :func:`run` so :func:`resume` can re-enter it after resolving a
    suspended tool batch. Returns the outcome; the caller emits run.started / run.ended.
    """
    scope_id = agent.scope.id
    trust = agent.scope.trust
    iterations = start_iteration
    tokens = 0
    total_usage = Usage()
    error: str | None = None
    reason = StopReason.completed
    pending: list[str] = []

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
        request = await _build_request(agent, store, session_id, registry, system_context)

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
            # Pre-effect fence (M3.6): re-check interrupt/cancel/lease-loss *after* the
            # provider returned and *immediately before* dispatching the tool batch. A
            # durable cancel or a lost lease that arrived during the (possibly long)
            # provider call must prevent every subsequent external tool effect — otherwise
            # a fenced-out/cancelled run could still act on the world for one more batch.
            if interrupt is not None and interrupt():
                reason = StopReason.interrupted
                break
            suspended = await _run_tools(
                store,
                registry,
                permissions,
                approve,
                session_id,
                scope_id,
                run_id,
                trust,
                turn.tool_calls,
                approvals,
                expires_at,
                binding,
            )
            if suspended:
                pending = suspended
                reason = StopReason.suspended
                break
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

    return _LoopOutcome(reason, error, iterations, tokens, total_usage, pending)


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
    approvals: ApprovalStore | None = None,
    expires_at: datetime | None = None,
    system_context: SystemContextFn | None = None,
    binding: ApprovalBinding | None = None,
    start_iteration: int = 0,
) -> RunResult:
    """Execute the agent loop until a named termination and return the result.

    ``on_event``/``on_delta`` are optional live-observation seams: ``on_event`` fires
    for every persisted event (in seq order) and ``on_delta`` for each streamed text
    delta, letting a surface render the run live without polling the store. A caller
    may pass ``run_id`` (e.g. a server that returned it to a client before the run
    finished); otherwise one is generated. With ``stream_deltas`` the loop also emits
    partial ``message.token`` events per delta (``payload.partial``) so a store's
    fan-out can relay token-by-token; the whole message is still emitted at turn end.

    ``start_iteration`` seeds the loop's iteration counter from the run's persisted
    cumulative count so ``max_iterations`` bounds the *whole* run across suspend/resume
    attempts (a resumed run cannot mint a fresh iteration budget)."""
    registry = registry or ToolRegistry()
    budget = budget or RunBudget(
        max_iterations=agent.max_iterations, token_budget=agent.token_budget
    )
    permissions = permissions or _ALLOW_ALL
    if on_event is not None:
        store = _ObservingStore(store, on_event)
    scope_id = agent.scope.id
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

    outcome = await _agent_loop(
        agent=agent,
        session_id=session_id,
        store=store,
        provider=provider,
        registry=registry,
        budget=budget,
        interrupt=interrupt,
        permissions=permissions,
        approve=approve,
        run_id=run_id,
        emit_delta=emit_delta,
        on_delta=on_delta,
        approvals=approvals,
        expires_at=expires_at,
        system_context=system_context,
        binding=binding,
        start_iteration=start_iteration,
    )

    if outcome.reason is StopReason.suspended:
        await _emit(
            store,
            EventType.run_suspended,
            session_id,
            scope_id,
            run_id,
            {"approval_ids": outcome.pending_approvals},
        )
    else:
        await _emit(
            store,
            EventType.run_ended,
            session_id,
            scope_id,
            run_id,
            {"reason": str(outcome.reason), "usage": outcome.usage.model_dump()},
        )
    return RunResult(
        run_id=run_id,
        reason=outcome.reason,
        iterations=outcome.iterations,
        tokens=outcome.tokens,
        error=outcome.error,
        usage=outcome.usage,
        pending_approvals=outcome.pending_approvals,
    )


async def _suspended_calls(
    store: EventStore, session_id: SessionId, run_id: RunId
) -> list[ToolCall]:
    """The tool.call events for **this run** that have no matching tool.result (the batch a
    suspended run stopped at).

    Filtered by ``event.run_id`` so a run never inspects, executes, or denies another run's
    suspended calls when a session is shared (M3.6 blocker 2). Only events belonging to
    ``run_id`` are considered, even if two runs in the session raised overlapping call ids."""
    calls: dict[str, ToolCall] = {}
    resulted: set[str] = set()
    async for event in store.read(session_id):
        if event.run_id != run_id:
            continue
        if event.type is EventType.tool_call:
            cid = str(event.payload["call_id"])
            calls[cid] = ToolCall(
                id=cid,
                name=str(event.payload["tool"]),
                arguments=dict(event.payload.get("args", {})),
            )
        elif event.type is EventType.tool_result:
            resulted.add(str(event.payload.get("call_id")))
    return [call for cid, call in calls.items() if cid not in resulted]


async def resume(
    *,
    agent: AgentSpec,
    session_id: SessionId,
    run_id: RunId,
    store: EventStore,
    provider: ProviderGateway,
    registry: ToolRegistry,
    permissions: PermissionEngine,
    approvals: ApprovalStore,
    budget: RunBudget | None = None,
    interrupt: Callable[[], bool] | None = None,
    on_event: EventObserver | None = None,
    on_delta: DeltaObserver | None = None,
    stream_deltas: bool = False,
    expires_at: datetime | None = None,
    system_context: SystemContextFn | None = None,
    binding: ApprovalBinding | None = None,
    start_iteration: int = 0,
) -> RunResult:
    """Resume a suspended run: resolve its pending tool batch, then continue the loop.

    The event log is the checkpoint (no separate store). The suspended calls are the
    tool.call events with no matching tool.result; each granted (or auto-allowed) call
    executes idempotently, each denied/expired call gets a failed result. Only after the
    thread is complete does :func:`_agent_loop` call the provider again — so the model's
    continuation never depends on non-deterministically reproducing the same tool call.

    ``interrupt`` fences the resume against a lost lease / durable cancel: if it trips
    before a pending call is replayed, the (possibly external) tool is **not** executed and
    the batch is left unresolved for a fresh owner to replay — no effect proceeds under a
    stale lease."""
    budget = budget or RunBudget(
        max_iterations=agent.max_iterations, token_budget=agent.token_budget
    )
    if on_event is not None:
        store = _ObservingStore(store, on_event)
    scope_id = agent.scope.id
    trust = agent.scope.trust

    await _emit(store, EventType.run_resumed, session_id, scope_id, run_id, {})

    prior = [event async for event in store.read(session_id)]
    ctx = ToolContext(
        scope_id=scope_id,
        session_id=session_id,
        trust=trust,
        content_taint=taint_from_events(prior),
    )
    # Approval id per call, restricted to THIS run's approval.requested events — never adopt
    # another run's approval decision when a session is shared (M3.6 blocker 2).
    approval_of: dict[str, str] = {
        str(event.payload["call_id"]): str(event.payload["approval_id"])
        for event in prior
        if event.type is EventType.approval_requested and event.run_id == run_id
    }

    suspended_calls = await _suspended_calls(store, session_id, run_id)
    # A run must not resume while any approval in its suspended batch is still pending — a
    # pending decision may never be *implicitly denied* (M3.6 blocker 5). Re-suspend cleanly
    # so the still-pending approvals are preserved for a later, complete resolution.
    still_pending = [
        approval_of[call.id]
        for call in suspended_calls
        if call.id in approval_of
        and (rec := await approvals.get(approval_of[call.id])) is not None
        and rec.status == "pending"
    ]
    if still_pending:
        await _emit(
            store,
            EventType.run_suspended,
            session_id,
            scope_id,
            run_id,
            {"approval_ids": still_pending},
        )
        return RunResult(
            run_id=run_id, reason=StopReason.suspended, pending_approvals=still_pending
        )

    for call in suspended_calls:
        # Fail closed under a lost lease / durable cancel: never run an external effect and
        # never write a result the fresh owner would double-apply. Leave the batch pending.
        if interrupt is not None and interrupt():
            return RunResult(run_id=run_id, reason=StopReason.interrupted)
        decision = permissions.evaluate(call.name, call.arguments, ctx)
        granted = decision is PermissionDecision.allow
        if decision is PermissionDecision.ask and call.id in approval_of:
            # Preserve the *exact* durable decision: only an explicitly granted approval runs;
            # a denied/expired one is refused (never executed).
            record = await approvals.get(approval_of[call.id])
            granted = record is not None and record.status == "granted"
        if granted:
            tool = registry.get(call.name)
            result = (
                await tool.run(call.arguments, ctx)
                if tool is not None
                else ToolResult(ok=False, output="unknown tool")
            )
            payload = _tool_result_payload(call.id, result)
        else:
            payload = {
                "call_id": call.id,
                "ok": False,
                "output": "approval denied",
                "citations": [],
            }
        await _emit(store, EventType.tool_result, session_id, scope_id, run_id, payload)

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

    outcome = await _agent_loop(
        agent=agent,
        session_id=session_id,
        store=store,
        provider=provider,
        registry=registry,
        budget=budget,
        interrupt=interrupt,
        permissions=permissions,
        approve=None,
        run_id=run_id,
        emit_delta=emit_delta,
        on_delta=on_delta,
        approvals=approvals,
        expires_at=expires_at,
        system_context=system_context,
        binding=binding,
        start_iteration=start_iteration,
    )

    if outcome.reason is StopReason.suspended:
        await _emit(
            store,
            EventType.run_suspended,
            session_id,
            scope_id,
            run_id,
            {"approval_ids": outcome.pending_approvals},
        )
    else:
        await _emit(
            store,
            EventType.run_ended,
            session_id,
            scope_id,
            run_id,
            {"reason": str(outcome.reason), "usage": outcome.usage.model_dump()},
        )
    return RunResult(
        run_id=run_id,
        reason=outcome.reason,
        iterations=outcome.iterations,
        tokens=outcome.tokens,
        error=outcome.error,
        usage=outcome.usage,
        pending_approvals=outcome.pending_approvals,
    )
