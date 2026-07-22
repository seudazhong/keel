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
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy import text as _sa_text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.agents import AgentSpec
from keel_core.approvals import (
    ApprovalRecord,
    ApprovalStore,
    insert_pending_in_transaction,
)
from keel_core.connectors import taint_from_events
from keel_core.errors import KeelError
from keel_core.events import Event, EventType
from keel_core.evolution import current_event_version
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
from keel_core.state import append_event_in_transaction
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


_SET_SCOPE = _sa_text("SELECT set_config('app.scope_id', :scope, true)")


def _tool_result_payload(call_id: str, result: ToolResult) -> dict[str, object]:
    payload: dict[str, object] = {
        "call_id": call_id,
        "ok": result.ok,
        "output": result.output,
        "taint": str(result.taint),
        "citations": [citation.model_dump() for citation in result.citations],
    }
    # R1B: surface the durable Effect ledger identity/status/provider reference an
    # outbound connector action reserved — never a secret, only ids/enum values already
    # safe to persist on the Effect row itself (keel_core.effects.EffectRecord).
    if result.effect_id is not None:
        payload["effect_id"] = result.effect_id
    if result.effect_status is not None:
        payload["effect_status"] = result.effect_status
    if result.provider_ref is not None:
        payload["provider_ref"] = result.provider_ref
    return payload


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


async def admit_external(
    store: EventStore,
    session_id: SessionId,
    scope_id: ScopeId,
    content: str,
    idempotency_key: str,
) -> None:
    """Durably admit tainted external input exactly once."""
    await _emit(
        store,
        EventType.message_token,
        session_id,
        scope_id,
        None,
        {
            "role": "user",
            "text": content,
            "taint": str(ContentTaint.tainted),
            "dedup_key": idempotency_key,
        },
    )


async def admit_run(
    store: EventStore,
    session_id: SessionId,
    scope_id: ScopeId,
    content: str,
    run_id: RunId,
    *,
    model: str | None = None,
    extra: Mapping[str, object] | None = None,
) -> None:
    """Persist a durable run's user turn, tagged with an admission marker for ``run_id``.

    The ``admission_run`` payload marker makes admission **idempotent**: a repair/retry can
    detect an already-persisted prompt (see the durable run service) and never append a
    duplicate user turn or dispatch a prompt-less run (invariant I2, M3.6). The ``dedup_key``
    is enforced by a partial-unique index on ``events`` so two concurrent admitters (or a
    retried request across processes) can never both append the prompt — the loser raises
    :class:`~keel_core.errors.DuplicateEventError` and observes the winner's turn.

    ``admission_model`` records the model selected at admission (no schema migration: it lives
    in the event payload) so the worker executes the run with the admitted model rather than
    its own process default (reproducibility). ``extra`` carries additional surface metadata
    bound to the admission (e.g. the IM provider/chat context) under its own payload keys so a
    worker can rebuild the surface-specific Agent + reply target from the durable log."""
    payload: dict[str, object] = {
        "role": "user",
        "text": content,
        "admission_run": run_id,
        "dedup_key": f"admit:{run_id}",
    }
    if model:
        payload["admission_model"] = model
    if extra:
        payload.update(extra)
    await _emit(
        store,
        EventType.message_token,
        session_id,
        scope_id,
        run_id,
        payload,
    )


async def admission_model_in_log(
    store: EventStore, session_id: SessionId, run_id: RunId
) -> str | None:
    """The model recorded on ``run_id``'s durable admission turn, if any (M3.6 item 5).

    The worker reads the admitted model from the event log so it executes with the model the
    request selected at admission — not the worker's own process default. Returns ``None`` for
    an older admission that predates model capture (the worker then falls back to its
    default)."""
    async for event in store.read(session_id):
        if event.payload.get("admission_run") == run_id:
            model = event.payload.get("admission_model")
            return model if isinstance(model, str) and model else None
    return None


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


def _unwrap_store(store: EventStore) -> tuple[EventStore, EventObserver | None]:
    """Peel a live-observation decorator so the durable store (and its engine) is reachable."""
    if isinstance(store, _ObservingStore):
        return store._inner, store._observer
    return store, None


def _shared_suspension_engine(store: EventStore, approvals: ApprovalStore) -> AsyncEngine | None:
    """The Postgres engine shared by the event store *and* the approval store, or ``None``.

    When both are Postgres-backed by the **same** engine, a suspended tool batch persists its
    ``tool.call`` events, its approval rows, and its ``approval.requested`` events in a single
    transaction (all-or-nothing), so a crash can never leave an approval row without its events
    (or a partially-raised batch). Otherwise (in-memory / mixed) the sequential single-process
    path applies each write in turn — there is no crash boundary in one process anyway."""
    engine = getattr(store, "_engine", None)
    ap_engine = getattr(approvals, "_engine", None)
    if isinstance(engine, AsyncEngine) and engine is ap_engine:
        return engine
    return None


def _tool_call_event(
    session_id: SessionId, scope_id: ScopeId, run_id: RunId, call: ToolCall
) -> Event:
    return Event(
        type=EventType.tool_call,
        version=current_event_version(EventType.tool_call),
        seq=0,
        session_id=session_id,
        scope_id=scope_id,
        run_id=run_id,
        ts=_now(),
        payload={"tool": call.name, "call_id": call.id, "args": call.arguments},
    )


def _approval_requested_event(
    session_id: SessionId, scope_id: ScopeId, run_id: RunId, call: ToolCall, approval_id: str
) -> Event:
    return Event(
        type=EventType.approval_requested,
        version=current_event_version(EventType.approval_requested),
        seq=0,
        session_id=session_id,
        scope_id=scope_id,
        run_id=run_id,
        ts=_now(),
        payload={
            "approval_id": approval_id,
            "tool": call.name,
            "args": call.arguments,
            "call_id": call.id,
        },
    )


def _notify(observer: EventObserver | None, event: Event) -> None:
    if observer is None:
        return
    try:
        observer(event)
    except Exception:  # noqa: BLE001 - a live observer must never crash a durable run
        pass


async def persist_suspension_events_and_rows(
    conn: AsyncConnection,
    *,
    calls: Sequence[ToolCall],
    asks: Sequence[ToolCall],
    session_id: SessionId,
    scope_id: ScopeId,
    run_id: RunId,
    reason: str,
    expires_at: datetime,
    binding: ApprovalBinding | None,
    batch_id: str,
) -> tuple[list[str], list[Event]]:
    """Persist a suspended batch's events + approval rows inside the caller's transaction.

    Writes a ``tool.call`` event for **every** call, a pending approval row per ask, and each
    ask's ``approval.requested`` event — all on the caller's ``conn`` (no commit here). Returns
    ``(approval_ids in ask order, events to fire on the live observer post-commit)``. The
    caller owns the transaction + ``app.scope_id`` GUC, so a run checkpoint (or any other
    fenced write) can join the *same* transaction and commit/roll back atomically with it."""
    org_id = binding.org_id if binding else ""
    actor = binding.actor if binding else ""
    run_attempt = binding.run_attempt if binding else 0
    keys = {
        call.id: str(call.arguments.get("idempotency_key") or uuid.uuid4().hex) for call in asks
    }
    approval_ids = {call.id: uuid.uuid4().hex for call in asks}
    notify: list[Event] = []
    for call in calls:
        event = _tool_call_event(session_id, scope_id, run_id, call)
        event.seq = await append_event_in_transaction(conn, event)
        notify.append(event)
    for call in asks:
        await insert_pending_in_transaction(
            conn,
            id=approval_ids[call.id],
            scope_id=scope_id,
            run_id=run_id,
            session_id=session_id,
            tool=call.name,
            args=call.arguments,
            call_id=call.id,
            idempotency_key=keys[call.id],
            reason=reason,
            expires_at=expires_at,
            org_id=org_id,
            actor=actor,
            action_hash=_action_hash(call.name, call.arguments),
            run_attempt=run_attempt,
            batch_id=batch_id,
        )
        event = _approval_requested_event(session_id, scope_id, run_id, call, approval_ids[call.id])
        event.seq = await append_event_in_transaction(conn, event)
        notify.append(event)
    return [approval_ids[call.id] for call in asks], notify


CheckpointInTx = Callable[[AsyncConnection], Awaitable[None]]


async def persist_suspension_batch_in_engine(
    engine: AsyncEngine,
    store: EventStore,
    approvals: ApprovalStore,
    *,
    calls: Sequence[ToolCall],
    asks: Sequence[ToolCall],
    session_id: SessionId,
    scope_id: ScopeId,
    run_id: RunId,
    reason: str,
    expires_at: datetime,
    binding: ApprovalBinding | None,
    batch_id: str,
    checkpoint_in_tx: CheckpointInTx | None = None,
) -> list[str]:
    """Persist a suspended batch (and optionally a run checkpoint) in ONE Postgres transaction.

    Opens a single transaction on the shared ``engine``, runs the optional ``checkpoint_in_tx``
    callback first (a durable run injects its fenced ``runs`` checkpoint UPDATE here — the loop
    itself stays free of any concrete run-store SQL), then writes the batch's ``tool.call``
    events, approval rows, and ``approval.requested`` events. The whole unit commits or rolls
    back together: a crash (or a lease-lost ``RunLeaseLostError`` from the checkpoint) before
    commit leaves no checkpoint, no rows, and no events. The best-effort live observer fires
    only post-commit, in seq order."""
    inner, observer = _unwrap_store(store)
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        if checkpoint_in_tx is not None:
            await checkpoint_in_tx(conn)
        approval_ids, notify = await persist_suspension_events_and_rows(
            conn,
            calls=calls,
            asks=asks,
            session_id=session_id,
            scope_id=scope_id,
            run_id=run_id,
            reason=reason,
            expires_at=expires_at,
            binding=binding,
            batch_id=batch_id,
        )
    # The batch is durable; fire the (best-effort) live observer post-commit, in order.
    for event in notify:
        _notify(observer, event)
    return approval_ids


async def _persist_suspension_batch(
    store: EventStore,
    approvals: ApprovalStore,
    *,
    calls: Sequence[ToolCall],
    asks: Sequence[ToolCall],
    session_id: SessionId,
    scope_id: ScopeId,
    run_id: RunId,
    reason: str,
    expires_at: datetime,
    binding: ApprovalBinding | None,
    batch_id: str,
) -> list[str]:
    """Durably persist a suspended tool batch as one unit and return the created approval ids.

    The unit is: a ``tool.call`` event for **every** call in the batch, a pending approval row
    per ask call, and each ask's ``approval.requested`` event. When the event store and the
    approval store share a Postgres engine the whole unit commits in a **single transaction**,
    so a crash can never leave an approval row without its events, nor a partially-raised batch
    — the exact gap that let resume silently deny a granted approval. Otherwise (in-memory /
    mixed engines, one process, no crash boundary) it falls back to the sequential writes with
    identical observable effects.

    This is the **default** persister (no run checkpoint). A durable run injects a
    checkpointing :data:`SuspensionPersister` so the checkpoint joins the same transaction."""
    inner, _observer = _unwrap_store(store)
    engine = _shared_suspension_engine(inner, approvals)

    if engine is not None:
        return await persist_suspension_batch_in_engine(
            engine,
            store,
            approvals,
            calls=calls,
            asks=asks,
            session_id=session_id,
            scope_id=scope_id,
            run_id=run_id,
            reason=reason,
            expires_at=expires_at,
            binding=binding,
            batch_id=batch_id,
        )

    return await _persist_suspension_batch_sequential(
        store,
        approvals,
        calls=calls,
        asks=asks,
        session_id=session_id,
        scope_id=scope_id,
        run_id=run_id,
        reason=reason,
        expires_at=expires_at,
        binding=binding,
        batch_id=batch_id,
    )


async def _persist_suspension_batch_sequential(
    store: EventStore,
    approvals: ApprovalStore,
    *,
    calls: Sequence[ToolCall],
    asks: Sequence[ToolCall],
    session_id: SessionId,
    scope_id: ScopeId,
    run_id: RunId,
    reason: str,
    expires_at: datetime,
    binding: ApprovalBinding | None,
    batch_id: str,
) -> list[str]:
    """Sequential (single-process) fallback: identical writes, no cross-store transaction.

    Used when the event + approval stores do not share a Postgres engine (in-memory / mixed).
    A checkpointing :data:`SuspensionPersister` wraps this call in a snapshot/rollback so a
    partial batch never survives an injected failure in that single-process path."""
    org_id = binding.org_id if binding else ""
    actor = binding.actor if binding else ""
    run_attempt = binding.run_attempt if binding else 0
    keys = {
        call.id: str(call.arguments.get("idempotency_key") or uuid.uuid4().hex) for call in asks
    }
    for call in calls:
        await _emit(
            store,
            EventType.tool_call,
            session_id,
            scope_id,
            run_id,
            {"tool": call.name, "call_id": call.id, "args": call.arguments},
        )
    created: list[str] = []
    for call in asks:
        approval_id = await approvals.create_pending(
            scope_id=scope_id,
            run_id=run_id,
            session_id=session_id,
            tool=call.name,
            args=call.arguments,
            call_id=call.id,
            idempotency_key=keys[call.id],
            reason=reason,
            expires_at=expires_at,
            org_id=org_id,
            actor=actor,
            action_hash=_action_hash(call.name, call.arguments),
            run_attempt=run_attempt,
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
    return created


class SuspensionPersister(Protocol):
    """Persists a suspended tool batch (its run checkpoint + rows + events) as one atomic unit.

    A durable run injects this from :func:`keel_core.run_service.execute_run` so the run's
    fenced suspension checkpoint (source attempt + batch id) commits in the SAME transaction as
    the batch's approval rows and its ``tool.call`` / ``approval.requested`` events (Postgres
    shared-engine path), or applies atomically via snapshot/rollback (in-memory single-process
    path). It returns the created approval ids in ask order. Raising rolls the whole unit back,
    so the run stays cleanly ``running`` with no partial checkpoint/batch and a reclaim restarts
    fresh; on commit the reclaim always observes a complete batch bound to the checkpoint."""

    async def __call__(
        self,
        *,
        store: EventStore,
        approvals: ApprovalStore,
        calls: Sequence[ToolCall],
        asks: Sequence[ToolCall],
        session_id: SessionId,
        scope_id: ScopeId,
        run_id: RunId,
        reason: str,
        expires_at: datetime,
        binding: ApprovalBinding | None,
        batch_id: str,
    ) -> list[str]: ...


async def _default_suspension_persister(
    *,
    store: EventStore,
    approvals: ApprovalStore,
    calls: Sequence[ToolCall],
    asks: Sequence[ToolCall],
    session_id: SessionId,
    scope_id: ScopeId,
    run_id: RunId,
    reason: str,
    expires_at: datetime,
    binding: ApprovalBinding | None,
    batch_id: str,
) -> list[str]:
    """Default (no-checkpoint) persister for a standalone loop with no durable run row."""
    return await _persist_suspension_batch(
        store,
        approvals,
        calls=calls,
        asks=asks,
        session_id=session_id,
        scope_id=scope_id,
        run_id=run_id,
        reason=reason,
        expires_at=expires_at,
        binding=binding,
        batch_id=batch_id,
    )


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
    suspension_persister: SuspensionPersister | None = None,
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
        run_id=run_id,
        org_id=binding.org_id if binding is not None else "",
        actor_id=binding.actor if binding is not None else "",
    )

    # Decide suspension BEFORE any write: an ask-gated batch persists its tool.call events,
    # approval rows, and approval.requested events as one durable unit (so a crash cannot
    # split an approval row from its events), while a non-suspending batch emits tool.call
    # events then executes.
    asks: list[ToolCall] = []
    if approvals is not None:
        asks = [
            call
            for call in calls
            if permissions.evaluate(call.name, call.arguments, ctx) is PermissionDecision.ask
        ]

    if approvals is not None and asks:
        reason = "tainted" if ctx.content_taint is ContentTaint.tainted else "first_use"
        # Every approval raised by this one suspended batch shares a batch_id, so the run
        # resumes only once *all* of them are terminal (M3.6 blocker 5).
        batch_id = uuid.uuid4().hex
        # Durable suspension (M3.6 crash boundary): an injected persister writes the run's
        # fenced checkpoint (source attempt + this batch_id) in the SAME transaction as the
        # batch's approval rows + events — never a separate commit before it — so a crash
        # either rolls the whole unit back (reclaim restarts fresh) or leaves a complete
        # durable batch bound to the checkpoint (reclaim resumes). Without a persister
        # (standalone loop, no run row) the default persists the batch atomically, sans
        # checkpoint.
        persist = suspension_persister or _default_suspension_persister
        return await persist(
            store=store,
            approvals=approvals,
            calls=calls,
            asks=asks,
            session_id=session_id,
            scope_id=scope_id,
            run_id=run_id,
            reason=reason,
            expires_at=expires_at or _now(),
            binding=binding,
            batch_id=batch_id,
        )  # SUSPEND: no execute(), no tool.result

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
    suspension_persister: SuspensionPersister | None = None,
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
                suspension_persister,
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
    permissions: PermissionEngine,
    registry: ToolRegistry | None = None,
    budget: RunBudget | None = None,
    interrupt: Callable[[], bool] | None = None,
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
    suspension_persister: SuspensionPersister | None = None,
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
        suspension_persister=suspension_persister,
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


async def _reconstruct_missing_approvals(
    approvals: ApprovalStore,
    approval_of: dict[str, str],
    suspended_calls: Sequence[ToolCall],
    *,
    session_id: SessionId,
    run_id: RunId,
    expected_attempt: int | None,
    expected_batch_id: str | None,
) -> list[tuple[ToolCall, str]]:
    """Map suspended calls to durable approval rows when the ``approval.requested`` event was
    lost to a crash before it committed. Mutates ``approval_of`` in place and returns the
    ``(call, approval_id)`` pairs that were reconstructed (so the caller can back-fill the
    missing event for audit consistency).

    The durable approval row is the authoritative record of the decision + its exact bound
    action, so reconstruction is safe **only** on an exact identity match. Every field must
    agree with the run's suspension checkpoint: the row's ``run_id`` (the scan is run-scoped)
    and ``session_id``, the ``call_id`` (keyed), the recomputed ``action_hash``, and — when the
    checkpoint recorded them — the exact source ``run_attempt`` (``expected_attempt``) and
    ``batch_id`` (``expected_batch_id``). A foreign/older/newer attempt or batch, a mismatched
    session/call/hash, or an ambiguous (>1) candidate is rejected — never adopted — so a
    stale/duplicate/injected row cannot hijack a call (fail closed). When the checkpoint did
    not persist a batch id (``expected_batch_id`` is ``None`` — a legitimate older-build
    checkpoint) the batch constraint is **not** relaxed to a wildcard: only a row whose own
    ``batch_id`` is equally empty (a true old-build row, from before batch ids existed) may be
    adopted. A row that carries a real (non-empty) batch id is always a *foreign* batch to a
    batch-less checkpoint — never adopted, fail closed — so an empty legacy checkpoint can never
    be tricked into adopting another (possibly unrelated) batch's approval. Every other field
    (including source attempt when known) is still enforced exactly."""
    missing = [call for call in suspended_calls if call.id not in approval_of]
    if not missing:
        return []
    rows = await approvals.list_for_run(run_id)
    by_call: dict[str, list[ApprovalRecord]] = {}
    for row in rows:
        if row.run_id != run_id or row.session_id != session_id:
            continue  # run/session guard (never adopt a foreign run's or session's row)
        if expected_attempt is not None and row.run_attempt != expected_attempt:
            continue  # foreign/older/newer source attempt -> fail closed
        if expected_batch_id is None:
            if row.batch_id:
                continue  # legacy/batch-less checkpoint: a row with a real batch is foreign
                # to it -> never wildcard-adopted, fail closed (only an equally batch-less
                # true old-build row may be adopted below)
        elif row.batch_id != expected_batch_id:
            continue  # foreign/older/newer batch -> fail closed
        by_call.setdefault(row.call_id, []).append(row)
    reconstructed: list[tuple[ToolCall, str]] = []
    for call in missing:
        expected_hash = _action_hash(call.name, call.arguments)
        candidates = [row for row in by_call.get(call.id, []) if row.action_hash == expected_hash]
        if len(candidates) != 1:
            continue  # 0 == nothing durable to honour; >1 == ambiguous -> fail closed
        approval_id = candidates[0].id
        approval_of[call.id] = approval_id
        reconstructed.append((call, approval_id))
    return reconstructed


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
    suspension_persister: SuspensionPersister | None = None,
    reconstruct_attempt: int | None = None,
    reconstruct_batch_id: str | None = None,
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
        run_id=run_id,
        org_id=binding.org_id if binding is not None else "",
        actor_id=binding.actor if binding is not None else "",
    )
    # Approval id per call, restricted to THIS run's approval.requested events — never adopt
    # another run's approval decision when a session is shared (M3.6 blocker 2).
    approval_of: dict[str, str] = {
        str(event.payload["call_id"]): str(event.payload["approval_id"])
        for event in prior
        if event.type is EventType.approval_requested and event.run_id == run_id
    }

    suspended_calls = await _suspended_calls(store, session_id, run_id)

    # Reconstruction repair (M3.6 approval-event atomicity): if a crash under an older build
    # committed an approval row but lost its ``approval.requested`` event, the event-derived
    # map above misses that call and resume would *silently deny* a granted approval. The
    # durable approval row carries the complete immutable action payload, so we rebuild the
    # call -> approval association from the rows for this run and back-fill the missing event
    # (audit repair). Adoption is exact: the row must belong to this run (scan is run-scoped)
    # and session, its ``action_hash`` must equal the suspended call's, and — when the run's
    # suspension checkpoint recorded them — its source ``run_attempt`` and ``batch_id`` must
    # match the checkpoint exactly. A foreign/older/newer attempt or batch, or an
    # ambiguous/duplicate/injected candidate, is rejected (never adopted), fail closed.
    if approvals is not None:
        repaired = await _reconstruct_missing_approvals(
            approvals,
            approval_of,
            suspended_calls,
            session_id=session_id,
            run_id=run_id,
            expected_attempt=reconstruct_attempt,
            expected_batch_id=reconstruct_batch_id,
        )
        for call, approval_id in repaired:
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
        call_ctx = ctx.model_copy(update={"tool_call_id": call.id})
        decision = permissions.evaluate(call.name, call.arguments, call_ctx)
        granted = decision is PermissionDecision.allow
        if decision is PermissionDecision.ask and call.id in approval_of:
            # Preserve the *exact* durable decision: only an explicitly granted approval runs;
            # a denied/expired one is refused (never executed).
            record = await approvals.get(approval_of[call.id])
            granted = record is not None and record.status == "granted"
        if granted:
            tool = registry.get(call.name)
            result = (
                await tool.run(call.arguments, call_ctx)
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
        suspension_persister=suspension_persister,
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
