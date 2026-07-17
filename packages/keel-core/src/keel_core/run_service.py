"""Durable run admission + worker-owned execution + reconciliation (M3.6, WS-M).

This is the seam that moves interactive execution **out** of the server process and into a
worker that owns a fenced lease. The three collaborators here are surface-agnostic (Web,
IM, API, schedule all reuse them):

* :class:`DurableRunService` — the *admission* side. Persists the run row + user turn
  (exactly once, idempotently), enqueues a worker job, and records durable
  interrupt / cancel / steering requests + approval resolutions. The server never owns the
  run task or an approval future.
* :func:`execute_run` — the *execution* side a worker job body calls: claim the fenced
  lease, re-check Agent visibility at use, drive the reused :func:`keel_core.loop.run` /
  :func:`~keel_core.loop.resume`, honour durable interrupt/steer, suspend on a durable
  approval (release to ``waiting_approval``), and terminalize idempotently. It reuses the
  existing loop, event store, approvals, tracing, and tools — there is no second agent loop.
* :func:`reconcile_runs` — the *recovery* side: re-dispatch admitted-but-undispatched runs,
  reclaim expired running/waiting leases, and fail-closed expire past-deadline runs.

No server-local ``asyncio`` task/future is the source of truth: the durable ``runs`` row is.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from keel_core.agents import AgentSpec
from keel_core.approvals import ApprovalStore
from keel_core.loop import ApprovalBinding, RunBudget, ToolRegistry, admit_run
from keel_core.loop import resume as loop_resume
from keel_core.loop import run as loop_run
from keel_core.protocols import EventStore, PermissionEngine, ProviderGateway
from keel_core.runs import (
    RunBudgetSpec,
    RunControlKind,
    RunCost,
    RunLease,
    RunLeaseLostError,
    RunRecord,
    RunStatus,
    RunStore,
)
from keel_core.types import RunId, ScopeId, SessionId, StopReason

logger = logging.getLogger("keel.run_service")

# admit(...) persists a user/system turn before the first model call (loop invariant I2).
AdmitFn = Callable[[EventStore, SessionId, ScopeId, str], Awaitable[None]]
EnqueueFn = Callable[[RunId], Awaitable[None]]
VisibilityCheck = Callable[[RunRecord], Awaitable[bool]]
SystemContextFn = Callable[[], Awaitable[str]]
PromptPersistedCheck = Callable[[RunRecord], Awaitable[bool]]

_ADMISSION_MARKER = "admission_run"

# StopReason -> terminal RunStatus. ``suspended`` is handled separately (release, not
# terminalize); ``interrupted`` maps to interrupted unless a cancel control was consumed.
_TERMINAL_FOR: dict[StopReason, RunStatus] = {
    StopReason.completed: RunStatus.completed,
    StopReason.max_iterations: RunStatus.completed,
    StopReason.budget_exhausted: RunStatus.completed,
    StopReason.halted: RunStatus.completed,
    StopReason.interrupted: RunStatus.interrupted,
    StopReason.error: RunStatus.failed,
}


@dataclass(frozen=True)
class AdmitResult:
    """The outcome of admitting (or idempotently re-observing) a run."""

    run_id: RunId
    created: bool


async def prompt_persisted_in_log(
    event_store: EventStore, session_id: SessionId, run_id: RunId
) -> bool:
    """Whether the durable admission user turn for ``run_id`` is already in the event log.

    The admission event carries an ``admission_run`` marker (see
    :func:`keel_core.loop.admit_run`), so this is the authoritative, crash-safe idempotency
    check: a repair/retry never appends a duplicate prompt, and reconciliation never
    dispatches a run whose prompt was not durably admitted (invariant I2)."""
    async for event in event_store.read(session_id):
        if event.payload.get(_ADMISSION_MARKER) == run_id:
            return True
    return False


class DurableRunService:
    """Admission + control surface over a durable :class:`RunStore` (worker executes)."""

    def __init__(
        self,
        *,
        run_store: RunStore,
        event_store: EventStore,
        approvals: ApprovalStore,
        scope_id: ScopeId,
        enqueue: EnqueueFn,
        admit_fn: AdmitFn,
        default_ttl_seconds: int = 24 * 3600,
    ) -> None:
        self._runs = run_store
        self._events = event_store
        self._approvals = approvals
        self._scope_id = scope_id
        self._enqueue = enqueue
        self._admit = admit_fn
        self._default_ttl_seconds = default_ttl_seconds

    @property
    def scope_id(self) -> ScopeId:
        return self._scope_id

    async def admit(
        self,
        *,
        org_id: str,
        actor: str,
        agent_id: str,
        session_id: SessionId,
        surface: str,
        content: str,
        idempotency_key: str,
        budget: RunBudgetSpec | None = None,
        ttl_seconds: int | None = None,
        run_id: RunId | None = None,
        now: datetime | None = None,
    ) -> AdmitResult:
        """Idempotently admit a run and complete admission with crash-safe repair.

        Admission has four durable steps — create the run row, persist the user turn,
        transition ``admitted -> queued``, and enqueue a worker job. Each is applied
        idempotently here so a crash (or a retried request with the same
        ``(scope, idempotency_key)``) **repairs** a half-admitted row and completes exactly
        the missing steps — it never creates a duplicate run/message and never leaves a
        prompt-less run behind. The prompt is always persisted *before* the queued/dispatch
        transition (invariant I2).
        """
        now = now or datetime.now(UTC)
        run_id = run_id or uuid.uuid4().hex
        expires_at = now + timedelta(seconds=ttl_seconds or self._default_ttl_seconds)
        record, created = await self._runs.create(
            run_id=run_id,
            scope_id=self._scope_id,
            org_id=org_id,
            actor=actor,
            agent_id=agent_id,
            session_id=session_id,
            surface=surface,
            idempotency_key=idempotency_key,
            budget=budget or RunBudgetSpec(),
            expires_at=expires_at,
            now=now,
        )
        # A run that already reached (or passed) queued/owned/terminal is fully admitted:
        # the prompt is persisted and dispatch progressed, so a retry is an idempotent no-op
        # (a lost enqueue after the queue transition is repaired by the reconciler, not here).
        if record.status is not RunStatus.admitted:
            return AdmitResult(run_id=record.id, created=created)

        # Fresh admission, or crash-repair of a half-admitted row: complete the missing steps
        # idempotently, always persisting the prompt *before* the queued/dispatch transition.
        # 1) Persist the user turn *before* any dispatch (invariant I2), exactly once.
        await self._ensure_prompt(record.id, session_id, content)
        # 2) admitted -> queued (idempotent no-op once queued).
        await self._runs.mark_queued(record.id, now=now)
        # 3) Dispatch a worker job (a duplicate enqueue is deduped by the claim).
        await self._enqueue(record.id)
        if created:
            logger.info(
                "run admitted scope=%s run=%s org=%s actor=%s agent=%s session=%s surface=%s",
                self._scope_id,
                record.id,
                org_id,
                actor,
                agent_id,
                session_id,
                surface,
            )
        return AdmitResult(run_id=record.id, created=created)

    async def _ensure_prompt(self, run_id: RunId, session_id: SessionId, content: str) -> None:
        """Append the admission user turn exactly once (crash-safe via the log marker)."""
        if await prompt_persisted_in_log(self._events, session_id, run_id):
            await self._runs.mark_prompt_persisted(run_id)
            return
        await admit_run(self._events, session_id, self._scope_id, content, run_id)
        await self._runs.mark_prompt_persisted(run_id)

    async def interrupt(self, run_id: RunId, *, requested_by: str) -> bool:
        return await self._runs.request_control(
            run_id, kind=RunControlKind.interrupt, requested_by=requested_by
        )

    async def cancel(self, run_id: RunId, *, requested_by: str) -> bool:
        return await self._runs.request_control(
            run_id, kind=RunControlKind.cancel, requested_by=requested_by
        )

    async def steer(self, run_id: RunId, *, requested_by: str, text: str) -> bool:
        return await self._runs.request_control(
            run_id,
            kind=RunControlKind.steer,
            requested_by=requested_by,
            payload={"text": text},
        )

    async def resolve_approval(
        self,
        approval_id: str,
        *,
        approved: bool,
        resolved_by: str,
        org_id: str | None = None,
        expected_action_hash: str | None = None,
        expected_run_attempt: int | None = None,
    ) -> bool:
        """Resolve a durable interactive approval, fully bound, and resume its run.

        Binding is **never optional** on the durable path: the decision is checked against
        the run's org (cross-org denial), the run must currently be ``waiting_approval``
        (state check), and the approval row's ``action_hash`` + ``run_attempt`` must match
        (defaulting to the record's own values so a caller that omits them still gets the
        binding). An expired approval fails closed (``resolve`` no-ops on a non-pending row).
        On success the suspended run transitions ``waiting_approval -> queued`` with an
        explicit resume marker and a ``run_interactive`` job is enqueued.
        """
        record = await self._approvals.get(approval_id)
        if record is None:
            return False
        run = await self._runs.get(record.run_id)
        if run is None:
            # Not a durable interactive run (e.g. a legacy scheduled approval): not ours.
            return False
        if org_id is not None and run.org_id != org_id:
            return False  # cross-org resolution denied (fail closed)
        if run.status is not RunStatus.waiting_approval:
            return False  # only resolve a genuinely suspended run
        status = "granted" if approved else "denied"
        resolved = await self._approvals.resolve(
            approval_id,
            status,
            resolved_by,
            expected_action_hash=(
                expected_action_hash if expected_action_hash is not None else record.action_hash
            ),
            expected_run_attempt=(
                expected_run_attempt if expected_run_attempt is not None else record.run_attempt
            ),
        )
        if not resolved:
            return False
        # Requeue (waiting_approval -> queued, resume marked) then dispatch a resume job.
        if await self._runs.requeue(record.run_id):
            await self._enqueue(record.run_id)
        logger.info(
            "approval resolved scope=%s run=%s approval=%s decision=%s by=%s",
            self._scope_id,
            record.run_id,
            approval_id,
            status,
            resolved_by,
        )
        return True

    async def expire_approvals(self, *, now: datetime | None = None) -> int:
        """Expire past-deadline approvals and resume their suspended runs (fail closed).

        A timed-out approval is a denial: the run must resume so the loop records the
        denied tool result and drives to a named terminal state, rather than hanging in
        ``waiting_approval`` forever. Returns the number of runs requeued for resume."""
        now = now or datetime.now(UTC)
        expired = await self._approvals.expire_due(now)
        resumed = 0
        for approval_id in expired:
            record = await self._approvals.get(approval_id)
            if record is None:
                continue
            run = await self._runs.get(record.run_id)
            if run is None or run.status is not RunStatus.waiting_approval:
                continue
            if await self._runs.requeue(record.run_id):
                await self._enqueue(record.run_id)
                resumed += 1
        return resumed


@dataclass
class _ControlWatcher:
    """Polls durable control between loop awaits with **claim/ack** semantics (M3.6).

    Steering is admitted as a durable user turn and only then acked (never lost, never
    double-applied). Interrupt/cancel set a fail-closed flag but are **not** acked while the
    run executes: if the worker dies before the run reaches a terminal state reflecting the
    signal, the control stays pending so a reclaiming worker re-honors it. The owning
    execution acks the applied interrupt/cancel signals only after it durably terminalizes.
    """

    run_store: RunStore
    event_store: EventStore
    admit_fn: AdmitFn
    run_id: RunId
    session_id: SessionId
    scope_id: ScopeId
    poll_seconds: float = 1.0
    interrupted: bool = False
    cancelled: bool = False
    _signal_ids: list[str] = field(default_factory=list)
    _task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def _drain(self) -> None:
        for control in await self.run_store.peek_control(self.run_id):
            if control.kind is RunControlKind.interrupt:
                self.interrupted = True
                self._signal_ids.append(control.id)
            elif control.kind is RunControlKind.cancel:
                self.cancelled = True
                self.interrupted = True
                self._signal_ids.append(control.id)
            elif control.kind is RunControlKind.steer:
                text = str(control.payload.get("text", "")).strip()
                if text:
                    await self.admit_fn(self.event_store, self.session_id, self.scope_id, text)
                # Ack steer only AFTER the durable user turn is appended (never before).
                await self.run_store.ack_controls([control.id])

    async def _loop(self) -> None:
        try:
            while True:
                await self._drain()
                await asyncio.sleep(self.poll_seconds)
        except asyncio.CancelledError:
            raise

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._drain()  # final consume so a late steer is not lost

    async def ack_signals(self, now: datetime | None = None) -> None:
        """Ack the interrupt/cancel signals this watcher honored (call after terminalize)."""
        ids = list(dict.fromkeys(self._signal_ids))
        if ids:
            await self.run_store.ack_controls(ids, now=now)


@dataclass
class _LeaseKeeper:
    """Renews a fenced lease well before expiry; flags the lease **lost** on failure.

    A lost renewal (another worker reclaimed, or the row moved out from under us) means the
    lease is stale: ``lost`` trips the loop's interrupt predicate so no further model call,
    tool batch, event, or terminal write proceeds under the superseded lease (fail closed).
    """

    run_store: RunStore
    lease: RunLease
    interval_seconds: float
    lost: bool = False
    _task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.interval_seconds)
                renewed = await self.run_store.renew(
                    self.lease, lease_seconds=self.lease.lease_seconds
                )
                if not renewed:
                    self.lost = True
                    return
        except asyncio.CancelledError:
            raise

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


def _budget_for(lease: RunLease) -> RunBudget:
    """Authoritative loop budget from the persisted run caps (never a worker default).

    ``token_budget`` is decremented by usage already recorded on the run so the cap bounds
    the *whole* run across claim/resume attempts, not each invocation."""
    token_budget = lease.token_budget
    if token_budget is not None:
        used = lease.prompt_tokens + lease.completion_tokens
        token_budget = max(0, token_budget - used)
    return RunBudget(max_iterations=lease.max_iterations, token_budget=token_budget)


async def execute_run(
    *,
    lease: RunLease,
    run_store: RunStore,
    event_store: EventStore,
    approvals: ApprovalStore,
    agent: AgentSpec,
    provider: ProviderGateway,
    registry: ToolRegistry,
    permissions: PermissionEngine,
    admit_fn: AdmitFn,
    approval_ttl_hours: float = 24.0,
    resume: bool = False,
    visibility_check: VisibilityCheck | None = None,
    on_event: Callable[[object], None] | None = None,
    system_context: SystemContextFn | None = None,
    control_poll_seconds: float = 1.0,
    heartbeat_seconds: float | None = None,
    now: datetime | None = None,
) -> RunRecord:
    """Drive a claimed run to a named terminal (or ``waiting_approval``) state.

    Reuses the existing agent loop; a lease keeper renews the fenced lease well before
    expiry and, on a lost renewal, fences the run so no further effect or write proceeds
    (the reclaiming worker owns it). On suspend it releases the lease to
    ``waiting_approval``; otherwise it terminalizes idempotently. Agent visibility is
    re-checked at claim/use time. The loop budget is the authoritative persisted run budget.
    """
    now = now or datetime.now(UTC)
    record = await run_store.get(lease.run_id)
    if record is None:
        raise RunLeaseLostError(lease.run_id)

    # Re-authorize the selected Agent at use time (revocation between admit and claim).
    if visibility_check is not None and not await visibility_check(record):
        return await run_store.terminalize(
            lease,
            status=RunStatus.failed,
            stop_reason="forbidden",
            error_kind="agent_forbidden",
            error_message="selected Agent is not visible/permitted at claim time",
            now=now,
        )

    binding = ApprovalBinding(org_id=record.org_id, actor=record.actor, run_attempt=lease.attempt)
    expires_at = now + timedelta(hours=approval_ttl_hours)
    interval = heartbeat_seconds or max(1.0, lease.lease_seconds / 3.0)
    keeper = _LeaseKeeper(run_store=run_store, lease=lease, interval_seconds=interval)
    watcher = _ControlWatcher(
        run_store=run_store,
        event_store=event_store,
        admit_fn=admit_fn,
        run_id=lease.run_id,
        session_id=lease.session_id,
        scope_id=lease.scope_id,
        poll_seconds=control_poll_seconds,
    )

    def interrupted() -> bool:
        # A lost lease is a hard stop (fail closed) alongside a durable interrupt/cancel.
        return watcher.interrupted or keeper.lost

    budget = _budget_for(lease)
    keeper.start()
    watcher.start()
    try:
        if resume:
            result = await loop_resume(
                agent=agent,
                session_id=lease.session_id,
                run_id=lease.run_id,
                store=event_store,
                provider=provider,
                registry=registry,
                permissions=permissions,
                approvals=approvals,
                budget=budget,
                interrupt=interrupted,
                on_event=on_event,
                expires_at=expires_at,
                system_context=system_context,
                binding=binding,
            )
        else:
            result = await loop_run(
                agent=agent,
                session_id=lease.session_id,
                store=event_store,
                provider=provider,
                registry=registry,
                permissions=permissions,
                run_id=lease.run_id,
                budget=budget,
                interrupt=interrupted,
                approvals=approvals,
                expires_at=expires_at,
                on_event=on_event,
                system_context=system_context,
                binding=binding,
            )
    finally:
        await watcher.stop()
        await keeper.stop()

    # Fenced out mid-run: the lease is stale, so we must NOT write a terminal state (the
    # reclaiming owner drives it). Leave control signals pending for the fresh owner.
    if keeper.lost:
        current = await run_store.get(lease.run_id)
        return current if current is not None else record

    cost = RunCost(
        prompt_tokens=result.usage.prompt_tokens,
        completion_tokens=result.usage.completion_tokens,
    )
    terminal_now = datetime.now(UTC)

    if result.reason is StopReason.suspended:
        # A durable approval is pending: release the lease and wait to be resumed.
        return await run_store.release(
            lease, to_status=RunStatus.waiting_approval, now=terminal_now, cost=cost
        )

    status = _TERMINAL_FOR.get(result.reason, RunStatus.completed)
    if watcher.cancelled:
        status = RunStatus.cancelled
    final = await run_store.terminalize(
        lease,
        status=status,
        stop_reason=result.reason.value,
        now=terminal_now,
        cost=cost,
        error_kind="run_error" if status is RunStatus.failed else None,
        error_message=result.error if status is RunStatus.failed else None,
    )
    # Only now that a durable terminal state reflects the interrupt/cancel do we ack the
    # signals — a crash before this point leaves them pending for a reclaiming worker.
    await watcher.ack_signals(now=terminal_now)
    return final


@dataclass(frozen=True)
class ReconcileResult:
    """Counts from one reconciliation pass (observability)."""

    redispatched: int = 0
    reclaimed: int = 0
    expired: int = 0


async def reconcile_runs(
    *,
    run_store: RunStore,
    enqueue: EnqueueFn,
    prompt_persisted: PromptPersistedCheck | None = None,
    now: datetime | None = None,
    limit: int = 100,
    grace_seconds: int = 30,
) -> ReconcileResult:
    """Recover stuck runs: redispatch admitted/queued-but-undispatched, reclaim expired
    leases, and fail-closed expire past-deadline runs. Safe on a periodic cron; every action
    is idempotent (a duplicate enqueue is deduped by the claim, terminal writes are no-ops).

    A run is **never** dispatched unless its prompt is durably persisted (invariant I2): the
    optional ``prompt_persisted`` check gates redispatch, so a crash between run creation and
    prompt admission can never surface a prompt-less run to a worker. Reclaim targets only
    leases that have actually expired — a live, heartbeating run is never disturbed.
    """
    now = now or datetime.now(UTC)

    # 1) admitted/queued but not owned and past a small grace: an admit/enqueue that crashed
    #    (or a lost enqueue). Complete admission exactly once — but only for prompted runs.
    redispatchable = await run_store.redispatchable(now, limit, grace_seconds=grace_seconds)
    redispatched = 0
    for run_id in redispatchable:
        record = await run_store.get(run_id)
        if record is None:
            continue
        if prompt_persisted is not None and not await prompt_persisted(record):
            continue  # never dispatch a prompt-less run
        if record.status is RunStatus.admitted:
            await run_store.mark_queued(run_id, now=now)
        await enqueue(run_id)
        redispatched += 1

    # 2) expired running/waiting leases: make claimable again + re-enqueue for a fresh worker.
    reclaimable = await run_store.reclaimable(now, limit)
    for run_id in reclaimable:
        await enqueue(run_id)

    # 3) past the admission/lease deadline: terminal expired (fail closed).
    expired = await run_store.expire_due(now, limit)

    result = ReconcileResult(
        redispatched=redispatched,
        reclaimed=len(reclaimable),
        expired=len(expired),
    )
    if redispatched or reclaimable or expired:
        logger.info(
            "run reconcile redispatched=%d reclaimed=%d expired=%d",
            result.redispatched,
            result.reclaimed,
            result.expired,
        )
    return result


__all__ = [
    "AdmitResult",
    "DurableRunService",
    "ReconcileResult",
    "execute_run",
    "prompt_persisted_in_log",
    "reconcile_runs",
]
