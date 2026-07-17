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
from keel_core.approvals import ApprovalRecord, ApprovalStore
from keel_core.errors import DuplicateEventError
from keel_core.loop import (
    ApprovalBinding,
    RunBudget,
    ToolRegistry,
    admit_run,
    steer_persisted_in_log,
)
from keel_core.loop import admit_steer as loop_admit_steer
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
    action_hash,
)
from keel_core.types import RunId, ScopeId, SessionId, StopReason

logger = logging.getLogger("keel.run_service")

# admit(...) persists a user/system turn before the first model call (loop invariant I2).
AdmitFn = Callable[[EventStore, SessionId, ScopeId, str], Awaitable[None]]
EnqueueFn = Callable[[RunId], Awaitable[None]]
VisibilityCheck = Callable[[RunRecord], Awaitable[bool]]
SystemContextFn = Callable[[], Awaitable[str]]
PromptPersistedCheck = Callable[[RunRecord], Awaitable[bool]]
# Routes an expired *legacy* (non-durable-run) approval to its own resume path — the durable
# reconciler is the single owner of approval expiry, so it must also dispatch legacy resumes.
LegacyResumeFn = Callable[[ApprovalRecord], Awaitable[None]]

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
        # 1) Persist the user turn *before* any dispatch (invariant I2), exactly once — a
        #    concurrent/duplicate admitter loses the durable-append race (DuplicateEventError)
        #    and observes the winner's turn rather than appending a second prompt.
        await self._ensure_prompt(record.id, session_id, content)
        # 2) admitted -> queued is an atomic, single-winner transition; only the caller that
        #    wins it dispatches, so N concurrent admitters enqueue the worker job exactly once
        #    (a lost enqueue after this point is repaired by the reconciler, not re-sent here).
        queued = await self._runs.mark_queued(record.id, now=now)
        if queued:
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
        """Append the admission user turn exactly once (crash- and concurrency-safe).

        The ``dedup_key`` partial-unique index makes the append atomic across processes: two
        concurrent admitters cannot both persist the prompt — the loser raises
        :class:`~keel_core.errors.DuplicateEventError` and simply observes the winner's turn.
        A crash between run creation and this append is repaired by a later retry (the marker
        is absent, so the retry appends and wins)."""
        if await prompt_persisted_in_log(self._events, session_id, run_id):
            await self._runs.mark_prompt_persisted(run_id)
            return
        try:
            await admit_run(self._events, session_id, self._scope_id, content, run_id)
        except DuplicateEventError:
            pass  # a concurrent admitter won the durable append; observe, never duplicate
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
        actor: str | None = None,
    ) -> bool:
        """Resolve a durable interactive approval, fully bound to the **current** run.

        Every binding field is verified against the run row as it exists *now* — never
        defaulted from the approval's own copies (which an attacker/stale replay controls):

        * the approval's ``run_id`` must resolve to a durable run (else it is not ours);
        * the run must currently be ``waiting_approval`` (state gate);
        * the resolver's authenticated ``org_id`` (when supplied) must equal the run's org
          (cross-org denial), and the approval's stored ``org_id`` must equal the run's org;
        * the approval's ``actor`` must match the run's owner, and when the resolver's
          ``actor`` is supplied it must match too (no impersonation);
        * the approval's ``run_attempt`` must equal the run's **current** ``attempt`` — an
          attempt-0 approval can never resume an attempt-1 (reclaimed/advanced) run;
        * the ``action_hash`` is **recomputed** from the approval's stored exact tool + args
          and must equal the stored hash (tamper/replay defense).

        Only if all bindings hold does the transactional ``resolve`` fire (with the verified
        hash + current attempt as CAS guards), the run transition ``waiting_approval ->
        queued`` with a resume marker, and a ``run_interactive`` resume job enqueue.
        """
        record = await self._approvals.get(approval_id)
        if record is None:
            return False
        run = await self._runs.get(record.run_id)
        if run is None:
            # Not a durable interactive run (e.g. a legacy scheduled approval): not ours.
            return False
        if run.status is not RunStatus.waiting_approval:
            return False  # only resolve a genuinely suspended run
        # Bind to the CURRENT run, not the approval's own (possibly stale) copies.
        if record.org_id != run.org_id:
            return False  # approval was bound to a different org than the run now has
        if org_id is not None and run.org_id != org_id:
            return False  # cross-org resolution denied (fail closed)
        if record.actor != run.actor:
            return False  # approval was bound to a different actor than the run owner
        if actor is not None and record.actor != actor:
            return False  # resolver identity does not match the bound actor
        if record.run_attempt != run.attempt:
            return False  # attempt-0 approval cannot resume an attempt-1 run (fail closed)
        # Recompute the action hash from the stored exact action payload (never trust the
        # stored hash blindly): a mismatch means the row was tampered with -> fail closed.
        if record.action_hash != action_hash(record.tool, record.args):
            return False
        status = "granted" if approved else "denied"
        resolved = await self._approvals.resolve(
            approval_id,
            status,
            resolved_by,
            expected_action_hash=record.action_hash,
            expected_run_attempt=run.attempt,
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

    async def expire_approvals(
        self, *, now: datetime | None = None, legacy_resume: LegacyResumeFn | None = None
    ) -> int:
        """Expire past-deadline approvals and route each to the correct resume path.

        This is the **single owner** of durable-approval expiry (M3.6, item 6): the legacy
        scheduler no longer expires approvals, so both interactive and legacy approvals are
        classified + routed here transactionally. ``expire_due`` mutates each pending row to
        ``expired`` exactly once (RETURNING), so even under simultaneous reconciler ticks no
        approval is consumed twice. A durable **interactive** approval (its ``run_id`` is a
        durable run currently ``waiting_approval``) requeues the run and enqueues a
        ``run_interactive`` resume; a **legacy** approval (no durable run row) is dispatched
        via ``legacy_resume`` — never through the durable run state machine, and never left
        hanging. Returns the number of runs resumed."""
        now = now or datetime.now(UTC)
        expired = await self._approvals.expire_due(now)
        resumed = 0
        for approval_id in expired:
            record = await self._approvals.get(approval_id)
            if record is None:
                continue
            run = await self._runs.get(record.run_id)
            if run is None:
                # Legacy scheduled/digest approval: route to its own (non-durable) resume.
                if legacy_resume is not None:
                    await legacy_resume(record)
                    resumed += 1
                continue
            if run.status is not RunStatus.waiting_approval:
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
                    # Persist the steering turn keyed by the control id. If a prior owner
                    # already appended it (crash after append, before ack), the durable
                    # marker makes this a no-op instead of a duplicate steering message.
                    if not await steer_persisted_in_log(
                        self.event_store, self.session_id, control.id
                    ):
                        try:
                            await loop_admit_steer(
                                self.event_store,
                                self.session_id,
                                self.scope_id,
                                text,
                                self.run_id,
                                control.id,
                            )
                        except DuplicateEventError:
                            pass  # a racing watcher won the append; the turn is durable
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

    A lost renewal — a ``False`` return (another worker reclaimed, or the row moved out from
    under us) **or any exception** from ``renew`` — means the lease is stale: ``lost`` trips
    the loop's interrupt predicate so no further model call, tool batch, event, or terminal
    write proceeds under the superseded lease (fail closed). The keeper is supervised for the
    whole run; a failure inside it is never allowed to mask the run's primary outcome/error.
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
                try:
                    renewed = await self.run_store.renew(
                        self.lease, lease_seconds=self.lease.lease_seconds
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - any renew failure is a lost lease (fail closed)
                    logger.warning(
                        "lease renewal raised; marking lease lost run=%s",
                        self.lease.run_id,
                        exc_info=True,
                    )
                    self.lost = True
                    return
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
            except Exception:  # noqa: BLE001 - keeper cleanup must not mask the primary error
                logger.warning("lease keeper task error suppressed on stop", exc_info=True)


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
                start_iteration=lease.iterations_used,
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
                start_iteration=lease.iterations_used,
            )
    finally:
        await watcher.stop()
        await keeper.stop()

    # Fenced out mid-run: the lease is stale, so we must NOT write a terminal state (the
    # reclaiming owner drives it). Leave control signals pending for the fresh owner.
    if keeper.lost:
        current = await run_store.get(lease.run_id)
        return current if current is not None else record

    terminal_now = datetime.now(UTC)

    if result.reason is StopReason.suspended:
        # A durable approval is pending: release the lease and wait to be resumed. The
        # suspended tool batch executes on resume, so it consumes an iteration — persist
        # ``+1`` here so ``max_iterations`` bounds the run across the suspend/resume boundary
        # (a resumed run cannot mint a fresh iteration budget). Provider-reported cost is
        # propagated so cumulative cost_usd survives the suspension.
        suspend_cost = RunCost(
            prompt_tokens=result.usage.prompt_tokens,
            completion_tokens=result.usage.completion_tokens,
            cost_usd=result.usage.cost_usd,
            iterations=max(0, (result.iterations + 1) - lease.iterations_used),
        )
        return await run_store.release(
            lease, to_status=RunStatus.waiting_approval, now=terminal_now, cost=suspend_cost
        )

    cost = RunCost(
        prompt_tokens=result.usage.prompt_tokens,
        completion_tokens=result.usage.completion_tokens,
        cost_usd=result.usage.cost_usd,
        iterations=max(0, result.iterations - lease.iterations_used),
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
