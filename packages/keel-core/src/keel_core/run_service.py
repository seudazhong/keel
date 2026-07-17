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
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from keel_core.agents import AgentSpec
from keel_core.approvals import ApprovalStore
from keel_core.loop import ApprovalBinding, ToolRegistry
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
        """Idempotently admit a run: persist the row + user turn, then enqueue a worker job.

        A retried request (same ``(scope, idempotency_key)``) returns the existing run and
        performs **no** side effects — it can never create a duplicate message or run.
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
        if not created:
            return AdmitResult(run_id=record.id, created=False)
        # Persist the user turn *before* any model call (loop invariant I2), then dispatch.
        await self._admit(self._events, session_id, self._scope_id, content)
        await self._runs.mark_queued(record.id, now=now)
        await self._enqueue(record.id)
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
        return AdmitResult(run_id=record.id, created=True)

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
        expected_action_hash: str | None = None,
        expected_run_attempt: int | None = None,
    ) -> bool:
        """Resolve a durable approval and requeue+enqueue its suspended run for resume.

        The decision is bound to the exact action + attempt (fail closed on a stale hash /
        wrong attempt). On success the suspended run is requeued and a worker job enqueued.
        """
        record = await self._approvals.get(approval_id)
        if record is None:
            return False
        status = "granted" if approved else "denied"
        resolved = await self._approvals.resolve(
            approval_id,
            status,
            resolved_by,
            expected_action_hash=expected_action_hash,
            expected_run_attempt=expected_run_attempt,
        )
        if not resolved:
            return False
        # Requeue the suspended run (waiting_approval -> queued) then dispatch a resume job.
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


@dataclass
class _ControlWatcher:
    """Polls durable control between loop awaits: interrupt/cancel flip a flag; steer is
    admitted as a user turn so the next provider request sees it."""

    run_store: RunStore
    event_store: EventStore
    admit_fn: AdmitFn
    run_id: RunId
    session_id: SessionId
    scope_id: ScopeId
    poll_seconds: float = 1.0
    interrupted: bool = False
    cancelled: bool = False
    _task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def _drain(self) -> None:
        for control in await self.run_store.consume_control(self.run_id):
            if control.kind is RunControlKind.interrupt:
                self.interrupted = True
            elif control.kind is RunControlKind.cancel:
                self.cancelled = True
                self.interrupted = True
            elif control.kind is RunControlKind.steer:
                text = str(control.payload.get("text", "")).strip()
                if text:
                    await self.admit_fn(self.event_store, self.session_id, self.scope_id, text)

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
        await self._drain()  # final consume so a late interrupt is not lost


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
    now: datetime | None = None,
) -> RunRecord:
    """Drive a claimed run to a named terminal (or ``waiting_approval``) state.

    Reuses the existing agent loop; on suspend it releases the lease to
    ``waiting_approval`` (a worker resumes once the approval resolves); otherwise it
    terminalizes idempotently. Agent visibility is re-checked here at claim/use time — a
    revocation between admission and claim fails the run closed.
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
    watcher = _ControlWatcher(
        run_store=run_store,
        event_store=event_store,
        admit_fn=admit_fn,
        run_id=lease.run_id,
        session_id=lease.session_id,
        scope_id=lease.scope_id,
        poll_seconds=control_poll_seconds,
    )
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
                interrupt=lambda: watcher.interrupted,
                approvals=approvals,
                expires_at=expires_at,
                on_event=on_event,
                system_context=system_context,
                binding=binding,
            )
    finally:
        await watcher.stop()

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
    return await run_store.terminalize(
        lease,
        status=status,
        stop_reason=result.reason.value,
        now=terminal_now,
        cost=cost,
        error_kind="run_error" if status is RunStatus.failed else None,
        error_message=result.error if status is RunStatus.failed else None,
    )


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
    now: datetime | None = None,
    limit: int = 100,
) -> ReconcileResult:
    """Recover stuck runs: redispatch admitted-but-undispatched, reclaim expired leases,
    and fail-closed expire past-deadline runs. Safe to run on a periodic cron; every action
    is idempotent (a duplicate enqueue is deduped by the claim, terminal writes are no-ops).
    """
    now = now or datetime.now(UTC)

    # 1) admitted-but-undispatched: an admit that crashed before enqueue (or a lost enqueue).
    undispatched = await run_store.undispatched(now, limit)
    for run_id in undispatched:
        if await run_store.mark_queued(run_id, now=now):
            await enqueue(run_id)

    # 2) expired running/waiting leases: make claimable again + re-enqueue for a fresh worker.
    reclaimable = await run_store.reclaimable(now, limit)
    for run_id in reclaimable:
        await enqueue(run_id)

    # 3) past the admission/lease deadline: terminal expired (fail closed).
    expired = await run_store.expire_due(now, limit)

    result = ReconcileResult(
        redispatched=len(undispatched),
        reclaimed=len(reclaimable),
        expired=len(expired),
    )
    if undispatched or reclaimable or expired:
        logger.info(
            "run reconcile scope=? redispatched=%d reclaimed=%d expired=%d",
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
    "reconcile_runs",
]
