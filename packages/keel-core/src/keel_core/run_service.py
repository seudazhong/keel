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
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.agents import AgentSpec
from keel_core.approvals import ApprovalRecord, ApprovalStore
from keel_core.errors import DuplicateEventError
from keel_core.loop import (
    ApprovalBinding,
    RunBudget,
    SuspensionPersister,
    ToolRegistry,
    _persist_suspension_batch,
    _unwrap_store,
    admit_run,
    persist_suspension_batch_in_engine,
    steer_persisted_in_log,
)
from keel_core.loop import admit_steer as loop_admit_steer
from keel_core.loop import resume as loop_resume
from keel_core.loop import run as loop_run
from keel_core.protocols import EventStore, PermissionEngine, ProviderGateway, ToolCall
from keel_core.run_dispatch import RunDispatchOutbox
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
    admission_fingerprint,
    legacy_admission_fingerprint,
    mark_checkpoint_in_transaction,
)
from keel_core.types import RunId, ScopeId, SessionId, StopReason

logger = logging.getLogger("keel.run_service")

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")

# admit(...) persists a user/system turn before the first model call (loop invariant I2).
AdmitFn = Callable[[EventStore, SessionId, ScopeId, str], Awaitable[None]]
EnqueueFn = Callable[[RunId], Awaitable[None]]
VisibilityCheck = Callable[[RunRecord], Awaitable[bool]]
SystemContextFn = Callable[[], Awaitable[str]]
PromptPersistedCheck = Callable[[RunRecord], Awaitable[bool]]
# Routes an expired *legacy* (non-durable-run) approval to its own resume path — the durable
# reconciler is the single owner of approval expiry, so it must also dispatch legacy resumes.
LegacyResumeFn = Callable[[ApprovalRecord], Awaitable[None]]
# An intentional, documented delegation policy: whether ``actor`` may resolve an approval it
# does not own. Returns True to authorize a delegate (e.g. an org admin). Default: no
# delegation — only the bound run/approval actor resolves (cross-user resolution denied).
DelegatePolicy = Callable[[str, RunRecord, ApprovalRecord], Awaitable[bool]]

_ADMISSION_MARKER = "admission_run"


@dataclass(frozen=True)
class _ResolveTx:
    """Outcome of the atomic approval-resolve + (batch-gated) run-requeue transaction."""

    applied: bool  # this call moved the approval pending -> terminal
    current_status: str | None  # the approval's status after the transaction
    batch_terminal: bool  # no approvals in the run's batch remain pending
    requeued: bool  # the run transitioned waiting_approval -> queued in this transaction


def _shared_engine(run_store: RunStore, approvals: ApprovalStore) -> AsyncEngine | None:
    """The Postgres engine shared by both stores, or ``None`` (in-memory / mixed).

    When both stores are Postgres-backed by the *same* engine, the approval resolution and
    the run requeue can commit in a single transaction (blocker 1 atomicity). Otherwise the
    in-memory single-process path applies each step sequentially (no crash boundary)."""
    engine = getattr(run_store, "_engine", None)
    ap_engine = getattr(approvals, "_engine", None)
    if isinstance(engine, AsyncEngine) and engine is ap_engine:
        return engine
    return None


async def _pg_resolve_and_requeue(
    engine: AsyncEngine,
    scope_id: ScopeId,
    *,
    approval_id: str,
    run_id: RunId,
    batch_id: str,
    status: str,
    resolved_by: str,
    expected_action_hash: str,
    expected_run_attempt: int,
) -> _ResolveTx:
    """Resolve the approval, check batch terminality, and requeue the run — one transaction.

    All three writes commit atomically: a crash cannot leave a resolved approval with a run
    stuck in ``waiting_approval``. The run's ``queued`` + ``resume_requested`` is the durable
    dispatch-outbox marker; the reconciler re-enqueues it if the post-commit dispatch dies.
    """
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        applied = (
            await conn.execute(
                text(
                    "UPDATE approvals SET status = :status, resolved_at = now(), "
                    "resolved_by = :by WHERE scope_id = :scope AND id = :id "
                    "AND status = 'pending' AND action_hash = :hash AND run_attempt = :attempt "
                    "RETURNING id"
                ),
                {
                    "status": status,
                    "by": resolved_by,
                    "scope": scope_id,
                    "id": approval_id,
                    "hash": expected_action_hash,
                    "attempt": expected_run_attempt,
                },
            )
        ).rowcount == 1
        current = (
            await conn.execute(
                text("SELECT status FROM approvals WHERE scope_id = :scope AND id = :id"),
                {"scope": scope_id, "id": approval_id},
            )
        ).scalar_one_or_none()
        if batch_id:
            pending = (
                await conn.execute(
                    text(
                        "SELECT count(*) FROM approvals WHERE scope_id = :scope AND run_id = :run "
                        "AND batch_id = :batch AND status = 'pending'"
                    ),
                    {"scope": scope_id, "run": run_id, "batch": batch_id},
                )
            ).scalar_one()
            batch_terminal = int(pending) == 0
        else:
            # Legacy single-approval suspension (no batch): terminal once this one resolves.
            batch_terminal = current is not None and current != "pending"
        requeued = False
        if batch_terminal:
            requeued = (
                await conn.execute(
                    text(
                        "UPDATE runs SET status = 'queued', resume_requested = true, "
                        "version = version + 1, updated_at = now() WHERE scope_id = :scope "
                        "AND id = :run AND status = 'waiting_approval' RETURNING id"
                    ),
                    {"scope": scope_id, "run": run_id},
                )
            ).rowcount == 1
    return _ResolveTx(applied, current, batch_terminal, requeued)


async def _mem_resolve_and_requeue(
    run_store: RunStore,
    approvals: ApprovalStore,
    *,
    approval_id: str,
    run_id: RunId,
    batch_id: str,
    status: str,
    resolved_by: str,
    expected_action_hash: str,
    expected_run_attempt: int,
) -> _ResolveTx:
    """Sequential (single-process) equivalent of :func:`_pg_resolve_and_requeue`."""
    applied = await approvals.resolve(
        approval_id,
        status,
        resolved_by,
        expected_action_hash=expected_action_hash,
        expected_run_attempt=expected_run_attempt,
    )
    record = await approvals.get(approval_id)
    current = record.status if record is not None else None
    if batch_id:
        batch_terminal = await approvals.batch_pending_count(run_id, batch_id) == 0
    else:
        # Legacy single-approval suspension (no batch): terminal once this one resolves.
        batch_terminal = current is not None and current != "pending"
    requeued = await run_store.requeue(run_id) if batch_terminal else False
    return _ResolveTx(applied, current, batch_terminal, requeued)


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
    # True when the durable admission committed (run row + prompt + ``queued``) but the
    # post-commit worker enqueue could not be delivered (e.g. the queue was briefly
    # unavailable). The run is still accepted (202): the durable reconciler redispatches
    # queued-but-undispatched runs, so the caller must NOT surface an error / retry — doing
    # so would risk a duplicate run. See :meth:`DurableRunService.admit`.
    dispatch_pending: bool = False


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
        delegate_policy: DelegatePolicy | None = None,
        dispatch_outbox: RunDispatchOutbox | None = None,
    ) -> None:
        self._runs = run_store
        self._events = event_store
        self._approvals = approvals
        self._scope_id = scope_id
        self._enqueue = enqueue
        self._admit = admit_fn
        self._default_ttl_seconds = default_ttl_seconds
        self._delegate_policy = delegate_policy
        self._dispatch_outbox = dispatch_outbox

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
        model: str | None = None,
        admission_extra: Mapping[str, object] | None = None,
        now: datetime | None = None,
    ) -> AdmitResult:
        """Idempotently admit a run and complete admission with crash-safe repair.

        Admission has four durable steps — create the run row, persist the user turn,
        transition ``admitted -> queued``, and enqueue a worker job. Each is applied
        idempotently here so a crash (or a retried request with the same
        ``(scope, org, actor, idempotency_key)`` identity) **repairs** a half-admitted row and
        completes exactly the missing steps — it never creates a duplicate run/message and
        never leaves a prompt-less run behind. The prompt is always persisted *before* the
        queued/dispatch transition (invariant I2). A retry whose immutable fingerprint
        mismatches the persisted admission raises :class:`~keel_core.runs.RunAdmissionConflict`
        rather than repairing with the caller-supplied binding/content.

        ``model`` — the model selected at admission — is folded into the immutable fingerprint
        and persisted in the admission event so the worker executes the run with the admitted
        model (reproducibility) rather than its own process default. It is part of the
        immutable identity: a retry that reuses the idempotency key but changes the model is a
        conflict."""
        now = now or datetime.now(UTC)
        run_id = run_id or uuid.uuid4().hex
        expires_at = now + timedelta(seconds=ttl_seconds or self._default_ttl_seconds)
        # Immutable admission fingerprint: a retry that reuses the identity but mismatches the
        # tenant/actor/agent/session/surface/content/model is rejected as a conflict.
        fingerprint = admission_fingerprint(
            org_id=org_id,
            actor=actor,
            agent_id=agent_id,
            session_id=session_id,
            surface=surface,
            content=content,
            model=model,
        )
        # Deployment-rollout compatibility: a run admitted by a *pre-model* binary stored a
        # fingerprint that omitted the model. Recompute that precise legacy form so a retry of
        # such an in-flight run completes idempotently across the deploy. A model-aware stored
        # fingerprint always encodes the model field and can never equal this, so a changed
        # model cannot hijack a new-model row through the legacy path.
        legacy_fingerprint = legacy_admission_fingerprint(
            org_id=org_id,
            actor=actor,
            agent_id=agent_id,
            session_id=session_id,
            surface=surface,
            content=content,
        )
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
            fingerprint=fingerprint,
            legacy_fingerprint=legacy_fingerprint,
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
        await self._ensure_prompt(record.id, session_id, content, model, admission_extra)
        # 2) admitted -> queued is an atomic, single-winner transition; only the caller that
        #    wins it dispatches, so N concurrent admitters enqueue the worker job exactly once
        #    (a lost enqueue after this point is repaired by the reconciler, not re-sent here).
        #    When a global dispatch outbox is wired the queued transition AND the cross-scope
        #    dispatch intent commit in ONE transaction (finding 4): a committed ``queued`` run is
        #    always accompanied by a discoverable intent, and a failed intent write rolls the
        #    transition back (the run stays ``admitted``, recovered by retry/reconcile) rather
        #    than becoming an undiscoverable queued run. We therefore do NOT swallow that error.
        if self._dispatch_outbox is not None:
            queued = await self._runs.mark_queued_with_dispatch_intent(
                record.id, self._scope_id, self._dispatch_outbox, now=now
            )
        else:
            queued = await self._runs.mark_queued(record.id, now=now)
        dispatch_pending = False
        if queued:
            # 3) Dispatch a worker job (a duplicate enqueue is deduped by the claim). The
            #    durable admission (run row + prompt + queued + dispatch intent) has ALREADY
            #    committed, so a failed enqueue must NOT propagate as an error: that would make
            #    the caller retry and risk a duplicate run. We swallow the enqueue failure, mark
            #    the admission as dispatch-pending, and rely on the durable reconciler to
            #    redispatch queued-but-undispatched runs (durable-outbox reconciliation).
            try:
                await self._enqueue(record.id)
            except Exception:  # noqa: BLE001 - dispatch is best-effort; the reconciler backs it up
                dispatch_pending = True
                logger.warning(
                    "run dispatch pending scope=%s run=%s (enqueue failed; reconciler will "
                    "redispatch)",
                    self._scope_id,
                    record.id,
                    exc_info=True,
                )
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
        return AdmitResult(run_id=record.id, created=created, dispatch_pending=dispatch_pending)

    async def _ensure_prompt(
        self,
        run_id: RunId,
        session_id: SessionId,
        content: str,
        model: str | None = None,
        admission_extra: Mapping[str, object] | None = None,
    ) -> None:
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
            await admit_run(
                self._events,
                session_id,
                self._scope_id,
                content,
                run_id,
                model=model,
                extra=admission_extra,
            )
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

    async def _actor_authorized(self, actor: str, run: RunRecord, record: ApprovalRecord) -> bool:
        """Whether ``actor`` may resolve ``record`` for ``run`` (blocker 4).

        A stable, non-blank authenticated actor is mandatory. The default policy authorizes
        only the bound run/approval owner — a different user in the *same* org is denied
        (no ambient cross-user resolution). An intentional, documented delegation is opt-in
        via ``delegate_policy`` (e.g. an org-admin override), tested explicitly."""
        if not actor:
            return False  # never a blank/absent actor (local preview supplies a stable one)
        if actor == run.actor:
            return True
        if self._delegate_policy is not None:
            return await self._delegate_policy(actor, run, record)
        return False

    async def resolve_approval(
        self,
        approval_id: str,
        *,
        approved: bool,
        resolved_by: str,
        actor: str,
        org_id: str | None = None,
    ) -> bool:
        """Resolve a durable interactive approval, fully bound to the **current** run.

        Every binding field is verified against the run row as it exists *now* — never
        defaulted from the approval's own copies (which an attacker/stale replay controls):

        * the approval's ``run_id`` must resolve to a durable run (else it is not ours);
        * the run must not be terminal; resolution + requeue are gated on the current state;
        * the resolver's authenticated ``org_id`` (when supplied) must equal the run's org
          (cross-org denial), and the approval's stored ``org_id`` must equal the run's org;
        * the approval's ``actor`` must match the run's owner, and the resolver's ``actor``
          (**mandatory**, non-blank) must be the bound owner or a documented delegate — a
          same-org different user is denied unless a delegation policy authorizes it;
        * the approval's ``run_attempt`` must equal the run's **current** ``attempt`` — an
          attempt-0 approval can never resume an attempt-1 (reclaimed/advanced) run;
        * the ``action_hash`` is **recomputed** from the approval's stored exact tool + args
          and must equal the stored hash (tamper/replay defense).

        The approval-resolve, the batch-terminality check, and the run
        ``waiting_approval -> queued`` (resume marker) commit in **one transaction** (blocker
        1); the run is requeued only once *every* approval in its batch is terminal (blocker
        5). Dispatch of the resume job happens after commit; the reconciler re-enqueues a
        committed-but-undispatched run. A duplicate identical decision is idempotent
        (returns True); a conflicting decision on a terminal approval is rejected (False).
        """
        record = await self._approvals.get(approval_id)
        if record is None:
            return False
        run = await self._runs.get(record.run_id)
        if run is None:
            # Not a durable interactive run (e.g. a legacy scheduled approval): not ours.
            return False
        # Bind to the CURRENT run, not the approval's own (possibly stale) copies.
        if record.org_id != run.org_id:
            return False  # approval was bound to a different org than the run now has
        if org_id is not None and run.org_id != org_id:
            return False  # cross-org resolution denied (fail closed)
        if record.actor != run.actor:
            return False  # approval was bound to a different actor than the run owner
        if not await self._actor_authorized(actor, run, record):
            return False  # resolver is not the bound owner nor an authorized delegate
        # Bind the decision to the suspended *source* attempt, not the raw lease attempt. A
        # crash-reclaimed run advances ``attempt`` for lease fencing while ``checkpoint_attempt``
        # preserves the attempt whose approval batch is outstanding; an approval for that source
        # attempt stays applicable, while a stale approval from a different/older checkpoint is
        # rejected. Legacy rows (no checkpoint) fall back to the current attempt.
        expected_attempt = run.checkpoint_attempt or run.attempt
        if record.run_attempt != expected_attempt:
            return False  # approval bound to a superseded/foreign attempt (fail closed)
        # Recompute the action hash from the stored exact action payload (never trust the
        # stored hash blindly): a mismatch means the row was tampered with -> fail closed.
        if record.action_hash != action_hash(record.tool, record.args):
            return False
        if run.is_terminal:
            return False  # the run already finished; a late decision is moot
        status = "granted" if approved else "denied"
        tx = await self._resolve_and_requeue(
            approval_id=approval_id,
            run_id=record.run_id,
            batch_id=record.batch_id,
            status=status,
            resolved_by=resolved_by,
            expected_action_hash=record.action_hash,
            expected_run_attempt=expected_attempt,
        )
        if tx.requeued:
            await self._enqueue(record.run_id)  # dispatch after commit (reconciler backs up)
        if tx.applied:
            logger.info(
                "approval resolved scope=%s run=%s approval=%s decision=%s by=%s batch_done=%s",
                self._scope_id,
                record.run_id,
                approval_id,
                status,
                resolved_by,
                tx.batch_terminal,
            )
            return True
        # Not applied: either an idempotent duplicate of the same decision, or a conflict.
        if tx.current_status == status:
            return True  # duplicate identical decision is idempotent
        return False  # conflicting decision on an already-terminal approval is rejected

    async def _resolve_and_requeue(
        self,
        *,
        approval_id: str,
        run_id: RunId,
        batch_id: str,
        status: str,
        resolved_by: str,
        expected_action_hash: str,
        expected_run_attempt: int,
    ) -> _ResolveTx:
        """Atomic (Postgres) or sequential (in-memory) resolve + batch-gated requeue."""
        engine = _shared_engine(self._runs, self._approvals)
        if engine is not None:
            return await _pg_resolve_and_requeue(
                engine,
                self._scope_id,
                approval_id=approval_id,
                run_id=run_id,
                batch_id=batch_id,
                status=status,
                resolved_by=resolved_by,
                expected_action_hash=expected_action_hash,
                expected_run_attempt=expected_run_attempt,
            )
        return await _mem_resolve_and_requeue(
            self._runs,
            self._approvals,
            approval_id=approval_id,
            run_id=run_id,
            batch_id=batch_id,
            status=status,
            resolved_by=resolved_by,
            expected_action_hash=expected_action_hash,
            expected_run_attempt=expected_run_attempt,
        )

    async def expire_approvals(
        self, *, now: datetime | None = None, legacy_resume: LegacyResumeFn | None = None
    ) -> int:
        """Expire past-deadline approvals and route each to the correct resume path.

        This is the **single owner** of durable-approval expiry (M3.6, item 6): the legacy
        scheduler no longer expires approvals, so both interactive and legacy approvals are
        classified + routed here transactionally. ``expire_due`` mutates each pending row to
        ``expired`` exactly once (RETURNING), so even under simultaneous reconciler ticks no
        approval is consumed twice. A durable **interactive** approval (its ``run_id`` is a
        durable run currently ``waiting_approval``) requeues the run **only once every
        approval in its batch is terminal** (blocker 5); a **legacy** approval (no durable
        run row) is dispatched via ``legacy_resume`` — never through the durable run state
        machine, and never left hanging. Returns the number of runs resumed."""
        now = now or datetime.now(UTC)
        expired = await self._approvals.expire_due(now)
        resumed = 0
        requeued_runs: set[RunId] = set()
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
            if run.status is not RunStatus.waiting_approval or record.run_id in requeued_runs:
                continue
            # Resume only when the whole suspended batch is terminal (all expired/resolved). A
            # legacy single approval (no batch id) is terminal the moment it expires.
            if record.batch_id and (
                await self._approvals.batch_pending_count(record.run_id, record.batch_id) != 0
            ):
                continue
            if await self._runs.requeue(record.run_id):
                requeued_runs.add(record.run_id)
                await self._enqueue(record.run_id)
                resumed += 1
        return resumed

    async def repair_stuck_resumes(self, *, limit: int = 100) -> int:
        """Requeue any run stuck in ``waiting_approval`` whose approvals are all terminal.

        The single, crash-tolerant backstop for blocker 1: whether a crash happened between
        the atomic approval-resolve/expiry commit and the run requeue, or between the requeue
        and the post-commit dispatch, this reconciliation observes a durable run whose batch
        is fully resolved/expired and drives ``waiting_approval -> queued`` (resume marked) +
        re-enqueues. Idempotent — a run already requeued is skipped, a duplicate enqueue is
        deduped by the claim. Returns the number of runs repaired."""
        repaired = 0
        for run_id in await self._runs.waiting_approval_ids(limit):
            # No pending approvals for the run == its suspended batch is terminal.
            if await self._approvals.pending_for_run(run_id):
                continue
            if await self._runs.requeue(run_id):
                await self._enqueue(run_id)
                repaired += 1
        return repaired


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


def _suspension_engine(
    run_store: RunStore, event_store: EventStore, approvals: ApprovalStore
) -> AsyncEngine | None:
    """The Postgres engine shared by the run, event, AND approval stores, or ``None``.

    Only when all three are backed by the *same* engine can a suspended tool batch's run
    checkpoint, ``tool.call`` events, approval rows, and ``approval.requested`` events commit in
    a single transaction (true atomicity). Otherwise (in-memory / mixed) the persister applies
    the checkpoint + batch atomically via snapshot/rollback in the single process."""
    inner, _observer = _unwrap_store(event_store)
    engine = getattr(run_store, "_engine", None)
    ev_engine = getattr(inner, "_engine", None)
    ap_engine = getattr(approvals, "_engine", None)
    if isinstance(engine, AsyncEngine) and engine is ev_engine and engine is ap_engine:
        return engine
    return None


def _txn_snapshot(obj: object) -> tuple[object, object] | None:
    """Capture a rollback snapshot of an in-memory store, or ``None`` if it has no support."""
    fn = getattr(obj, "_txn_snapshot", None)
    return (obj, fn()) if callable(fn) else None


def _txn_restore(snapshots: list[tuple[object, object] | None]) -> None:
    """Restore each captured snapshot (undo a partially-applied in-memory suspension)."""
    for snap in snapshots:
        if snap is None:
            continue
        obj, state = snap
        restore = getattr(obj, "_txn_restore", None)
        if callable(restore):
            restore(state)


def _make_suspension_persister(
    run_store: RunStore, scope_id: ScopeId, lease: RunLease
) -> SuspensionPersister:
    """Build the durable-run :class:`~keel_core.loop.SuspensionPersister` for one attempt.

    It writes the run's fenced suspension checkpoint (source attempt + exact batch id) as part
    of the SAME unit that persists the batch's approval rows + events — never a separate
    ``mark_checkpoint`` commit before it:

    * Shared-engine (Postgres) path: one transaction. The checkpoint UPDATE runs first (fenced
      on the current lease token/attempt); a lost lease raises :class:`RunLeaseLostError`,
      rolling the whole unit back. On any failure nothing persists — a reclaim restarts fresh;
      on commit a reclaim always observes the complete batch bound to the checkpoint.
    * In-memory / mixed single-process path: snapshot the run/event/approval stores, mark the
      checkpoint, persist the batch, and on any injected failure restore the snapshots so no
      partial checkpoint/batch survives."""

    async def persist(
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
        engine = _suspension_engine(run_store, store, approvals)
        if engine is not None:

            async def checkpoint(conn: AsyncConnection) -> None:
                # Fenced on the current lease token/attempt; a lost lease aborts (rolls back)
                # the whole batch. Records this batch id + source attempt on the run row.
                await mark_checkpoint_in_transaction(conn, scope_id, lease, batch_id=batch_id)

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
                checkpoint_in_tx=checkpoint,
            )

        # In-memory / mixed: apply the checkpoint + batch atomically via snapshot/rollback.
        inner, _observer = _unwrap_store(store)
        snapshots = [
            _txn_snapshot(run_store),
            _txn_snapshot(inner),
            _txn_snapshot(approvals),
        ]
        try:
            await run_store.mark_checkpoint(lease, batch_id=batch_id)
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
        except BaseException:
            _txn_restore(snapshots)
            raise

    return persist


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

    # Durable suspension persister: writes the run's fenced checkpoint (source attempt + batch
    # id) in the SAME transaction/atomic unit as the batch's approval rows + events, so a crash
    # mid-suspension never leaves a partial checkpoint/batch (blocker 2). No separate
    # ``mark_checkpoint`` commit precedes it.
    persister = _make_suspension_persister(run_store, lease.scope_id, lease)

    # Reconstruction expectations for resume's approval-event repair: the durable checkpoint's
    # source attempt + exact batch id. A lost ``approval.requested`` event is repaired from the
    # durable rows only when their run/session/call/action-hash AND these match exactly — a
    # foreign/older/newer attempt or batch fails closed (blocker 1). ``checkpoint_batch_id``
    # empty (a legitimate older-build checkpoint that never persisted one) does NOT wildcard the
    # batch constraint: only an approval row whose own batch id is equally empty (a true
    # old-build row) may be adopted — a row carrying a real, non-empty batch id is always
    # foreign to a batch-less checkpoint and is rejected, fail closed. The source attempt is
    # still enforced exactly regardless.
    reconstruct_attempt = record.checkpoint_attempt or lease.attempt
    reconstruct_batch_id = record.checkpoint_batch_id or None

    keeper.start()
    watcher.start()
    lease_lost = False
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
                suspension_persister=persister,
                reconstruct_attempt=reconstruct_attempt,
                reconstruct_batch_id=reconstruct_batch_id,
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
                suspension_persister=persister,
            )
    except RunLeaseLostError:
        # A fenced checkpoint write found the lease lost mid-batch (another worker reclaimed):
        # fail closed. No approval batch was persisted under the stale lease and no terminal
        # state is written here — the reclaiming owner drives the run.
        lease_lost = True
        result = None
    finally:
        await watcher.stop()
        await keeper.stop()

    # Fenced out mid-run: the lease is stale, so we must NOT write a terminal state (the
    # reclaiming owner drives it). Leave control signals pending for the fresh owner.
    if keeper.lost or lease_lost or result is None:
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
