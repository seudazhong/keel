"""ReviewCoordinator — durable request/execute lifecycle for read-only reviews (WS-R).

Ties the pure :class:`~keel_core.review.service.ReviewService` to the existing durable
substrate without a new migration:

* **request** — authorize the actor/Agent (read+run == ``use``) on the project, create a
  durable :class:`~keel_core.runs.RunRecord` (``surface="review"``, idempotent by request key),
  associate it to the project (``project_runs``), and return a handle. Enqueueing the worker
  job is the caller's responsibility (it owns the job store).
* **execute** — the worker path: re-authorize at claim time (revocation fails closed), claim
  the run lease, run the review, and terminalize the run with ``result_ref`` pointing at the
  content-addressed JSON report. Idempotent and restart-safe: a run already terminal is a
  no-op; a reclaimed run re-produces identical (content-addressed) artifacts.

The GitHub App JIT token used to fetch PR metadata/diff lives entirely on the control plane
(the project service); it is never handed to the review service, the worktree, or the model.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from keel_core.coding.models import CodingRunId, ProjectId
from keel_core.coding.protocols import ArtifactStore
from keel_core.errors import DuplicateEventError, PermissionDenied
from keel_core.events import Event, EventType
from keel_core.jobs import JobCancellationRequested
from keel_core.projects.service import ProjectService
from keel_core.projects.storage import worktree_storage_id
from keel_core.protocols import EventStore, Usage
from keel_core.runs import (
    RunBudgetSpec,
    RunCost,
    RunLease,
    RunRecord,
    RunStatus,
    RunStore,
)
from keel_core.types import ScopeId

from .audit import LoggingReviewAuditSink, ReviewAuditAction, ReviewAuditEvent, ReviewAuditSink
from .errors import (
    ReviewBoundsExceeded,
    ReviewError,
    ReviewEvidenceError,
    ReviewLeaseLost,
    ReviewNotFound,
    ReviewProviderError,
    ReviewProviderUnavailable,
    ReviewValidationError,
)
from .models import (
    ReviewId,
    ReviewRecord,
    ReviewReport,
    ReviewRequest,
    ReviewSource,
    ReviewStatus,
    new_review_id,
)
from .refs import PullRequestResolver, build_materialization_plan
from .service import ReviewOutcome, ReviewService

logger = logging.getLogger("keel.review.coordinator")

REVIEW_SURFACE = "review"
DEFAULT_REVIEW_AGENT_ID = "review"
DEFAULT_REVIEW_TTL_SECONDS = 3600
DEFAULT_REVIEW_LEASE_SECONDS = 900
# Payload marker under which the immutable review request metadata is durably recorded on the
# run's event log at admission. Projections (pending/running/failed/list) reconstruct the
# request from this marker so they stay truthful across a process restart — the report artifact
# only exists once a review completes.
REVIEW_REQUEST_MARKER = "review_request"
# Schema version of the durably-persisted review request metadata. v2 adds the full budget/
# policy envelope (token/output budget, cost ceiling, provider-attempt/repair count) so the
# exact values a review was admitted under can be reconstructed for stranded re-dispatch. A
# record without these fields (legacy v1 / pre-budget) is reconstructed **fail-closed** (see
# ``load_request_metadata``) rather than back-filled with larger implicit defaults. v3 adds
# ``effective_agent_id`` — the CANONICAL EFFECTIVE agent (``request.agent_id or
# DEFAULT_REVIEW_AGENT_ID``, also bound into :func:`review_fingerprint`) recorded ALONGSIDE the
# raw ``agent_id`` field purely for audit/consistency (reconstruction cross-checks the two and
# fails closed on a mismatch — see ``_reconstruct_request``); it is never fed into authorization,
# which always replays the RAW field so an actor-direct (``agent_id=None``) review's stranded
# re-dispatch never spuriously requires a real Agent grant. A v2 record predates this field
# entirely and has nothing to cross-check, so it is reconstructed as-is (exact compatibility).
REVIEW_REQUEST_METADATA_VERSION = 3

# A durable job-lease heartbeat must fire safely *below* the lease expiry — never on/after it.
# The interval is derived from the ACTUAL job lease (``min(lease/3, cap)``): a third of the
# lease keeps two consecutive missed beats inside the window, and the absolute cap keeps a
# cancellation responsive under a very long lease. The result is ALWAYS strictly < the lease.
JOB_HEARTBEAT_MIN_INTERVAL_SECONDS = 1.0
JOB_HEARTBEAT_MAX_INTERVAL_SECONDS = 30.0


def _heartbeat_interval(lease_seconds: float) -> float:
    """A heartbeat interval strictly *below* ``lease_seconds`` (never equal to the expiry).

    Uses ``min(lease/3, cap)`` bounded to a small floor. The final value is guaranteed to be
    strictly less than the lease even for a tiny lease (e.g. a 1s lease yields 0.5s, not 1.0s),
    so a beat can never land exactly on lease expiry.
    """
    if lease_seconds <= 0:
        return JOB_HEARTBEAT_MIN_INTERVAL_SECONDS
    interval = min(lease_seconds / 3.0, JOB_HEARTBEAT_MAX_INTERVAL_SECONDS)
    interval = max(JOB_HEARTBEAT_MIN_INTERVAL_SECONDS, interval)
    if interval >= lease_seconds:
        # A very short lease (<= the floor): halve it so the beat stays strictly inside.
        interval = lease_seconds / 2.0
    return interval


class _JobSignalAbort(Exception):
    """Internal marker: the durable job heartbeat aborted the review (cancellation/lease loss).

    Bridges ``asyncio.CancelledError`` (a ``BaseException``) into the single ``except Exception``
    job-signal handler so the cancellation/lease-loss resolution runs exactly once. It never
    escapes :meth:`ReviewCoordinator.execute_review` — the handler always re-raises the real
    signal (``JobCancellationRequested`` / ``ReviewLeaseLost``) in its place.
    """


_STATUS_MAP: dict[RunStatus, ReviewStatus] = {
    RunStatus.admitted: ReviewStatus.pending,
    RunStatus.queued: ReviewStatus.pending,
    RunStatus.running: ReviewStatus.running,
    RunStatus.waiting_approval: ReviewStatus.running,
    RunStatus.completed: ReviewStatus.completed,
    RunStatus.failed: ReviewStatus.failed,
    RunStatus.cancelled: ReviewStatus.cancelled,
    RunStatus.interrupted: ReviewStatus.failed,
    RunStatus.expired: ReviewStatus.failed,
}


def review_fingerprint(request: ReviewRequest) -> str:
    """Immutable binding of a review request so a reused idempotency key can't be hijacked.

    Binds the full budget/policy envelope (token/output budget, cost ceiling, provider-attempt/
    repair count) AND the CANONICAL EFFECTIVE ``agent_id`` (``request.agent_id or
    DEFAULT_REVIEW_AGENT_ID`` — the identity the run is actually admitted/authorized under, see
    :meth:`ReviewCoordinator.request_review`) in addition to the identity/source/model, so a
    retry that reuses an idempotency key but presents a *different* budget OR a *different*
    Agent is a conflict rather than silently reusing the original run under mismatched limits or
    a mismatched Agent identity. The canonical form means an omitted ``agent_id`` and the
    explicit default string fingerprint identically (no spurious conflict from that alone).
    """
    payload = json.dumps(
        {
            "org_id": request.org_id,
            "project_id": request.project_id,
            "source": request.source.value,
            "head": request.head,
            "base": request.base or "",
            "model": request.model,
            "agent_id": request.agent_id or DEFAULT_REVIEW_AGENT_ID,
            "max_findings": request.max_findings,
            "max_diff_bytes": request.max_diff_bytes,
            "token_budget": request.token_budget,
            "output_max_tokens": request.output_max_tokens,
            "cost_ceiling_usd": request.cost_ceiling_usd,
            "max_provider_attempts": request.max_provider_attempts,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ReviewHandle:
    review_id: ReviewId
    run_id: str
    status: ReviewStatus
    created: bool


class ReviewCoordinator:
    """Durable orchestration of read-only reviews over runs + project_runs + artifacts."""

    def __init__(
        self,
        *,
        projects: ProjectService,
        runs: RunStore,
        review_service: ReviewService,
        artifacts: ArtifactStore,
        scope_id: ScopeId,
        events: EventStore | None = None,
        pr_resolver: PullRequestResolver | None = None,
        audit: ReviewAuditSink | None = None,
        worker_id: str = "review-worker",
        ttl_seconds: int = DEFAULT_REVIEW_TTL_SECONDS,
        lease_seconds: int = DEFAULT_REVIEW_LEASE_SECONDS,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._projects = projects
        self._runs = runs
        self._reviews = review_service
        self._artifacts = artifacts
        self._scope_id = scope_id
        self._events = events
        self._pr_resolver = pr_resolver
        self._audit = audit or LoggingReviewAuditSink()
        self._worker_id = worker_id
        self._ttl_seconds = ttl_seconds
        self._lease_seconds = lease_seconds
        self._clock = clock or (lambda: datetime.now(UTC))

    # --- request path (control plane) ------------------------------------------------
    async def request_review(self, request: ReviewRequest, *, actor: str) -> ReviewHandle:
        """Authorize + durably admit a review; returns a handle (idempotent by request key)."""
        await self._projects.authorize_review(
            request.org_id, actor, request.project_id, agent_id=request.agent_id
        )
        now = self._clock()
        run_id = new_review_id()
        record, created = await self._runs.create(
            run_id=run_id,
            scope_id=self._scope_id,
            org_id=request.org_id,
            actor=actor,
            agent_id=request.agent_id or DEFAULT_REVIEW_AGENT_ID,
            session_id=run_id,
            surface=REVIEW_SURFACE,
            idempotency_key=request.idempotency_key,
            budget=RunBudgetSpec(max_iterations=1, token_budget=None),
            expires_at=now + timedelta(seconds=self._ttl_seconds),
            fingerprint=review_fingerprint(request),
            now=now,
        )
        await self._projects.associate_run(
            request.org_id, actor, request.project_id, record.id, agent_id=request.agent_id
        )
        if created:
            await self._persist_request_metadata(record.id, request)
            self._audit.record(
                ReviewAuditEvent(
                    ReviewAuditAction.review_requested,
                    actor,
                    request.org_id,
                    record.id,
                    {
                        "project_id": request.project_id,
                        "source": request.source.value,
                        "model": request.model,
                    },
                )
            )
        return ReviewHandle(
            review_id=record.id,
            run_id=record.id,
            status=_STATUS_MAP[record.status],
            created=created,
        )

    async def _persist_request_metadata(self, run_id: str, request: ReviewRequest) -> None:
        """Durably record the immutable review request metadata on the run's event log.

        No schema migration: the metadata lives in the append-only ``events`` payload keyed by
        the run id (mirroring the durable-admission marker pattern). A duplicate append (retry /
        idempotent re-request) is ignored. When no event store is wired the coordinator degrades
        to report-only projections (completed reviews stay truthful via their report artifact).
        """
        if self._events is None:
            return
        event = Event(
            type=EventType.run_started,
            seq=0,
            session_id=run_id,
            scope_id=self._scope_id,
            run_id=run_id,
            ts=self._clock(),
            payload={
                REVIEW_REQUEST_MARKER: {
                    "version": REVIEW_REQUEST_METADATA_VERSION,
                    "org_id": request.org_id,
                    "project_id": request.project_id,
                    "source": request.source.value,
                    "head": request.head,
                    "base": request.base,
                    "model": request.model,
                    # RAW request agent_id (``None`` preserved): this is what re-authorization
                    # replays through (:meth:`load_request_metadata` -> ``execute_review`` ->
                    # ``authorize_review``), and ``None`` there means "actor acting directly" —
                    # NOT the same as an explicit Agent named after the default placeholder. Never
                    # canonicalize this field; doing so would force every actor-direct review's
                    # stranded-reconciliation replay through a non-existent Agent grant check.
                    "agent_id": request.agent_id,
                    # CANONICAL EFFECTIVE agent_id (v3): the identity this run is actually
                    # admitted/audited under (``request.agent_id or DEFAULT_REVIEW_AGENT_ID`` —
                    # mirrors the run record's own ``agent_id`` column and the fingerprint).
                    # Recorded alongside the raw field purely for audit/consistency; reconstruction
                    # cross-checks it against the raw field (fail-closed on mismatch/tamper) but
                    # never feeds it into authorization.
                    "effective_agent_id": request.agent_id or DEFAULT_REVIEW_AGENT_ID,
                    "max_findings": request.max_findings,
                    "max_diff_bytes": request.max_diff_bytes,
                    # Full budget/policy envelope: the exact values the review was admitted
                    # under, so a stranded re-dispatch reconstructs them precisely instead of
                    # applying (possibly larger) implicit defaults.
                    "token_budget": request.token_budget,
                    "output_max_tokens": request.output_max_tokens,
                    "cost_ceiling_usd": request.cost_ceiling_usd,
                    "max_provider_attempts": request.max_provider_attempts,
                },
                "dedup_key": f"review-meta:{run_id}",
            },
        )
        try:
            await self._events.append(event)
        except DuplicateEventError:
            pass

    async def load_request_metadata(self, run_id: str) -> ReviewRequest | None:
        """Reconstruct the durably-persisted review request for ``run_id`` (or ``None``).

        The full budget/policy envelope (token/output budget, cost ceiling, provider-attempt/
        repair count) is reconstructed **exactly** from the persisted metadata. A legacy record
        that predates the budget envelope (missing any of these fields, i.e. below the current
        metadata version) is reconstructed **fail-closed** — this returns ``None`` rather than
        back-filling larger implicit defaults, so a stranded re-dispatch never silently runs a
        review under a wider budget than it was admitted with.
        """
        if self._events is None:
            return None
        async for event in self._events.read(run_id):
            meta = event.payload.get(REVIEW_REQUEST_MARKER)
            if isinstance(meta, dict):
                return self._reconstruct_request(run_id, meta)
        return None

    def _reconstruct_request(self, run_id: str, meta: dict[str, Any]) -> ReviewRequest | None:
        budget_keys = (
            "token_budget",
            "output_max_tokens",
            "cost_ceiling_usd",
            "max_provider_attempts",
        )
        if any(meta.get(key) is None for key in budget_keys):
            # Legacy / pre-budget metadata: fail closed rather than assume implicit defaults.
            logger.warning(
                "review request metadata missing budget envelope; failing closed run=%s", run_id
            )
            return None
        raw_agent_id = str(meta["agent_id"]) if meta.get("agent_id") else None
        # ``effective_agent_id`` (v3+) is an audit companion to the raw field, not an input to
        # authorization: cross-check it and fail closed on a mismatch/tamper. A v2 record
        # (predates this field entirely) has nothing to cross-check against — it is reconstructed
        # from its raw ``agent_id`` exactly as before (exact version compatibility).
        effective_agent_id = meta.get("effective_agent_id")
        if effective_agent_id is not None and str(effective_agent_id) != (
            raw_agent_id or DEFAULT_REVIEW_AGENT_ID
        ):
            logger.warning(
                "review request metadata effective_agent_id mismatch; failing closed run=%s",
                run_id,
            )
            return None
        try:
            return ReviewRequest(
                org_id=str(meta["org_id"]),
                project_id=str(meta["project_id"]),
                source=ReviewSource(str(meta["source"])),
                head=str(meta["head"]),
                base=str(meta["base"]) if meta.get("base") is not None else None,
                model=str(meta["model"]),
                idempotency_key=f"review-meta:{run_id}",
                agent_id=raw_agent_id,
                max_findings=int(meta.get("max_findings", 0)) or 1,
                max_diff_bytes=int(meta.get("max_diff_bytes", 0)) or 1,
                token_budget=int(meta["token_budget"]),
                output_max_tokens=int(meta["output_max_tokens"]),
                cost_ceiling_usd=float(meta["cost_ceiling_usd"]),
                max_provider_attempts=int(meta["max_provider_attempts"]),
            )
        except (KeyError, ValueError, TypeError, ReviewError):
            return None

    async def build_review_record(
        self, record: RunRecord, *, report: ReviewReport | None = None
    ) -> ReviewRecord:
        """A truthful status projection: report (completed) or persisted request metadata."""
        request = None if report is not None else await self.load_request_metadata(record.id)
        return self.build_record(record, request=request, report=report)

    # --- execution path (worker) -----------------------------------------------------
    async def execute_review(
        self,
        request: ReviewRequest,
        *,
        run_id: str,
        job_checkpoint: Callable[[], Awaitable[None]] | None = None,
        job_lease_seconds: int | None = None,
    ) -> ReviewOutcome | None:
        """Claim + run a durably-admitted review. Idempotent/restart-safe.

        ``job_checkpoint`` is an optional durable job-lease heartbeat (the worker's
        ``JobContext.checkpoint``): it is invoked periodically throughout the review so a lost
        job lease or a cancellation request aborts execution *without* a terminal success under
        a superseded fence. ``job_lease_seconds`` is the ACTUAL durable job lease duration used
        to derive a heartbeat interval safely below that lease's expiry (never equal); when it is
        omitted the run lease duration is used as a conservative fallback.
        """
        record = await self._runs.get(run_id)
        if record is None:
            raise ReviewNotFound(f"review run not found: {run_id}")
        if record.is_terminal:
            return None
        await self._runs.mark_queued(run_id, now=self._clock())
        lease = await self._runs.claim(
            run_id,
            worker_id=self._worker_id,
            now=self._clock(),
            lease_seconds=self._lease_seconds,
        )
        if lease is None:
            # Could not claim: either the run raced to terminal (genuine no-op) or another
            # worker holds the lease. Contention is NEVER reported as an ``already_terminal``
            # success — it is a busy/retry so the durable job re-attempts once the fence frees.
            latest = await self._runs.get(run_id)
            if latest is not None and latest.is_terminal:
                return None
            raise ReviewLeaseLost(
                f"review run is contended; another worker holds the lease: {run_id}"
            )
        # Job heartbeat/cancellation supervision (and the run-lease keeper) must be LIVE before
        # authorization, PR resolution, the GitHub fetch, or Git materialization ever run — every
        # one of those can block on slow I/O (a control-plane HTTP call, a `git fetch` subprocess),
        # and a cancellation / lost lease reaching us mid-flight must abort that work rather than
        # let it complete unobserved with an effect written under a stale fence. Both keepers are
        # therefore started immediately after the run lease is claimed, each supervised
        # independently: the run-lease keeper watches OUR fence on the run row; the job-lease
        # keeper watches the durable JOB lease and cancels the whole review body (below) on loss.
        keeper = _LeaseKeeper(
            run_store=self._runs,
            lease=lease,
            interval_seconds=_heartbeat_interval(self._lease_seconds),
        )
        keeper.start()
        job_keeper: _JobHeartbeatKeeper | None = None
        if job_checkpoint is not None:
            # Heartbeat interval derives from the ACTUAL durable job lease (not the run lease):
            # a lost job lease / cancellation must be detected safely before the job lease
            # expires. Fall back to the run lease only when the job lease is not threaded through.
            heartbeat_lease = job_lease_seconds if job_lease_seconds else self._lease_seconds
            job_keeper = _JobHeartbeatKeeper(
                checkpoint=job_checkpoint,
                interval_seconds=_heartbeat_interval(heartbeat_lease),
            )
        prior_usage = Usage(
            prompt_tokens=record.prompt_tokens,
            completion_tokens=record.completion_tokens,
            cost_usd=record.cost_usd,
        )

        async def _run_review_body() -> ReviewOutcome:
            # Re-authorize at claim time UNDER the lease: access revoked since admission fails
            # closed. This, the PR resolution / GitHub fetch, and the Git materialization all run
            # INSIDE the task the job-lease keeper below is bound to, so a cancellation / lost
            # job lease aborts them exactly like it would abort the review itself.
            project = await self._projects.authorize_review(
                record.org_id, record.actor, request.project_id, agent_id=request.agent_id
            )
            handle = project.active_git_handle or project.id
            coding_run_id = worktree_storage_id(run_id)
            self._audit.record(
                ReviewAuditEvent(
                    ReviewAuditAction.review_started,
                    record.actor,
                    record.org_id,
                    run_id,
                    {"project_id": request.project_id, "source": request.source.value},
                )
            )
            plan = await build_materialization_plan(
                request,
                default_branch=getattr(project, "default_branch", None),
                pr_resolver=self._pr_resolver,
            )
            return await self._reviews.review(
                request,
                run_id=run_id,
                coding_run_id=coding_run_id,
                project_handle=handle,
                review_id=run_id,
                now=self._clock(),
                plan=plan,
                prior_usage=prior_usage,
            )

        try:
            try:
                work_task: asyncio.Task[ReviewOutcome] = asyncio.ensure_future(_run_review_body())
                if job_keeper is not None:
                    # Periodically heartbeat the durable JOB lease throughout authorization, PR
                    # resolution/GitHub fetch, Git materialization, AND the review itself; a lost
                    # job lease / cancellation cancels the whole in-flight task (abort, no
                    # terminal write under a superseded fence, no further HTTP/Git effect).
                    job_keeper.bind(work_task)
                    job_keeper.start()
                try:
                    outcome = await work_task
                except asyncio.CancelledError:
                    # ``asyncio.CancelledError`` is a ``BaseException`` (not ``Exception``) so the
                    # single job-signal handler in the ``except Exception`` below would miss it.
                    # When the heartbeat aborted the review (cancellation / lost job lease) convert
                    # it to an internal marker so that handler resolves it exactly once; otherwise
                    # (an external cancellation) propagate untouched.
                    if job_keeper is not None and job_keeper.lost:
                        raise _JobSignalAbort from None
                    raise
                finally:
                    if job_keeper is not None:
                        await job_keeper.stop()
            except Exception as exc:
                if job_keeper is not None and job_keeper.lost:
                    # A job cancellation / lost-job-lease signal (converted marker, or a normal
                    # review exception that raced with the heartbeat). Resolve it ONCE under the
                    # run fence and re-raise: never a terminal success, never a live running lease.
                    signal = job_keeper.error
                    if signal is None and not isinstance(exc, _JobSignalAbort):
                        signal = exc
                    await self._handle_lost_job_signal(
                        lease=lease,
                        run_id=run_id,
                        record=record,
                        request=request,
                        error=signal,
                        run_lease_lost=keeper.lost,
                    )
                if keeper.lost:
                    # Run lease lost mid-flight: another worker may own the run. Abort all
                    # effects WITHOUT terminalizing (contention is never a terminal success).
                    if isinstance(exc, ReviewLeaseLost):
                        raise
                    raise ReviewLeaseLost(f"review lease lost during execution: {run_id}") from exc
                if isinstance(exc, PermissionDenied):
                    # Access was revoked between admission and claim/authorize: a revocation
                    # must terminalize the run durably (cancelled, not failed — the review never
                    # ran on its own merits) before the job fails permanently, so the run never
                    # lingers admitted until its TTL and no reclaim can run it.
                    await self._runs.terminalize(
                        lease,
                        status=RunStatus.cancelled,
                        stop_reason="authorization_revoked",
                        now=self._clock(),
                        error_kind="authorization_revoked",
                        error_message=_safe_error_message(exc),
                    )
                    self._audit.record(
                        ReviewAuditEvent(
                            ReviewAuditAction.review_failed,
                            record.actor,
                            record.org_id,
                            run_id,
                            {
                                "project_id": request.project_id,
                                "error_kind": "authorization_revoked",
                            },
                        )
                    )
                    raise
                if _is_retryable(exc):
                    # Transient provider/infra failure: do NOT terminalize; the durable job
                    # retries until success or attempts are exhausted. Durably charge any partial
                    # token/cost usage consumed before the failure, then release the lease so the
                    # run is promptly reclaimable AND the next attempt gets only remaining budget.
                    await self._release(lease, cost=_partial_cost(exc))
                    raise
                error_kind = _error_kind(exc)
                await self._runs.terminalize(
                    lease,
                    status=RunStatus.failed,
                    stop_reason=error_kind,
                    now=self._clock(),
                    error_kind=error_kind,
                    error_message=_safe_error_message(exc),
                )
                self._audit.record(
                    ReviewAuditEvent(
                        ReviewAuditAction.review_failed,
                        record.actor,
                        record.org_id,
                        run_id,
                        {"project_id": request.project_id, "error_kind": error_kind},
                    )
                )
                raise
            if keeper.lost:
                # The review produced a result but our fence was superseded: do not claim
                # completion (the reclaiming worker owns the terminal write).
                raise ReviewLeaseLost(f"review lease lost before terminalization: {run_id}")
            cost = RunCost(
                prompt_tokens=outcome.usage.prompt_tokens,
                completion_tokens=outcome.usage.completion_tokens,
                cost_usd=outcome.usage.cost_usd,
                iterations=1,
            )
            await self._runs.terminalize(
                lease,
                status=RunStatus.completed,
                stop_reason="completed",
                now=self._clock(),
                cost=cost,
                result_ref=outcome.json_sha256,
            )
        finally:
            await keeper.stop()
        self._audit.record(
            ReviewAuditEvent(
                ReviewAuditAction.review_completed,
                record.actor,
                record.org_id,
                run_id,
                {
                    "project_id": request.project_id,
                    "finding_count": str(len(outcome.report.findings)),
                    "rejected_count": str(outcome.rejected_count),
                },
            )
        )
        return outcome

    async def terminalize_orphaned_run(
        self,
        run_id: str,
        *,
        status: RunStatus,
        stop_reason: str,
        error_kind: str,
        error_message: str,
    ) -> bool:
        """Terminalize a non-terminal review run whose durable job exhausted retries / cancelled.

        Called from the worker job's ``on_failed`` / ``on_cancelled`` hooks (and safe to call
        from a reconciler): when the ``review.run`` job reaches a terminal failed/cancelled state
        the associated queued/running run would otherwise linger admitted until its TTL. This
        claims the run and writes the terminal state **exactly once** (idempotent: an
        already-terminal run, a missing run, or a contended live lease is a no-op — the run TTL /
        expiry reconciler remains the ultimate backstop).
        """
        if status not in (RunStatus.failed, RunStatus.cancelled):
            raise ReviewValidationError("orphaned review run must terminalize failed/cancelled")
        record = await self._runs.get(run_id)
        if record is None or record.surface != REVIEW_SURFACE or record.is_terminal:
            return False
        lease = await self._runs.claim(
            run_id,
            worker_id=self._worker_id,
            now=self._clock(),
            lease_seconds=self._lease_seconds,
        )
        if lease is None:
            # Raced to terminal (no-op) or contended under a live lease (TTL/expiry backstop).
            return False
        await self._runs.terminalize(
            lease,
            status=status,
            stop_reason=stop_reason,
            now=self._clock(),
            error_kind=error_kind,
            error_message=error_message[:500],
        )
        self._audit.record(
            ReviewAuditEvent(
                ReviewAuditAction.review_failed,
                record.actor,
                record.org_id,
                run_id,
                {"project_id": record.org_id, "error_kind": error_kind},
            )
        )
        return True

    async def _release(self, lease: RunLease, *, cost: RunCost | None = None) -> None:
        """Transition a retryable run back to ``queued``, charging any partial usage/cost.

        On a retryable (transient) failure the run must not stay ``running`` under a stale
        fence: releasing it explicitly to ``queued`` (worker_id/lease_token cleared, version
        bumped) makes it promptly reclaimable by the next attempt, and durably charging the
        partial token/cost usage consumed before the failure means the next attempt gets only
        the remaining budget. Best-effort: if the lease was already lost/superseded the release
        simply no-ops (the lease also expires on its own).
        """
        release = getattr(self._runs, "release", None)
        if release is None:
            return
        try:
            await release(lease, to_status=RunStatus.queued, now=self._clock(), cost=cost)
        except Exception:  # noqa: BLE001 — release is best-effort; the lease also expires
            logger.debug("review lease release to queued failed run=%s", lease.run_id)

    async def _handle_lost_job_signal(
        self,
        *,
        lease: RunLease,
        run_id: str,
        record: RunRecord,
        request: ReviewRequest,
        error: BaseException | None,
        run_lease_lost: bool,
    ) -> None:
        """Resolve a cancellation / lost-job-lease signal, then re-raise it (never returns).

        * A **cancellation** while we still own the run fence terminalizes the run ``cancelled``
          atomically under the current lease (so it never lingers admitted/running), then
          propagates the cancellation untouched so the worker honours it.
        * A **lost job lease** while the run lease is still valid releases the run back to
          ``queued`` so it is promptly retryable — never leaving a live ``running`` lease behind;
          the durable job's hooks / reconciler then proceed. When the run lease is ALSO lost
          another worker owns the run, so no terminal/release write is attempted.

        Cleanup is best-effort and idempotent; the primary signal is always preserved.
        """
        if isinstance(error, JobCancellationRequested):
            if not run_lease_lost:
                await self._terminalize_cancelled(
                    lease, run_id=run_id, record=record, request=request
                )
            raise error from None
        # Lost job lease: the checkpoint raised a non-cancellation error (lease reclaimed).
        if not run_lease_lost:
            await self._release(lease)
        if isinstance(error, ReviewLeaseLost):
            raise error from None
        raise ReviewLeaseLost(f"review job lease lost during execution: {run_id}") from error

    async def _terminalize_cancelled(
        self, lease: RunLease, *, run_id: str, record: RunRecord, request: ReviewRequest
    ) -> None:
        """Best-effort terminalize the run ``cancelled`` under the current lease (idempotent)."""
        try:
            await self._runs.terminalize(
                lease,
                status=RunStatus.cancelled,
                stop_reason="review_cancelled",
                now=self._clock(),
                error_kind="review_cancelled",
                error_message="review cancelled",
            )
        except Exception:  # noqa: BLE001 — best-effort; hook/reconciler backstop, signal preserved
            logger.warning("review cancellation terminalization failed run=%s", run_id)
            return
        self._audit.record(
            ReviewAuditEvent(
                ReviewAuditAction.review_failed,
                record.actor,
                record.org_id,
                run_id,
                {"project_id": request.project_id, "error_kind": "review_cancelled"},
            )
        )

    # --- read path -------------------------------------------------------------------
    async def get_run(self, run_id: str) -> RunRecord:
        record = await self._runs.get(run_id)
        if record is None or record.surface != REVIEW_SURFACE:
            raise ReviewNotFound(f"review not found: {run_id}")
        return record

    async def get_run_optional(self, run_id: str) -> RunRecord | None:
        """Return the review run, or ``None`` if it is missing / not a review run."""
        record = await self._runs.get(run_id)
        if record is None or record.surface != REVIEW_SURFACE:
            return None
        return record

    async def stranded_review_run_ids(
        self, *, now: datetime | None = None, limit: int = 100, grace_seconds: int = 30
    ) -> list[str]:
        """Review runs admitted/queued but not yet leased, past a small grace — dispatch backstop.

        Used by the durable stranded-admission reconciler: a run whose ``review.run`` job/outbox
        intent was lost (the API returns 202 even if the in-line enqueue failed, relying on this
        backstop) is still ``admitted``/``queued`` with no worker lease. The reconciler
        idempotently (re-)creates its dispatch intent so it is never stranded until TTL.
        """
        return await self._runs.surface_pending_dispatch(
            REVIEW_SURFACE,
            now or self._clock(),
            limit,
            grace_seconds=grace_seconds,
        )

    def read_report(self, *, project_handle: str, run_id: str, json_sha256: str) -> ReviewReport:
        data = self._artifacts.read(
            ProjectId(project_handle), CodingRunId(worktree_storage_id(run_id)), json_sha256
        )
        return ReviewReport.from_dict(json.loads(data.decode("utf-8")))

    def read_report_safe(
        self, *, project_handle: str, run_id: str, json_sha256: str
    ) -> ReviewReport | None:
        """Read a completed review's report, returning ``None`` on a missing/corrupt artifact.

        Used by the list projection so a completed record surfaces its *actual* findings/severity/
        hash from the immutable report artifact, while a lost or corrupt artifact is reported
        honestly (the caller annotates it) instead of fabricating a zero-finding projection.
        """
        try:
            return self.read_report(
                project_handle=project_handle, run_id=run_id, json_sha256=json_sha256
            )
        except Exception:  # noqa: BLE001 — missing/corrupt artifact is reported, not fabricated
            logger.warning("review report artifact unreadable run=%s", run_id)
            return None

    def read_report_markdown(
        self, *, project_handle: str, run_id: str, markdown_sha256: str
    ) -> bytes:
        return self._artifacts.read(
            ProjectId(project_handle), CodingRunId(worktree_storage_id(run_id)), markdown_sha256
        )

    def build_record(
        self,
        record: RunRecord,
        *,
        request: ReviewRequest | None = None,
        report: ReviewReport | None = None,
    ) -> ReviewRecord:
        status = _STATUS_MAP[record.status]
        if report is None and request is None:
            # Fail closed rather than fabricate source/head/model defaults: a truthful
            # projection requires either a completed report or the persisted request metadata.
            raise ReviewNotFound(
                f"review metadata for {record.id} is not available; cannot project a record"
            )
        source = report.source if report else (request.source if request else ReviewSource.branch)
        head = report.head_sha if report else (request.head if request else "")
        base = report.base_sha if report else (request.base if request else None)
        model = report.model if report else (request.model if request else "")
        return ReviewRecord(
            review_id=record.id,
            org_id=record.org_id,
            project_id=report.project_id if report else (request.project_id if request else ""),
            run_id=record.id,
            status=status,
            source=source,
            head=head,
            base=base,
            model=model,
            created_at=record.created_at,
            updated_at=record.updated_at,
            finding_count=len(report.findings) if report else 0,
            severity_counts=report.severity_counts if report else {},
            prompt_tokens=record.prompt_tokens,
            completion_tokens=record.completion_tokens,
            cost_usd=record.cost_usd,
            report_json_sha256=record.result_ref,
            report_markdown_sha256=report.markdown_sha256 if report else None,
            error_kind=record.error_kind,
            error_message=record.error_message,
        )


def _error_kind(exc: BaseException) -> str:
    if isinstance(exc, ReviewError):
        return exc.__class__.__name__
    return "review_execution_error"


def _partial_cost(exc: BaseException) -> RunCost | None:
    """Extract partial token/cost usage carried on a retryable provider failure (or ``None``).

    The engine attaches the tokens/cost consumed before a transient stream/provider failure to
    :class:`ReviewProviderUnavailable`; charging it before release means the next attempt runs
    under only the remaining budget, so cumulative usage never exceeds the review envelope.
    """
    usage = getattr(exc, "usage", None)
    if not isinstance(usage, Usage):
        return None
    if usage.prompt_tokens <= 0 and usage.completion_tokens <= 0 and usage.cost_usd <= 0.0:
        return None
    return RunCost(
        prompt_tokens=max(0, usage.prompt_tokens),
        completion_tokens=max(0, usage.completion_tokens),
        cost_usd=max(0.0, usage.cost_usd),
        iterations=0,
    )


_PERMANENT_ERRORS = (
    ReviewValidationError,
    ReviewBoundsExceeded,
    ReviewEvidenceError,
    ReviewProviderError,
    ReviewNotFound,
    PermissionDenied,
)


def _is_retryable(exc: BaseException) -> bool:
    """Whether ``exc`` is a transient failure that must NOT terminalize the run.

    Transport/timeout/rate-limit provider failures and lease loss are retryable; a validation,
    bounds, evidence, contract, or authorization-revocation error is permanent. An unexpected
    (infra) error is treated as transient — the durable run's TTL/reconciler is the terminal
    safety net rather than an eager, possibly-spurious failure.
    """
    if isinstance(exc, ReviewProviderUnavailable | ReviewLeaseLost):
        return True
    if isinstance(exc, _PERMANENT_ERRORS):
        return False
    return True


@dataclass
class _LeaseKeeper:
    """Renews a run lease before expiry; flags the lease lost on any failed renewal.

    Mirrors the interactive run keeper: a ``False`` return (reclaimed) or any exception marks
    the lease lost so the review is abandoned without a terminal write under a stale fence.
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
                except Exception:  # noqa: BLE001 — any renew failure is a lost lease (fail closed)
                    logger.warning(
                        "review lease renewal raised; lease lost run=%s", self.lease.run_id
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
            except Exception:  # noqa: BLE001 — keeper cleanup must not mask the primary outcome
                logger.warning("review lease keeper cleanup error suppressed")


@dataclass
class _JobHeartbeatKeeper:
    """Periodically heartbeats the durable JOB lease; cancels the review on a lost lease.

    Mirrors :class:`_LeaseKeeper` but for the worker's ``JobContext.checkpoint`` (the durable job
    lease, distinct from the run lease). If a checkpoint raises (job lease reclaimed by another
    worker, or a cancellation was requested) the bound review task is cancelled so the current
    worker abandons its effects without a terminal write under a superseded fence.
    """

    checkpoint: Callable[[], Awaitable[None]]
    interval_seconds: float
    lost: bool = False
    error: BaseException | None = None
    _target: asyncio.Task[Any] | None = None
    _task: asyncio.Task[None] | None = None

    def bind(self, target: asyncio.Task[Any]) -> None:
        self._target = target

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.interval_seconds)
                try:
                    await self.checkpoint()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 — any checkpoint failure is a lost lease
                    logger.warning("review job checkpoint failed; job lease lost")
                    self.lost = True
                    self.error = exc
                    if self._target is not None:
                        self._target.cancel()
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
            except Exception:  # noqa: BLE001 — keeper cleanup must not mask the primary outcome
                logger.warning("review job heartbeat cleanup error suppressed")


def _safe_error_message(exc: BaseException) -> str:
    # Domain errors carry safe, human messages; anything else is reduced to its type so a
    # provider/storage failure never leaks a token, a path, or source content.
    if isinstance(exc, ReviewError):
        return str(exc)[:500]
    return exc.__class__.__name__


__all__ = [
    "DEFAULT_REVIEW_AGENT_ID",
    "DEFAULT_REVIEW_LEASE_SECONDS",
    "DEFAULT_REVIEW_TTL_SECONDS",
    "REVIEW_SURFACE",
    "ReviewCoordinator",
    "ReviewHandle",
    "review_fingerprint",
]
