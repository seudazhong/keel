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
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from keel_core.coding.models import CodingRunId, ProjectId
from keel_core.coding.protocols import ArtifactStore
from keel_core.errors import DuplicateEventError, PermissionDenied
from keel_core.events import Event, EventType
from keel_core.projects.service import ProjectService
from keel_core.projects.storage import worktree_storage_id
from keel_core.protocols import EventStore
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
    """Immutable binding of a review request so a reused idempotency key can't be hijacked."""
    payload = json.dumps(
        {
            "org_id": request.org_id,
            "project_id": request.project_id,
            "source": request.source.value,
            "head": request.head,
            "base": request.base or "",
            "model": request.model,
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
                    "org_id": request.org_id,
                    "project_id": request.project_id,
                    "source": request.source.value,
                    "head": request.head,
                    "base": request.base,
                    "model": request.model,
                    "agent_id": request.agent_id,
                    "max_findings": request.max_findings,
                    "max_diff_bytes": request.max_diff_bytes,
                },
                "dedup_key": f"review-meta:{run_id}",
            },
        )
        try:
            await self._events.append(event)
        except DuplicateEventError:
            pass

    async def load_request_metadata(self, run_id: str) -> ReviewRequest | None:
        """Reconstruct the durably-persisted review request for ``run_id`` (or ``None``)."""
        if self._events is None:
            return None
        async for event in self._events.read(run_id):
            meta = event.payload.get(REVIEW_REQUEST_MARKER)
            if isinstance(meta, dict):
                try:
                    return ReviewRequest(
                        org_id=str(meta["org_id"]),
                        project_id=str(meta["project_id"]),
                        source=ReviewSource(str(meta["source"])),
                        head=str(meta["head"]),
                        base=str(meta["base"]) if meta.get("base") is not None else None,
                        model=str(meta["model"]),
                        idempotency_key=f"review-meta:{run_id}",
                        agent_id=str(meta["agent_id"]) if meta.get("agent_id") else None,
                        max_findings=int(meta.get("max_findings", 0)) or 1,
                        max_diff_bytes=int(meta.get("max_diff_bytes", 0)) or 1,
                    )
                except (KeyError, ValueError, ReviewError):
                    return None
        return None

    async def build_review_record(
        self, record: RunRecord, *, report: ReviewReport | None = None
    ) -> ReviewRecord:
        """A truthful status projection: report (completed) or persisted request metadata."""
        request = None if report is not None else await self.load_request_metadata(record.id)
        return self.build_record(record, request=request, report=report)

    # --- execution path (worker) -----------------------------------------------------
    async def execute_review(self, request: ReviewRequest, *, run_id: str) -> ReviewOutcome | None:
        """Claim + run a durably-admitted review. Idempotent/restart-safe."""
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
        # Re-authorize at claim time UNDER the lease: access revoked since admission fails closed.
        # A revocation must terminalize the run durably (cancelled) before the job fails
        # permanently, so the run never lingers admitted until its TTL and no reclaim can run it.
        try:
            project = await self._projects.authorize_review(
                record.org_id, record.actor, request.project_id, agent_id=request.agent_id
            )
        except PermissionDenied as exc:
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
                    {"project_id": request.project_id, "error_kind": "authorization_revoked"},
                )
            )
            raise
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
        # Renew the lease well before expiry so a long review keeps its fence; a lost renewal
        # trips ``keeper.lost`` and the run is abandoned without terminalizing (reclaimable).
        keeper = _LeaseKeeper(
            run_store=self._runs,
            lease=lease,
            interval_seconds=max(1.0, self._lease_seconds / 3),
        )
        keeper.start()
        try:
            try:
                plan = await build_materialization_plan(
                    request,
                    default_branch=getattr(project, "default_branch", None),
                    pr_resolver=self._pr_resolver,
                )
                outcome = await self._reviews.review(
                    request,
                    run_id=run_id,
                    coding_run_id=coding_run_id,
                    project_handle=handle,
                    review_id=run_id,
                    now=self._clock(),
                    plan=plan,
                )
            except Exception as exc:
                if keeper.lost:
                    # Lease lost mid-flight: another worker may own the run. Abort all effects
                    # WITHOUT terminalizing (contention is never a terminal success).
                    raise ReviewLeaseLost(f"review lease lost during execution: {run_id}") from exc
                if _is_retryable(exc):
                    # Transient provider/infra failure: do NOT terminalize; the durable job
                    # retries until success or attempts are exhausted. Release the lease so the
                    # run is promptly reclaimable.
                    await self._release(lease)
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

    async def _release(self, lease: RunLease) -> None:
        """Transition a retryable run back to ``queued`` and clear/fence its lease.

        On a retryable (transient) failure the run must not stay ``running`` under a stale
        fence: releasing it explicitly to ``queued`` (worker_id/lease_token cleared, version
        bumped) makes it promptly reclaimable by the next attempt. Best-effort: if the lease was
        already lost/superseded the release simply no-ops (the lease also expires on its own).
        """
        release = getattr(self._runs, "release", None)
        if release is None:
            return
        try:
            await release(lease, to_status=RunStatus.queued, now=self._clock())
        except Exception:  # noqa: BLE001 — release is best-effort; the lease also expires
            logger.debug("review lease release to queued failed run=%s", lease.run_id)

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

    def read_report(self, *, project_handle: str, run_id: str, json_sha256: str) -> ReviewReport:
        data = self._artifacts.read(
            ProjectId(project_handle), CodingRunId(worktree_storage_id(run_id)), json_sha256
        )
        return ReviewReport.from_dict(json.loads(data.decode("utf-8")))

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
