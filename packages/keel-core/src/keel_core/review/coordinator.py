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

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from keel_core.coding.models import CodingRunId, ProjectId
from keel_core.coding.protocols import ArtifactStore
from keel_core.projects.service import ProjectService
from keel_core.projects.storage import worktree_storage_id
from keel_core.runs import (
    RunBudgetSpec,
    RunCost,
    RunRecord,
    RunStatus,
    RunStore,
)
from keel_core.types import ScopeId

from .audit import LoggingReviewAuditSink, ReviewAuditAction, ReviewAuditEvent, ReviewAuditSink
from .errors import ReviewError, ReviewNotFound
from .models import (
    ReviewId,
    ReviewRecord,
    ReviewReport,
    ReviewRequest,
    ReviewSource,
    ReviewStatus,
    new_review_id,
)
from .service import ReviewOutcome, ReviewService

REVIEW_SURFACE = "review"
DEFAULT_REVIEW_AGENT_ID = "review"
DEFAULT_REVIEW_TTL_SECONDS = 3600
DEFAULT_REVIEW_LEASE_SECONDS = 900

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

    # --- execution path (worker) -----------------------------------------------------
    async def execute_review(self, request: ReviewRequest, *, run_id: str) -> ReviewOutcome | None:
        """Claim + run a durably-admitted review. Idempotent/restart-safe."""
        record = await self._runs.get(run_id)
        if record is None:
            raise ReviewNotFound(f"review run not found: {run_id}")
        if record.is_terminal:
            return None
        # Re-authorize at claim time: access revoked since request fails closed.
        project = await self._projects.authorize_review(
            record.org_id, record.actor, request.project_id, agent_id=request.agent_id
        )
        await self._runs.mark_queued(run_id, now=self._clock())
        lease = await self._runs.claim(
            run_id,
            worker_id=self._worker_id,
            now=self._clock(),
            lease_seconds=self._lease_seconds,
        )
        if lease is None:
            return None
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
        try:
            outcome = await self._reviews.review(
                request,
                run_id=run_id,
                coding_run_id=coding_run_id,
                project_handle=handle,
                review_id=run_id,
                now=self._clock(),
            )
        except Exception as exc:
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
