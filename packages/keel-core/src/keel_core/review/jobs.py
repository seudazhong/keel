"""Worker-agnostic durable review job (WS-R).

A read-only review is a durable, restart-safe, idempotent background job on the existing
jobs/outbox substrate: the API creates the run + enqueues one ``review.run`` job keyed by the
review's idempotency key, so a crash mid-review retries and a duplicate request is a no-op.
This module owns the payload contract + handler; a thin worker adapter wraps it in a
``JobDefinition`` (mirroring :mod:`keel_core.projects.jobs`).
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
)

from keel_core.jobs import JobResult, PermanentJobError, RetryableJobError

from .coordinator import ReviewCoordinator
from .errors import (
    ReviewBoundsExceeded,
    ReviewEvidenceError,
    ReviewNotFound,
    ReviewProviderError,
    ReviewValidationError,
)
from .models import (
    DEFAULT_MAX_DIFF_BYTES,
    MAX_FINDINGS,
    ReviewRequest,
    ReviewSource,
)

REVIEW_RUN_KIND = "review.run"
REVIEW_RUN_MAX_ATTEMPTS = 3


def review_idempotency_key(review_id: str) -> str:
    """A stable job idempotency key so a retried enqueue schedules at most one review job."""
    return f"review.run:{review_id}"


class ReviewJobPayload(BaseModel):
    """The durable payload carried by a ``review.run`` job."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True, strict=True)

    org_id: StrictStr
    project_id: StrictStr
    run_id: StrictStr
    source: StrictStr
    head: StrictStr
    base: StrictStr | None = None
    model: StrictStr
    agent_id: StrictStr | None = None
    idempotency_key: StrictStr
    max_findings: StrictInt = Field(default=MAX_FINDINGS)
    max_diff_bytes: StrictInt = Field(default=DEFAULT_MAX_DIFF_BYTES)

    @field_validator("source")
    @classmethod
    def _known_source(cls, value: str) -> str:
        if value not in {source.value for source in ReviewSource}:
            raise ValueError("unknown review source")
        return value

    def to_request(self) -> ReviewRequest:
        return ReviewRequest(
            org_id=self.org_id,
            project_id=self.project_id,
            source=ReviewSource(self.source),
            head=self.head,
            base=self.base,
            model=self.model,
            agent_id=self.agent_id,
            idempotency_key=self.idempotency_key,
            max_findings=self.max_findings,
            max_diff_bytes=self.max_diff_bytes,
        )

    @classmethod
    def from_request(cls, request: ReviewRequest, *, run_id: str) -> ReviewJobPayload:
        return cls(
            org_id=request.org_id,
            project_id=request.project_id,
            run_id=run_id,
            source=request.source.value,
            head=request.head,
            base=request.base,
            model=request.model,
            agent_id=request.agent_id,
            idempotency_key=request.idempotency_key,
            max_findings=request.max_findings,
            max_diff_bytes=request.max_diff_bytes,
        )


@runtime_checkable
class ReviewJobContext(Protocol):
    """The subset of the worker job context the review handler needs."""

    @property
    def scope_id(self) -> str: ...

    @property
    def job_id(self) -> str: ...

    async def checkpoint(self) -> None: ...


# Errors that mean "this review can never succeed" — terminal, no retry.
_PERMANENT = (
    ReviewValidationError,
    ReviewBoundsExceeded,
    ReviewEvidenceError,
    ReviewProviderError,
    ReviewNotFound,
)


class ReviewJobHandlers:
    """Durable handler that executes a read-only review idempotently."""

    def __init__(self, coordinator: ReviewCoordinator) -> None:
        self._coordinator = coordinator

    async def run(self, context: ReviewJobContext, raw_payload: dict[str, Any]) -> JobResult:
        try:
            payload = ReviewJobPayload.model_validate(raw_payload)
        except ValidationError as exc:
            raise PermanentJobError(
                "invalid_review_payload", "Review job payload is invalid."
            ) from exc
        await context.checkpoint()
        try:
            outcome = await self._coordinator.execute_review(
                payload.to_request(), run_id=payload.run_id
            )
        except _PERMANENT as exc:
            raise PermanentJobError("review_failed", "Review failed.") from exc
        except Exception as exc:  # transient storage/provider/infra — allow retry.
            raise RetryableJobError("review_transient", "Review temporarily failed.") from exc
        if outcome is None:
            return JobResult(
                data={"run_id": payload.run_id, "status": "already_terminal"},
                message="review already terminal",
            )
        return JobResult(
            data={
                "run_id": payload.run_id,
                "review_id": outcome.review_id,
                "status": "completed",
                "finding_count": len(outcome.report.findings),
                "rejected_count": outcome.rejected_count,
                "report_json_sha256": outcome.json_sha256,
                "report_markdown_sha256": outcome.markdown_sha256,
            },
            message=f"review completed with {len(outcome.report.findings)} finding(s)",
        )


__all__ = [
    "REVIEW_RUN_KIND",
    "REVIEW_RUN_MAX_ATTEMPTS",
    "ReviewJobContext",
    "ReviewJobHandlers",
    "ReviewJobPayload",
    "review_idempotency_key",
]
