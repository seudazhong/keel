"""REST API v0 data-transfer objects (ARCHITECTURE §9.2).

Versioned under ``/v1`` and evolved additively only (DESIGN-REVIEW G14). These
DTOs freeze the request/response shapes; the server mounts stub routes so the
OpenAPI schema (and generated SDK) reflect the contract in M0.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .jobs import JobRecord, JobStatus
from .types import PermissionDecision, RunId, SessionId


class CreateMessageRequest(BaseModel):
    """Admit a user message into a session (durable admission, FR-C5)."""

    content: str
    # At-most-once admission for retried surfaces (mirrors outbound idempotency).
    idempotency_key: str | None = None


class CreateMessageResponse(BaseModel):
    """Acknowledgement that input was admitted and a run scheduled."""

    session_id: SessionId
    run_id: RunId
    accepted: bool = True


class ApprovalResolution(BaseModel):
    """Resolve a pending approval (bus-mediated, correlation-id)."""

    approval_id: str
    decision: PermissionDecision


class HealthResponse(BaseModel):
    """Liveness payload."""

    status: str = "ok"
    service: str
    version: str


class ReadinessResponse(BaseModel):
    """Readiness payload with per-dependency checks."""

    ready: bool
    checks: dict[str, str] = Field(default_factory=dict)


class JobResponse(BaseModel):
    """Strict public read model for a durable background job."""

    model_config = ConfigDict(extra="forbid")

    id: str
    kind: str
    status: JobStatus
    target_session_id: str | None
    attempt: int
    max_attempts: int
    next_attempt_at: datetime
    lease_expires_at: datetime | None
    cancel_requested: bool
    progress_current: int
    progress_total: int | None
    progress_message: str | None
    progress_updated_at: datetime | None
    result: dict[str, Any] | None
    result_message: str | None
    error_kind: str | None
    error_message: str | None
    injected_event_seq: int | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

    @classmethod
    def from_record(cls, record: JobRecord) -> JobResponse:
        return cls(
            id=record.id,
            kind=record.kind,
            status=record.status,
            target_session_id=record.target_session_id,
            attempt=record.attempt,
            max_attempts=record.max_attempts,
            next_attempt_at=record.next_attempt_at,
            lease_expires_at=record.lease_expires_at,
            cancel_requested=record.cancel_requested_at is not None,
            progress_current=record.progress_current,
            progress_total=record.progress_total,
            progress_message=record.progress_message,
            progress_updated_at=record.progress_updated_at,
            result=record.result,
            result_message=record.result_message,
            error_kind=record.error_kind,
            error_message=record.error_message,
            injected_event_seq=record.injected_event_seq,
            created_at=record.created_at,
            updated_at=record.updated_at,
            started_at=record.started_at,
            finished_at=record.finished_at,
        )
