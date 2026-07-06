"""REST API v0 data-transfer objects (ARCHITECTURE §9.2).

Versioned under ``/v1`` and evolved additively only (DESIGN-REVIEW G14). These
DTOs freeze the request/response shapes; the server mounts stub routes so the
OpenAPI schema (and generated SDK) reflect the contract in M0.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

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
