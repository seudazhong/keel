"""REST API v0 (``/v1``) — contract stubs (no behaviour until M1).

Routes are declared with their frozen request/response models so the OpenAPI
schema (and the SDK generated from it) reflect the contract now. Handlers
return HTTP 501 until the loop, state and approvals land in M1.

Evolution policy (DESIGN-REVIEW G14): ``/v1`` is additive-only.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from keel_core.api import ApprovalResolution, CreateMessageRequest, CreateMessageResponse

router = APIRouter(prefix="/v1", tags=["v1"])

_STUB = "contract stub — implemented in M1"


@router.post(
    "/sessions/{session_id}/messages",
    response_model=CreateMessageResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Admit a user message and schedule a run",
)
async def create_message(session_id: str, body: CreateMessageRequest) -> CreateMessageResponse:
    """Durably admit input (FR-C5) and return a ``run_id``."""
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, _STUB)


@router.get(
    "/sessions/{session_id}/events",
    summary="Stream session events (SSE, replayable via after=)",
)
async def stream_events(session_id: str, after: int | None = None) -> None:
    """Replayable event stream; ``after`` is the last-seen ``seq`` cursor."""
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, _STUB)


@router.post(
    "/approvals/{approval_id}",
    summary="Resolve a pending approval",
)
async def resolve_approval(approval_id: str, body: ApprovalResolution) -> None:
    """Resolve a bus-mediated approval (allow/ask/deny)."""
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, _STUB)
