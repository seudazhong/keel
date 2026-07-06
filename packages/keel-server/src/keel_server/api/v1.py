"""REST API v1 (``/v1``) — session runs, SSE event stream, approvals (WS-E).

Evolution policy (DESIGN-REVIEW G14): ``/v1`` is additive-only.

- ``POST /sessions/{id}/messages`` durably admits input and launches a run.
- ``GET  /sessions/{id}/events``   streams the event log as SSE (replayable via
  ``after=``, then live). The client closes the stream when it sees ``run.ended``.
- ``POST /approvals/{id}``         resolves a pending tool approval.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import StreamingResponse

from keel_core.api import ApprovalResolution, CreateMessageRequest, CreateMessageResponse
from keel_core.types import PermissionDecision
from keel_server.runtime import AgentRuntime

router = APIRouter(prefix="/v1", tags=["v1"])


def _runtime(request: Request) -> AgentRuntime:
    runtime: AgentRuntime | None = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "runtime unavailable")
    return runtime


@router.post(
    "/sessions/{session_id}/messages",
    response_model=CreateMessageResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Admit a user message and schedule a run",
)
async def create_message(
    session_id: str, body: CreateMessageRequest, request: Request
) -> CreateMessageResponse:
    """Durably admit input (FR-C5) and launch a run; return its ``run_id``."""
    runtime = _runtime(request)
    run_id = await runtime.admit_and_run(session_id, body.content)
    return CreateMessageResponse(session_id=session_id, run_id=run_id)


@router.get(
    "/sessions/{session_id}/events",
    summary="Stream session events (SSE, replayable via after=)",
)
async def stream_events(
    session_id: str, request: Request, after: int | None = None
) -> StreamingResponse:
    """Server-Sent Events: replay from ``after`` then follow the run live."""
    runtime = _runtime(request)

    async def _events() -> AsyncIterator[str]:
        async for event in runtime.tail(session_id, after):
            if await request.is_disconnected():
                break
            yield f"id: {event.seq}\ndata: {event.model_dump_json()}\n\n"

    return StreamingResponse(
        _events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post(
    "/approvals/{approval_id}",
    summary="Resolve a pending approval",
)
async def resolve_approval(
    approval_id: str, body: ApprovalResolution, request: Request
) -> dict[str, bool]:
    """Resolve a pending tool approval (allow -> run the tool, deny -> refuse)."""
    runtime = _runtime(request)
    approved = body.decision is PermissionDecision.allow
    resolved = runtime.resolve_approval(approval_id, approved)
    if not resolved:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown or already-resolved approval")
    return {"resolved": True, "approved": approved}
