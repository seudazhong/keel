"""REST API v1 (``/v1``) — session runs, SSE event stream, approvals (WS-E).

Evolution policy (DESIGN-REVIEW G14): ``/v1`` is additive-only.

- ``POST /sessions/{id}/messages`` durably admits input and launches a run.
- ``GET  /sessions/{id}/events``   streams the event log as SSE (replayable via
  ``after=``, then live). The client closes the stream when it sees ``run.ended``.
- ``POST /approvals/{id}``         resolves a pending tool approval.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse

from keel_core.api import ApprovalResolution, CreateMessageRequest, CreateMessageResponse
from keel_core.approvals import ApprovalStore
from keel_core.gmail import GMAIL_CONNECTOR_ID, GMAIL_SCOPES
from keel_core.types import PermissionDecision
from keel_server.runtime import AgentRuntime

router = APIRouter(prefix="/v1", tags=["v1"])


def _runtime(request: Request) -> AgentRuntime:
    runtime: AgentRuntime | None = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "runtime unavailable")
    return runtime


def _durable_approvals(request: Request) -> tuple[ApprovalStore, str]:
    store: ApprovalStore | None = getattr(request.app.state, "durable_approvals", None)
    if store is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "durable approvals unavailable")
    scope: str = getattr(request.app.state, "durable_scope", "web:local")
    return store, scope


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
            data = f"data: {event.model_dump_json()}\n\n"
            # Partial deltas carry no durable seq; only real events advance the cursor.
            yield f"id: {event.seq}\n{data}" if event.seq else data

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


@router.get("/approvals", summary="List durable approvals for the current scope")
async def list_approvals(
    request: Request, status_filter: str = Query("pending", alias="status")
) -> list[dict[str, object]]:
    """Durable approvals raised by unattended runs (pending queue by default)."""
    store, scope = _durable_approvals(request)
    rows = await store.list_pending(scope) if status_filter == "pending" else []
    return [
        {
            "id": r.id,
            "run_id": r.run_id,
            "session_id": r.session_id,
            "tool": r.tool,
            "args": r.args,
            "call_id": r.call_id,
            "reason": r.reason,
            "status": r.status,
            "created_at": r.created_at.isoformat(),
            "expires_at": r.expires_at.isoformat(),
        }
        for r in rows
    ]


async def _resolve_durable(request: Request, approval_id: str, decision: str) -> dict[str, bool]:
    store, _ = _durable_approvals(request)
    ok = await store.resolve(approval_id, decision, "web")
    if ok:
        record = await store.get(approval_id)
        enqueue = getattr(request.app.state, "enqueue", None)
        if record is not None and enqueue is not None:
            # Continue the suspended run: resume executes-or-denies the gated call (G5).
            await enqueue("resume_run", record.session_id, record.run_id, record.scope_id)
    return {"ok": ok}


@router.post("/approvals/{approval_id}/approve", summary="Approve a durable approval")
async def approve_durable(approval_id: str, request: Request) -> dict[str, bool]:
    return await _resolve_durable(request, approval_id, "granted")


@router.post("/approvals/{approval_id}/reject", summary="Reject a durable approval")
async def reject_durable(approval_id: str, request: Request) -> dict[str, bool]:
    return await _resolve_durable(request, approval_id, "denied")


def _short_scope(scope: str) -> str:
    return scope.rsplit("/", 1)[-1]


# Static catalog of known connectors; connection status is joined per-scope at request time.
CONNECTOR_CATALOG: list[dict[str, object]] = [
    {
        "id": GMAIL_CONNECTOR_ID,
        "name": "Gmail",
        "icon": "✉️",
        "kind": "oauth",
        "scopes": [_short_scope(s) for s in GMAIL_SCOPES],
    },
]


@router.get("/connectors", summary="List connectors and connection status for the scope")
async def list_connectors(request: Request) -> list[dict[str, object]]:
    """Known connectors joined with per-scope connection status (no token decryption)."""
    engine = getattr(request.app.state, "engine", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    connected: dict[str, str | None] = {}
    if engine is not None:
        from keel_core.tokens import list_connected

        for info in await list_connected(engine, scope):
            connected[info.connector_id] = info.updated_at.isoformat() if info.updated_at else None
    return [
        {**c, "connected": str(c["id"]) in connected, "updated_at": connected.get(str(c["id"]))}
        for c in CONNECTOR_CATALOG
    ]
