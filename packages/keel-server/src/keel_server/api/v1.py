"""REST API v1 (``/v1``) — session runs, SSE event stream, approvals (WS-E).

Evolution policy (DESIGN-REVIEW G14): ``/v1`` is additive-only.

- ``POST /sessions/{id}/messages`` durably admits input and launches a run.
- ``GET  /sessions/{id}/events``   streams the event log as SSE (replayable via
  ``after=``, then live). The client closes the stream when it sees ``run.ended``.
- ``POST /approvals/{id}``         resolves a pending tool approval.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse

from keel_core.api import ApprovalResolution, CreateMessageRequest, CreateMessageResponse
from keel_core.approvals import ApprovalStore
from keel_core.gmail import GMAIL_CONNECTOR_ID, GMAIL_SCOPES
from keel_core.search import search_sessions
from keel_core.state import PostgresEventStore, list_sessions
from keel_core.tokens import delete_token, list_connected
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


@router.post("/runs/{run_id}/interrupt", summary="Interrupt a running agent run")
async def interrupt_run(run_id: str, request: Request) -> dict[str, bool]:
    """Ask an in-flight run to stop at its next iteration (StopReason.interrupted)."""
    runtime = _runtime(request)
    return {"ok": runtime.interrupt_run(run_id)}


@router.get("/schedules", summary="List the scope's schedules (management view)")
async def list_schedules(request: Request) -> list[dict[str, object]]:
    engine = getattr(request.app.state, "engine", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    if engine is None:
        return []
    from keel_scheduler.store import PostgresScheduleStore

    store = PostgresScheduleStore(engine, scope)
    return [
        {
            "id": r.id,
            "agent_id": r.agent_id,
            "trigger_kind": r.trigger_kind,
            "spec": r.spec,
            "interval_s": r.interval_s,
            "enabled": r.enabled,
            "next_run_at": r.next_run_at.isoformat(),
            "last_run_at": r.last_run_at.isoformat() if r.last_run_at else None,
            "last_status": r.last_status,
        }
        for r in await store.list_all()
    ]


@router.post("/schedules/{schedule_id}/toggle", summary="Pause/resume a schedule")
async def toggle_schedule(
    schedule_id: str, request: Request, body: dict[str, Any]
) -> dict[str, object]:
    engine = getattr(request.app.state, "engine", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    if engine is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "datastore unavailable")
    from keel_scheduler.store import PostgresScheduleStore

    enabled = bool(body.get("enabled", True))
    ok = await PostgresScheduleStore(engine, scope).set_enabled(schedule_id, enabled)
    return {"ok": ok, "enabled": enabled}


@router.post("/schedules/{schedule_id}/run", summary="Trigger a schedule's run now")
async def run_schedule(schedule_id: str, request: Request) -> dict[str, bool]:
    enqueue = getattr(request.app.state, "enqueue", None)
    if enqueue is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "job queue unavailable")
    await enqueue("run_agent", schedule_id)
    return {"ok": True}


# Suggested Copilot models; gpt-5.3-codex works via the Responses API path (A1).
_AVAILABLE_MODELS = [
    "github_copilot/claude-sonnet-4.5",
    "github_copilot/claude-opus-4.5",
    "github_copilot/gpt-4o",
    "github_copilot/gpt-4.1",
    "github_copilot/gemini-2.5-pro",
    "github_copilot/gpt-5.3-codex",
]


@router.get("/settings/model", summary="Current model + suggested choices")
async def get_model(request: Request) -> dict[str, object]:
    runtime = _runtime(request)
    current = runtime.model
    available = _AVAILABLE_MODELS if current in _AVAILABLE_MODELS else [current, *_AVAILABLE_MODELS]
    return {"current": current, "available": available}


@router.put("/settings/model", summary="Switch the model for subsequent runs")
async def set_model(request: Request, body: dict[str, Any]) -> dict[str, object]:
    model = str(body.get("model", "")).strip()
    if not model:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "model is required")
    runtime = _runtime(request)
    runtime.set_model(model)
    return {"ok": True, "current": model}


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
        for info in await list_connected(engine, scope):
            connected[info.connector_id] = info.updated_at.isoformat() if info.updated_at else None
    return [
        {**c, "connected": str(c["id"]) in connected, "updated_at": connected.get(str(c["id"]))}
        for c in CONNECTOR_CATALOG
    ]


@router.delete("/connectors/{connector_id}", summary="Revoke a connector's stored token")
async def revoke_connector(connector_id: str, request: Request) -> dict[str, bool]:
    """Delete the scope's stored token for a connector (revoke access)."""
    engine = getattr(request.app.state, "engine", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    revoked = engine is not None and await delete_token(engine, scope, connector_id)
    return {"ok": bool(revoked)}


@router.get("/sessions", summary="List the scope's sessions (newest first)")
async def list_sessions_endpoint(request: Request) -> list[dict[str, object]]:
    """Session summaries for the Sessions list (title preview + message count)."""
    engine = getattr(request.app.state, "engine", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    if engine is None:
        return []
    return [
        {
            "id": s.id,
            "title": s.title,
            "messages": s.messages,
            "created_at": s.created_at.isoformat(),
            "updated_at": s.updated_at.isoformat(),
        }
        for s in await list_sessions(engine, scope)
    ]


@router.get("/sessions/search", summary="Search the scope's sessions (lexical hybrid)")
async def search_sessions_endpoint(request: Request, q: str = Query("")) -> list[dict[str, object]]:
    """Rank sessions by a trigram ⊕ FTS match over their messages, with a snippet."""
    engine = getattr(request.app.state, "engine", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    if engine is None or not q.strip():
        return []
    return [
        {
            "id": h.id,
            "title": h.title,
            "snippet": h.snippet,
            "messages": h.messages,
            "updated_at": h.updated_at.isoformat() if h.updated_at else None,
        }
        for h in await search_sessions(engine, scope, q)
    ]


@router.get("/sessions/{session_id}/history", summary="Durable event history for a session")
async def session_history(session_id: str, request: Request) -> list[dict[str, object]]:
    """The session's durable event log (oldest first) for a read-only replay."""
    engine = getattr(request.app.state, "engine", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    if engine is None:
        return []
    store = PostgresEventStore(engine, scope)
    return [event.model_dump(mode="json") async for event in store.read(session_id)]
