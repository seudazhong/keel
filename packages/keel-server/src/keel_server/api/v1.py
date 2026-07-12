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

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse

from keel_core.api import ApprovalResolution, CreateMessageRequest, CreateMessageResponse
from keel_core.approvals import ApprovalStore
from keel_core.consolidation import (
    MemoryProposal,
    MemoryProposalStore,
    ProposalOutcome,
    ProposalResolution,
    consolidation_schedule_id,
)
from keel_core.gmail import GMAIL_CONNECTOR_ID, GMAIL_SCOPES
from keel_core.search import hybrid_search_sessions
from keel_core.state import PostgresEventStore, list_sessions
from keel_core.tokens import delete_token, list_connected
from keel_core.types import PermissionDecision
from keel_server.auth import Role, require_role
from keel_server.runtime import AgentRuntime

# Baseline authorization: every /v1 route needs at least `viewer`. In open mode (no
# KEEL_API_KEYS) that resolves to an implicit admin, so single-user stays unauthenticated;
# with keys configured, reads need viewer while mutations/admin raise the bar per-route.
router = APIRouter(prefix="/v1", tags=["v1"], dependencies=[Depends(require_role(Role.viewer))])


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
    dependencies=[Depends(require_role(Role.operator))],
)
async def create_message(
    session_id: str, body: CreateMessageRequest, request: Request
) -> CreateMessageResponse:
    """Durably admit input (FR-C5) and launch a run; return its ``run_id``."""
    runtime = _runtime(request)
    run_id = await runtime.admit_and_run(session_id, body.content)
    return CreateMessageResponse(session_id=session_id, run_id=run_id)


@router.post(
    "/runs/{run_id}/interrupt",
    summary="Interrupt a running agent run",
    dependencies=[Depends(require_role(Role.operator))],
)
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


@router.post(
    "/schedules/{schedule_id}/toggle",
    summary="Pause/resume a schedule",
    dependencies=[Depends(require_role(Role.operator))],
)
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


@router.post(
    "/schedules/{schedule_id}/run",
    summary="Trigger a schedule's run now",
    dependencies=[Depends(require_role(Role.operator))],
)
async def run_schedule(schedule_id: str, request: Request) -> dict[str, bool]:
    enqueue = getattr(request.app.state, "enqueue", None)
    if enqueue is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "job queue unavailable")
    await enqueue("run_agent", schedule_id)
    return {"ok": True}


@router.get(
    "/admin/overview",
    summary="Scope-wide counts + token/cost totals (admin dashboard)",
    dependencies=[Depends(require_role(Role.admin))],
)
async def admin_overview(request: Request) -> dict[str, object]:
    engine = getattr(request.app.state, "engine", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    if engine is None:
        return {
            "sessions": 0,
            "schedules": {"total": 0, "enabled": 0},
            "approvals": {"pending": 0, "granted": 0, "denied": 0, "expired": 0},
            "connectors": 0,
            "usage": {
                "runs": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "cache_read_tokens": 0,
                "cost_usd": 0.0,
            },
        }
    from keel_core.admin import compute_overview

    return await compute_overview(engine, scope)


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


@router.put(
    "/settings/model",
    summary="Switch the model for subsequent runs",
    dependencies=[Depends(require_role(Role.operator))],
)
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
    dependencies=[Depends(require_role(Role.operator))],
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


@router.post(
    "/approvals/{approval_id}/approve",
    summary="Approve a durable approval",
    dependencies=[Depends(require_role(Role.operator))],
)
async def approve_durable(approval_id: str, request: Request) -> dict[str, bool]:
    return await _resolve_durable(request, approval_id, "granted")


@router.post(
    "/approvals/{approval_id}/reject",
    summary="Reject a durable approval",
    dependencies=[Depends(require_role(Role.operator))],
)
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


@router.delete(
    "/connectors/{connector_id}",
    summary="Revoke a connector's stored token",
    dependencies=[Depends(require_role(Role.operator))],
)
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


@router.get("/sessions/search", summary="Search the scope's sessions (hybrid recall)")
async def search_sessions_endpoint(
    request: Request,
    response: Response,
    q: str = Query(""),
) -> list[dict[str, object]]:
    """Rank sessions by lexical + semantic RRF, with explicit degradation mode."""
    engine = getattr(request.app.state, "engine", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    runtime = getattr(request.app.state, "runtime", None)
    embedder = getattr(runtime, "embedder", None)
    batch_size = int(getattr(runtime, "session_embedding_batch_size", 64))
    catchup_limit = int(getattr(runtime, "session_embedding_catchup_limit", 500))
    if engine is None or not q.strip():
        response.headers["X-Keel-Search-Mode"] = "hybrid" if embedder is not None else "lexical"
        return []

    hits, recall_status = await hybrid_search_sessions(
        engine,
        scope,
        q,
        embedder=embedder,
        batch_size=batch_size,
        catchup_limit=catchup_limit,
    )
    response.headers["X-Keel-Search-Mode"] = recall_status.mode
    return [
        {
            "id": hit.id,
            "title": hit.title,
            "snippet": hit.snippet,
            "messages": hit.messages,
            "updated_at": hit.updated_at.isoformat() if hit.updated_at else None,
        }
        for hit in hits
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


def _proposal_store(request: Request) -> tuple[MemoryProposalStore, str]:
    engine = getattr(request.app.state, "engine", None)
    scope: str = getattr(request.app.state, "durable_scope", "web:local")
    if engine is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "datastore unavailable")
    return MemoryProposalStore(engine, scope), scope


def _proposal_dict(proposal: MemoryProposal) -> dict[str, object]:
    return {
        "id": proposal.id,
        "block": proposal.block,
        "expected_version": proposal.expected_version,
        "proposed_value": proposal.proposed_value,
        "reason": proposal.reason,
        "confidence": proposal.confidence,
        "source_event_ids": proposal.source_event_ids,
        "status": proposal.status,
        "created_at": proposal.created_at.isoformat(),
        "resolved_at": proposal.resolved_at.isoformat() if proposal.resolved_at else None,
        "resolved_by": proposal.resolved_by,
    }


def _resolution_response(resolution: ProposalResolution) -> JSONResponse:
    """Map a proposal resolution to its HTTP response (404 missing, 409 conflict, 200 ok)."""
    outcome = resolution.outcome
    if outcome is ProposalOutcome.not_found:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"ok": False, "status": outcome.value, "version": None},
        )
    if outcome in (ProposalOutcome.stale, ProposalOutcome.already_resolved):
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"ok": False, "status": outcome.value, "version": None},
        )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"ok": True, "status": outcome.value, "version": resolution.version},
    )


@router.get("/memory/proposals", summary="List core-memory rewrite proposals for the scope")
async def list_memory_proposals(
    request: Request, status_filter: str | None = Query(None, alias="status")
) -> list[dict[str, object]]:
    """Core-memory rewrite proposals awaiting (or past) human review."""
    store, _ = _proposal_store(request)
    return [_proposal_dict(p) for p in await store.list_proposals(status=status_filter)]


@router.post(
    "/memory/proposals/{proposal_id}/approve",
    summary="Approve a proposal (atomically apply it to core memory)",
    dependencies=[Depends(require_role(Role.operator))],
)
async def approve_memory_proposal(proposal_id: str, request: Request) -> JSONResponse:
    """Apply the proposal under an optimistic version check; stale ones 409."""
    store, _ = _proposal_store(request)
    return _resolution_response(await store.approve(proposal_id, "web"))


@router.post(
    "/memory/proposals/{proposal_id}/reject",
    summary="Reject a proposal (no change to core memory)",
    dependencies=[Depends(require_role(Role.operator))],
)
async def reject_memory_proposal(proposal_id: str, request: Request) -> JSONResponse:
    """Mark the proposal rejected; core memory is untouched."""
    store, _ = _proposal_store(request)
    return _resolution_response(await store.reject(proposal_id, "web"))


@router.post(
    "/memory/consolidation/run",
    summary="Enqueue a memory-consolidation run for the scope now",
    dependencies=[Depends(require_role(Role.operator))],
)
async def run_consolidation(request: Request) -> dict[str, bool]:
    """Manually trigger the scope's consolidation schedule (same path as the daily tick)."""
    enqueue = getattr(request.app.state, "enqueue", None)
    scope = getattr(request.app.state, "durable_scope", "web:local")
    if enqueue is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "job queue unavailable")
    await enqueue("run_agent", consolidation_schedule_id(scope))
    return {"ok": True}
