"""REST API v1 (``/v1``) — session runs, SSE event stream, approvals (WS-E).

Evolution policy (DESIGN-REVIEW G14): ``/v1`` is additive-only.

- ``POST /sessions/{id}/messages`` **durably admits** input via the identity-bound
  :class:`~keel_core.run_service.DurableRunService` and dispatches the worker-owned
  ``run_interactive`` job — the default path (M3.6). There is no server-local asyncio run
  task and no in-process fallback: a live run queue is required (explicit 503 otherwise).
  The tenant/actor/Agent identity is derived from the request actor + selected org/Agent
  (re-authorized here), never from the ambient data-plane scope; an open-mode local operator
  maps to the explicit local-preview compatibility profile, gated to non-cloud mode.
- ``GET  /sessions/{id}/events``   streams the event log as SSE (replayable via
  ``after=``, then live). The client closes the stream when it sees ``run.ended``.
- ``POST /approvals/{id}/approve|reject`` resolves a durable approval through the bound
  :class:`~keel_core.run_service.DurableRunService` (org/actor-bound); the legacy
  ``POST /approvals/{id}`` resolves an **in-process** future and is local-preview only.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from keel_core.agent_config_snapshot import (
    AgentConfigSnapshot,
    AgentConfigSnapshotError,
    MemoryPolicySnapshot,
    ResourceGrantSnapshot,
)
from keel_core.api import (
    ApprovalResolution,
    CreateMessageRequest,
    CreateMessageResponse,
    JobResponse,
)
from keel_core.approvals import ApprovalStore, InMemoryApprovalStore, PostgresApprovalStore
from keel_core.config import Settings
from keel_core.connector_actions import build_connector_actions
from keel_core.consolidation import (
    MemoryProposal,
    MemoryProposalStore,
    ProposalOutcome,
    ProposalResolution,
    consolidation_schedule_id,
)
from keel_core.errors import CrossScopeError
from keel_core.events import EventType
from keel_core.identity import (
    AgentAccessPrincipalType,
    AuditAction,
    AuditEvent,
    IdentityService,
    NotFoundError,
)
from keel_core.interactive import (
    LOCAL_PREVIEW_AGENT_ID,
    LOCAL_PREVIEW_AGENT_NAME,
    InteractiveCapabilities,
    build_interactive_tool_names,
)
from keel_core.jobs import JobStatus, JobStore, JobValidationError
from keel_core.loop import admit
from keel_core.memory import PostgresMemoryStore
from keel_core.protocols import EventStore
from keel_core.run_service import DurableRunService
from keel_core.runs import (
    PostgresRunStore,
    RunAdmissionConflict,
    RunBudgetSpec,
    RunControlKind,
    RunRecord,
    RunStore,
    RunSurface,
)
from keel_core.scoping import LOCAL_PREVIEW_SCOPE
from keel_core.search import hybrid_search_sessions
from keel_core.session_visibility import (
    InMemorySessionAccessStore,
    PostgresSessionAccessStore,
    SessionAccessStore,
    SessionVisibility,
    can_view_session,
    ensure_session_identity,
    get_session_identity,
    set_session_visibility,
)
from keel_core.state import InMemoryEventStore, PostgresEventStore, SessionSummary, list_sessions
from keel_core.types import PermissionDecision, ScopeId
from keel_server.auth import Role, require_role
from keel_server.endpoint_auth import (
    EndpointAuth,
    EndpointPrivilege,
    require_authenticated,
    require_privilege,
)
from keel_server.identity_context import Actor, resolve_actor
from keel_server.runtime import AgentRuntime

# Baseline authorization: every /v1 route needs an authenticated caller. Unlike the previous
# API-key-only gate, a verified OIDC user is accepted here (not rejected as an unknown API
# key — the fix for the JWT-rejected-first defect); API-key machines and the non-cloud local
# operator keep the hashed-key/open path. Fine-grained privilege + the derived per-Agent
# data-plane scope are enforced per-route via require_privilege; scope-wide management routes
# keep the coarse API-key require_role gate (preserving API-key behavior).
router = APIRouter(prefix="/v1", tags=["v1"], dependencies=[Depends(require_authenticated)])


def _runtime(request: Request) -> AgentRuntime:
    runtime: AgentRuntime | None = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "runtime unavailable")
    return runtime


def _jobs(request: Request) -> JobStore:
    store: JobStore | None = getattr(request.app.state, "jobs", None)
    scope: object = getattr(request.app.state, "durable_scope", None)
    if store is None or not isinstance(scope, str) or not scope:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "jobs datastore unavailable")
    if store.scope_id != scope:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "jobs scope misconfigured")
    return store


def _schedule_store(request: Request, scope_id: ScopeId):  # type: ignore[no-untyped-def]
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "datastore unavailable")
    from keel_scheduler.store import PostgresScheduleStore

    return PostgresScheduleStore(engine, scope_id)


def _shared_substrate(request: Request) -> bool:
    """Whether the durable run substrate is shared with the worker (M3.6, item 2).

    Worker-owned durable admission is only valid when the event/run/job/approval stores are a
    **shared** Postgres substrate a separate worker process can read. In-memory / process-local
    stores are private to this server process, so a dispatched ``run_interactive`` job would
    reference a run the worker can never see. The signal defaults to ``engine is not None``
    (Postgres wired); tests may set ``app.state.shared_run_substrate`` to exercise either mode
    with in-memory doubles.
    """
    explicit = getattr(request.app.state, "shared_run_substrate", None)
    if isinstance(explicit, bool):
        return explicit
    return getattr(request.app.state, "engine", None) is not None


def _scoped_runs(request: Request, scope_id: ScopeId) -> RunStore:
    """A run store bound to ``scope_id`` (Postgres per-scope, or the in-memory double)."""
    store = _maybe_scoped_runs(request, scope_id)
    if store is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "durable runs unavailable")
    return store


def _maybe_scoped_runs(request: Request, scope_id: ScopeId) -> RunStore | None:
    """A run store bound to ``scope_id``, or ``None`` when no durable substrate is wired."""
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        return PostgresRunStore(engine, scope_id)
    store: RunStore | None = getattr(request.app.state, "runs", None)
    return store


def _scoped_events(request: Request, scope_id: ScopeId) -> EventStore:
    """An event store bound to ``scope_id`` (Postgres per-scope, or the in-memory double)."""
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        return PostgresEventStore(engine, scope_id)
    store: EventStore | None = getattr(request.app.state, "events", None)
    return store if store is not None else InMemoryEventStore()


def _scoped_approvals(request: Request, scope_id: ScopeId) -> ApprovalStore:
    """An approval store bound to ``scope_id`` (Postgres per-scope, or the in-memory double)."""
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        return PostgresApprovalStore(engine, scope_id)
    store: ApprovalStore | None = getattr(request.app.state, "durable_approvals", None)
    return store if store is not None else InMemoryApprovalStore()


async def _authorize_run(
    request: Request, record: RunRecord, scope_id: ScopeId | None = None
) -> None:
    """Authorize the request's actor + derived scope against a durable run (M3.6, items 3/6).

    A run may only be touched from within its own data-plane ``scope_id`` — a run admitted
    under one org/Agent's ``agent:<org>/<agent>`` scope is invisible from another's (cross-org
    /Agent session ids return the same 404 as an unknown run, no existence leak). A real
    authenticated **user** must additionally be an active member of the run's ``org_id``. The
    open-mode **local** operator / **API-key** machine stays within its single ``web:local``
    tenant.
    """
    if scope_id is not None and record.scope_id != scope_id:
        # The run belongs to a different derived scope — deny without leaking existence.
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    actor = await resolve_actor(request)
    if not actor.is_user:
        return  # local-preview / API-key machine: single scope-bound tenant
    service = getattr(request.app.state, "identity", None)
    if service is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "identity service unavailable")
    assert actor.user_id is not None
    try:
        await service.select_org(actor.user_id, record.org_id)
    except NotFoundError:
        # Not a member of the run's org: same response as a nonexistent run (no info leak).
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found") from None


def durable_actor_id(actor: Actor) -> str:
    """A stable, non-blank actor identity for durable run admission/resolution (blocker 4).

    A real user resolves to their durable ``user_id`` — the same value durable admission
    binds as the run's ``actor`` — so a *different* same-org user is rejected by the service
    (no ambient cross-user resolution). The open-mode local operator and API-key machines map
    to a stable ``"<kind>:<name>"`` id: an explicit, never-blank local actor rather than a
    blank/ambient one, matching their single scope-bound tenant."""
    if actor.is_user:
        assert actor.user_id is not None
        return actor.user_id
    return f"{actor.kind.value}:{actor.display_name}"


def _admission_model(request: Request) -> str:
    """The model captured in the durable admission (reproducibility, M3.6 item 5).

    The selected model is persisted with the admission (fingerprint + admission event) so the
    worker executes the run with the **admitted** model rather than its own process default.
    The source is the server's configured/selected model (mutable only in non-cloud local
    preview via ``/settings/model``); a cloud deployment captures the deployment default.
    """
    runtime = getattr(request.app.state, "runtime", None)
    model = getattr(runtime, "model", None)
    if isinstance(model, str) and model:
        return model
    settings = getattr(request.app.state, "settings", None)
    default = getattr(settings, "default_model", None)
    return default if isinstance(default, str) and default else "github_copilot/claude-sonnet-4.5"


async def _admission_snapshot(
    request: Request, auth: EndpointAuth, model: str, budget: RunBudgetSpec
) -> AgentConfigSnapshot:
    """The immutable :class:`AgentConfigSnapshot` bound into this web admission (R1B).

    An authenticated caller's snapshot reflects the *persisted* Agent
    :func:`~keel_server.endpoint_auth.resolve_endpoint_auth` already resolved (name/persona/
    optimistic version) plus its currently active resource grants (non-secret descriptors —
    type/id/capability only). The non-cloud local-preview compatibility profile has no
    persisted Agent record, so it gets an explicit local snapshot bound to the same stable
    compatibility identity the run row itself carries instead.
    """
    settings = getattr(request.app.state, "settings", None)
    if not isinstance(settings, Settings):
        settings = Settings()
    engine = getattr(request.app.state, "engine", None)
    runtime = getattr(request.app.state, "runtime", None)
    embedder = getattr(runtime, "embedder", None)
    connector_actions = await build_connector_actions(
        engine=engine,
        settings=settings,
        scope_id=auth.scope_id,
        registry=getattr(request.app.state, "connector_registry", None),
    )
    tools = build_interactive_tool_names(
        engine=engine,
        scope_id=auth.scope_id,
        embedder=embedder,
        caps=InteractiveCapabilities(
            memory_block_max_chars=settings.memory_block_max_chars,
            session_embedding_batch_size=settings.session_embedding_batch_size,
            session_embedding_catchup_limit=settings.session_embedding_catchup_limit,
            knowledge_search_query_max_chars=settings.knowledge_search_query_max_chars,
            knowledge_search_k_max=settings.knowledge_search_k_max,
            knowledge_tool_output_max_chars=settings.knowledge_tool_output_max_chars,
        ),
        connector_actions=connector_actions,
    )
    if auth.agent is not None and auth.org_id is not None:
        identity = getattr(request.app.state, "identity", None)
        grants: list[Any] = []
        if identity is not None:
            grants = await identity.active_resource_grants(auth.org_id, auth.agent.id)
        return AgentConfigSnapshot(
            agent_id=auth.agent.id,
            agent_version=auth.agent.version,
            agent_name=auth.agent.name,
            persona=auth.agent.persona,
            model=model,
            max_iterations=budget.max_iterations,
            token_budget=budget.token_budget,
            permission_profile="default",
            tools=tools,
            memory_policy=MemoryPolicySnapshot(archival_enabled=embedder is not None),
            resource_grants=tuple(
                ResourceGrantSnapshot(g.resource_type, g.resource_id, g.capability.value)
                for g in grants
            ),
        )
    return AgentConfigSnapshot(
        agent_id=LOCAL_PREVIEW_AGENT_ID,
        agent_version=1,
        agent_name=LOCAL_PREVIEW_AGENT_NAME,
        persona="",
        model=model,
        max_iterations=budget.max_iterations,
        token_budget=budget.token_budget,
        permission_profile="local_preview",
        tools=tools,
        memory_policy=MemoryPolicySnapshot(),
        resource_grants=(),
    )


def _durable_run_service(request: Request, scope_id: ScopeId) -> DurableRunService:
    """Build a :class:`DurableRunService` bound to ``scope_id`` (per-Agent data plane).

    The run/approval/event stores are all constructed for the *derived* scope so admission,
    approval resolution, and dispatch stay within one org/Agent's isolated data plane — never
    the ambient ``web:local`` shared across organizations.
    """
    approvals = _scoped_approvals(request, scope_id)
    # resolve_approval never reads the event store; a lightweight fallback is safe when the
    # server runs without Postgres (in-memory preview).
    events = _scoped_events(request, scope_id)
    enqueue_raw = getattr(request.app.state, "enqueue", None)

    async def _enqueue(run_id: str) -> None:
        if enqueue_raw is not None:
            await enqueue_raw("run_interactive", run_id, scope_id)

    return DurableRunService(
        run_store=_scoped_runs(request, scope_id),
        event_store=events,
        approvals=approvals,
        scope_id=scope_id,
        enqueue=_enqueue,
        admit_fn=admit,
        dispatch_outbox=getattr(request.app.state, "dispatch_outbox", None),
    )


def _cloud_mode(request: Request) -> bool:
    """Whether the server runs in cloud (fail-closed auth) mode; gates local-preview."""
    return bool(getattr(request.app.state, "auth_required", False))


def _idempotency_key(request: Request, body: CreateMessageRequest) -> str:
    """A stable request identifier for at-most-once admission (header, body, or generated).

    A client that supplies a stable ``Idempotency-Key`` header (or body field) makes a
    retried message idempotent — the durable admission dedups on
    ``(scope, org, actor, idempotency_key)`` and never creates a second run/message. When
    none is supplied a fresh key is generated (each request is a distinct admission)."""
    header = (request.headers.get("idempotency-key") or "").strip()
    if header:
        return header
    if body.idempotency_key and body.idempotency_key.strip():
        return body.idempotency_key.strip()
    return uuid.uuid4().hex


@router.post(
    "/sessions/{session_id:path}/messages",
    response_model=CreateMessageResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Admit a user message and schedule a run",
)
async def create_message(
    session_id: str,
    body: CreateMessageRequest,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> CreateMessageResponse:
    """Durably admit input (FR-C5) and dispatch a **worker-owned** run; return its ``run_id``.

    Admission binds the request actor's org / actor / selected Agent (re-authorized by the
    unified :func:`require_privilege` dependency, which accepts a verified OIDC user or the
    non-cloud API-key/local operator and fails a cloud caller with no user closed) and derives
    the canonical ``agent:<org>/<agent>`` data-plane scope — never the ambient ``web:local``
    shared across orgs. It persists the user turn + run row idempotently and enqueues
    ``run_interactive`` for a worker to execute; there is no server-local run task.

    Worker-owned admission requires a **shared** durable substrate (Postgres stores + a live
    queue): with in-memory / process-local stores a worker in another process could never see
    the run, so we fail closed with 503 rather than accept a run a worker cannot execute.
    """
    if not _shared_substrate(request):
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "worker-owned durable admission requires a shared Postgres run substrate; "
            "this server is running with in-memory/process-local stores",
        )
    enqueue = getattr(request.app.state, "enqueue", None)
    if enqueue is None:
        # Fail closed: without a live queue the run could not be dispatched to a worker.
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "run queue unavailable")
    idempotency_key = _idempotency_key(request, body)
    actor_id = durable_actor_id(auth.actor)
    assert auth.org_id is not None and auth.agent_id is not None
    service = _durable_run_service(request, auth.scope_id)
    model = _admission_model(request)
    budget = RunBudgetSpec()
    snapshot = await _admission_snapshot(request, auth, model, budget)
    # R1B: durably record this session's owner + default-private visibility BEFORE the
    # admitted prompt (idempotent, atomic first-writer-wins insert) — a crash/retry between
    # this call and the prompt append can never leave an ownerless or ambiguously-visible
    # accepted session (keel_core.session_visibility.ensure_session_identity).
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        owner_user_id = auth.actor.user_id if auth.is_user else None
        session_org_id = None if auth.scope_id == LOCAL_PREVIEW_SCOPE else auth.org_id
        await ensure_session_identity(
            engine,
            auth.scope_id,
            session_id,
            org_id=session_org_id,
            owner_user_id=owner_user_id,
            visibility=(
                SessionVisibility.private
                if owner_user_id is not None
                else SessionVisibility.agent_members
            ),
        )
        await _authorize_session_visibility(request, auth, session_id)
    try:
        result = await service.admit(
            org_id=auth.org_id,
            actor=actor_id,
            agent_id=auth.agent_id,
            session_id=session_id,
            surface=RunSurface.web.value,
            content=body.content,
            idempotency_key=idempotency_key,
            model=model,
            budget=budget,
            snapshot=snapshot,
        )
    except RunAdmissionConflict:
        # Same idempotency identity, different immutable binding/content: reject (never repair
        # with attacker/client-supplied values). The caller must use a fresh idempotency key.
        raise HTTPException(status.HTTP_409_CONFLICT, "admission identity conflict") from None
    except CrossScopeError:
        # The session id is globally owned by a different org/Agent's scope. Deny with the same
        # 404 as an unknown session (no cross-scope write, no existence leak).
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found") from None
    # Even when the post-commit worker enqueue could not be delivered (dispatch_pending), the
    # run is durably accepted (202): the reconciler redispatches it. Returning an error here
    # would make the client retry and risk a duplicate run.
    return CreateMessageResponse(
        session_id=session_id,
        run_id=result.run_id,
        idempotency_key=idempotency_key,
        dispatch_pending=result.dispatch_pending,
    )


@router.post(
    "/runs/{run_id}/interrupt",
    summary="Interrupt a running agent run",
)
async def interrupt_run(
    run_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, bool]:
    """Ask an in-flight run to stop at its next iteration (StopReason.interrupted).

    Records a **durable** interrupt against the run so a worker honours it across a
    server/worker restart, and (best-effort) also flips the local-preview in-process runtime
    flag so a same-process run stops immediately. The run is looked up within the caller's
    derived scope, so a cross-org/Agent run id is invisible (404).
    """
    durable = False
    store = _maybe_scoped_runs(request, auth.scope_id)
    if store is not None:
        record = await store.get(run_id)
        if record is not None and record.scope_id == auth.scope_id:
            await _authorize_run(request, record, auth.scope_id)
            await _authorize_session_visibility(request, auth, record.session_id)
            durable = await store.request_control(
                run_id, kind=RunControlKind.interrupt, requested_by="web"
            )
    local = _runtime(request).interrupt_run(run_id)
    return {"ok": durable or local}


@router.post(
    "/runs/{run_id}/steer",
    summary="Steer a running agent run (durable steering message)",
)
async def steer_run(
    run_id: str,
    body: dict[str, Any],
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, bool]:
    """Record a durable steering message consumed by the owning worker mid-run."""
    text_value = str(body.get("text", "")).strip()
    if not text_value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "text is required")
    store = _scoped_runs(request, auth.scope_id)
    record = await store.get(run_id)
    if record is None or record.scope_id != auth.scope_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown or terminal run")
    await _authorize_run(request, record, auth.scope_id)
    await _authorize_session_visibility(request, auth, record.session_id)
    ok = await store.request_control(
        run_id, kind=RunControlKind.steer, requested_by="web", payload={"text": text_value}
    )
    if not ok:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown or terminal run")
    return {"ok": True}


@router.get("/runs/{run_id}", summary="Get durable run status")
async def get_run(
    run_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> dict[str, object]:
    """Read the durable run's authoritative status/attempt/cost (worker-owned execution)."""
    record = await _scoped_runs(request, auth.scope_id).get(run_id)
    if record is None or record.scope_id != auth.scope_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    await _authorize_run(request, record, auth.scope_id)
    await _authorize_session_visibility(request, auth, record.session_id)
    try:
        snapshot = record.snapshot
    except AgentConfigSnapshotError:
        # Corrupt/tampered persisted snapshot: never surface unverifiable content, only that
        # verification failed (the run itself is unaffected — this is diagnostic metadata).
        snapshot = None
    snapshot_meta: dict[str, object] | None = None
    if snapshot is not None:
        # Non-sensitive Agent configuration metadata only (R1B): no credential/secret material
        # ever flows through a snapshot (resource grants are type/id/capability descriptors).
        snapshot_meta = {
            "schema_version": snapshot.schema_version,
            "hash": record.snapshot_hash,
            "agent_version": snapshot.agent_version,
            "agent_name": snapshot.agent_name,
            "persona": snapshot.persona,
            "model": snapshot.model,
            "max_iterations": snapshot.max_iterations,
            "token_budget": snapshot.token_budget,
            "permission_profile": snapshot.permission_profile,
            "tools": list(snapshot.tools),
            "memory_policy": snapshot.memory_policy.to_dict(),
            "resource_grants": [grant.to_dict() for grant in snapshot.resource_grants],
        }
    return {
        "id": record.id,
        "status": record.status.value,
        "stop_reason": record.stop_reason,
        "surface": record.surface,
        "agent_id": record.agent_id,
        "session_id": record.session_id,
        "attempt": record.attempt,
        "version": record.version,
        "prompt_tokens": record.prompt_tokens,
        "completion_tokens": record.completion_tokens,
        "cost_usd": record.cost_usd,
        "created_at": record.created_at.isoformat(),
        "updated_at": record.updated_at.isoformat(),
        "finished_at": record.finished_at.isoformat() if record.finished_at else None,
        "error_kind": record.error_kind,
        "agent_config_snapshot": snapshot_meta,
    }


@router.get("/jobs", response_model=list[JobResponse], summary="List durable jobs")
async def list_jobs(
    request: Request,
    status_filter: Annotated[JobStatus | None, Query(alias="status")] = None,
    kind: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> list[JobResponse]:
    rows = await _jobs(request).list(
        status=status_filter,
        kind=kind,
        limit=limit,
    )
    return [JobResponse.from_record(row) for row in rows]


@router.get(
    "/jobs/{job_id}",
    response_model=JobResponse,
    summary="Get a durable job",
)
async def get_job(job_id: str, request: Request) -> JobResponse:
    row = await _jobs(request).get(job_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return JobResponse.from_record(row)


@router.post(
    "/jobs/{job_id}/cancel",
    response_model=JobResponse,
    summary="Request durable-job cancellation",
    dependencies=[Depends(require_role(Role.operator))],
)
async def cancel_job(job_id: str, request: Request) -> JobResponse:
    try:
        row = await _jobs(request).request_cancel(job_id, datetime.now(UTC))
    except JobValidationError as exc:
        if exc.code in {"job_finalizing", "job_not_cancellable"}:
            raise HTTPException(status.HTTP_409_CONFLICT, exc.public_message) from None
        raise
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return JobResponse.from_record(row)


@router.get("/schedules", summary="List the scope's schedules (management view)")
async def list_schedules(
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> list[dict[str, object]]:
    store = _schedule_store(request, auth.scope_id)
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
)
async def toggle_schedule(
    schedule_id: str,
    request: Request,
    body: dict[str, Any],
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, object]:
    enabled = bool(body.get("enabled", True))
    store = _schedule_store(request, auth.scope_id)
    if await store.get(schedule_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "schedule not found")
    await store.set_enabled(schedule_id, enabled)
    return {"ok": True, "enabled": enabled}


@router.post(
    "/schedules/{schedule_id}/run",
    summary="Trigger a schedule's run now",
)
async def run_schedule(
    schedule_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, bool]:
    if await _schedule_store(request, auth.scope_id).get(schedule_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "schedule not found")
    enqueue = getattr(request.app.state, "enqueue", None)
    if enqueue is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "job queue unavailable")
    await enqueue("run_agent", schedule_id, auth.scope_id)
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
    # In cloud/authenticated mode the model is captured per-run at admission (bound to the
    # selected Agent), not a mutable server-global; surface that so a client does not expect a
    # global switch to take effect.
    return {"current": current, "available": available, "mutable": not _cloud_mode(request)}


@router.put(
    "/settings/model",
    summary="Switch the model for subsequent runs (local preview only)",
    dependencies=[Depends(require_role(Role.operator))],
)
async def set_model(request: Request, body: dict[str, Any]) -> dict[str, object]:
    """Switch the process model — **local preview only**.

    In non-cloud local preview the server runtime executes runs, so switching its model takes
    effect for subsequent admissions (the admitted model is captured per-run). In cloud mode
    the worker executes runs with the model captured at admission from the selected Agent, so
    a server-global switch would be silently ignored — we fail closed (409) rather than report
    a success the worker never honours. Model selection is per-Agent/per-run in cloud.
    """
    model = str(body.get("model", "")).strip()
    if not model:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "model is required")
    if _cloud_mode(request):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "model selection is bound to the selected Agent at admission in cloud mode; "
            "the server-global model cannot be switched here",
        )
    runtime = _runtime(request)
    runtime.set_model(model)
    return {"ok": True, "current": model}


def _session_access_store(request: Request, scope_id: ScopeId) -> SessionAccessStore:
    """The explicit-share store bound to ``scope_id`` (Postgres, or the in-memory double)."""
    engine = getattr(request.app.state, "engine", None)
    if engine is not None:
        return PostgresSessionAccessStore(engine)
    store: SessionAccessStore | None = getattr(request.app.state, "session_access", None)
    return store if store is not None else InMemorySessionAccessStore()


async def _authorize_session_visibility(
    request: Request, auth: EndpointAuth, session_id: str
) -> None:
    """Enforce R1B session ownership/visibility — independent of the selected Agent scope.

    Using a team Agent (passing ``require_privilege``, which already re-authorizes at least
    ``discover``-level Agent Access) never by itself grants reading another user's private
    session: this additionally checks the session's own ``owner_user_id``/``visibility``/
    explicit shares (:mod:`keel_core.session_visibility`). Denies with 404 (no existence
    disclosure) rather than 403. Only applies to a real authenticated user against a
    Postgres-backed durable substrate; the non-cloud local-preview single operator and
    API-key machine credentials have no additional per-user session-ownership axis and keep
    their existing single-tenant behavior.
    """
    if auth.scope_id == LOCAL_PREVIEW_SCOPE or not auth.is_user:
        return
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        return
    identity = await get_session_identity(engine, auth.scope_id, session_id)
    if identity is None:
        return  # no durable identity row yet; other checks (existence) handle the 404
    actor_user_id = auth.actor.user_id
    has_share = False
    if identity.visibility is SessionVisibility.explicit and actor_user_id is not None:
        share_store = _session_access_store(request, auth.scope_id)
        has_share = await share_store.has_active_share(auth.scope_id, session_id, actor_user_id)
    if not can_view_session(
        identity,
        actor_user_id=actor_user_id,
        # require_privilege(viewer) already re-authorized at least discover-level Agent
        # Access (or the local/machine bypass handled above) to reach this point.
        has_active_agent_access=True,
        has_explicit_share=has_share,
    ):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")


async def _session_in_scope(request: Request, session_id: str, scope_id: ScopeId) -> bool:
    """Whether ``session_id`` has any durable event in ``scope_id`` (ownership check).

    Used to authorize history/SSE access: a session id that belongs to a different org/Agent's
    derived scope has no events in *this* scope, so it is treated as not found (no cross-scope
    read, no existence leak). The local-preview scope is the single-operator tenant and is
    always allowed.
    """
    if scope_id == LOCAL_PREVIEW_SCOPE:
        return True
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        # No durable substrate to check against; the scoped in-memory replay is empty anyway.
        store = _scoped_events(request, scope_id)
    else:
        store = PostgresEventStore(engine, scope_id)
    async for _event in store.read(session_id):
        return True
    return False


# SSE tailing bounds: how often to poll the durable log for new worker events, and a hard
# safety cap so a run that never terminates cannot pin a connection open indefinitely.
_SSE_POLL_INTERVAL_SECONDS = 1.0
_SSE_MAX_STREAM_SECONDS = 30 * 60


def _sse_frame(event: Any) -> str:
    """One SSE frame; durable events carry their ``seq`` as the ``id`` (the resume cursor)."""
    data = f"data: {event.model_dump_json()}\n\n"
    return f"id: {event.seq}\n{data}" if getattr(event, "seq", 0) else data


def _resume_cursor(request: Request, after: int | None) -> int | None:
    """The replay cursor: the SSE ``Last-Event-ID`` reconnect header, else the ``after`` param.

    A reconnecting browser resends the id of the last event it received; honoring it (over the
    original ``after``) means the tail resumes exactly where the dropped connection left off,
    so no event is missed or replayed twice.
    """
    header = (request.headers.get("last-event-id") or "").strip()
    if header:
        try:
            return int(header)
        except ValueError:
            return after
    return after


@router.get(
    "/sessions/{session_id:path}/events",
    summary="Stream session events (SSE, replayable via after=)",
)
async def stream_events(
    session_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
    after: int | None = None,
) -> StreamingResponse:
    """Server-Sent Events: durable replay from the cursor, then tail the live run.

    The stream is bound to the caller's derived data-plane scope: a session id from another
    org/Agent's scope has no events here and answers 404 (no cross-scope read). The local
    single-operator preview streams live via the in-process runtime fan-out; an authenticated
    per-Agent scope replays its isolated durable event log **and then keeps the connection open**,
    tailing new worker-produced events via bounded durable polling until the run ends or the
    client disconnects — it must not close immediately after replay.

    The replay cursor is the SSE ``Last-Event-ID`` header (sent automatically by the browser on
    reconnect) when present, else the ``after`` query parameter. Only events with ``seq`` beyond
    the cursor are emitted, so a reconnect neither misses nor duplicates events.
    """
    if not await _session_in_scope(request, session_id, auth.scope_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    await _authorize_session_visibility(request, auth, session_id)

    # The resume cursor (SSE ``Last-Event-ID`` reconnect header, else ``after``) is honored on
    # BOTH the local-preview live path and the scoped durable path (finding 6), so a reconnect
    # resumes exactly where it dropped — no missed or duplicated events — on either surface.
    cursor = _resume_cursor(request, after)

    if auth.scope_id == LOCAL_PREVIEW_SCOPE:
        runtime = _runtime(request)

        async def _live() -> AsyncIterator[str]:
            async for event in runtime.tail(session_id, cursor):
                if await request.is_disconnected():
                    break
                data = f"data: {event.model_dump_json()}\n\n"
                # Partial deltas carry no durable seq; only real events advance the cursor.
                yield f"id: {event.seq}\n{data}" if event.seq else data

        return StreamingResponse(
            _live(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    store = _scoped_events(request, auth.scope_id)

    async def _replay_and_tail() -> AsyncIterator[str]:
        nonlocal cursor
        # 1) Durable replay from the resume cursor (exclusive: seq > cursor).
        async for event in store.read(session_id, cursor):
            if await request.is_disconnected():
                return
            if event.seq:
                cursor = max(cursor or 0, event.seq)
            yield _sse_frame(event)
            if event.type is EventType.run_ended:
                return
        # 2) Tail: keep the connection open and poll the durable log for new worker events,
        #    advancing the cursor so nothing is missed or duplicated, until the run ends, the
        #    client disconnects, or a safety cap elapses (a run that never terminates cannot pin
        #    a connection open forever).
        deadline = asyncio.get_event_loop().time() + _SSE_MAX_STREAM_SECONDS
        while asyncio.get_event_loop().time() < deadline:
            if await request.is_disconnected():
                return
            await asyncio.sleep(_SSE_POLL_INTERVAL_SECONDS)
            terminated = False
            async for event in store.read(session_id, cursor):
                if await request.is_disconnected():
                    return
                if event.seq:
                    cursor = max(cursor or 0, event.seq)
                yield _sse_frame(event)
                if event.type is EventType.run_ended:
                    terminated = True
            if terminated:
                return

    return StreamingResponse(
        _replay_and_tail(),
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
    """Resolve an **in-process** tool approval (local-preview only).

    This resolves a server-local :class:`~keel_server.runtime.ApprovalRegistry` future, which
    only exists for the local-preview in-process runtime. Production Web/IM approvals are
    durable and go through :func:`_resolve_durable` (``/approvals/{id}/approve|reject``) —
    org/actor-bound, no in-memory futures. In a cloud deployment there are no in-process runs,
    so this returns 404 (nothing pending).
    """
    runtime = _runtime(request)
    approved = body.decision is PermissionDecision.allow
    resolved = runtime.resolve_approval(approval_id, approved)
    if not resolved:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown or already-resolved approval")
    return {"resolved": True, "approved": approved}


@router.get("/approvals", summary="List durable approvals for the current scope")
async def list_approvals(
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
    status_filter: str = Query("pending", alias="status"),
) -> list[dict[str, object]]:
    """Durable approvals for the caller's derived data-plane scope (pending queue by default).

    Approvals are listed within the caller's derived ``agent:<org>/<agent>`` scope (or the
    local-preview scope), so one org/Agent's tool/args are never surfaced to another. Each
    durable-interactive approval (its ``run_id`` is a durable run) is still cross-checked
    against the run's org membership for a user; a **legacy local-preview** approval (no
    durable run row) is isolated to the local/machine operator and labeled
    ``origin=local-preview``.
    """
    store = _scoped_approvals(request, auth.scope_id)
    if status_filter != "pending":
        return []
    actor = auth.actor
    run_store = _maybe_scoped_runs(request, auth.scope_id)
    rows = await store.list_pending(auth.scope_id)
    org_access: dict[str, bool] = {}
    result: list[dict[str, object]] = []
    for r in rows:
        run = await run_store.get(r.run_id) if run_store is not None else None
        if run is not None:
            org_id = run.org_id
            if org_id not in org_access:
                org_access[org_id] = await _may_access_org(request, actor, org_id)
            if not org_access[org_id]:
                continue  # cross-org durable interactive approval — never exposed
            origin: str = "interactive"
        else:
            # Legacy local-preview approval: isolated to the local/machine operator.
            if actor.is_user:
                continue
            origin = "local-preview"
        result.append(
            {
                "id": r.id,
                "run_id": r.run_id,
                "session_id": r.session_id,
                "tool": r.tool,
                "args": r.args,
                "call_id": r.call_id,
                "reason": r.reason,
                "status": r.status,
                "origin": origin,
                "org_id": run.org_id if run is not None else None,
                "created_at": r.created_at.isoformat(),
                "expires_at": r.expires_at.isoformat(),
            }
        )
    return result


async def _may_access_org(request: Request, actor: Any, org_id: str) -> bool:
    """Whether ``actor`` may see approvals bound to ``org_id`` (fail closed for users).

    A non-user (local operator / API-key machine) stays within its single scope-bound tenant
    (local-preview compatibility). A cloud user must be an *active member* of the run's org —
    resolved through the identity service exactly like :func:`_authorize_run`, so a non-member
    is treated identically to an unknown org (no exposure, no existence leak)."""
    if not actor.is_user:
        return True
    service = getattr(request.app.state, "identity", None)
    if service is None or actor.user_id is None:
        return False
    try:
        await service.select_org(actor.user_id, org_id)
    except NotFoundError:
        return False
    return True


async def _resolve_durable(
    request: Request, approval_id: str, decision: str, auth: EndpointAuth
) -> dict[str, bool]:
    """Resolve a durable approval, routing durable-interactive runs through the bound service.

    A durable **interactive** run's approval (its ``run_id`` is a row in the derived-scope
    run store) resolves through :class:`DurableRunService` — fully bound to the resolver's
    org/actor and the approval's action-hash/attempt, gated on the run being
    ``waiting_approval`` and not expired — which transitions ``waiting_approval -> queued``
    and enqueues ``run_interactive`` (the worker owns the resume). A **legacy** scheduled/
    digest approval keeps the existing ``resume_run`` behavior. Everything is scoped to the
    caller's derived data plane, so a cross-org/Agent approval id is invisible.
    """
    store = _scoped_approvals(request, auth.scope_id)
    record = await store.get(approval_id)
    if record is None:
        return {"ok": False}
    run_store = _maybe_scoped_runs(request, auth.scope_id)
    durable_run = await run_store.get(record.run_id) if run_store is not None else None
    if durable_run is not None:
        # Durable interactive path — bound resolution + run_interactive resume.
        await _authorize_run(request, durable_run, auth.scope_id)
        actor = auth.actor
        service = _durable_run_service(request, auth.scope_id)
        ok = await service.resolve_approval(
            approval_id,
            approved=decision == "granted",
            resolved_by=actor.display_name,
            actor=durable_actor_id(actor),
            org_id=durable_run.org_id if actor.is_user else None,
        )
        return {"ok": ok}
    # Legacy scheduled/digest path — unchanged resume_run behavior.
    ok = await store.resolve(approval_id, decision, "web")
    if ok:
        resolved = await store.get(approval_id)
        enqueue = getattr(request.app.state, "enqueue", None)
        if resolved is not None and enqueue is not None:
            # Continue the suspended run: resume executes-or-denies the gated call (G5).
            await enqueue("resume_run", resolved.session_id, resolved.run_id, resolved.scope_id)
    return {"ok": ok}


@router.post(
    "/approvals/{approval_id}/approve",
    summary="Approve a durable approval",
)
async def approve_durable(
    approval_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, bool]:
    return await _resolve_durable(request, approval_id, "granted", auth)


@router.post(
    "/approvals/{approval_id}/reject",
    summary="Reject a durable approval",
)
async def reject_durable(
    approval_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, bool]:
    return await _resolve_durable(request, approval_id, "denied", auth)


def _summary_visible(summary: SessionSummary, actor_user_id: str | None) -> bool:
    """Filter :func:`~keel_core.state.list_sessions` rows by R1B visibility (no extra query).

    ``agent_members`` is satisfied by definition here: reaching this endpoint already
    re-authorized at least ``discover``-level Agent Access. ``explicit`` sessions are
    conservatively excluded (an owner match is checked first); a caller with an explicit
    share still sees the session via a direct read (history/events), and the list endpoint
    favors a fast, query-free filter over an N+1 share lookup per row.
    """
    if actor_user_id is not None and summary.owner_user_id == actor_user_id:
        return True
    if (
        summary.org_id is None
        and summary.owner_user_id is None
        and summary.channel_provider is None
    ):
        return True  # legacy/not-yet-identity-aware row: pre-R1B Agent-scope gate only
    return summary.visibility == "agent_members"


async def _visible_session_ids(
    request: Request, auth: EndpointAuth, session_ids: list[str]
) -> set[str]:
    """Which of ``session_ids`` the caller may see under R1B session visibility.

    Used for surfaces without a cheap denormalized visibility column (session search hits);
    bounded to the small result set already returned by the search/list query."""
    if auth.scope_id == LOCAL_PREVIEW_SCOPE or not auth.is_user:
        return set(session_ids)
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        return set(session_ids)
    actor_user_id = auth.actor.user_id
    visible: set[str] = set()
    share_store: SessionAccessStore | None = None
    for sid in session_ids:
        identity = await get_session_identity(engine, auth.scope_id, sid)
        if identity is None:
            continue
        has_share = False
        if identity.visibility is SessionVisibility.explicit and actor_user_id is not None:
            if share_store is None:
                share_store = _session_access_store(request, auth.scope_id)
            has_share = await share_store.has_active_share(auth.scope_id, sid, actor_user_id)
        if can_view_session(
            identity,
            actor_user_id=actor_user_id,
            has_active_agent_access=True,
            has_explicit_share=has_share,
        ):
            visible.add(sid)
    return visible


@router.get("/sessions", summary="List the scope's sessions (newest first)")
async def list_sessions_endpoint(
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> list[dict[str, object]]:
    """Session summaries for the Sessions list (title preview + message count), scoped.

    Filtered by R1B session visibility (:mod:`keel_core.session_visibility`) — using the
    scope's team Agent does not by itself surface another user's private sessions.
    """
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        return []
    actor_user_id = auth.actor.user_id if auth.is_user else None
    return [
        {
            "id": s.id,
            "title": s.title,
            "messages": s.messages,
            "created_at": s.created_at.isoformat(),
            "updated_at": s.updated_at.isoformat(),
        }
        for s in await list_sessions(engine, auth.scope_id)
        if auth.scope_id == LOCAL_PREVIEW_SCOPE
        or not auth.is_user
        or _summary_visible(s, actor_user_id)
    ]


@router.get("/sessions/search", summary="Search the scope's sessions (hybrid recall)")
async def search_sessions_endpoint(
    request: Request,
    response: Response,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
    q: str = Query(""),
) -> list[dict[str, object]]:
    """Rank sessions by lexical + semantic RRF within the derived scope, with degradation."""
    engine = getattr(request.app.state, "engine", None)
    runtime = getattr(request.app.state, "runtime", None)
    embedder = getattr(runtime, "embedder", None)
    batch_size = int(getattr(runtime, "session_embedding_batch_size", 64))
    catchup_limit = int(getattr(runtime, "session_embedding_catchup_limit", 500))
    if engine is None or not q.strip():
        response.headers["X-Keel-Search-Mode"] = "hybrid" if embedder is not None else "lexical"
        return []

    hits, recall_status = await hybrid_search_sessions(
        engine,
        auth.scope_id,
        q,
        embedder=embedder,
        batch_size=batch_size,
        catchup_limit=catchup_limit,
    )
    response.headers["X-Keel-Search-Mode"] = recall_status.mode
    visible_ids = await _visible_session_ids(request, auth, [hit.id for hit in hits])
    return [
        {
            "id": hit.id,
            "title": hit.title,
            "snippet": hit.snippet,
            "messages": hit.messages,
            "updated_at": hit.updated_at.isoformat() if hit.updated_at else None,
        }
        for hit in hits
        if hit.id in visible_ids
    ]


@router.get("/sessions/{session_id:path}/history", summary="Durable event history for a session")
async def session_history(
    session_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> list[dict[str, object]]:
    """The session's durable event log (oldest first) for a read-only replay, scoped.

    The session must belong to the caller's derived scope: a cross-org/Agent session id has no
    events here and answers 404 (no cross-scope read). Session visibility (R1B) is enforced
    independent of the selected Agent scope — see :func:`_authorize_session_visibility`.
    """
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        return []
    if not await _session_in_scope(request, session_id, auth.scope_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    await _authorize_session_visibility(request, auth, session_id)
    store = PostgresEventStore(engine, auth.scope_id)
    return [event.model_dump(mode="json") async for event in store.read(session_id)]


# --- session visibility / explicit shares (R1B) ---------------------------------------


class UpdateSessionVisibilityRequest(BaseModel):
    visibility: SessionVisibility


class ShareSessionRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=200)


def _session_audit_service(request: Request) -> IdentityService:
    identity = getattr(request.app.state, "identity", None)
    if not isinstance(identity, IdentityService):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "identity service unavailable")
    return identity


async def _actor_can_manage_agent(request: Request, auth: EndpointAuth) -> bool:
    """Whether the actor holds "manage" authority on the scope's selected Agent.

    org admin/owner, a personal Agent's owner, or a delegated ``manage``-level Agent Access
    edge holder — the same authority :meth:`IdentityService.can_manage_agent_access` composes,
    reused here to gate session-visibility/share mutation for a non-owner (R1B item 7)."""
    if not auth.is_user or auth.org_id is None or auth.agent is None or auth.actor.user_id is None:
        return False
    identity = getattr(request.app.state, "identity", None)
    if identity is None:
        return False
    membership = await identity.store.get_membership(auth.org_id, auth.actor.user_id)
    access_edges = await identity.store.list_agent_access(
        auth.org_id,
        principal_type=AgentAccessPrincipalType.user,
        principal_id=auth.actor.user_id,
    )
    decision = identity.authz.can_manage_agent(
        auth.actor.user_id, membership, auth.agent, access_edges
    )
    return bool(decision)


def _identity_response(identity_row: Any) -> dict[str, object]:
    return {
        "session_id": identity_row.session_id,
        "owner_user_id": identity_row.owner_user_id,
        "channel_provider": identity_row.channel_provider,
        "channel_external_id": identity_row.channel_external_id,
        "visibility": identity_row.visibility.value,
    }


@router.get(
    "/sessions/{session_id:path}/visibility", summary="Read a session's ownership/visibility"
)
async def get_session_visibility(
    session_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> dict[str, object]:
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    if not await _session_in_scope(request, session_id, auth.scope_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    await _authorize_session_visibility(request, auth, session_id)
    identity = await get_session_identity(engine, auth.scope_id, session_id)
    if identity is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    return _identity_response(identity)


@router.patch(
    "/sessions/{session_id:path}/visibility", summary="Change a session's visibility policy"
)
async def update_session_visibility(
    session_id: str,
    body: UpdateSessionVisibilityRequest,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> dict[str, object]:
    """Mutating a session's visibility requires ownership or Agent-manage authority (R1B)."""
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    if not await _session_in_scope(request, session_id, auth.scope_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    await _authorize_session_visibility(request, auth, session_id)
    identity = await get_session_identity(engine, auth.scope_id, session_id)
    if identity is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    actor_user_id = auth.actor.user_id if auth.is_user else None
    is_owner = actor_user_id is not None and identity.owner_user_id == actor_user_id
    if not is_owner and not await _actor_can_manage_agent(request, auth):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "changing session visibility requires ownership or Agent-manage authority",
        )
    audit = _session_audit_service(request)
    updated = await set_session_visibility(engine, auth.scope_id, session_id, body.visibility)
    if updated is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    audit.audit.record(
        AuditEvent(
            AuditAction.session_visibility_changed,
            actor_user_id,
            auth.org_id,
            session_id,
            {"visibility": body.visibility.value},
        )
    )
    return _identity_response(updated)


@router.get("/sessions/{session_id:path}/shares", summary="List a session's explicit shares")
async def list_session_shares(
    session_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> list[dict[str, object]]:
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    if not await _session_in_scope(request, session_id, auth.scope_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    await _authorize_session_visibility(request, auth, session_id)
    identity = await get_session_identity(engine, auth.scope_id, session_id)
    if identity is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    actor_user_id = auth.actor.user_id if auth.is_user else None
    is_owner = actor_user_id is not None and identity.owner_user_id == actor_user_id
    if not is_owner and not await _actor_can_manage_agent(request, auth):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "listing session shares requires ownership or manage"
        )
    shares = await _session_access_store(request, auth.scope_id).list_shares(
        auth.scope_id, session_id
    )
    return [
        {
            "id": s.id,
            "user_id": s.user_id,
            "granted_by_user_id": s.granted_by_user_id,
            "status": s.status.value,
            "created_at": s.created_at.isoformat() if s.created_at else None,
        }
        for s in shares
        if s.is_active
    ]


@router.post(
    "/sessions/{session_id:path}/shares",
    summary="Grant a user explicit read access to a session",
    status_code=status.HTTP_201_CREATED,
)
async def create_session_share(
    session_id: str,
    body: ShareSessionRequest,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> dict[str, object]:
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    if not await _session_in_scope(request, session_id, auth.scope_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    await _authorize_session_visibility(request, auth, session_id)
    identity = await get_session_identity(engine, auth.scope_id, session_id)
    if identity is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    actor_user_id = auth.actor.user_id if auth.is_user else None
    is_owner = actor_user_id is not None and identity.owner_user_id == actor_user_id
    if not is_owner and not await _actor_can_manage_agent(request, auth):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "sharing a session requires ownership or manage"
        )
    audit = _session_audit_service(request)
    grantor = actor_user_id or durable_actor_id(auth.actor)
    share = await _session_access_store(request, auth.scope_id).create_share(
        auth.scope_id, session_id, body.user_id, granted_by_user_id=grantor
    )
    audit.audit.record(
        AuditEvent(
            AuditAction.session_share_granted,
            actor_user_id,
            auth.org_id,
            share.id,
            {"session_id": session_id, "user_id": share.user_id},
        )
    )
    return {
        "id": share.id,
        "user_id": share.user_id,
        "granted_by_user_id": share.granted_by_user_id,
        "status": share.status.value,
    }


@router.delete(
    "/sessions/{session_id:path}/shares/{user_id}",
    summary="Revoke a user's explicit session share",
)
async def revoke_session_share(
    session_id: str,
    user_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> dict[str, object]:
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    if not await _session_in_scope(request, session_id, auth.scope_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    await _authorize_session_visibility(request, auth, session_id)
    identity = await get_session_identity(engine, auth.scope_id, session_id)
    if identity is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session not found")
    actor_user_id = auth.actor.user_id if auth.is_user else None
    is_owner = actor_user_id is not None and identity.owner_user_id == actor_user_id
    if not is_owner and not await _actor_can_manage_agent(request, auth):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "revoking a session share requires ownership or manage"
        )
    audit = _session_audit_service(request)
    revoked = await _session_access_store(request, auth.scope_id).revoke_share(
        auth.scope_id, session_id, user_id
    )
    if revoked is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "share not found")
    audit.audit.record(
        AuditEvent(
            AuditAction.session_share_revoked,
            actor_user_id,
            auth.org_id,
            revoked.id,
            {"session_id": session_id, "user_id": revoked.user_id},
        )
    )
    return {"id": revoked.id, "user_id": revoked.user_id, "status": revoked.status.value}


def _proposal_store(request: Request, scope_id: ScopeId) -> MemoryProposalStore:
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "datastore unavailable")
    return MemoryProposalStore(engine, scope_id)


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
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
    status_filter: str | None = Query(None, alias="status"),
) -> list[dict[str, object]]:
    """Core-memory rewrite proposals awaiting (or past) human review, scoped."""
    store = _proposal_store(request, auth.scope_id)
    return [_proposal_dict(p) for p in await store.list_proposals(status=status_filter)]


@router.get("/memory/blocks", summary="List the scope's current core-memory blocks")
async def list_memory_blocks(
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.viewer))],
) -> list[dict[str, object]]:
    """Return the current block values that interactive memory tools read and update."""
    engine = getattr(request.app.state, "engine", None)
    if engine is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "datastore unavailable")
    rows = await PostgresMemoryStore(engine, auth.scope_id).snapshot()
    return [{"key": key, "value": value, "version": version} for key, value, version in rows]


@router.post(
    "/memory/proposals/{proposal_id}/approve",
    summary="Approve a proposal (atomically apply it to core memory)",
)
async def approve_memory_proposal(
    proposal_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> JSONResponse:
    """Apply the proposal under an optimistic version check; stale ones 409."""
    store = _proposal_store(request, auth.scope_id)
    return _resolution_response(await store.approve(proposal_id, "web"))


@router.post(
    "/memory/proposals/{proposal_id}/reject",
    summary="Reject a proposal (no change to core memory)",
)
async def reject_memory_proposal(
    proposal_id: str,
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> JSONResponse:
    """Mark the proposal rejected; core memory is untouched."""
    store = _proposal_store(request, auth.scope_id)
    return _resolution_response(await store.reject(proposal_id, "web"))


@router.post(
    "/memory/consolidation/run",
    summary="Enqueue a memory-consolidation run for the scope now",
)
async def run_consolidation(
    request: Request,
    auth: Annotated[EndpointAuth, Depends(require_privilege(EndpointPrivilege.operator))],
) -> dict[str, bool]:
    """Manually trigger the derived scope's consolidation schedule (same path as the tick)."""
    schedule_id = consolidation_schedule_id(auth.scope_id)
    if await _schedule_store(request, auth.scope_id).get(schedule_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "consolidation schedule not found")
    enqueue = getattr(request.app.state, "enqueue", None)
    if enqueue is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "job queue unavailable")
    await enqueue("run_agent", schedule_id, auth.scope_id)
    return {"ok": True}
