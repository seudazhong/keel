"""FastAPI application factory: probes, the agent runtime, and the web UI.

Liveness/readiness let ``compose --profile dev`` report a healthy stack. A
``lifespan`` builds the :class:`~keel_server.runtime.AgentRuntime` (Redis fan-out +
durable store + provider) and stores it on ``app.state`` for the ``/v1`` routes.
Datastore clients connect lazily, so importing this module never needs a live
datastore; the runtime is only built when the app actually starts.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core import __version__
from keel_core.api import HealthResponse, ReadinessResponse
from keel_core.approvals import InMemoryApprovalStore, PostgresApprovalStore
from keel_core.coding import (
    LocalActiveGitStore,
    LocalArtifactStore,
    LocalCodingStorage,
    LocalWorktreeStore,
)
from keel_core.coding.storage_root import (
    SharedStorageUnavailable,
    resolve_project_storage_root,
    verify_shared_storage,
)
from keel_core.config import Settings, get_settings, load_env_file
from keel_core.connector_credentials import ConnectorCredentialStore
from keel_core.connector_schedule_index import PostgresConnectorScheduleIndex
from keel_core.connector_webhook_routes import (
    InMemoryConnectorWebhookRouteStore,
    PostgresConnectorWebhookRouteStore,
)
from keel_core.db import make_async_engine, make_redis
from keel_core.embeddings import Embedder
from keel_core.errors import RuntimePrincipalError
from keel_core.identity import (
    HTTPJWKSProvider,
    IdentityService,
    InMemoryIdentityStore,
    LoggingAuditSink,
    OIDCConfig,
    OIDCVerifier,
    PostgresIdentityStore,
)
from keel_core.job_dispatch import (
    JobDispatchOutbox,
    PostgresJobDispatchOutbox,
)
from keel_core.jobs import (
    InMemoryJobStore,
    JobLimits,
    JobStore,
    PostgresJobStore,
)
from keel_core.knowledge.search import KnowledgeSearcher
from keel_core.knowledge.service import DispatchJob, KnowledgeService
from keel_core.knowledge.store import KnowledgeStore, PostgresKnowledgeStore
from keel_core.oauth_state import InMemoryOAuthStateStore, PostgresOAuthStateStore
from keel_core.projects import (
    GitHubIntegration,
    InMemoryProjectStore,
    LocalProjectStorage,
    PostgresProjectStore,
    ProjectService,
    ProjectStorage,
)
from keel_core.projects.github_factory import build_github_integration
from keel_core.projects.jobs import (
    PROJECT_SYNC_CANCEL_MODE,
    PROJECT_SYNC_KIND,
    PROJECT_SYNC_MAX_ATTEMPTS,
    ProjectSyncPayload,
    sync_idempotency_key,
)
from keel_core.providers import LiteLLMGateway
from keel_core.review import (
    REVIEW_RUN_MAX_ATTEMPTS,
    ReviewCoordinator,
    ReviewService,
)
from keel_core.run_dispatch import PostgresRunDispatchOutbox
from keel_core.runs import InMemoryRunStore, PostgresRunStore
from keel_core.runtime_db import inspect_runtime_principal, verify_runtime_principal
from keel_core.tools import build_service_execution_environment
from keel_core.webhooks import InMemoryWebhookReplayStore, PostgresWebhookReplayStore
from keel_server.api import connectors as connectors_api
from keel_server.api import gateway as gateway_api
from keel_server.api import identity as identity_api
from keel_server.api import im_routing as im_routing_api
from keel_server.api import knowledge as knowledge_api
from keel_server.api import lifecycle as lifecycle_api
from keel_server.api import oauth as oauth_api
from keel_server.api import projects as projects_api
from keel_server.api import reviews as reviews_api
from keel_server.api import v1
from keel_server.auth import parse_api_keys
from keel_server.gateway import OneBotGateway, RateLimiter, TelegramGateway
from keel_server.runtime import AgentRuntime
from keel_server.webui import INDEX_HTML, pages_router

logger = logging.getLogger("keel.server")

_DURABLE_SCOPE = "web:local"

# Bound the per-scope Knowledge service cache so a long-lived server that serves many distinct
# per-Agent scopes cannot grow the map without limit. Scoped services hold no dedicated resources
# (all share the process engine), so a coarse clear-on-full eviction is sufficient.
_KNOWLEDGE_SERVICE_CACHE_MAX = 256


def _build_job_store(
    engine: AsyncEngine | None,
    scope_id: str,
    settings: Settings,
) -> JobStore:
    limits = JobLimits.from_settings(settings)
    if engine is None:
        return InMemoryJobStore(scope_id, limits=limits)
    return PostgresJobStore(engine, scope_id, limits=limits)


async def _enqueue_arq(pool: Any, name: str, *args: object, **options: object) -> None:
    await pool.enqueue_job(name, *args, **options)


def _build_erasure_service(
    engine: AsyncEngine | None,
    scope_id: str,
    jobs: JobStore,
    *,
    redis_client: Any,
    dispatch_job: Any,
) -> Any:
    """Build the scope-bound erasure admission service (None in the lite/memory profile)."""
    if engine is None:
        return None
    from keel_core.lifecycle.coordinator import ErasureCoordinator, UnsupportedExternalStep
    from keel_core.lifecycle.redis import RedisLifecycleCleaner
    from keel_core.lifecycle.service import ErasureService
    from keel_core.lifecycle.store import PostgresErasureStore

    coordinator = ErasureCoordinator(
        engine,
        PostgresErasureStore(engine, scope_id),
        redis_cleaner=RedisLifecycleCleaner(redis_client),
        external_steps=[UnsupportedExternalStep("provider_telemetry")],
    )
    return ErasureService(coordinator, jobs, dispatch_job=dispatch_job)


def _build_knowledge_service(
    engine: AsyncEngine | None,
    scope_id: str,
    settings: Settings,
    jobs: JobStore,
    *,
    embedder: Embedder | None,
    dispatch_job: DispatchJob | None = None,
    dispatch_outbox: JobDispatchOutbox | None = None,
) -> KnowledgeService | None:
    if engine is None:
        return None
    store = PostgresKnowledgeStore(
        engine,
        scope_id,
        document_max_bytes=settings.knowledge_document_max_bytes,
    )
    searcher = KnowledgeSearcher(
        engine,
        scope_id,
        embedder,
        query_max_chars=settings.knowledge_search_query_max_chars,
        k_max=settings.knowledge_search_k_max,
    )
    return KnowledgeService(
        cast(KnowledgeStore, store),
        jobs,
        settings,
        searcher=searcher,
        dispatch_job=dispatch_job,
        dispatch_outbox=dispatch_outbox,
        embedding_model=(None if embedder is None else embedder.model),
        embedding_dim=(None if embedder is None else embedder.dim),
    )


def _build_identity(engine: AsyncEngine | None, settings: Settings) -> tuple[Any, Any]:
    """Build the identity service + OIDC verifier (durable when an engine is configured)."""
    store: Any = PostgresIdentityStore(engine) if engine is not None else InMemoryIdentityStore()
    service = IdentityService(
        store,
        audit=LoggingAuditSink(),
        allow_jit_provisioning=settings.identity_allow_jit_provisioning,
    )
    verifier: OIDCVerifier | None = None
    if (
        settings.oidc_enabled
        and settings.oidc_issuer
        and settings.oidc_audience
        and settings.oidc_jwks_uri
    ):
        algorithms = tuple(a.strip() for a in settings.oidc_algorithms.split(",") if a.strip())
        config = OIDCConfig.from_settings(
            issuer=settings.oidc_issuer,
            audience=settings.oidc_audience,
            algorithms=algorithms or None,
            leeway_seconds=settings.oidc_leeway_seconds,
            client_id=settings.oidc_client_id or None,
        )
        provider = HTTPJWKSProvider(
            settings.oidc_jwks_uri,
            cache_ttl_seconds=settings.oidc_jwks_cache_ttl_seconds,
            min_refresh_interval_seconds=settings.oidc_jwks_min_refresh_interval_seconds,
            failure_cooldown_seconds=settings.oidc_jwks_failure_cooldown_seconds,
        )
        verifier = OIDCVerifier(config, provider)
    return service, verifier


def _build_github_integration(settings: Settings) -> GitHubIntegration | None:
    """Build the GitHub App integration when configured (else ``None`` — feature disabled).

    Delegates to the shared control-plane factory, which validates ``github_api_base_url`` is an
    HTTPS, non-private, allow-listed host BEFORE any authenticated client is built, so a
    just-in-time installation token is never sent to an arbitrary/plain-HTTP/internal endpoint.
    """
    return build_github_integration(settings)


def _build_project_service(
    engine: AsyncEngine | None,
    settings: Settings,
    identity_store: Any,
    enqueue_sync: Any,
    coding_root: Path | None,
) -> ProjectService:
    """Build the managed-project service (durable when an engine is configured)."""
    store: Any = PostgresProjectStore(engine) if engine is not None else InMemoryProjectStore()
    storage: ProjectStorage | None = None
    if engine is not None and coding_root is not None:
        hosts = tuple(
            h.strip().lower() for h in settings.github_allowed_hosts.split(",") if h.strip()
        )
        coding = LocalCodingStorage(coding_root, allowed_https_hosts=hosts)
        storage = LocalProjectStorage(LocalActiveGitStore(coding), LocalWorktreeStore(coding))
    return ProjectService(
        store,
        identity_store,
        storage=storage,
        github=_build_github_integration(settings),
        enqueue_sync=enqueue_sync,
    )


def _resolve_coding_root(settings: Settings) -> Path | None:
    """Resolve the shared project/coding storage root, verifying it is usable (fail closed).

    Returns ``None`` when the root cannot be provisioned (cloud without
    ``KEEL_PROJECT_STORAGE_ROOT``, or an unwritable volume) so project storage + review degrade
    to unavailable and readiness reflects it, instead of silently splitting server/worker
    storage.
    """
    try:
        root = resolve_project_storage_root(settings.project_storage_root, app_env=settings.app_env)
        verify_shared_storage(root)
        return root
    except SharedStorageUnavailable:
        logging.getLogger("keel.server").warning(
            "shared project storage root unavailable; managed projects + review disabled",
            exc_info=True,
        )
        return None


def _build_review_coordinator(
    engine: AsyncEngine | None,
    settings: Settings,
    projects: ProjectService,
    coding_root: Path | None,
) -> ReviewCoordinator | None:
    """Build the read-only review coordinator over the shared project/coding storage root.

    Requires a durable Postgres substrate (a separate worker process claims the review run) and
    the SAME storage root the worker uses, so a worker-written review artifact is readable by
    the server's report APIs. The provider is the shared ``LiteLLMGateway`` (the same provider
    path the agent loop uses) — no second, policy-bypassing provider route. Request metadata is
    durably recorded on the review scope's event log so status projections survive a restart.
    """
    if engine is None or coding_root is None:
        return None
    if not settings.review_enabled:
        # Review explicitly disabled: the server neither builds the coordinator nor accepts/
        # enqueues review jobs (the reviews API then fails closed with 503).
        return None
    from keel_core.state import PostgresEventStore

    hosts = tuple(h.strip().lower() for h in settings.github_allowed_hosts.split(",") if h.strip())
    coding = LocalCodingStorage(coding_root, allowed_https_hosts=hosts)
    review_service = ReviewService(
        worktrees=LocalWorktreeStore(coding),
        artifacts=LocalArtifactStore(coding),
        provider=LiteLLMGateway(),
        price_book=settings.review_price_book,
        report_retention_days=settings.review_report_retention_days,
    )
    return ReviewCoordinator(
        projects=projects,
        runs=PostgresRunStore(engine, _DURABLE_SCOPE),
        review_service=review_service,
        artifacts=LocalArtifactStore(coding),
        scope_id=_DURABLE_SCOPE,
        events=PostgresEventStore(engine, _DURABLE_SCOPE),
    )


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build datastore clients + the agent runtime on startup; dispose on shutdown."""
    settings = get_settings()
    load_env_file()  # provider keys (OPENAI/ANTHROPIC/...) for LiteLLM
    redis_client = make_redis(settings)
    engine = make_async_engine(settings) if settings.event_store == "postgres" else None
    app.state.redis = redis_client
    app.state.engine = engine
    app.state.settings = settings
    app.state.durable_scope = _DURABLE_SCOPE
    # Worker-owned durable admission is only valid with a shared Postgres substrate a separate
    # worker process can read; in-memory stores are process-local (M3.6, item 2).
    app.state.shared_run_substrate = engine is not None
    # Global cross-scope dispatch outbox: admission records a run's dispatch intent here so the
    # worker reconciler can recover it from any scope (M3.6, finding 4). Only meaningful with a
    # shared Postgres substrate; None with in-memory/process-local stores.
    app.state.dispatch_outbox = PostgresRunDispatchOutbox(engine) if engine is not None else None
    # Global cross-scope Knowledge/durable-job dispatch outbox (M3.6, finding 3): Knowledge job
    # admission records a job's dispatch intent here (atomically with the job insert) so the
    # worker's job reconciler can dispatch it from any per-Agent scope, and a document created in
    # a per-Agent scope is actually indexed rather than orphaned. None with in-memory stores.
    app.state.job_dispatch_outbox = (
        PostgresJobDispatchOutbox(engine) if engine is not None else None
    )
    # Global connector schedule index (finding 1): a binding that arms a recurring sync/renewal
    # registers its scope here so the worker's cross-scope recurring reconciler fires it. None
    # (in-memory/lite) keeps the single-tenant behavior.
    app.state.connector_schedule_index = (
        PostgresConnectorScheduleIndex(engine) if engine is not None else None
    )
    # Global connector webhook routing capability (webhook scope routing): a webhook-capable
    # connector mints a high-entropy route token at setup so an auth-headerless delivery resolves
    # back to its exact org/Agent scope + binding, never the app-global ``web:local`` scope.
    app.state.connector_webhook_route_store = (
        PostgresConnectorWebhookRouteStore(engine)
        if engine is not None
        else InMemoryConnectorWebhookRouteStore()
    )
    app.state.jobs = _build_job_store(engine, _DURABLE_SCOPE, settings)
    execution_environment = build_service_execution_environment(
        settings,
        Path.cwd(),
        service="server",
    )
    # Stored so ``/readiness`` can actively probe the sandbox RPC boundary (reachable +
    # authenticated) rather than merely confirming a client object was constructed.
    app.state.execution_environment = execution_environment
    app.state.runtime = AgentRuntime(
        redis_client=redis_client,
        engine=engine,
        scope_id=_DURABLE_SCOPE,
        model=settings.default_model,
        workspace=Path.cwd(),
        execution_environment=execution_environment,
        embedding_model=settings.embedding_model,
        embedding_dim=settings.embedding_dim,
        embedding_send_dimensions=settings.embedding_send_dimensions,
        embedding_timeout_seconds=settings.embedding_timeout_seconds,
        memory_block_max_chars=settings.memory_block_max_chars,
        session_embedding_batch_size=settings.session_embedding_batch_size,
        session_embedding_catchup_limit=settings.session_embedding_catchup_limit,
        knowledge_search_query_max_chars=settings.knowledge_search_query_max_chars,
        knowledge_search_k_max=settings.knowledge_search_k_max,
        knowledge_tool_output_max_chars=settings.knowledge_tool_output_max_chars,
    )
    # Durable approvals raised by unattended (scheduled) runs — the Approvals page +
    # API read this; approving enqueues a resume_run onto the worker's arq queue (G5).
    app.state.api_keys = parse_api_keys(settings.api_keys)  # RBAC: empty -> open mode
    # Cloud mode fails closed: with no API keys the auth layer rejects every request
    # instead of falling back to implicit-admin open mode.
    app.state.auth_required = settings.cloud_mode
    # Legacy API-key migration mapping (review finding 5): an explicit, cloud-only default
    # org+Agent that pre-identity ``key:role`` credentials bind to during migration. ``None``
    # (the default) keeps unbound bare keys failing closed in cloud mode.
    app.state.legacy_machine_binding = settings.legacy_machine_binding
    # Durable, expiring, one-time OAuth CSRF state + webhook replay protection (M3.3).
    app.state.oauth_state_store = (
        PostgresOAuthStateStore(engine, ttl_seconds=settings.oauth_state_ttl_seconds)
        if engine is not None
        else InMemoryOAuthStateStore(ttl_seconds=settings.oauth_state_ttl_seconds)
    )
    app.state.webhook_replay_store = (
        PostgresWebhookReplayStore(engine, ttl_seconds=settings.webhook_replay_ttl_seconds)
        if engine is not None
        else InMemoryWebhookReplayStore(ttl_seconds=settings.webhook_replay_ttl_seconds)
    )
    app.state.durable_approvals = (
        PostgresApprovalStore(engine, _DURABLE_SCOPE)
        if engine is not None
        else InMemoryApprovalStore()
    )
    # Durable, worker-owned interactive runs (M3.6). The server *reads* run status/events
    # and routes admission/interrupt through the durable run store; a worker owns execution.
    app.state.runs = (
        PostgresRunStore(engine, _DURABLE_SCOPE) if engine is not None else InMemoryRunStore()
    )
    app.state.arq = None
    app.state.enqueue = None
    try:
        from arq import create_pool
        from arq.connections import RedisSettings

        arq_pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
        app.state.arq = arq_pool

        async def _enqueue(name: str, *args: object, **options: object) -> None:
            await _enqueue_arq(arq_pool, name, *args, **options)

        app.state.enqueue = _enqueue
    except Exception:  # noqa: BLE001 - resume enqueue is best-effort; the page still renders
        logger.warning("arq queue unavailable; durable-approval resume enqueue disabled")

    async def _dispatch_knowledge_job(scope_id: str, job_id: str) -> None:
        enqueue = getattr(app.state, "enqueue", None)
        if enqueue is None:
            logger.warning(
                "knowledge job queue unavailable; dispatcher will recover scope=%s job=%s",
                scope_id,
                job_id,
            )
            return
        await enqueue("run_job", scope_id, job_id)

    app.state.knowledge = _build_knowledge_service(
        engine,
        _DURABLE_SCOPE,
        settings,
        app.state.jobs,
        embedder=app.state.runtime.embedder,
        dispatch_job=_dispatch_knowledge_job,
        dispatch_outbox=app.state.job_dispatch_outbox,
    )
    # Per-request scoped Knowledge factory (M3.6, finding 3). An authenticated call derives its
    # canonical ``agent:<org>/<agent>`` scope (or the non-cloud ``web:local`` local-preview scope)
    # and this factory builds a KnowledgeService bound to exactly that scope — so no authenticated
    # call ever touches a foreign scope's Knowledge (a singleton ``web:local`` service). All stores
    # share the process engine (disposed at shutdown), so a bounded cache holds no extra resources;
    # it only caps distinct-scope service objects. Knowledge mutation is enabled only when the
    # queue + shared substrate exist so a document's ingest job is actually dispatched.
    knowledge_cache: dict[str, KnowledgeService] = {}
    knowledge_embedder = app.state.runtime.embedder
    job_outbox = app.state.job_dispatch_outbox

    def _knowledge_factory(scope_id: str) -> KnowledgeService | None:
        if engine is None:
            return None
        cached = knowledge_cache.get(scope_id)
        if cached is not None:
            return cached
        service = _build_knowledge_service(
            engine,
            scope_id,
            settings,
            _build_job_store(engine, scope_id, settings),
            embedder=knowledge_embedder,
            dispatch_job=_dispatch_knowledge_job,
            dispatch_outbox=job_outbox,
        )
        if service is None:
            return None
        if len(knowledge_cache) >= _KNOWLEDGE_SERVICE_CACHE_MAX:
            # Simple bounded eviction: drop the whole map rather than track LRU order; scoped
            # services are cheap to rebuild (they hold no dedicated connections).
            knowledge_cache.clear()
        knowledge_cache[scope_id] = service
        return service

    app.state.knowledge_factory = _knowledge_factory
    app.state.knowledge_cache = knowledge_cache
    # Knowledge mutation (create/update/reindex/delete) needs a live queue + shared substrate so an
    # ingest/delete job is actually dispatched to a worker (else the reconciler heals it). Readiness
    # surfaces this so a load balancer can drain an instance that would silently orphan documents.
    app.state.knowledge_mutation_enabled = engine is not None

    # Per-scope connector factory (M3.6 routing integration). The connector foundation (main's
    # 0016) was built single-scope (``web:local``); this factory derives every connector
    # collaborator — repository, encrypted credential store, job store, durable change sink, and
    # typed-target validator — from an arbitrary canonical scope so an authenticated connector
    # management call binds to the caller's ``agent:<org>/<agent>`` scope (via ``auth.scope_id``)
    # rather than the shared app-global ``web:local`` singleton. All stores share the process
    # engine, so a per-request service is cheap; scope-partitioned RLS keeps a foreign scope's
    # bindings/credentials invisible. The unauthenticated webhook ingress path binds the app's
    # ``durable_scope`` (``web:local``), matching main's single-tenant ingress.
    from keel_core.connector_contracts import ConnectorTargetKind
    from keel_core.connector_registry import get_connector_registry
    from keel_core.connector_repository import (
        InMemoryConnectorRepository,
        PostgresConnectorRepository,
    )
    from keel_core.connector_service import (
        ConnectorService,
        DurableConnectorChangeSink,
    )
    from keel_core.errors import DuplicateEventError
    from keel_core.knowledge.models import KnowledgeBaseStatus
    from keel_core.loop import admit_external
    from keel_core.outbox import purge_connector as _purge_outbound_connector
    from keel_core.secrets import SecretsError, keyring_from_settings
    from keel_core.state import InMemoryEventStore, PostgresEventStore, session_exists
    from keel_core.tokens import PostgresTokenStore, delete_token

    connector_registry = getattr(app.state, "connector_registry", None) or get_connector_registry()
    app.state.connector_registry = connector_registry

    def _connector_credentials(scope_id: str) -> Any:
        if engine is None or (not settings.secret_key and not settings.secret_keys):
            return None
        try:
            token_store = PostgresTokenStore(engine, scope_id, keyring_from_settings(settings))
        except SecretsError:
            return None
        return ConnectorCredentialStore(token_store)

    def _build_connector_service(scope_id: str) -> ConnectorService:
        repository = (
            PostgresConnectorRepository(engine, scope_id)
            if engine is not None
            else InMemoryConnectorRepository(scope_id)
        )
        event_store = (
            PostgresEventStore(engine, scope_id) if engine is not None else InMemoryEventStore()
        )
        knowledge_service = _knowledge_factory(scope_id)

        async def _admit_connector_event(session_id: str, content: str, run_id: str) -> None:
            try:
                await admit_external(event_store, session_id, scope_id, content, run_id)
            except DuplicateEventError:
                pass

        async def _validate_connector_target(kind: ConnectorTargetKind, target_id: str) -> bool:
            if kind is ConnectorTargetKind.knowledge:
                if knowledge_service is None:
                    return False
                base = await knowledge_service.get_base(target_id)
                return base is not None and base.status is KnowledgeBaseStatus.active
            if kind is ConnectorTargetKind.trigger_session and engine is not None:
                return await session_exists(engine, scope_id, target_id)
            if kind is ConnectorTargetKind.trigger_session:
                return cast(InMemoryEventStore, event_store).has_session(target_id, scope_id)
            if engine is None:
                return False
            async with engine.begin() as conn:
                await conn.execute(
                    text("SELECT set_config('app.scope_id', :scope, true)"),
                    {"scope": scope_id},
                )
                return bool(
                    await conn.scalar(
                        text(
                            "SELECT EXISTS(SELECT 1 FROM schedules "
                            "WHERE scope_id = :scope AND id = :routine)"
                        ),
                        {"scope": scope_id, "routine": target_id},
                    )
                )

        async def _resolve_connector_trigger(kind: ConnectorTargetKind, target_id: str) -> str:
            if kind is ConnectorTargetKind.trigger_session:
                return target_id
            if engine is None:
                raise RuntimeError("connector trigger routine resolver is unavailable")
            async with engine.begin() as conn:
                await conn.execute(
                    text("SELECT set_config('app.scope_id', :scope, true)"),
                    {"scope": scope_id},
                )
                session_id = await conn.scalar(
                    text(
                        "SELECT session_id FROM schedules WHERE scope_id = :scope AND id = :routine"
                    ),
                    {"scope": scope_id, "routine": target_id},
                )
            if session_id is None:
                raise RuntimeError("connector trigger routine target is unavailable")
            return str(session_id)

        async def _dispatch(dispatch_scope: str, job_id: str) -> None:
            enqueue = getattr(app.state, "enqueue", None)
            if enqueue is not None:
                await enqueue("run_job", dispatch_scope, job_id)

        async def _delete_credential(connector_id: str) -> bool:
            if engine is None:
                return False
            return await delete_token(engine, scope_id, connector_id)

        async def _purge_outbound(connector_id: str) -> int:
            if engine is None:
                return 0
            return await _purge_outbound_connector(engine, scope_id, connector_id)

        return ConnectorService(
            connector_registry,
            repository,
            credentials=_connector_credentials(scope_id),
            jobs=_build_job_store(engine, scope_id, settings),
            dispatch_job=_dispatch,
            dispatch_outbox=getattr(app.state, "job_dispatch_outbox", None),
            schedule_index=getattr(app.state, "connector_schedule_index", None),
            webhook_route_store=getattr(app.state, "connector_webhook_route_store", None),
            change_sink=DurableConnectorChangeSink(
                repository,
                knowledge=knowledge_service,
                admit_event=_admit_connector_event,
                resolve_trigger=_resolve_connector_trigger,
            ),
            target_validator=_validate_connector_target,
            delete_credential=_delete_credential,
            purge_outbound=_purge_outbound,
        )

    app.state.connector_scope_factory = _build_connector_service
    # Default-scope repository for the unauthenticated webhook ingress path and any code that
    # reads the app-global connector repository directly (single-tenant ``web:local`` parity).
    app.state.connector_repository = (
        PostgresConnectorRepository(engine, _DURABLE_SCOPE)
        if engine is not None
        else InMemoryConnectorRepository(_DURABLE_SCOPE)
    )
    app.state.erasure = _build_erasure_service(
        engine,
        _DURABLE_SCOPE,
        app.state.jobs,
        redis_client=redis_client,
        dispatch_job=_dispatch_knowledge_job,
    )
    # Durable identity (users/orgs/memberships/Agents/grants) + OIDC verification (M3.6).
    identity_service, oidc_verifier = _build_identity(engine, settings)
    app.state.identity = identity_service
    app.state.oidc_verifier = oidc_verifier

    # Managed projects + GitHub synchronization (M3.7). Shares the identity store so project
    # authorization reads the same memberships/agents/grants. A GitHub-sourced project's
    # fetch is enqueued as a durable ``projects.sync`` job (restart-safe, delivery-idempotent).
    async def _enqueue_project_sync(org_id: str, project_id: str, delivery_id: str | None) -> None:
        jobs = app.state.jobs
        payload = ProjectSyncPayload(
            org_id=org_id, project_id=project_id, delivery_id=delivery_id
        ).model_dump()
        job, _created = await jobs.enqueue_once(
            kind=PROJECT_SYNC_KIND,
            payload=payload,
            target_session_id=None,
            idempotency_key=sync_idempotency_key(project_id, delivery_id),
            max_attempts=PROJECT_SYNC_MAX_ATTEMPTS,
            cancel_mode=PROJECT_SYNC_CANCEL_MODE,
        )
        await _dispatch_knowledge_job(jobs.scope_id, job.id)

    coding_root = _resolve_coding_root(settings) if engine is not None else None
    app.state.project_storage_root = str(coding_root) if coding_root is not None else None
    app.state.projects = _build_project_service(
        engine, settings, identity_service.store, _enqueue_project_sync, coding_root
    )

    # Read-only managed-code review (WS-R): a durable ``review.run`` job on the existing
    # jobs/outbox substrate, keyed by the review's idempotency key (duplicate = no-op).
    app.state.review_coordinator = _build_review_coordinator(
        engine, settings, app.state.projects, coding_root
    )

    async def _enqueue_review(payload: dict[str, Any], idempotency_key: str) -> None:
        jobs = app.state.jobs
        outbox = getattr(app.state, "job_dispatch_outbox", None)
        if outbox is not None:
            # Record the cross-scope dispatch intent atomically with the job insert so a
            # committed durable review job always has a discoverable dispatch pointer: the
            # worker's job reconciler can dispatch ``review.run`` from any scope, and a lost
            # in-line dispatch can never strand an admitted run.
            job, _created = await jobs.enqueue_once_with_dispatch_intent(
                kind="review.run",
                payload=payload,
                target_session_id=None,
                idempotency_key=idempotency_key,
                max_attempts=REVIEW_RUN_MAX_ATTEMPTS,
                outbox=outbox,
            )
        else:
            job, _created = await jobs.enqueue_once(
                kind="review.run",
                payload=payload,
                target_session_id=None,
                idempotency_key=idempotency_key,
                max_attempts=REVIEW_RUN_MAX_ATTEMPTS,
            )
        await _dispatch_knowledge_job(jobs.scope_id, job.id)

    app.state.enqueue_review = _enqueue_review
    # OneBot IM gateway (optional): only wired when an API base is configured.
    if settings.onebot_api_base:
        app.state.onebot_gateway = OneBotGateway(
            provider=LiteLLMGateway(),
            send=gateway_api.make_onebot_sender(
                settings.onebot_api_base, settings.onebot_access_token
            ),
            workspace=Path.cwd(),
            execution_environment=execution_environment,
            self_id=settings.onebot_self_id or None,
            model=settings.default_model,
            rate_limiter=RateLimiter(limit=settings.im_rate_limit),
        )
    # Telegram IM gateway (optional): only wired when a bot token is configured.
    if settings.telegram_bot_token:
        app.state.telegram_gateway = TelegramGateway(
            provider=LiteLLMGateway(),
            send=gateway_api.make_telegram_sender(settings.telegram_bot_token),
            workspace=Path.cwd(),
            execution_environment=execution_environment,
            bot_username=settings.telegram_bot_username or None,
            model=settings.default_model,
            rate_limiter=RateLimiter(limit=settings.im_rate_limit),
        )
    # Durable IM routing (M3.7): when a Postgres substrate is wired, OneBot/Telegram webhooks
    # resolve the inbound chat through the global route index and admit a durable ``surface="im"``
    # run (worker-owned, safe Agent, durable encrypted reply) instead of the in-process gateway.
    if engine is not None:
        from keel_core.approvals import PostgresApprovalStore as _PgApprovals
        from keel_core.im_routing import PostgresImMappingStore as _PgMappingStore
        from keel_core.im_routing import PostgresImRouteIndex as _PgRouteIndex
        from keel_core.loop import admit as _loop_admit
        from keel_core.run_service import DurableRunService as _DurableRunService
        from keel_core.runs import PostgresRunStore as _PgRunStore
        from keel_core.state import PostgresEventStore as _PgEventStore
        from keel_server.gateway.durable import DurableImIngress

        _im_engine = engine

        def _im_mapping_store(org_id: str) -> _PgMappingStore:
            return _PgMappingStore(_im_engine, org_id)

        def _im_run_service(scope_id: str) -> _DurableRunService:
            async def _enqueue(run_id: str) -> None:
                enq = getattr(app.state, "enqueue", None)
                if enq is not None:
                    await enq("run_interactive", run_id, scope_id)

            return _DurableRunService(
                run_store=_PgRunStore(_im_engine, scope_id),
                event_store=_PgEventStore(_im_engine, scope_id),
                approvals=_PgApprovals(_im_engine, scope_id),
                scope_id=scope_id,
                enqueue=_enqueue,
                admit_fn=_loop_admit,
                dispatch_outbox=getattr(app.state, "dispatch_outbox", None),
            )

        app.state.im_route_index = _PgRouteIndex(engine)
        app.state.im_ingress = DurableImIngress(
            route_index=app.state.im_route_index,
            mapping_store_factory=_im_mapping_store,
            run_service_factory=_im_run_service,
            cloud_mode=settings.cloud_mode,
            default_model=settings.default_model,
        )
    # M3A runtime-role gate (WS-DB). In cloud mode the data plane MUST be served from a
    # least-privilege, non-owner runtime login so ``FORCE ROW LEVEL SECURITY`` is a hard
    # boundary — a superuser / BYPASSRLS / table-owner connection silently bypasses RLS for
    # every tenant. Verify the *connected* principal at startup and fail closed (crash-loop)
    # when it is over-privileged: an over-privileged principal is a definitive misconfiguration
    # that must never serve traffic. A transient DB error is tolerated here (the readiness probe
    # re-checks and keeps the instance drained until the DB is reachable). Outside cloud mode the
    # local-preview single-owner login is expected, so the gate is readiness-informational only.
    app.state.runtime_db_principal = None
    if settings.cloud_mode and engine is not None:
        try:
            async with engine.connect() as conn:
                app.state.runtime_db_principal = await verify_runtime_principal(conn)
        except RuntimePrincipalError as exc:
            logger.critical("refusing to start: %s", exc)
            raise
        except Exception:  # noqa: BLE001 - transient DB error; readiness re-checks, stay drained
            logger.warning(
                "could not verify runtime DB principal at startup; readiness will re-check"
            )
    try:
        yield
    finally:
        cache = getattr(app.state, "knowledge_cache", None)
        if isinstance(cache, dict):
            cache.clear()  # scoped services share the engine disposed below; drop references
        await app.state.runtime.aclose()
        await redis_client.aclose()
        arq = getattr(app.state, "arq", None)
        if arq is not None:
            await arq.aclose()
        if engine is not None:
            await engine.dispose()


def create_app() -> FastAPI:
    """Build the Keel FastAPI application."""
    app = FastAPI(title="Keel", version=__version__, lifespan=_lifespan)
    knowledge_api.register_exception_handlers(app)
    identity_api.register_exception_handlers(app)
    projects_api.register_exception_handlers(app)
    reviews_api.register_exception_handlers(app)

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def index() -> str:
        """Serve the minimal web chat UI."""
        return INDEX_HTML

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        """Liveness: the process is up and serving."""
        return HealthResponse(service="keel-server", version=__version__)

    @app.get("/readiness", response_model=ReadinessResponse)
    async def readiness() -> JSONResponse:
        """Readiness: dependencies (Postgres, Redis) are reachable."""
        settings = get_settings()
        checks: dict[str, str] = {}
        ready = True
        engine = getattr(app.state, "engine", None)
        redis_client = getattr(app.state, "redis", None)

        if redis_client is None:
            body = ReadinessResponse(ready=False, checks={"runtime": "not initialized"})
            return JSONResponse(body.model_dump(), status_code=503)

        if settings.event_store == "postgres":
            try:
                async with engine.connect() as conn:  # type: ignore[union-attr]
                    await conn.execute(text("SELECT 1"))
                    principal_report = await inspect_runtime_principal(conn)
                checks["postgres"] = "ok"
                # Runtime-role gate (see startup): cloud must serve the data plane from a
                # least-privilege, non-owner login or FORCE RLS is not a real boundary. An
                # over-privileged principal fails readiness (503) so the instance is drained
                # rather than serving every tenant from an RLS-exempt connection. In local
                # preview the single owner login is expected and surfaced informationally (never
                # reported as "least-privilege", so the posture is not misrepresented).
                if principal_report.least_privilege:
                    checks["runtime_db_principal"] = (
                        f"least-privilege ({principal_report.principal})"
                    )
                elif settings.cloud_mode:
                    checks["runtime_db_principal"] = (
                        f"over-privileged: {principal_report.describe_violation()}"
                    )
                    ready = False
                else:
                    checks["runtime_db_principal"] = "owner (local-preview)"
            except Exception as exc:  # noqa: BLE001 - report, never crash the probe
                checks["postgres"] = f"error: {exc.__class__.__name__}"
                ready = False
        else:
            checks["event_store"] = "memory"

        try:
            await redis_client.ping()
            checks["redis"] = "ok"
        except Exception as exc:  # noqa: BLE001
            checks["redis"] = f"error: {exc.__class__.__name__}"
            ready = False

        # Surface whether worker-owned durable admission is available. It requires a shared
        # Postgres run substrate a separate worker can read; with in-memory/process-local
        # stores the message endpoint fails closed (503) rather than accept a run a worker
        # cannot see (M3.6, item 2).
        shared = bool(getattr(app.state, "shared_run_substrate", engine is not None))
        checks["run_substrate"] = "shared-postgres" if shared else "in-memory (local-preview)"

        # Readiness must reflect whether the *default* durable message admission path can
        # actually execute. `POST /v1/sessions/{id}/messages` fails closed with 503 unless it
        # has both a shared run substrate and a live run queue to dispatch `run_interactive`.
        # Reporting 200-ready while every message would 503 is a lie; mark the probe degraded
        # so a load balancer drains this instance instead of black-holing traffic (item 7).
        queue_ready = getattr(app.state, "enqueue", None) is not None
        checks["run_queue"] = "ok" if queue_ready else "unavailable"
        admission_ready = shared and queue_ready
        checks["run_admission"] = "ready" if admission_ready else "degraded"
        if not admission_ready:
            ready = False

        # Knowledge mutation (create/update/reindex/delete) enqueues a durable ingest/delete job
        # that a worker must dispatch across scopes via the job-dispatch outbox. When Knowledge
        # mutation is enabled the dispatch path requires both the global job outbox and a live
        # queue; report degraded (503) so a load balancer drains an instance that would accept a
        # document but never index it (finding 3).
        if getattr(app.state, "knowledge_mutation_enabled", False):
            dispatcher_ready = (
                getattr(app.state, "job_dispatch_outbox", None) is not None and queue_ready
            )
            checks["knowledge_dispatch"] = "ready" if dispatcher_ready else "degraded"
            if not dispatcher_ready:
                ready = False

        # Read-only review serves its reports from the SAME shared project-storage volume the
        # worker writes them to. In cloud a durable substrate means reviews are offered; if the
        # shared root was unavailable/unwritable at startup the coordinator is absent, so report
        # reads (and admission) would fail. Report degraded so a load balancer drains this
        # instance instead of accepting reviews whose reports it can never serve (WS-R, F1). When
        # review is explicitly disabled this instance offers no review API, so it is not a
        # readiness concern.
        if settings.cloud_mode and engine is not None and settings.review_enabled:
            storage_root = getattr(app.state, "project_storage_root", None)
            review_ready = (
                getattr(app.state, "review_coordinator", None) is not None
                and storage_root is not None
            )
            checks["project_storage"] = "ok" if storage_root is not None else "unavailable"
            checks["review"] = "ready" if review_ready else "degraded"
            if not review_ready:
                ready = False

        # Execution boundary: with the fail-closed ``sandbox`` backend, ACTIVELY probe the
        # isolated executor's authenticated ``/v1/ping`` (signed empty body) so readiness proves
        # the RPC is reachable AND the HMAC contract holds — not merely that a client object was
        # constructed. A broken/misauthenticated/unreachable sandbox reports degraded (503) so a
        # load balancer drains this instance instead of accepting runs whose tool calls would all
        # fail. The in-process preview backend has no RPC to probe and is reported as such.
        execution_environment = getattr(app.state, "execution_environment", None)
        if settings.execution_backend == "sandbox":
            probe = getattr(execution_environment, "probe_ready", None)
            if probe is None:
                checks["sandbox"] = "unavailable"
                ready = False
            else:
                try:
                    result = await probe()
                except Exception as exc:  # noqa: BLE001 - report, never crash the probe
                    checks["sandbox"] = f"error: {exc.__class__.__name__}"
                    ready = False
                else:
                    if result.ok:
                        checks["sandbox"] = "ok"
                    else:
                        code = result.error.code.value if result.error else "unavailable"
                        checks["sandbox"] = f"degraded: {code}"
                        ready = False
        else:
            checks["sandbox"] = "unsafe-local-dev (in-process preview)"

        body = ReadinessResponse(ready=ready, checks=checks)
        return JSONResponse(body.model_dump(), status_code=200 if ready else 503)

    app.include_router(v1.router)
    app.include_router(identity_api.router)
    app.include_router(projects_api.router)
    app.include_router(reviews_api.router)
    app.include_router(knowledge_api.router)
    app.include_router(lifecycle_api.router)
    app.include_router(connectors_api.router)
    app.include_router(im_routing_api.router)
    # Backward-compatible concrete Gmail OAuth routes (published operation ids
    # ``gmail_oauth_connect``/``gmail_oauth_callback``) for old clients. Registered after the
    # generic connector routes so those richer manifest-driven handlers serve ``/v1/connectors/
    # gmail/*`` at runtime, while these concrete routes keep the published ``/v1`` contract intact.
    app.include_router(oauth_api.router)
    app.include_router(gateway_api.router)
    app.include_router(pages_router)

    return app


app = create_app()
