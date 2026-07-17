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
from keel_core.config import Settings, get_settings, load_env_file
from keel_core.db import make_async_engine, make_redis
from keel_core.embeddings import Embedder
from keel_core.identity import (
    HTTPJWKSProvider,
    IdentityService,
    InMemoryIdentityStore,
    LoggingAuditSink,
    OIDCConfig,
    OIDCVerifier,
    PostgresIdentityStore,
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
from keel_core.providers import LiteLLMGateway
from keel_core.tools import build_service_execution_environment
from keel_core.webhooks import InMemoryWebhookReplayStore, PostgresWebhookReplayStore
from keel_server.api import gateway as gateway_api
from keel_server.api import identity as identity_api
from keel_server.api import knowledge as knowledge_api
from keel_server.api import lifecycle as lifecycle_api
from keel_server.api import oauth as oauth_api
from keel_server.api import v1
from keel_server.auth import parse_api_keys
from keel_server.gateway import OneBotGateway, RateLimiter, TelegramGateway
from keel_server.runtime import AgentRuntime
from keel_server.webui import INDEX_HTML, pages_router

logger = logging.getLogger("keel.server")

_DURABLE_SCOPE = "web:local"


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
        )
        provider = HTTPJWKSProvider(
            settings.oidc_jwks_uri,
            cache_ttl_seconds=settings.oidc_jwks_cache_ttl_seconds,
        )
        verifier = OIDCVerifier(config, provider)
    return service, verifier


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build datastore clients + the agent runtime on startup; dispose on shutdown."""
    settings = get_settings()
    load_env_file()  # provider keys (OPENAI/ANTHROPIC/...) for LiteLLM
    redis_client = make_redis(settings)
    engine = make_async_engine(settings) if settings.event_store == "postgres" else None
    app.state.redis = redis_client
    app.state.engine = engine
    app.state.durable_scope = _DURABLE_SCOPE
    app.state.jobs = _build_job_store(engine, _DURABLE_SCOPE, settings)
    execution_environment = build_service_execution_environment(
        settings,
        Path.cwd(),
        service="server",
    )
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
    try:
        yield
    finally:
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
                checks["postgres"] = "ok"
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

        body = ReadinessResponse(ready=ready, checks=checks)
        return JSONResponse(body.model_dump(), status_code=200 if ready else 503)

    app.include_router(v1.router)
    app.include_router(identity_api.router)
    app.include_router(knowledge_api.router)
    app.include_router(lifecycle_api.router)
    app.include_router(oauth_api.router)
    app.include_router(gateway_api.router)
    app.include_router(pages_router)

    return app


app = create_app()
