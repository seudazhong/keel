"""arq worker: agent-run + resume + scheduler tick for the autonomy slice.

Run with: ``arq keel_worker.main.WorkerSettings``
Health:   ``arq keel_worker.main.WorkerSettings --check``

A single-process due-loop (``scheduler_tick``) advances persistent schedules at most
once and enqueues ``run_agent``; an unattended run suspends at a tainted outbound
(durable approval, G5) and ``resume_run`` continues it once the approval resolves. In
production ``ctx`` carries scope-bound Postgres stores (wired in ``startup``); tests
inject in-memory doubles into ``ctx`` directly."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from arq import cron
from arq.connections import RedisSettings
from arq.worker import func

from keel_core import __version__
from keel_core.config import Settings, get_settings, load_env_file
from keel_core.connector_contracts import ConnectorAction, ConnectorActionContext
from keel_core.consolidation.agent import (
    MEMORY_CONSOLIDATOR_AGENT_ID,
    build_consolidation_agent,
    consolidation_permissions,
    consolidation_registry,
    consolidation_session_id,
    consolidation_system_context,
    format_consolidation_prompt,
)
from keel_core.consolidation.context import ConsolidationRunContext, should_advance_cursor
from keel_core.consolidation.cursor import ConsolidationCursorStore
from keel_core.consolidation.reader import ConsolidationBatchReader
from keel_core.digest import (
    DIGEST_INSTRUCTION,
    build_digest_agent,
    digest_permissions,
    digest_registry,
)
from keel_core.jobs import JobLimits, PostgresJobStore
from keel_core.loop import ToolRegistry, admit, resume, run
from keel_core.memory import PostgresMemoryStore
from keel_core.observability import configure_logging, configure_tracing
from keel_core.runs import PostgresRunStore
from keel_core.state import PostgresEventStore
from keel_core.tools import build_service_execution_environment
from keel_scheduler.store import ScheduleRow, due_tick
from keel_worker.connectors import reconcile_connectors_tick, register_connector_jobs
from keel_worker.jobs import dispatch_jobs, reconcile_job_dispatch_tick, run_job
from keel_worker.knowledge import knowledge_job_registry
from keel_worker.review import reconcile_stranded_reviews_tick, review_artifact_reaper_tick
from keel_worker.runs import reconcile_dispatch_tick, reconcile_runs_tick, run_interactive

logger = logging.getLogger("keel.worker")

# The autonomy slice operates on a single scope (matches the web server's default).
_DURABLE_SCOPE = "web:local"


async def _enqueue_arq(redis: Any, name: str, *args: object, **options: object) -> None:
    await redis.enqueue_job(name, *args, **options)


def _connector_actions(
    ctx: dict[str, Any], settings: Settings, scope_id: str
) -> tuple[ConnectorAction, ...]:
    from keel_core.connector_registry import get_connector_registry
    from keel_core.secrets import keyring_from_settings
    from keel_core.tokens import PostgresTokenStore

    registry = ctx.get("connector_registry") or get_connector_registry()
    credential_store = ctx.get("connector_action_credentials")
    engine = ctx.get("engine")
    repository = ctx.get("connector_repository")
    if repository is None and engine is not None:
        from keel_core.connector_repository import PostgresConnectorRepository

        repository = PostgresConnectorRepository(engine, scope_id)
    if (
        credential_store is None
        and engine is not None
        and (settings.secret_key or settings.secret_keys)
    ):
        credential_store = PostgresTokenStore(
            engine,
            scope_id,
            keyring_from_settings(settings),
        )
    idempotency_store = None
    if engine is not None:
        from keel_core.outbox import PostgresOutboundStore

        idempotency_store = PostgresOutboundStore(engine)
    action_context = (
        ConnectorActionContext(
            scope_id,
            credential_store=credential_store,
            idempotency_store=idempotency_store,
        )
        if repository is None
        else ConnectorActionContext.with_repository(
            scope_id,
            repository,
            credential_store=credential_store,
            idempotency_store=idempotency_store,
        )
    )
    return registry.build_actions(action_context)


def _digest_registry(
    ctx: dict[str, Any],
    settings: Settings,
    scope_id: str,
    actions: tuple[ConnectorAction, ...] | None = None,
) -> ToolRegistry:
    connector_actions = (
        actions if actions is not None else _connector_actions(ctx, settings, scope_id)
    )
    idempotency_store = None
    engine = ctx.get("engine")
    if engine is not None:
        from keel_core.outbox import PostgresOutboundStore

        idempotency_store = PostgresOutboundStore(engine)
    return digest_registry(
        ctx.get("sent"),
        idempotency_store=idempotency_store,
        connector_actions=connector_actions,
    )


async def _run_digest(ctx: dict[str, Any], row: ScheduleRow, settings: Settings) -> str:
    """Start an unattended digest run for a due schedule; suspend on a gated send."""
    store, approvals, provider = ctx["store"], ctx["approvals"], ctx["provider"]
    actions = _connector_actions(ctx, settings, row.scope_id)
    agent = build_digest_agent(row.scope_id, actions).model_copy(
        update={"model": settings.default_model}
    )
    # The scheduled trigger is a *user* turn (the agent's standing behavior is its
    # persona/system prompt); a system-only message list is rejected by chat providers.
    await admit(store, row.session_id, row.scope_id, DIGEST_INSTRUCTION)
    result = await run(
        agent=agent,
        session_id=row.session_id,
        store=store,
        provider=provider,
        registry=_digest_registry(ctx, settings, row.scope_id, actions),
        permissions=digest_permissions(actions),
        approvals=approvals,
        expires_at=datetime.now(UTC) + timedelta(hours=settings.approval_timeout_hours),
    )
    await ctx["schedules"].mark_run(row.id, row.next_run_at, result.reason.value)
    return result.reason.value


async def run_agent(ctx: dict[str, Any], schedule_id: str) -> str:
    """Dispatch a due schedule to its agent runner (digest or memory consolidation)."""
    settings = get_settings()
    row = await ctx["schedules"].get(schedule_id)
    if row is None:
        return "missing"
    if row.agent_id == MEMORY_CONSOLIDATOR_AGENT_ID:
        return await consolidate_memory(ctx, row, settings)
    if row.agent_id == "digest":
        return await _run_digest(ctx, row, settings)
    logger.warning("run_agent: unsupported agent_id %r (schedule %s)", row.agent_id, schedule_id)
    return "unsupported"


async def consolidate_memory(ctx: dict[str, Any], row: ScheduleRow, settings: Settings) -> str:
    """Run one memory-consolidation pass for a due schedule (lease -> batch -> agent run).

    Fail-closed: the cursor only advances past the batch when the run reaches ``completed``
    with zero validation errors; otherwise the lease is released with an ``error`` status
    and the same window is retried on the next tick. Best-effort fail-closed: the outer
    ``except`` block attempts cleanup writes (``cursors.fail``, ``schedules.mark_run``) and
    logs the exception; those cleanup writes may themselves raise, propagating to the arq
    task layer. Under normal operation the arq task receives a status string so one bad
    scope cannot crash the worker.
    """
    engine = ctx["engine"]
    provider = ctx["provider"]
    embedder = ctx["embedder"]
    schedules = ctx["schedules"]
    scope_id = row.scope_id

    cursors = ConsolidationCursorStore(engine, scope_id)
    lease = await cursors.claim(
        datetime.now(UTC), lease_seconds=settings.consolidation_lease_seconds
    )
    if lease is None:
        return "busy"  # another worker holds a live lease for this scope
    try:
        reader = ConsolidationBatchReader(engine, scope_id)
        batch = await reader.read(
            lease.last_event_id,
            limit=settings.consolidation_batch_messages,
            message_max_chars=settings.consolidation_message_max_chars,
            input_max_chars=settings.consolidation_input_max_chars,
        )
        if batch.eligible_count < settings.consolidation_min_messages:
            await cursors.fail(lease, "skipped")
            await schedules.mark_run(row.id, row.next_run_at, "skipped")
            return "skipped"

        run_context = ConsolidationRunContext(
            allowed_event_ids=frozenset(message.event_id for message in batch.messages),
            allowed_user_event_ids=batch.user_event_ids,
        )
        memory = PostgresMemoryStore(engine, scope_id)
        store = PostgresEventStore(engine, scope_id)
        run_id = uuid.uuid4().hex
        session_id = consolidation_session_id(scope_id, run_id)
        prompt = format_consolidation_prompt(
            await memory.blocks(), await memory.versions(), batch.messages
        )
        await admit(store, session_id, scope_id, prompt)
        result = await run(
            agent=build_consolidation_agent(
                scope_id,
                settings.default_model,
                token_budget=settings.consolidation_token_budget,
            ),
            session_id=session_id,
            store=store,
            provider=provider,
            registry=consolidation_registry(engine, embedder, run_context, settings),
            permissions=consolidation_permissions(),
            run_id=run_id,
            system_context=consolidation_system_context,
        )
        if should_advance_cursor(result.reason, run_context.validation_errors):
            await cursors.complete(lease, batch.max_event_id, "completed")
            await schedules.mark_run(row.id, row.next_run_at, "completed")
            return "completed"
        await cursors.fail(lease, "error")
        await schedules.mark_run(row.id, row.next_run_at, "error")
        return "error"
    except Exception:
        logger.exception("consolidation run failed for scope %s", scope_id)
        await cursors.fail(lease, "error")
        await schedules.mark_run(row.id, row.next_run_at, "error")
        return "error"


async def resume_run(ctx: dict[str, Any], session_id: str, run_id: str, scope_id: str) -> str:
    """Continue a suspended run after its approval resolved (grant/deny/expire)."""
    settings = get_settings()
    store, approvals, provider = ctx["store"], ctx["approvals"], ctx["provider"]
    actions = _connector_actions(ctx, settings, scope_id)
    agent = build_digest_agent(scope_id, actions).model_copy(
        update={"model": settings.default_model}
    )
    result = await resume(
        agent=agent,
        session_id=session_id,
        run_id=run_id,
        store=store,
        provider=provider,
        registry=_digest_registry(ctx, settings, scope_id, actions),
        permissions=digest_permissions(actions),
        approvals=approvals,
    )
    return result.reason.value


async def scheduler_tick(ctx: dict[str, Any]) -> int:
    """One due-loop tick: enqueue due scheduled runs (at most once).

    Approval expiry is **not** owned here (M3.6, item 6): the durable run reconciler
    (:func:`keel_worker.runs.reconcile_runs_tick`) is the single owner of approval expiry so
    a durable interactive approval is never consumed by this legacy scheduler and routed to
    the wrong (``resume_run``) job. This tick only advances the schedule due-loop."""
    schedules, claim, enqueue = (
        ctx["schedules"],
        ctx["claim"],
        ctx["enqueue"],
    )
    now = datetime.now(UTC)
    enqueued = await due_tick(
        schedules=schedules,
        claim=claim,
        now=now,
        enqueue=lambda sid: enqueue("run_agent", sid),
    )
    return len(enqueued)


async def startup(ctx: dict[str, Any]) -> None:
    load_env_file()  # provider keys visible to LiteLLM before any agent task runs
    settings = get_settings()
    configure_logging(settings.log_level)
    configure_tracing("keel-worker")

    from sqlalchemy.ext.asyncio import create_async_engine

    from keel_core.approvals import PostgresApprovalStore
    from keel_core.embeddings import LiteLLMEmbedder
    from keel_core.identity import (
        IdentityService,
        LoggingAuditSink,
        PostgresIdentityStore,
    )
    from keel_core.knowledge import KnowledgeStore, PostgresKnowledgeStore
    from keel_core.providers import LiteLLMGateway
    from keel_scheduler.store import PostgresClaimStore, PostgresScheduleStore

    engine = create_async_engine(settings.database_url)
    redis = ctx["redis"]
    ctx["engine"] = engine
    ctx["workspace_root"] = Path.cwd()
    ctx["execution_environment"] = build_service_execution_environment(
        settings,
        Path.cwd(),
        service="worker",
    )
    ctx["durable_scope"] = _DURABLE_SCOPE
    ctx["job_settings"] = settings
    ctx["jobs"] = PostgresJobStore(
        engine,
        _DURABLE_SCOPE,
        limits=JobLimits.from_settings(settings),
    )
    ctx["store"] = PostgresEventStore(engine, _DURABLE_SCOPE)
    ctx["approvals"] = PostgresApprovalStore(engine, _DURABLE_SCOPE)
    ctx["runs"] = PostgresRunStore(engine, _DURABLE_SCOPE)
    # Global cross-scope dispatch index: the worker reconciler enumerates every scope with open
    # work from here (not just the pinned _DURABLE_SCOPE) — see reconcile_dispatch_tick.
    from keel_core.run_dispatch import PostgresRunDispatchOutbox

    ctx["dispatch_outbox"] = PostgresRunDispatchOutbox(engine)
    # Global cross-scope Knowledge/durable-job dispatch index: the job reconciler dispatches
    # Knowledge indexing/deletion jobs across every per-Agent scope from here (finding 3), not just
    # the pinned _DURABLE_SCOPE — see reconcile_job_dispatch_tick.
    from keel_core.job_dispatch import PostgresJobDispatchOutbox

    ctx["job_dispatch_outbox"] = PostgresJobDispatchOutbox(engine)
    ctx["schedules"] = PostgresScheduleStore(engine, _DURABLE_SCOPE)
    ctx["claim"] = PostgresClaimStore(engine, _DURABLE_SCOPE)
    ctx["provider"] = LiteLLMGateway()
    embedder = LiteLLMEmbedder(
        settings.embedding_model,
        settings.embedding_dim,
        send_dimensions=settings.embedding_send_dimensions,
        timeout_seconds=settings.embedding_timeout_seconds,
    )
    knowledge = PostgresKnowledgeStore(
        engine,
        _DURABLE_SCOPE,
        document_max_bytes=settings.knowledge_document_max_bytes,
    )
    ctx["embedder"] = embedder
    ctx["knowledge"] = knowledge
    # Durable identity for worker-owned interactive runs (M3.6): the run's persisted selected
    # Agent profile is rebuilt from here, and Agent visibility / org membership / archived
    # status is re-checked at claim time (a revoke between admit and claim fails closed).
    ctx["identity"] = IdentityService(
        PostgresIdentityStore(engine),
        audit=LoggingAuditSink(),
        allow_jit_provisioning=settings.identity_allow_jit_provisioning,
    )
    job_registry = knowledge_job_registry(
        cast(KnowledgeStore, knowledge),
        embedder,
        settings,
    )
    from sqlalchemy import text

    from keel_core.connector_contracts import ConnectorTargetKind
    from keel_core.connector_credentials import ConnectorCredentialStore
    from keel_core.connector_registry import get_connector_registry
    from keel_core.connector_repository import PostgresConnectorRepository
    from keel_core.connector_schedule_index import PostgresConnectorScheduleIndex
    from keel_core.connector_service import ConnectorService, DurableConnectorChangeSink
    from keel_core.connector_webhook_routes import PostgresConnectorWebhookRouteStore
    from keel_core.errors import DuplicateEventError
    from keel_core.knowledge.models import KnowledgeBaseStatus
    from keel_core.knowledge.service import KnowledgeService
    from keel_core.loop import admit_external
    from keel_core.secrets import keyring_from_settings
    from keel_core.state import session_exists
    from keel_core.tokens import PostgresTokenStore

    connector_registry = get_connector_registry()
    ctx["connector_registry"] = connector_registry
    keyring = (
        keyring_from_settings(settings) if (settings.secret_key or settings.secret_keys) else None
    )
    connector_schedule_index = PostgresConnectorScheduleIndex(engine)
    ctx["connector_schedule_index"] = connector_schedule_index
    connector_webhook_route_store = PostgresConnectorWebhookRouteStore(engine)
    ctx["connector_webhook_route_store"] = connector_webhook_route_store

    async def _dispatch_connector_job(dispatch_scope: str, job_id: str) -> None:
        enqueue_fn = ctx.get("enqueue")
        if enqueue_fn is not None:
            await enqueue_fn("run_job", dispatch_scope, job_id)

    def build_connector_service(scope_id: str) -> ConnectorService:
        """Build a fully scope-bound connector service for ``scope_id`` (no ``web:local`` fallback).

        Used both to reconcile recurring schedules across every active scope and by ``run_job`` to
        execute an Agent-scoped ``connector.sync``/``connector.renew`` job against its own scope's
        repository, credentials, and change sink (finding 1).
        """
        repository = PostgresConnectorRepository(engine, scope_id)
        credentials = (
            ConnectorCredentialStore(PostgresTokenStore(engine, scope_id, keyring))
            if keyring is not None
            else None
        )
        scope_knowledge = KnowledgeService(
            cast(
                KnowledgeStore,
                PostgresKnowledgeStore(
                    engine,
                    scope_id,
                    document_max_bytes=settings.knowledge_document_max_bytes,
                ),
            ),
            PostgresJobStore(engine, scope_id, limits=JobLimits.from_settings(settings)),
            settings,
            embedding_model=embedder.model,
            embedding_dim=embedder.dim,
        )

        async def _validate_target(kind: ConnectorTargetKind, target_id: str) -> bool:
            if kind is ConnectorTargetKind.knowledge:
                base = await scope_knowledge.get_base(target_id)
                return base is not None and base.status is KnowledgeBaseStatus.active
            if kind is ConnectorTargetKind.trigger_session:
                return await session_exists(engine, scope_id, target_id)
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

        async def _resolve_trigger(kind: ConnectorTargetKind, target_id: str) -> str:
            if kind is ConnectorTargetKind.trigger_session:
                return target_id
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

        async def _admit_event(session_id: str, content: str, run_id: str) -> None:
            try:
                await admit_external(
                    PostgresEventStore(engine, scope_id), session_id, scope_id, content, run_id
                )
            except DuplicateEventError:
                pass

        return ConnectorService(
            connector_registry,
            repository,
            credentials=credentials,
            jobs=PostgresJobStore(engine, scope_id, limits=JobLimits.from_settings(settings)),
            dispatch_job=_dispatch_connector_job,
            dispatch_outbox=ctx["job_dispatch_outbox"],
            schedule_index=connector_schedule_index,
            webhook_route_store=connector_webhook_route_store,
            change_sink=DurableConnectorChangeSink(
                repository,
                knowledge=scope_knowledge,
                admit_event=_admit_event,
                resolve_trigger=_resolve_trigger,
            ),
            target_validator=_validate_target,
        )

    ctx["connector_scope_factory"] = build_connector_service

    # The durable-scope (``web:local``) connector action credentials + repository are read by the
    # per-scope digest/agent tool actions (``_connector_actions``); keep them wired.
    connector_action_credentials = None
    if keyring is not None:
        connector_action_credentials = PostgresTokenStore(engine, _DURABLE_SCOPE, keyring)
    connector_repository = PostgresConnectorRepository(engine, _DURABLE_SCOPE)
    ctx["connector_repository"] = connector_repository
    ctx["connector_action_credentials"] = connector_action_credentials

    connector_service = build_connector_service(_DURABLE_SCOPE)
    ctx["connector_sync_service"] = connector_service
    register_connector_jobs(job_registry, connector_service, settings)

    # Read-only managed-code review (WS-R): a durable ``review.run`` job that materializes an
    # isolated worktree from the project's coding storage, reviews the diff through the shared
    # provider (no tools), verifies evidence, and stores content-addressed report artifacts.
    from keel_core.coding import (
        LocalArtifactStore as _ReviewArtifactStore,
    )
    from keel_core.coding import (
        LocalCodingStorage as _ReviewCodingStorage,
    )
    from keel_core.coding import (
        LocalWorktreeStore as _ReviewWorktreeStore,
    )
    from keel_core.coding.storage_root import (
        SharedStorageUnavailable,
        resolve_project_storage_root,
        verify_shared_storage,
    )
    from keel_core.projects import PostgresProjectStore as _ReviewProjectStore
    from keel_core.projects import ProjectService as _ReviewProjectService
    from keel_core.review import ReviewCoordinator, ReviewService
    from keel_core.state import PostgresEventStore as _ReviewEventStore
    from keel_worker.review import register_review_jobs

    # Server and worker MUST resolve the SAME storage root so a worker-written review artifact
    # is readable by the server's report APIs (shared/RWX volume in cloud). Fail closed when it
    # is unavailable rather than silently splitting storage.
    _review_coding: _ReviewCodingStorage | None = None
    try:
        _coding_root = resolve_project_storage_root(
            settings.project_storage_root, app_env=settings.app_env
        )
        verify_shared_storage(_coding_root)
        _review_hosts = tuple(
            h.strip().lower() for h in settings.github_allowed_hosts.split(",") if h.strip()
        )
        _review_coding = _ReviewCodingStorage(_coding_root, allowed_https_hosts=_review_hosts)
    except SharedStorageUnavailable:
        logger.warning("shared project storage root unavailable; review job disabled")

    # Durable data-erasure coordinator + job (M3.5). Bounded Redis stream cleanup uses the
    # worker's Redis connection; external provider/telemetry deletion has no API and is
    # recorded as an incomplete step so a request finishes 'partial', never 'completed'. The
    # coding artifact cleaner removes review/coding worktrees + report artifacts on project
    # erasure over the SAME shared storage the review job writes to.
    from keel_core.lifecycle.coding import CodingArtifactCleaner
    from keel_core.lifecycle.coordinator import ErasureCoordinator, UnsupportedExternalStep
    from keel_core.lifecycle.redis import RedisLifecycleCleaner
    from keel_core.lifecycle.store import PostgresErasureStore
    from keel_worker.lifecycle import register_erasure_jobs

    _coding_cleaner: CodingArtifactCleaner | None = None
    if _review_coding is not None:
        from keel_core.coding.models import ProjectId as _CleanerProjectId

        class _CodingProjectPurger:
            """Adapt ``LocalCodingStorage.purge_project`` to the ``ProjectPurger`` seam."""

            def __init__(self, storage: _ReviewCodingStorage) -> None:
                self._storage = storage

            def purge_project(self, project_id: str) -> bool:
                return self._storage.purge_project(_CleanerProjectId(project_id))

        _coding_cleaner = CodingArtifactCleaner(_CodingProjectPurger(_review_coding))

    erasure_coordinator = ErasureCoordinator(
        engine,
        PostgresErasureStore(engine, _DURABLE_SCOPE),
        redis_cleaner=RedisLifecycleCleaner(redis),
        external_steps=[UnsupportedExternalStep("provider_telemetry")],
        coding_cleaner=_coding_cleaner,
    )
    ctx["erasure_coordinator"] = erasure_coordinator
    register_erasure_jobs(
        job_registry, erasure_coordinator, lease_seconds=settings.job_lease_seconds
    )

    if _review_coding is not None:
        from keel_core.projects.github_factory import build_github_integration
        from keel_core.review.github_refs import GitHubPullRequestResolver
        from keel_core.review.ref_materializer import GitHubRefMaterializer

        _review_service = ReviewService(
            worktrees=_ReviewWorktreeStore(_review_coding),
            artifacts=_ReviewArtifactStore(_review_coding),
            provider=ctx["provider"],
            price_book=settings.review_price_book,
            report_retention_days=settings.review_report_retention_days,
        )
        _review_project_service = _ReviewProjectService(
            _ReviewProjectStore(engine), ctx["identity"].store
        )
        # PR review resolves exact base/head SHAs on the control plane (GitHub App). When the
        # App is unconfigured the resolver is absent and a PR review fails explicitly (a PR
        # number is never used as a Git ref). The ref materializer fetches those exact commits
        # into the authoritative repo with the JIT token kept off the sandbox/worktree/logs.
        _review_github = build_github_integration(settings)
        _pr_resolver = (
            GitHubPullRequestResolver(
                projects=_review_project_service,
                github=_review_github,
                ensure_refs=GitHubRefMaterializer(
                    projects=_review_project_service,
                    github=_review_github,
                    storage=_review_coding,
                ),
            )
            if _review_github is not None
            else None
        )
        review_coordinator = ReviewCoordinator(
            projects=_review_project_service,
            runs=ctx["runs"],
            review_service=_review_service,
            artifacts=_ReviewArtifactStore(_review_coding),
            scope_id=_DURABLE_SCOPE,
            events=_ReviewEventStore(engine, _DURABLE_SCOPE),
            pr_resolver=_pr_resolver,
        )
        ctx["review_coordinator"] = review_coordinator
        # Shared stores for the scheduled retention/orphan reaper (retained_until/TTL + stale
        # crash-orphaned worktrees). Active worktrees (younger than the stale cutoff) are kept.
        ctx["review_artifacts"] = _ReviewArtifactStore(_review_coding)
        ctx["review_worktrees"] = _ReviewWorktreeStore(_review_coding)
        ctx["review_worktree_stale_hours"] = settings.review_worktree_stale_hours
        register_review_jobs(job_registry, review_coordinator, settings)
    ctx["job_registry"] = job_registry

    async def enqueue(name: str, *args: object, **options: object) -> None:
        await _enqueue_arq(redis, name, *args, **options)

    ctx["enqueue"] = enqueue
    logger.info("keel-worker %s starting", __version__)


async def shutdown(ctx: dict[str, Any]) -> None:
    execution_environment = ctx.get("execution_environment")
    if execution_environment is not None:
        await execution_environment.aclose()
    engine = ctx.get("engine")
    if engine is not None:
        await engine.dispose()
    logger.info("keel-worker shutting down")


def _redis_settings() -> RedisSettings:
    return RedisSettings.from_dsn(get_settings().redis_url)


class WorkerSettings:
    """arq worker configuration (referenced by the ``arq`` CLI)."""

    functions = [
        run_agent,
        resume_run,
        run_interactive,
        scheduler_tick,
        reconcile_connectors_tick,
        reconcile_runs_tick,
        reconcile_dispatch_tick,
        reconcile_job_dispatch_tick,
        review_artifact_reaper_tick,
        reconcile_stranded_reviews_tick,
        func(
            run_job,
            timeout=get_settings().job_execution_timeout_seconds,
            max_tries=1,
        ),
        dispatch_jobs,
    ]
    cron_jobs = [
        cron(scheduler_tick, second={0, 30}),
        cron(dispatch_jobs, second={0, 30}),
        cron(reconcile_connectors_tick, second={0, 30}),
        cron(reconcile_runs_tick, second={0, 30}),
        cron(reconcile_dispatch_tick, second={0, 30}),
        cron(reconcile_job_dispatch_tick, second={0, 30}),
        cron(reconcile_stranded_reviews_tick, second={0, 30}),
        cron(review_artifact_reaper_tick, minute={0}),
    ]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = _redis_settings()
