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
from keel_worker.connectors import register_connector_jobs
from keel_worker.jobs import dispatch_jobs, run_job
from keel_worker.knowledge import knowledge_job_registry
from keel_worker.runs import reconcile_runs_tick, run_interactive

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
    from keel_core.knowledge import KnowledgeStore, PostgresKnowledgeStore
    from keel_core.providers import LiteLLMGateway
    from keel_scheduler.store import PostgresClaimStore, PostgresScheduleStore

    engine = create_async_engine(settings.database_url)
    redis = ctx["redis"]
    ctx["engine"] = engine
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
    from keel_core.connector_service import ConnectorService, DurableConnectorChangeSink
    from keel_core.errors import DuplicateEventError
    from keel_core.knowledge.models import KnowledgeBaseStatus
    from keel_core.knowledge.service import KnowledgeService
    from keel_core.loop import admit_external
    from keel_core.secrets import keyring_from_settings
    from keel_core.state import session_exists
    from keel_core.tokens import PostgresTokenStore

    connector_credentials = None
    connector_action_credentials = None
    if settings.secret_key or settings.secret_keys:
        connector_action_credentials = PostgresTokenStore(
            engine, _DURABLE_SCOPE, keyring_from_settings(settings)
        )
        connector_credentials = ConnectorCredentialStore(connector_action_credentials)
    connector_repository = PostgresConnectorRepository(engine, _DURABLE_SCOPE)
    ctx["connector_repository"] = connector_repository
    connector_knowledge = KnowledgeService(
        cast(KnowledgeStore, knowledge),
        ctx["jobs"],
        settings,
        embedding_model=embedder.model,
        embedding_dim=embedder.dim,
    )
    connector_registry = get_connector_registry()
    ctx["connector_registry"] = connector_registry
    ctx["connector_action_credentials"] = connector_action_credentials

    async def validate_connector_target(kind: ConnectorTargetKind, target_id: str) -> bool:
        if kind is ConnectorTargetKind.knowledge:
            base = await connector_knowledge.get_base(target_id)
            return base is not None and base.status is KnowledgeBaseStatus.active
        if kind is ConnectorTargetKind.trigger_session:
            return await session_exists(engine, _DURABLE_SCOPE, target_id)
        async with engine.begin() as conn:
            await conn.execute(
                text("SELECT set_config('app.scope_id', :scope, true)"),
                {"scope": _DURABLE_SCOPE},
            )
            return bool(
                await conn.scalar(
                    text(
                        "SELECT EXISTS(SELECT 1 FROM schedules "
                        "WHERE scope_id = :scope AND id = :routine)"
                    ),
                    {"scope": _DURABLE_SCOPE, "routine": target_id},
                )
            )

    async def resolve_connector_trigger(kind: ConnectorTargetKind, target_id: str) -> str:
        if kind is ConnectorTargetKind.trigger_session:
            return target_id
        async with engine.begin() as conn:
            await conn.execute(
                text("SELECT set_config('app.scope_id', :scope, true)"),
                {"scope": _DURABLE_SCOPE},
            )
            session_id = await conn.scalar(
                text("SELECT session_id FROM schedules WHERE scope_id = :scope AND id = :routine"),
                {"scope": _DURABLE_SCOPE, "routine": target_id},
            )
        if session_id is None:
            raise RuntimeError("connector trigger routine target is unavailable")
        return str(session_id)

    async def admit_connector_event(session_id: str, content: str, run_id: str) -> None:
        try:
            await admit_external(
                ctx["store"],
                session_id,
                _DURABLE_SCOPE,
                content,
                run_id,
            )
        except DuplicateEventError:
            pass

    connector_service = ConnectorService(
        connector_registry,
        connector_repository,
        credentials=connector_credentials,
        jobs=ctx["jobs"],
        change_sink=DurableConnectorChangeSink(
            connector_repository,
            knowledge=connector_knowledge,
            admit_event=admit_connector_event,
            resolve_trigger=resolve_connector_trigger,
        ),
        target_validator=validate_connector_target,
    )
    ctx["connector_sync_service"] = connector_service
    register_connector_jobs(job_registry, connector_service, settings)

    # Durable data-erasure coordinator + job (M3.5). Bounded Redis stream cleanup uses the
    # worker's Redis connection; external provider/telemetry deletion has no API and is
    # recorded as an incomplete step so a request finishes 'partial', never 'completed'.
    from keel_core.lifecycle.coordinator import ErasureCoordinator, UnsupportedExternalStep
    from keel_core.lifecycle.redis import RedisLifecycleCleaner
    from keel_core.lifecycle.store import PostgresErasureStore
    from keel_worker.lifecycle import register_erasure_jobs

    erasure_coordinator = ErasureCoordinator(
        engine,
        PostgresErasureStore(engine, _DURABLE_SCOPE),
        redis_cleaner=RedisLifecycleCleaner(redis),
        external_steps=[UnsupportedExternalStep("provider_telemetry")],
    )
    ctx["erasure_coordinator"] = erasure_coordinator
    register_erasure_jobs(
        job_registry, erasure_coordinator, lease_seconds=settings.job_lease_seconds
    )
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
        reconcile_runs_tick,
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
        cron(reconcile_runs_tick, second={0, 30}),
    ]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = _redis_settings()
