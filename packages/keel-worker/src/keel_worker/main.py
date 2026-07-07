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
from datetime import UTC, datetime, timedelta
from typing import Any

from arq import cron
from arq.connections import RedisSettings

from keel_core import __version__
from keel_core.config import get_settings, load_env_file
from keel_core.digest import (
    DIGEST_INSTRUCTION,
    build_digest_agent,
    digest_permissions,
    digest_registry,
)
from keel_core.loop import admit, resume, run
from keel_core.observability import configure_logging, configure_tracing
from keel_scheduler.store import due_tick

logger = logging.getLogger("keel.worker")

# The autonomy slice operates on a single scope (matches the web server's default).
_SLICE_SCOPE = "web:local"


async def run_agent(ctx: dict[str, Any], schedule_id: str) -> str:
    """Start an unattended digest run for a due schedule; suspend on a gated send."""
    settings = get_settings()
    schedules = ctx["schedules"]
    row = await schedules.get(schedule_id)
    if row is None:
        return "missing"
    store, approvals, provider = ctx["store"], ctx["approvals"], ctx["provider"]
    agent = build_digest_agent(row.scope_id).model_copy(update={"model": settings.default_model})
    # The scheduled trigger is a *user* turn (the agent's standing behavior is its
    # persona/system prompt); a system-only message list is rejected by chat providers.
    await admit(store, row.session_id, row.scope_id, DIGEST_INSTRUCTION)
    result = await run(
        agent=agent,
        session_id=row.session_id,
        store=store,
        provider=provider,
        registry=digest_registry(ctx.get("sent")),
        permissions=digest_permissions(),
        approvals=approvals,
        expires_at=datetime.now(UTC) + timedelta(hours=settings.approval_timeout_hours),
    )
    await schedules.mark_run(schedule_id, row.next_run_at, result.reason.value)
    return result.reason.value


async def resume_run(ctx: dict[str, Any], session_id: str, run_id: str, scope_id: str) -> str:
    """Continue a suspended run after its approval resolved (grant/deny/expire)."""
    settings = get_settings()
    store, approvals, provider = ctx["store"], ctx["approvals"], ctx["provider"]
    agent = build_digest_agent(scope_id).model_copy(update={"model": settings.default_model})
    result = await resume(
        agent=agent,
        session_id=session_id,
        run_id=run_id,
        store=store,
        provider=provider,
        registry=digest_registry(ctx.get("sent")),
        permissions=digest_permissions(),
        approvals=approvals,
    )
    return result.reason.value


async def scheduler_tick(ctx: dict[str, Any]) -> int:
    """One due-loop tick: enqueue due runs (at most once) + fail-closed expired approvals."""
    schedules, claim, approvals, enqueue = (
        ctx["schedules"],
        ctx["claim"],
        ctx["approvals"],
        ctx["enqueue"],
    )
    now = datetime.now(UTC)
    enqueued = await due_tick(
        schedules=schedules,
        claim=claim,
        now=now,
        enqueue=lambda sid: enqueue("run_agent", sid),
    )
    for approval_id in await approvals.expire_due(now):
        record = await approvals.get(approval_id)
        if record is not None:
            await enqueue("resume_run", record.session_id, record.run_id, record.scope_id)
    return len(enqueued)


async def startup(ctx: dict[str, Any]) -> None:
    load_env_file()  # provider keys visible to LiteLLM before any agent task runs
    settings = get_settings()
    configure_logging(settings.log_level)
    configure_tracing("keel-worker")

    from sqlalchemy.ext.asyncio import create_async_engine

    from keel_core.approvals import PostgresApprovalStore
    from keel_core.providers import LiteLLMGateway
    from keel_core.state import PostgresEventStore
    from keel_scheduler.store import PostgresClaimStore, PostgresScheduleStore

    engine = create_async_engine(settings.database_url)
    redis = ctx["redis"]
    ctx["engine"] = engine
    ctx["store"] = PostgresEventStore(engine, _SLICE_SCOPE)
    ctx["approvals"] = PostgresApprovalStore(engine, _SLICE_SCOPE)
    ctx["schedules"] = PostgresScheduleStore(engine, _SLICE_SCOPE)
    ctx["claim"] = PostgresClaimStore(engine, _SLICE_SCOPE)
    ctx["provider"] = LiteLLMGateway()
    ctx["enqueue"] = lambda name, *args: redis.enqueue_job(name, *args)
    logger.info("keel-worker %s starting", __version__)


async def shutdown(ctx: dict[str, Any]) -> None:
    engine = ctx.get("engine")
    if engine is not None:
        await engine.dispose()
    logger.info("keel-worker shutting down")


def _redis_settings() -> RedisSettings:
    return RedisSettings.from_dsn(get_settings().redis_url)


class WorkerSettings:
    """arq worker configuration (referenced by the ``arq`` CLI)."""

    functions = [run_agent, resume_run, scheduler_tick]
    cron_jobs = [cron(scheduler_tick, second={0, 30})]  # tick twice a minute
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = _redis_settings()
