"""Generic durable connector sync/renew jobs and recurring reconciliation."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from keel_core.config import Settings
from keel_core.connector_service import (
    CONNECTOR_RENEW_JOB_KIND,
    CONNECTOR_RENEW_MAX_ATTEMPTS,
    CONNECTOR_SYNC_JOB_KIND,
    CONNECTOR_SYNC_MAX_ATTEMPTS,
    ConnectorService,
)
from keel_core.jobs import CancelMode, JobResult, PermanentJobError

from .jobs import JobContext, JobDefinition, JobRegistry


def connector_job_definition(service: ConnectorService, settings: Settings) -> JobDefinition:
    async def sync(context: JobContext, payload: dict[str, Any]) -> JobResult:
        connector_id = payload.get("connector_id")
        binding_id = payload.get("binding_id")
        if not isinstance(connector_id, str) or not connector_id:
            raise PermanentJobError("connector_payload_invalid", "connector_id is required")
        if not isinstance(binding_id, str) or not binding_id:
            raise PermanentJobError("connector_payload_invalid", "binding_id is required")
        await context.checkpoint()
        changes = await service.sync(connector_id, binding_id)
        return JobResult(
            data={"connector_id": connector_id, "changes": changes},
            message=f"connector sync completed ({changes} changes)",
        )

    return JobDefinition(
        kind=CONNECTOR_SYNC_JOB_KIND,
        handler=sync,
        max_attempts=CONNECTOR_SYNC_MAX_ATTEMPTS,
        lease_seconds=settings.job_lease_seconds,
        cancel_mode=CancelMode.cooperative,
    )


def connector_renew_job_definition(service: ConnectorService, settings: Settings) -> JobDefinition:
    async def renew(context: JobContext, payload: dict[str, Any]) -> JobResult:
        connector_id = payload.get("connector_id")
        binding_id = payload.get("binding_id")
        if not isinstance(connector_id, str) or not connector_id:
            raise PermanentJobError("connector_payload_invalid", "connector_id is required")
        if not isinstance(binding_id, str) or not binding_id:
            raise PermanentJobError("connector_payload_invalid", "binding_id is required")
        await context.checkpoint()
        await service.renew(connector_id, binding_id)
        return JobResult(
            data={"connector_id": connector_id},
            message="connector renewal completed",
        )

    return JobDefinition(
        kind=CONNECTOR_RENEW_JOB_KIND,
        handler=renew,
        max_attempts=CONNECTOR_RENEW_MAX_ATTEMPTS,
        lease_seconds=settings.job_lease_seconds,
        cancel_mode=CancelMode.cooperative,
    )


def register_connector_jobs(
    registry: JobRegistry, service: ConnectorService, settings: Settings
) -> None:
    registry.register(connector_job_definition(service, settings))
    registry.register(connector_renew_job_definition(service, settings))


async def reconcile_connectors_tick(ctx: dict[str, Any]) -> int:
    """Fire due connector sync/renewal schedules across **every** active scope (finding 1).

    Connector schedules live on the RLS-forced ``connector_bindings`` table, so a worker bound to
    one scope cannot see another scope's due work. The global connector schedule index enumerates
    every scope with a recurring schedule; this tick binds each in turn (via the connector scope
    factory) and runs its RLS-scoped ``reconcile_recurring`` (which claims that scope's due
    schedules and enqueues the sync/renew jobs, each recording a cross-scope dispatch intent).

    For the single-tenant / lite profile (and the unit doubles) it also reconciles the pinned
    ``connector_sync_service`` when present, so existing single-scope behavior is preserved.
    """
    settings = cast(Settings, ctx["job_settings"])
    clock = ctx.get("connector_clock")
    now = clock() if callable(clock) else datetime.now(UTC)
    if not isinstance(now, datetime):
        raise TypeError("connector_clock must return a datetime")

    async def _reconcile(service: ConnectorService) -> int:
        return await service.reconcile_recurring(
            now,
            limit=settings.connector_schedule_batch_size,
            lease_seconds=settings.connector_schedule_lease_seconds,
            retry_base_seconds=settings.job_retry_base_seconds,
            retry_max_seconds=settings.job_retry_max_seconds,
        )

    total = 0
    reconciled_scopes: set[str] = set()
    index = ctx.get("connector_schedule_index")
    factory = ctx.get("connector_scope_factory")
    if index is not None and factory is not None:
        for scope_id in sorted(await index.active_scopes()):
            service = cast(ConnectorService, factory(scope_id))
            total += await _reconcile(service)
            reconciled_scopes.add(service.repository.scope_id)

    pinned = cast(ConnectorService | None, ctx.get("connector_sync_service"))
    if pinned is not None and pinned.repository.scope_id not in reconciled_scopes:
        total += await _reconcile(pinned)
    return total


__all__ = [
    "connector_job_definition",
    "connector_renew_job_definition",
    "reconcile_connectors_tick",
    "register_connector_jobs",
]
