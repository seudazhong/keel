"""Generic durable connector sync job registration."""

from __future__ import annotations

from typing import Any

from keel_core.config import Settings
from keel_core.connector_service import (
    CONNECTOR_SYNC_JOB_KIND,
    CONNECTOR_SYNC_MAX_ATTEMPTS,
    ConnectorService,
)
from keel_core.jobs import CancelMode, JobResult, PermanentJobError

from .jobs import JobContext, JobDefinition, JobRegistry


def connector_job_definition(
    service: ConnectorService, settings: Settings
) -> JobDefinition:
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


def register_connector_jobs(
    registry: JobRegistry, service: ConnectorService, settings: Settings
) -> None:
    registry.register(connector_job_definition(service, settings))


__all__ = ["connector_job_definition", "register_connector_jobs"]
