"""Worker-agnostic durable project-sync job (M3.7, WS-P).

A GitHub-sourced project's fetch is a **durable, restart-safe, idempotent** background job on
the existing jobs/outbox substrate: a webhook push (or a manual sync request) enqueues one
``projects.sync`` job keyed by ``(project, delivery)`` so a crash mid-fetch retries and a
duplicate/replayed delivery is a no-op (the sync ledger's unique ``delivery_id`` collapses it).

This module owns the payload contract + handler logic; a thin worker adapter wraps it in a
``JobDefinition`` and registers it (mirroring :mod:`keel_core.knowledge.jobs`).
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, StrictStr, ValidationError

from keel_core.jobs import CancelMode, JobResult, PermanentJobError, RetryableJobError
from keel_core.projects.models import ProjectError
from keel_core.projects.service import ProjectService

PROJECT_SYNC_KIND = "projects.sync"
PROJECT_SYNC_MAX_ATTEMPTS = 5
PROJECT_SYNC_CANCEL_MODE = CancelMode.cooperative


class ProjectSyncPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True, strict=True)

    org_id: StrictStr
    project_id: StrictStr
    delivery_id: StrictStr | None = None


def sync_idempotency_key(project_id: str, delivery_id: str | None) -> str:
    """A stable job idempotency key so a replayed delivery enqueues at most one job."""
    return f"projects.sync:{project_id}:{delivery_id or 'manual'}"


@runtime_checkable
class ProjectSyncJobContext(Protocol):
    """The subset of the worker job context the sync handler needs."""

    @property
    def scope_id(self) -> str: ...

    @property
    def job_id(self) -> str: ...

    async def checkpoint(self) -> None: ...


class ProjectSyncJobHandlers:
    """Durable handler that fetches a GitHub-sourced project idempotently."""

    def __init__(self, service: ProjectService) -> None:
        self._service = service

    async def sync(self, context: ProjectSyncJobContext, raw_payload: dict[str, Any]) -> JobResult:
        try:
            payload = ProjectSyncPayload.model_validate(raw_payload)
        except ValidationError as exc:
            raise PermanentJobError(
                "invalid_project_sync_payload", "Project sync payload is invalid."
            ) from exc
        await context.checkpoint()
        try:
            entry = await self._service.sync_project(
                payload.org_id, payload.project_id, delivery_id=payload.delivery_id
            )
        except ProjectError as exc:
            # Domain errors are terminal (a bad/removed project won't heal on retry).
            raise PermanentJobError("project_sync_failed", "Project sync failed.") from exc
        except Exception as exc:  # transient storage/network — let the job retry.
            raise RetryableJobError(
                "project_sync_transient", "Project sync temporarily failed."
            ) from exc
        status = entry.status.value if entry is not None else "noop"
        return JobResult(
            data={"project_id": payload.project_id, "status": status},
            message=f"project sync {status}",
        )


__all__ = [
    "PROJECT_SYNC_CANCEL_MODE",
    "PROJECT_SYNC_KIND",
    "PROJECT_SYNC_MAX_ATTEMPTS",
    "ProjectSyncJobContext",
    "ProjectSyncJobHandlers",
    "ProjectSyncPayload",
    "sync_idempotency_key",
]
