"""Production durable-job definition for managed-project GitHub synchronization."""

from __future__ import annotations

from keel_core.config import Settings
from keel_core.projects.jobs import (
    PROJECT_SYNC_CANCEL_MODE,
    PROJECT_SYNC_KIND,
    PROJECT_SYNC_MAX_ATTEMPTS,
    ProjectSyncJobHandlers,
)
from keel_core.projects.service import ProjectService

from .jobs import JobDefinition, JobRegistry


def project_job_definition(service: ProjectService, settings: Settings) -> JobDefinition:
    handlers = ProjectSyncJobHandlers(service)
    return JobDefinition(
        kind=PROJECT_SYNC_KIND,
        handler=handlers.sync,
        max_attempts=PROJECT_SYNC_MAX_ATTEMPTS,
        lease_seconds=settings.job_lease_seconds,
        cancel_mode=PROJECT_SYNC_CANCEL_MODE,
    )


def register_project_jobs(
    registry: JobRegistry, service: ProjectService, settings: Settings
) -> None:
    registry.register(project_job_definition(service, settings))


__all__ = ["project_job_definition", "register_project_jobs"]
