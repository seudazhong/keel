"""Production durable-job definition for scope/session/project erasure (M3.5, WS-K)."""

from __future__ import annotations

from keel_core.lifecycle.coordinator import ErasureCoordinator
from keel_core.lifecycle.jobs import (
    ERASURE_CANCEL_MODE,
    ERASURE_KIND,
    ERASURE_MAX_ATTEMPTS,
    ErasureJobHandlers,
)

from .jobs import JobDefinition, JobRegistry


def erasure_job_definition(coordinator: ErasureCoordinator, *, lease_seconds: int) -> JobDefinition:
    handlers = ErasureJobHandlers(coordinator)
    return JobDefinition(
        kind=ERASURE_KIND,
        handler=handlers.erase,
        max_attempts=ERASURE_MAX_ATTEMPTS,
        lease_seconds=lease_seconds,
        cancel_mode=ERASURE_CANCEL_MODE,
        on_failed=handlers.erase_failed,
    )


def register_erasure_jobs(
    registry: JobRegistry, coordinator: ErasureCoordinator, *, lease_seconds: int
) -> JobRegistry:
    registry.register(erasure_job_definition(coordinator, lease_seconds=lease_seconds))
    return registry


__all__ = ["erasure_job_definition", "register_erasure_jobs"]
