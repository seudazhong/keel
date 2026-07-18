"""Production durable-job definition for read-only managed-code review (WS-R)."""

from __future__ import annotations

from keel_core.config import Settings
from keel_core.review.coordinator import ReviewCoordinator
from keel_core.review.jobs import (
    REVIEW_RUN_KIND,
    REVIEW_RUN_MAX_ATTEMPTS,
    ReviewJobHandlers,
)

from .jobs import JobDefinition, JobRegistry


def review_job_definition(coordinator: ReviewCoordinator, settings: Settings) -> JobDefinition:
    handlers = ReviewJobHandlers(coordinator)
    return JobDefinition(
        kind=REVIEW_RUN_KIND,
        handler=handlers.run,
        max_attempts=REVIEW_RUN_MAX_ATTEMPTS,
        lease_seconds=settings.job_lease_seconds,
    )


def register_review_jobs(
    registry: JobRegistry, coordinator: ReviewCoordinator, settings: Settings
) -> None:
    registry.register(review_job_definition(coordinator, settings))


__all__ = ["register_review_jobs", "review_job_definition"]
