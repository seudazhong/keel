"""Production durable-job definition for read-only managed-code review (WS-R)."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from keel_core.config import Settings
from keel_core.review.coordinator import ReviewCoordinator
from keel_core.review.jobs import (
    REVIEW_RUN_KIND,
    REVIEW_RUN_MAX_ATTEMPTS,
    ReviewJobHandlers,
)

from .jobs import JobDefinition, JobRegistry

logger = logging.getLogger("keel.worker.review")

# Single-owner fence for the artifact reaper: only one worker reaps per interval (the reap
# itself is idempotent + atomic, so this is a de-duplication optimization, not a correctness
# requirement). Held a little under the cron cadence so a crashed holder recovers next tick.
REVIEW_REAPER_LOCK_KEY = "keel:review:artifact-reaper:lock"
REVIEW_REAPER_LOCK_TTL_SECONDS = 3000


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


async def review_artifact_reaper_tick(ctx: dict[str, Any]) -> int:
    """Bounded, single-owner reap of expired review report artifacts (retained_until/TTL).

    Reclaims content-addressed review artifacts whose explicit ``retained_until`` has elapsed
    (and any ephemeral coding artifacts past their window) over the SAME shared storage the
    review job writes to. Immediate project erasure is handled separately by the erasure
    coordinator's coding cleaner; this tick is the scheduled retention backstop. It logs only
    aggregate counts — never a project id, path, or artifact digest.
    """
    artifacts = ctx.get("review_artifacts")
    if artifacts is None:
        return 0
    redis = ctx.get("redis")
    if redis is not None:
        acquired = await redis.set(
            REVIEW_REAPER_LOCK_KEY,
            str(ctx.get("worker_id", "review-worker")),
            nx=True,
            ex=REVIEW_REAPER_LOCK_TTL_SECONDS,
        )
        if not acquired:
            return 0
    now = datetime.now(UTC)
    try:
        result = await asyncio.to_thread(artifacts.reap, older_than=now)
    except Exception:  # noqa: BLE001 — a reaper failure must not crash the worker cron loop
        logger.warning("review artifact reaper tick failed", exc_info=True)
        return 0
    removed = int(result.removed)
    if removed:
        logger.info(
            "review artifact reaper removed %d artifact(s), reclaimed %d bytes",
            removed,
            int(result.reclaimed_bytes),
        )
    return removed


__all__ = [
    "REVIEW_REAPER_LOCK_KEY",
    "register_review_jobs",
    "review_artifact_reaper_tick",
    "review_job_definition",
]
