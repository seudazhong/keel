"""Production durable-job definition for read-only managed-code review (WS-R)."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from keel_core.config import Settings
from keel_core.jobs import JobError, JobRecord
from keel_core.review.coordinator import ReviewCoordinator
from keel_core.review.jobs import (
    REVIEW_RUN_KIND,
    REVIEW_RUN_MAX_ATTEMPTS,
    ReviewJobHandlers,
    ReviewJobPayload,
    review_idempotency_key,
)
from keel_core.runs import RunStatus

from .jobs import JobDefinition, JobRegistry

logger = logging.getLogger("keel.worker.review")

# Single-owner fence for the artifact reaper: only one worker reaps per interval (the reap
# itself is idempotent + atomic, so this is a de-duplication optimization, not a correctness
# requirement). Held a little under the cron cadence so a crashed holder recovers next tick.
REVIEW_REAPER_LOCK_KEY = "keel:review:artifact-reaper:lock"
REVIEW_REAPER_LOCK_TTL_SECONDS = 3000


def _run_id_from_payload(payload: dict[str, Any]) -> str | None:
    """Best-effort extraction of the review run id from a durable job payload."""
    try:
        return ReviewJobPayload.model_validate(payload).run_id
    except Exception:  # noqa: BLE001 — a malformed payload has no run to terminalize
        run_id = payload.get("run_id")
        return run_id if isinstance(run_id, str) and run_id else None


def review_job_definition(coordinator: ReviewCoordinator, settings: Settings) -> JobDefinition:
    handlers = ReviewJobHandlers(coordinator)

    async def _on_failed(row: JobRecord, error: JobError) -> None:
        # The durable ``review.run`` job exhausted its retries (or failed permanently). Terminalize
        # the associated queued/running run exactly once so it never lingers admitted until TTL.
        run_id = _run_id_from_payload(row.payload)
        if run_id is None:
            return
        try:
            await coordinator.terminalize_orphaned_run(
                run_id,
                status=RunStatus.failed,
                stop_reason="review_job_failed",
                error_kind=error.kind or "review_job_failed",
                error_message=error.message or "review job failed",
            )
        except Exception:  # noqa: BLE001 — hook must not crash the worker; reconciler backstops
            logger.warning("review on_failed run terminalization failed", exc_info=True)

    async def _on_cancelled(row: JobRecord) -> None:
        run_id = _run_id_from_payload(row.payload)
        if run_id is None:
            return
        try:
            await coordinator.terminalize_orphaned_run(
                run_id,
                status=RunStatus.cancelled,
                stop_reason="review_job_cancelled",
                error_kind="review_job_cancelled",
                error_message="review job cancelled",
            )
        except Exception:  # noqa: BLE001 — hook must not crash the worker; reconciler backstops
            logger.warning("review on_cancelled run terminalization failed", exc_info=True)

    return JobDefinition(
        kind=REVIEW_RUN_KIND,
        handler=handlers.run,
        max_attempts=REVIEW_RUN_MAX_ATTEMPTS,
        lease_seconds=settings.job_lease_seconds,
        on_failed=_on_failed,
        on_cancelled=_on_cancelled,
    )


def register_review_jobs(
    registry: JobRegistry, coordinator: ReviewCoordinator, settings: Settings
) -> None:
    registry.register(review_job_definition(coordinator, settings))


async def review_artifact_reaper_tick(ctx: dict[str, Any]) -> int:
    """Bounded, single-owner reap of expired review report artifacts AND stale worktrees.

    Under one Redis-fenced, single-owner cron tick it:

    * reclaims content-addressed review report artifacts whose explicit ``retained_until`` has
      elapsed (retention/TTL), and
    * reclaims **crash-orphaned** review worktrees older than ``review_worktree_stale_hours`` —
      a healthy review's worktree is materialized and disposed within a single job, so any
      worktree older than the stale cutoff was left behind by a crash. Worktrees younger than the
      cutoff (i.e. currently active reviews) are NEVER removed.

    Both operate over the SAME shared storage the review job writes to. Immediate project erasure
    is handled separately by the erasure coordinator's coding cleaner; this tick is the scheduled
    retention/orphan backstop. It logs only aggregate counts — never a project id, path, or digest.
    """
    artifacts = ctx.get("review_artifacts")
    worktrees = ctx.get("review_worktrees")
    if artifacts is None and worktrees is None:
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
    removed = 0
    if artifacts is not None:
        try:
            result = await asyncio.to_thread(artifacts.reap, older_than=now)
        except Exception:  # noqa: BLE001 — a reaper failure must not crash the worker cron loop
            logger.warning("review artifact reaper tick failed", exc_info=True)
        else:
            artifact_removed = int(result.removed)
            removed += artifact_removed
            if artifact_removed:
                logger.info(
                    "review artifact reaper removed %d artifact(s), reclaimed %d bytes",
                    artifact_removed,
                    int(result.reclaimed_bytes),
                )
    if worktrees is not None:
        stale_hours = int(ctx.get("review_worktree_stale_hours", 6) or 6)
        cutoff = now - timedelta(hours=max(1, stale_hours))
        try:
            wt_result = await asyncio.to_thread(worktrees.reap, older_than=cutoff)
        except Exception:  # noqa: BLE001 — a reaper failure must not crash the worker cron loop
            logger.warning("review worktree reaper tick failed", exc_info=True)
        else:
            wt_removed = int(wt_result.removed)
            removed += wt_removed
            if wt_removed:
                logger.info(
                    "review worktree reaper removed %d stale worktree(s), reclaimed %d bytes",
                    wt_removed,
                    int(wt_result.reclaimed_bytes),
                )
    return removed


REVIEW_DISPATCH_RECONCILE_LIMIT = 100
# Small grace so we don't race the in-line enqueue on the request path (the API enqueues on
# every request); only runs still un-leased after the grace are treated as stranded.
REVIEW_DISPATCH_RECONCILE_GRACE_SECONDS = 30


async def reconcile_stranded_reviews_tick(ctx: dict[str, Any]) -> int:
    """Durable backstop that re-dispatches admitted/queued review runs with no ``review.run`` job.

    The API returns ``202`` even if its in-line enqueue fails, on the strength of this reconciler:
    it scans review-surface runs that are still ``admitted``/``queued`` (un-leased) past a small
    grace and, reconstructing the payload from the run's durably-persisted request metadata,
    idempotently (re-)creates the ``review.run`` job + cross-scope dispatch intent
    (``enqueue_once_with_dispatch_intent`` dedupes by idempotency key, so a run that already has a
    job is a no-op). The existing job-dispatch reconciler then dispatches the intent. Bounded per
    tick; a failure on one run never blocks the others or crashes the cron loop.
    """
    coordinator: ReviewCoordinator | None = ctx.get("review_coordinator")
    jobs = ctx.get("jobs")
    outbox = ctx.get("job_dispatch_outbox")
    enqueue = ctx.get("enqueue")
    if coordinator is None or jobs is None:
        return 0
    try:
        run_ids = await coordinator.stranded_review_run_ids(
            limit=REVIEW_DISPATCH_RECONCILE_LIMIT,
            grace_seconds=REVIEW_DISPATCH_RECONCILE_GRACE_SECONDS,
        )
    except Exception:  # noqa: BLE001 — a reconcile query failure must not crash the cron loop
        logger.warning("stranded review dispatch scan failed", exc_info=True)
        return 0
    dispatched = 0
    for run_id in run_ids:
        try:
            request = await coordinator.load_request_metadata(run_id)
            if request is None:
                # No durable request metadata: cannot rebuild the payload. The run TTL / expiry
                # reconciler terminalizes it rather than dispatch a fabricated request.
                continue
            payload = ReviewJobPayload.from_request(request, run_id=run_id).model_dump(mode="json")
            idem = review_idempotency_key(run_id)
            if outbox is not None:
                job, created = await jobs.enqueue_once_with_dispatch_intent(
                    kind=REVIEW_RUN_KIND,
                    payload=payload,
                    target_session_id=None,
                    idempotency_key=idem,
                    max_attempts=REVIEW_RUN_MAX_ATTEMPTS,
                    outbox=outbox,
                )
            else:
                job, created = await jobs.enqueue_once(
                    kind=REVIEW_RUN_KIND,
                    payload=payload,
                    target_session_id=None,
                    idempotency_key=idem,
                    max_attempts=REVIEW_RUN_MAX_ATTEMPTS,
                )
            if created:
                dispatched += 1
                if enqueue is not None:
                    # Nudge the arq dispatcher so the freshly-created job runs promptly (the
                    # job dispatch reconciler is the durable backstop if the nudge is lost).
                    try:
                        await enqueue("run_job", jobs.scope_id, job.id)
                    except Exception:  # noqa: BLE001 — dispatch nudge is best-effort
                        logger.debug("stranded review dispatch nudge failed run=%s", run_id)
        except Exception:  # noqa: BLE001 — one run's failure must not block the rest
            logger.warning("stranded review re-dispatch failed", exc_info=True)
    if dispatched:
        logger.info("stranded review reconciler re-dispatched %d review run(s)", dispatched)
    return dispatched


__all__ = [
    "REVIEW_REAPER_LOCK_KEY",
    "reconcile_stranded_reviews_tick",
    "register_review_jobs",
    "review_artifact_reaper_tick",
    "review_job_definition",
]
