"""Erasure admission service: submit / status / retry over durable jobs (M3.5, WS-K).

Mirrors the Knowledge service's admission pattern: a governance request persists a durable
:class:`~keel_core.lifecycle.models.ErasureRequest` (deduped by the caller's idempotency
key), enqueues a durable background job that runs the idempotent, resumable
:class:`~keel_core.lifecycle.coordinator.ErasureCoordinator`, and best-effort dispatches it
(the Postgres job dispatcher heals a missed delivery). Status reads the request + its step
ledger; retry enqueues a fresh job that resumes the same request.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from keel_core.jobs import JobRecord, JobStore
from keel_core.lifecycle.coordinator import ErasureCoordinator
from keel_core.lifecycle.jobs import ERASURE_CANCEL_MODE, ERASURE_KIND, ERASURE_MAX_ATTEMPTS
from keel_core.lifecycle.models import (
    ErasureRequest,
    ErasureStatus,
    ErasureStep,
    ErasureTarget,
)

logger = logging.getLogger("keel.core.lifecycle.service")

DispatchJob = Callable[[str, str], Awaitable[None]]


@dataclass(frozen=True)
class ErasureSubmission:
    """The result of admitting an erasure request."""

    request: ErasureRequest
    job: JobRecord
    created: bool


@dataclass(frozen=True)
class ErasureStatusView:
    """A request plus its step ledger for the status endpoint."""

    request: ErasureRequest
    steps: tuple[ErasureStep, ...]


class ErasureService:
    """Admits, reports, and retries scope/session/project erasure requests."""

    def __init__(
        self,
        coordinator: ErasureCoordinator,
        jobs: JobStore,
        *,
        dispatch_job: DispatchJob | None = None,
    ) -> None:
        self._coordinator = coordinator
        self._jobs = jobs
        self._dispatch_job = dispatch_job

    @property
    def scope_id(self) -> str:
        return self._coordinator.store.scope_id

    async def submit(
        self,
        target: ErasureTarget,
        idempotency_key: str,
        *,
        requested_by: str | None = None,
        reason: str | None = None,
    ) -> ErasureSubmission:
        """Persist (dedupe) an erasure request and enqueue its durable job."""
        request = await self._coordinator.submit(
            target, idempotency_key, requested_by=requested_by, reason=reason
        )
        job, created = await self._jobs.enqueue_once(
            kind=ERASURE_KIND,
            payload={"request_id": request.id},
            target_session_id=None,
            idempotency_key=request.id,
            max_attempts=ERASURE_MAX_ATTEMPTS,
            cancel_mode=ERASURE_CANCEL_MODE,
        )
        await self._dispatch(job)
        return ErasureSubmission(request=request, job=job, created=created)

    async def get(self, request_id: str) -> ErasureStatusView | None:
        request = await self._coordinator.store.get(request_id)
        if request is None:
            return None
        steps = await self._coordinator.store.steps(request_id)
        return ErasureStatusView(request=request, steps=tuple(steps))

    async def list(
        self, *, status: ErasureStatus | None = None, limit: int = 50
    ) -> list[ErasureRequest]:
        return await self._coordinator.store.list_requests(status=status, limit=limit)

    async def retry(self, request_id: str) -> JobRecord | None:
        """Enqueue a fresh job that resumes a non-completed request. Idempotent per request."""
        request = await self._coordinator.store.get(request_id)
        if request is None or request.status is ErasureStatus.completed:
            return None
        job, _ = await self._jobs.enqueue_once(
            kind=ERASURE_KIND,
            payload={"request_id": request.id},
            target_session_id=None,
            idempotency_key=f"{request.id}:retry:{uuid.uuid4().hex[:12]}",
            max_attempts=ERASURE_MAX_ATTEMPTS,
            cancel_mode=ERASURE_CANCEL_MODE,
        )
        await self._dispatch(job)
        return job

    async def _dispatch(self, job: JobRecord) -> None:
        if self._dispatch_job is None:
            return
        try:
            await self._dispatch_job(self.scope_id, job.id)
        except Exception as exc:  # noqa: BLE001 - Postgres dispatcher heals a missed delivery
            logger.warning(
                "erasure job dispatch failed scope=%s job=%s error=%s",
                self.scope_id,
                job.id,
                type(exc).__name__,
            )


__all__ = [
    "DispatchJob",
    "ErasureService",
    "ErasureStatusView",
    "ErasureSubmission",
]
