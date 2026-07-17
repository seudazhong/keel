"""Unit tests for erasure admission service + durable-job handler (M3.5)."""

from __future__ import annotations

import pytest
from lifecycle_helpers import FakePurge, RecordingRedis

from keel_core.jobs import InMemoryJobStore, JobError, JobStatus
from keel_core.lifecycle.coordinator import ErasureCoordinator, UnsupportedExternalStep
from keel_core.lifecycle.jobs import ERASURE_KIND, ErasureJobHandlers
from keel_core.lifecycle.models import ErasureStatus, ErasureTarget, ErasureTargetKind
from keel_core.lifecycle.service import ErasureService
from keel_core.lifecycle.store import InMemoryErasureStore

_SCOPE = "web:local"


class FakeJobContext:
    def __init__(self, job_id: str, scope_id: str) -> None:
        self.job_id = job_id
        self.scope_id = scope_id

    async def checkpoint(self) -> None:
        return None


def _service(
    *, external: tuple[object, ...] = (), sessions: tuple[str, ...] = ("s1",)
) -> tuple[ErasureService, InMemoryErasureStore, InMemoryJobStore, ErasureCoordinator, list[str]]:
    store = InMemoryErasureStore(_SCOPE)
    jobs = InMemoryJobStore(_SCOPE)
    coord = ErasureCoordinator(
        None,
        store,
        purge=FakePurge(sessions=sessions),  # type: ignore[arg-type]
        redis_cleaner=RecordingRedis(),  # type: ignore[arg-type]
        external_steps=external,  # type: ignore[arg-type]
    )
    dispatched: list[str] = []

    async def dispatch(scope_id: str, job_id: str) -> None:
        dispatched.append(job_id)

    return ErasureService(coord, jobs, dispatch_job=dispatch), store, jobs, coord, dispatched


async def test_submit_persists_request_and_enqueues_job() -> None:
    service, store, jobs, _coord, dispatched = _service()
    submission = await service.submit(ErasureTarget(_SCOPE), "key-1", requested_by="admin:abcd")

    assert submission.created
    assert submission.job.kind == ERASURE_KIND
    assert submission.job.payload == {"request_id": submission.request.id}
    assert submission.request.requested_by == "admin:abcd"
    assert dispatched == [submission.job.id]
    # Duplicate submit dedupes to the same request + job.
    again = await service.submit(ErasureTarget(_SCOPE), "key-1")
    assert again.request.id == submission.request.id
    assert again.job.id == submission.job.id


async def test_session_submit_records_session_target_on_request() -> None:
    service, store, _jobs, _coord, _d = _service()
    submission = await service.submit(
        ErasureTarget(_SCOPE, ErasureTargetKind.session, "s1"), "sess-key"
    )
    # The job is not bound to the target session (it is about to be erased), but the
    # durable request records the session it targets.
    assert submission.job.target_session_id is None
    assert submission.request.target_kind is ErasureTargetKind.session
    assert submission.request.target_id == "s1"


async def test_job_handler_executes_coordinator_to_completion() -> None:
    service, store, _jobs, coord, _d = _service()
    submission = await service.submit(ErasureTarget(_SCOPE), "key-1")
    handler = ErasureJobHandlers(coord)

    result = await handler.erase(FakeJobContext(submission.job.id, _SCOPE), submission.job.payload)

    assert result.data["status"] == ErasureStatus.completed.value
    request = await store.get(submission.request.id)
    assert request is not None and request.status is ErasureStatus.completed


async def test_job_handler_partial_is_a_job_success() -> None:
    service, store, _jobs, coord, _d = _service(
        external=(UnsupportedExternalStep("provider_telemetry"),)
    )
    submission = await service.submit(ErasureTarget(_SCOPE), "key-1")
    handler = ErasureJobHandlers(coord)

    result = await handler.erase(FakeJobContext(submission.job.id, _SCOPE), submission.job.payload)

    assert result.data["status"] == ErasureStatus.partial.value
    assert result.data["external_incomplete"] is True


async def test_job_handler_rejects_bad_payload() -> None:
    _service_obj, _store, _jobs, coord, _d = _service()
    handler = ErasureJobHandlers(coord)
    from keel_core.jobs import PermanentJobError

    with pytest.raises(PermanentJobError):
        await handler.erase(FakeJobContext("j", _SCOPE), {})


async def test_on_failed_hook_marks_request_failed() -> None:
    service, store, _jobs, coord, _d = _service()
    submission = await service.submit(ErasureTarget(_SCOPE), "key-1")
    handler = ErasureJobHandlers(coord)

    await handler.erase_failed(submission.job, JobError("internal_error", "boom"))
    request = await store.get(submission.request.id)
    assert request is not None and request.status is ErasureStatus.failed


async def test_retry_enqueues_a_fresh_job_for_partial_request() -> None:
    service, store, jobs, coord, dispatched = _service(
        external=(UnsupportedExternalStep("provider_telemetry"),)
    )
    submission = await service.submit(ErasureTarget(_SCOPE), "key-1")
    handler = ErasureJobHandlers(coord)
    await handler.erase(FakeJobContext(submission.job.id, _SCOPE), submission.job.payload)

    retry_job = await service.retry(submission.request.id)
    assert retry_job is not None
    assert retry_job.id != submission.job.id
    assert retry_job.payload == {"request_id": submission.request.id}


async def test_retry_noop_on_completed_request() -> None:
    service, store, _jobs, coord, _d = _service()
    submission = await service.submit(ErasureTarget(_SCOPE), "key-1")
    handler = ErasureJobHandlers(coord)
    await handler.erase(FakeJobContext(submission.job.id, _SCOPE), submission.job.payload)

    assert await service.retry(submission.request.id) is None


async def test_job_dispatch_survives_dispatch_failure() -> None:
    store = InMemoryErasureStore(_SCOPE)
    jobs = InMemoryJobStore(_SCOPE)
    coord = ErasureCoordinator(
        None,
        store,
        purge=FakePurge(sessions=("s1",)),  # type: ignore[arg-type]
        redis_cleaner=RecordingRedis(),  # type: ignore[arg-type]
    )

    async def failing_dispatch(scope_id: str, job_id: str) -> None:
        raise RuntimeError("queue down")

    service = ErasureService(coord, jobs, dispatch_job=failing_dispatch)
    # Enqueue still succeeds; dispatch failure is swallowed (the dispatcher heals it).
    submission = await service.submit(ErasureTarget(_SCOPE), "key-1")
    stored = await jobs.get(submission.job.id)
    assert stored is not None and stored.status is JobStatus.queued
