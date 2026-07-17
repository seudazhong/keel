"""Unit tests for the erasure coordinator state machine (M3.5).

These exercise the coordinator against in-memory doubles (no Postgres): a fake scoped
purge repository, a recording Redis cleaner, and the real in-memory erasure store. The
Postgres path is covered by tests/integration/test_lifecycle_postgres.py.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from lifecycle_helpers import (
    SCOPE as _SCOPE,
)
from lifecycle_helpers import (
    FailingExternalStep,
    FakePurge,
    RecordingProjectPurger,
    RecordingRedis,
)

from keel_core.events import Event, EventType
from keel_core.lifecycle.coding import CodingArtifactCleaner
from keel_core.lifecycle.coordinator import (
    ErasureCoordinator,
    UnsupportedExternalStep,
)
from keel_core.lifecycle.models import ErasureStatus, ErasureTarget, ErasureTargetKind, StepStatus
from keel_core.lifecycle.store import InMemoryErasureStore
from keel_core.lifecycle.tombstones import load_tombstone_hook, make_tombstone_hook
from keel_core.rebuild import InMemoryProjectionCheckpoints, ProjectionRebuilder
from keel_core.state import InMemoryEventStore

_SCOPE_STORE_STEPS = (
    "message_embeddings",
    "events_and_sessions",
    "archival",
    "memory",
    "memory_proposals",
    "consolidation_cursor",
    "knowledge",
    "connector_state",
    "connector_tokens",
    "connector_outbox",
    "oauth_states",
    "schedules",
    "approvals",
    "jobs",
)


def _coordinator(
    store: InMemoryErasureStore,
    purge: FakePurge,
    *,
    redis: RecordingRedis | None = None,
    external: tuple[object, ...] = (),
    coding: CodingArtifactCleaner | None = None,
) -> ErasureCoordinator:
    return ErasureCoordinator(
        None,
        store,
        purge=purge,  # type: ignore[arg-type]
        redis_cleaner=redis,  # type: ignore[arg-type]
        coding_cleaner=coding,
        external_steps=external,  # type: ignore[arg-type]
    )


async def _submit_scope(store: InMemoryErasureStore, coord: ErasureCoordinator) -> str:
    request = await coord.submit(ErasureTarget(_SCOPE), "key-1", reason="gdpr")
    return request.id


async def test_scope_erasure_runs_every_store_and_cleans_redis() -> None:
    store = InMemoryErasureStore(_SCOPE)
    purge = FakePurge(sessions=("s1", "s2"), spill=("/spill/a.txt",))
    redis = RecordingRedis()
    coord = _coordinator(store, purge, redis=redis)

    request_id = await _submit_scope(store, coord)
    result = await coord.execute(request_id, current_job_id="job-1")

    assert result.status is ErasureStatus.completed
    assert not result.external_incomplete
    done = {s.step for s in result.steps if s.status is StepStatus.done}
    for step in _SCOPE_STORE_STEPS:
        assert step in done, step
    assert await store.tombstoned_sessions() == {"s1", "s2"}
    assert sorted(redis.purged) == ["s1", "s2"]
    # coding is skipped for a scope erasure (no project reference).
    coding_step = next(s for s in result.steps if s.step == "coding_artifacts")
    assert coding_step.status is StepStatus.skipped


async def test_repeated_execute_is_idempotent() -> None:
    store = InMemoryErasureStore(_SCOPE)
    purge = FakePurge(sessions=("s1",))
    coord = _coordinator(store, purge)
    request_id = await _submit_scope(store, coord)

    first = await coord.execute(request_id)
    calls_after_first = list(purge.calls)
    second = await coord.execute(request_id)

    assert first.status is second.status is ErasureStatus.completed
    # No store purge is repeated on the second run (all steps already done).
    assert purge.calls == calls_after_first


async def test_duplicate_submit_dedupes_to_one_request() -> None:
    store = InMemoryErasureStore(_SCOPE)
    coord = _coordinator(store, FakePurge())
    first = await coord.submit(ErasureTarget(_SCOPE), "same-key")
    second = await coord.submit(ErasureTarget(_SCOPE), "same-key")
    assert first.id == second.id


async def test_crash_mid_run_resumes_and_completes() -> None:
    store = InMemoryErasureStore(_SCOPE)
    purge = FakePurge(sessions=("s1",), fail_on="archival")
    coord = _coordinator(store, purge)
    request_id = await _submit_scope(store, coord)

    with pytest.raises(RuntimeError):
        await coord.execute(request_id)

    request = await store.get(request_id)
    assert request is not None
    assert request.status is ErasureStatus.running
    assert request.attempts == 1
    ledger = {s.step: s.status for s in await store.steps(request_id)}
    assert ledger["message_embeddings"] is StepStatus.done
    assert ledger.get("archival") is StepStatus.pending

    # Resume: archival succeeds the second time, run completes.
    result = await coord.execute(request_id)
    assert result.status is ErasureStatus.completed
    assert "archival" in purge.calls


async def test_unsupported_external_step_forces_partial() -> None:
    store = InMemoryErasureStore(_SCOPE)
    coord = _coordinator(
        store,
        FakePurge(sessions=("s1",)),
        external=(UnsupportedExternalStep("provider_telemetry"),),
    )
    request_id = await _submit_scope(store, coord)
    result = await coord.execute(request_id)

    assert result.status is ErasureStatus.partial
    assert result.external_incomplete
    telemetry = next(s for s in result.steps if s.step == "provider_telemetry")
    assert telemetry.status is StepStatus.unsupported
    request = await store.get(request_id)
    assert request is not None and request.external_incomplete


async def test_failed_external_step_is_reported_not_swallowed() -> None:
    store = InMemoryErasureStore(_SCOPE)
    coord = _coordinator(store, FakePurge(), external=(FailingExternalStep(),))
    request_id = await _submit_scope(store, coord)
    result = await coord.execute(request_id)

    assert result.status is ErasureStatus.partial
    telemetry = next(s for s in result.steps if s.step == "provider_telemetry")
    assert telemetry.status is StepStatus.failed
    assert telemetry.detail == "provider returned 500"


async def test_session_erasure_only_touches_that_session() -> None:
    store = InMemoryErasureStore(_SCOPE)
    purge = FakePurge()
    redis = RecordingRedis()
    coord = _coordinator(store, purge, redis=redis)
    request = await coord.submit(ErasureTarget(_SCOPE, ErasureTargetKind.session, "s1"), "sess-key")
    result = await coord.execute(request.id)

    assert result.status is ErasureStatus.completed
    steps = {s.step for s in result.steps}
    assert steps == {
        "tombstone_session",
        "tool_spill",
        "redis_stream",
        "session_message_embeddings",
        "session_events",
    }
    # Scope-wide store purges are never invoked for a single-session erasure.
    assert "memory" not in purge.calls
    assert "knowledge" not in purge.calls
    assert redis.purged == ["s1"]
    assert await store.is_tombstoned("s1")


async def test_project_erasure_purges_coding_artifacts() -> None:
    store = InMemoryErasureStore(_SCOPE)
    project_purger = RecordingProjectPurger(removed=True)
    coord = _coordinator(store, FakePurge(), coding=CodingArtifactCleaner(project_purger))
    request = await coord.submit(
        ErasureTarget(_SCOPE, ErasureTargetKind.project, "proj-1"), "proj-key"
    )
    result = await coord.execute(request.id)

    assert result.status is ErasureStatus.completed
    assert project_purger.projects == ["proj-1"]
    coding = next(s for s in result.steps if s.step == "coding_artifacts")
    assert coding.status is StepStatus.done


async def test_tombstone_prevents_projection_resurrection() -> None:
    # A residual event source still holds the erased session's events...
    events = InMemoryEventStore()
    for seq in range(1, 4):
        await events.append(
            Event(
                type=EventType.message_token,
                seq=seq,
                session_id="s1",
                scope_id=_SCOPE,
                ts=datetime.now(UTC),
                payload={"role": "user", "text": f"m{seq}", "partial": False},
            )
        )

    # ...but the session is tombstoned, so a rebuild must withhold every event.
    store = InMemoryErasureStore(_SCOPE)
    await store.add_tombstone("s1", reason="erased")
    hook = await load_tombstone_hook(store)

    applied: list[Event] = []

    class _Sink:
        async def apply(self, event: Event) -> None:
            applied.append(event)

    rebuilder = ProjectionRebuilder(events, InMemoryProjectionCheckpoints(), tombstone_hook=hook)
    result = await rebuilder.rebuild("recall", "s1", _Sink())

    assert result.processed == 3
    assert result.skipped_tombstones == 3
    assert applied == []


async def test_make_tombstone_hook_allows_untombstoned_sessions() -> None:
    hook = make_tombstone_hook({"s1"})
    tombstoned = Event(
        type=EventType.message_token,
        seq=1,
        session_id="s1",
        scope_id=_SCOPE,
        ts=datetime.now(UTC),
        payload={"role": "user", "text": "x", "partial": False},
    )
    kept = tombstoned.model_copy(update={"session_id": "s2"})
    assert hook(tombstoned) is True
    assert hook(kept) is False
