"""Strict durable-job contracts, limits and deterministic retry math."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone

import pytest

from keel_core.config import Settings
from keel_core.events import Event, EventType
from keel_core.jobs import (
    InMemoryJobStore,
    JobError,
    JobLease,
    JobLeaseLostError,
    JobLimits,
    JobResult,
    JobStatus,
    JobValidationError,
    PermanentJobError,
    RetryableJobError,
    _job_dedupe_lock_id,
    retry_delay_seconds,
)
from keel_core.state import InMemoryEventStore

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


async def _session(events: InMemoryEventStore, session_id: str, scope: str) -> None:
    await events.append(
        Event(
            type=EventType.message_token,
            seq=0,
            session_id=session_id,
            scope_id=scope,
            ts=_NOW,
            payload={"role": "user", "text": "seed"},
        )
    )


def test_job_contracts_are_frozen_and_typed() -> None:
    lease = JobLease(
        job_id="job_1",
        scope_id="web:local",
        token="lease_1",
        kind="test.echo",
        payload={"value": 1},
        attempt=1,
        max_attempts=3,
        lease_seconds=300,
    )
    assert lease.attempt == 1
    with pytest.raises(FrozenInstanceError):
        lease.attempt = 2  # type: ignore[misc]
    assert JobStatus("succeeded") is JobStatus.succeeded
    with pytest.raises(ValueError):
        JobStatus("done")


def test_public_handler_errors_keep_code_and_safe_message() -> None:
    retryable = RetryableJobError("provider_timeout", "Provider timed out.")
    permanent = PermanentJobError("invalid_document", "Document is invalid.")
    assert (retryable.code, retryable.public_message) == (
        "provider_timeout",
        "Provider timed out.",
    )
    assert (permanent.code, permanent.public_message) == (
        "invalid_document",
        "Document is invalid.",
    )
    assert str(retryable) == "Provider timed out."
    assert str(JobValidationError("invalid_payload", "Payload is invalid.")) == (
        "invalid_payload: Payload is invalid."
    )
    with pytest.raises(ValueError, match="code"):
        RetryableJobError("", "safe")
    with pytest.raises(ValueError, match="public_message"):
        PermanentJobError("bad", " ")


def test_limits_validate_json_bytes_and_clip_public_text() -> None:
    limits = JobLimits(
        payload_max_bytes=12,
        result_max_bytes=12,
        result_message_max_chars=5,
        error_message_max_chars=4,
    )
    assert limits.validate_payload({"a": 1}) == {"a": 1}
    assert limits.validate_result({"a": 1}) == {"a": 1}
    with pytest.raises(JobValidationError, match="payload_too_large"):
        limits.validate_payload({"long": "value"})
    with pytest.raises(JobValidationError, match="result_too_large"):
        limits.validate_result({"long": "value"})
    with pytest.raises(JobValidationError, match="json_object_required"):
        limits.validate_payload(["not", "an", "object"])  # type: ignore[arg-type]
    with pytest.raises(JobValidationError, match="json_native_required"):
        limits.validate_result({"bad": object()})
    with pytest.raises(JobValidationError, match="json_serializable"):
        limits.validate_payload({"bad": float("nan")})
    assert limits.result_message("123456") == "12345"
    assert limits.error_message("12345") == "1234"


def test_limits_require_postgres_safe_json_and_return_a_detached_value() -> None:
    limits = JobLimits()
    source = {"items": [{"name": "safe"}]}
    normalized = limits.validate_payload(source)
    assert normalized == source
    assert normalized is not source
    assert normalized["items"] is not source["items"]

    source["items"][0]["name"] = "mutated"
    assert normalized == {"items": [{"name": "safe"}]}

    with pytest.raises(JobValidationError, match="json_native_required"):
        limits.validate_payload({"bad": (1, 2)})
    with pytest.raises(JobValidationError, match="json_native_required"):
        limits.validate_payload({1: "bad"})  # type: ignore[dict-item]
    with pytest.raises(JobValidationError, match="storage_text_invalid"):
        limits.validate_payload({"bad": "\x00"})
    with pytest.raises(JobValidationError, match="storage_text_invalid"):
        limits.result_message("bad\x00message")


def test_limits_reject_cycles_without_echoing_payload_keys() -> None:
    limits = JobLimits()
    cyclic: dict[str, object] = {}
    cyclic["self"] = cyclic
    with pytest.raises(JobValidationError, match="json_cycle"):
        limits.validate_payload(cyclic)

    secret_key = "secret-token-" + ("x" * 5_000) + "\nnext-line"
    with pytest.raises(JobValidationError) as caught:
        limits.validate_payload({secret_key: object()})
    assert secret_key not in caught.value.public_message
    assert len(caught.value.public_message) < 200


def test_limits_reject_excessive_json_depth_with_a_bounded_error() -> None:
    limits = JobLimits()
    deep: dict[str, object] = {}
    cursor = deep
    for _ in range(150):
        child: dict[str, object] = {}
        cursor["child"] = child
        cursor = child

    with pytest.raises(JobValidationError, match="json_too_deep") as caught:
        limits.validate_payload(deep)
    assert len(caught.value.public_message) < 200


def test_persisted_error_fields_reject_storage_invalid_text() -> None:
    with pytest.raises(ValueError, match="storage-safe"):
        RetryableJobError("bad\x00code", "safe")
    with pytest.raises(ValueError, match="storage-safe"):
        PermanentJobError("bad", "unsafe\ud800")
    with pytest.raises(ValueError, match="storage-safe"):
        JobError(kind="bad\x00kind", message="safe")
    with pytest.raises(ValueError, match="storage-safe"):
        JobError(kind="safe", message="bad\x00message")


def test_job_limits_are_built_from_settings() -> None:
    settings = Settings(
        job_payload_max_bytes=101,
        job_result_max_bytes=102,
        job_result_message_max_chars=103,
        job_error_message_max_chars=104,
    )
    assert JobLimits.from_settings(settings) == JobLimits(
        payload_max_bytes=101,
        result_max_bytes=102,
        result_message_max_chars=103,
        error_message_max_chars=104,
    )


def test_result_and_error_models_reject_empty_required_text() -> None:
    assert JobResult(data={"ok": True}, message="done").message == "done"
    assert JobError(kind="provider_timeout", message="safe").kind == "provider_timeout"
    with pytest.raises(ValueError, match="message"):
        JobResult(data={}, message="")
    with pytest.raises(ValueError, match="kind"):
        JobError(kind="", message="safe")


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(1, 5), (2, 10), (3, 20), (7, 300)],
)
def test_retry_delay_is_deterministic_and_capped(attempt: int, expected: int) -> None:
    assert retry_delay_seconds(attempt, 5, 300) == expected


def test_retry_delay_rejects_non_positive_inputs() -> None:
    with pytest.raises(ValueError):
        retry_delay_seconds(0, 5, 300)
    with pytest.raises(ValueError):
        retry_delay_seconds(1, 0, 300)


def test_dedupe_lock_key_encoding_is_unambiguous() -> None:
    assert _job_dedupe_lock_id("scope:a", "kind\x1fx", "key") != (
        _job_dedupe_lock_id("scope:a\x1fkind", "x", "key")
    )


async def test_in_memory_enqueue_once_dedupes_and_first_request_wins() -> None:
    store = InMemoryJobStore("web:local")
    first, created = await store.enqueue_once(
        kind="test.echo",
        payload={"value": 1},
        target_session_id=None,
        idempotency_key="request-1",
        max_attempts=3,
        now=_NOW,
    )
    duplicate, duplicate_created = await store.enqueue_once(
        kind="test.echo",
        payload={"bad": object()},
        target_session_id="missing-on-retry",
        idempotency_key="request-1",
        max_attempts=1,
        now=_NOW + timedelta(seconds=1),
    )

    assert created is True
    assert duplicate_created is False
    assert duplicate.id == first.id
    assert duplicate.payload == {"value": 1}
    assert duplicate.max_attempts == 3
    assert first.status is JobStatus.queued
    assert first.attempt == 0
    assert first.next_attempt_at == _NOW


@pytest.mark.parametrize(
    ("kind", "key", "max_attempts", "error_code"),
    [
        (" ", "request", 3, "invalid_kind"),
        ("test.echo", " ", 3, "invalid_idempotency_key"),
        ("test.echo", "request", 0, "invalid_max_attempts"),
    ],
)
async def test_in_memory_enqueue_rejects_invalid_identity_and_attempt_policy(
    kind: str,
    key: str,
    max_attempts: int,
    error_code: str,
) -> None:
    store = InMemoryJobStore("web:local")
    with pytest.raises(JobValidationError, match=error_code):
        await store.enqueue_once(
            kind=kind,
            payload={},
            target_session_id=None,
            idempotency_key=key,
            max_attempts=max_attempts,
            now=_NOW,
        )


async def test_in_memory_enqueue_validates_target_session_and_scope() -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore("web:local", events=events)

    accepted, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target",
        idempotency_key="accepted",
        max_attempts=3,
        now=_NOW,
    )
    assert accepted.target_session_id == "target"

    with pytest.raises(JobValidationError, match="target_session_not_found"):
        await store.enqueue_once(
            kind="test.echo",
            payload={},
            target_session_id="missing",
            idempotency_key="missing",
            max_attempts=3,
            now=_NOW,
        )

    other_scope = InMemoryJobStore("scope:other", events=events)
    with pytest.raises(JobValidationError, match="target_session_not_found"):
        await other_scope.enqueue_once(
            kind="test.echo",
            payload={},
            target_session_id="target",
            idempotency_key="cross-scope",
            max_attempts=3,
            now=_NOW,
        )


async def test_in_memory_rejects_storage_invalid_identities() -> None:
    with pytest.raises(ValueError, match="scope_id"):
        InMemoryJobStore("bad\x00scope")

    store = InMemoryJobStore("web:local")
    with pytest.raises(JobValidationError, match="invalid_kind"):
        await store.enqueue_once(
            kind="bad\x00kind",
            payload={},
            target_session_id=None,
            idempotency_key="request",
            max_attempts=3,
            now=_NOW,
        )
    with pytest.raises(JobValidationError, match="invalid_idempotency_key"):
        await store.enqueue_once(
            kind="test.echo",
            payload={},
            target_session_id=None,
            idempotency_key="bad\ud800key",
            max_attempts=3,
            now=_NOW,
        )
    with pytest.raises(JobValidationError, match="invalid_target_session_id"):
        await store.enqueue_once(
            kind="test.echo",
            payload={},
            target_session_id="bad\x00target",
            idempotency_key="target",
            max_attempts=3,
            now=_NOW,
        )


async def test_in_memory_normalizes_aware_timestamps_and_rejects_naive_values() -> None:
    store = InMemoryJobStore("web:local")
    local_time = datetime(2026, 7, 14, 17, 0, tzinfo=timezone(timedelta(hours=8)))
    row, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="aware",
        max_attempts=3,
        now=local_time,
    )
    assert row.created_at == _NOW
    assert await store.dispatchable(_NOW, 100) == [row.id]

    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.enqueue_once(
            kind="test.echo",
            payload={},
            target_session_id=None,
            idempotency_key="naive",
            max_attempts=3,
            now=datetime(2026, 7, 14, 9, 0),
        )
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.dispatchable(datetime(2026, 7, 14, 9, 0), 100)


async def test_in_memory_get_and_list_are_copied_filtered_and_newest_first() -> None:
    store = InMemoryJobStore("web:local")
    older, _ = await store.enqueue_once(
        kind="test.a",
        payload={"nested": {"value": 1}},
        target_session_id=None,
        idempotency_key="older",
        max_attempts=3,
        now=_NOW,
    )
    newer, _ = await store.enqueue_once(
        kind="test.b",
        payload={},
        target_session_id=None,
        idempotency_key="newer",
        max_attempts=3,
        now=_NOW + timedelta(seconds=1),
    )

    fetched = await store.get(older.id)
    assert fetched is not None
    fetched.payload["nested"]["value"] = 99
    assert (await store.get(older.id)).payload == {"nested": {"value": 1}}  # type: ignore[union-attr]
    assert [row.id for row in await store.list()] == [newer.id, older.id]
    assert [row.id for row in await store.list(kind="test.a")] == [older.id]
    assert await store.list(status=JobStatus.running) == []
    with pytest.raises(ValueError, match="limit"):
        await store.list(limit=0)
    with pytest.raises(ValueError, match="limit"):
        await store.list(limit=101)
    assert await store.get("bad\x00id") is None
    assert await store.list(kind="bad\x00kind") == []


async def test_in_memory_dispatch_selection_respects_due_time_and_limit() -> None:
    store = InMemoryJobStore("web:local")
    due, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="due",
        max_attempts=3,
        now=_NOW,
    )
    later, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="later",
        max_attempts=3,
        now=_NOW + timedelta(minutes=5),
    )

    assert await store.dispatchable(_NOW, 1) == [due.id]
    assert later.id not in await store.dispatchable(_NOW, 100)
    assert await store.exhausted(_NOW, 100) == []


async def _queued(
    store: InMemoryJobStore,
    key: str,
    *,
    max_attempts: int = 3,
) -> str:
    row, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key=key,
        max_attempts=max_attempts,
        now=_NOW,
    )
    return row.id


async def test_claim_is_exclusive_and_reclaim_replaces_token_and_resets_progress() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "claim")
    first = await store.claim(job_id, _NOW, 60)
    assert first is not None
    assert first.attempt == 1
    assert await store.claim(job_id, _NOW, 60) is None

    await store.progress(
        first,
        current=3,
        total=10,
        message="first attempt",
        now=_NOW + timedelta(seconds=10),
    )
    second = await store.claim(job_id, _NOW + timedelta(seconds=71), 60)
    assert second is not None
    assert second.attempt == 2
    assert second.token != first.token
    record = await store.get(job_id)
    assert record is not None
    assert record.progress_current == 0
    assert record.progress_total is None
    assert record.started_at == _NOW


async def test_claim_attempt_ceiling_is_enforced_inside_store() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "ceiling", max_attempts=1)
    lease = await store.claim(job_id, _NOW, 10)
    assert lease is not None
    assert await store.claim(job_id, _NOW + timedelta(seconds=11), 10) is None
    assert await store.dispatchable(_NOW + timedelta(seconds=11), 100) == []
    assert await store.exhausted(_NOW + timedelta(seconds=11), 100) == [job_id]


async def test_heartbeat_refreshes_lease_and_stale_token_is_rejected() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "heartbeat")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    assert await store.heartbeat(lease, _NOW + timedelta(seconds=30)) is False
    record = await store.get(job_id)
    assert record is not None
    assert record.heartbeat_at == _NOW + timedelta(seconds=30)
    assert record.lease_expires_at == _NOW + timedelta(seconds=90)

    reclaimed = await store.claim(job_id, _NOW + timedelta(seconds=91), 60)
    assert reclaimed is not None
    with pytest.raises(JobLeaseLostError):
        await store.heartbeat(lease, _NOW + timedelta(seconds=92))
    with pytest.raises(JobLeaseLostError):
        await store.progress(
            lease,
            current=1,
            total=None,
            message=None,
            now=_NOW + timedelta(seconds=92),
        )


async def test_expired_owner_cannot_resurrect_a_lease() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "expired-owner", max_attempts=1)
    lease = await store.claim(job_id, _NOW, 10)
    assert lease is not None

    with pytest.raises(JobLeaseLostError):
        await store.heartbeat(lease, _NOW + timedelta(seconds=10))
    with pytest.raises(JobLeaseLostError):
        await store.progress(
            lease,
            current=1,
            total=1,
            message="too late",
            now=_NOW + timedelta(seconds=11),
        )
    assert await store.exhausted(_NOW + timedelta(seconds=11), 100) == [job_id]


async def test_progress_is_monotonic_bounded_and_refreshes_lease() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "progress")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None

    updated = await store.progress(
        lease,
        current=4,
        total=10,
        message="embedding batch 2",
        now=_NOW + timedelta(seconds=5),
    )
    assert updated.record.progress_current == 4
    assert updated.record.progress_total == 10
    assert updated.record.progress_message == "embedding batch 2"
    assert updated.record.lease_expires_at == _NOW + timedelta(seconds=65)
    assert updated.cancel_requested is False

    with pytest.raises(JobValidationError, match="progress_regression"):
        await store.progress(
            lease, current=3, total=10, message=None, now=_NOW + timedelta(seconds=6)
        )
    with pytest.raises(JobValidationError, match="invalid_progress"):
        await store.progress(
            lease, current=11, total=10, message=None, now=_NOW + timedelta(seconds=6)
        )
    with pytest.raises(JobValidationError, match="invalid_progress"):
        await store.progress(
            lease, current=-1, total=None, message=None, now=_NOW + timedelta(seconds=6)
        )
    for current, total in [
        (True, None),
        (1.5, 2),
        (2**63, None),
        (1, 2**63),
    ]:
        with pytest.raises(JobValidationError, match="invalid_progress"):
            await store.progress(  # type: ignore[arg-type]
                lease,
                current=current,
                total=total,
                message=None,
                now=_NOW + timedelta(seconds=6),
            )
    with pytest.raises(JobValidationError, match="storage_text_invalid"):
        await store.progress(
            lease,
            current=5,
            total=10,
            message="bad\x00message",
            now=_NOW + timedelta(seconds=6),
        )


async def test_lease_updates_report_cancel_and_normalize_timestamps_to_utc() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "cancel-aware")
    local_zone = timezone(timedelta(hours=8))
    lease = await store.claim(job_id, _NOW.astimezone(local_zone), 60)
    assert lease is not None
    claimed = await store.get(job_id)
    assert claimed is not None
    assert claimed.heartbeat_at is not None and claimed.heartbeat_at.tzinfo is UTC
    assert claimed.lease_expires_at is not None and claimed.lease_expires_at.tzinfo is UTC

    async with store._lock:
        store._rows[job_id] = replace(
            store._rows[job_id],
            cancel_requested_at=_NOW + timedelta(seconds=1),
        )

    heartbeat_at = (_NOW + timedelta(seconds=2)).astimezone(local_zone)
    assert await store.heartbeat(lease, heartbeat_at) is True
    heartbeat_record = await store.get(job_id)
    assert heartbeat_record is not None
    assert heartbeat_record.heartbeat_at is not None
    assert heartbeat_record.heartbeat_at.tzinfo is UTC

    progress_at = (_NOW + timedelta(seconds=3)).astimezone(local_zone)
    progress = await store.progress(
        lease,
        current=1,
        total=None,
        message=None,
        now=progress_at,
    )
    assert progress.cancel_requested is True
    assert progress.record.progress_updated_at is not None
    assert progress.record.progress_updated_at.tzinfo is UTC
    assert progress.record.lease_expires_at is not None
    assert progress.record.lease_expires_at.tzinfo is UTC


async def test_lease_operations_reject_invalid_duration_and_naive_timestamps() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "naive")
    with pytest.raises(ValueError, match="lease_seconds"):
        await store.claim(job_id, _NOW, 0)
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.claim(job_id, datetime(2026, 7, 14, 9, 0), 60)

    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.heartbeat(lease, datetime(2026, 7, 14, 9, 1))
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.progress(
            lease,
            current=1,
            total=None,
            message=None,
            now=datetime(2026, 7, 14, 9, 1),
        )


async def test_queued_cancel_is_terminal_idempotent_and_injects_once() -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore("web:local", events=events)
    job, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target",
        idempotency_key="cancel-queued",
        max_attempts=3,
        now=_NOW,
    )

    cancelled = await store.request_cancel(job.id, _NOW + timedelta(seconds=1))
    again = await store.request_cancel(job.id, _NOW + timedelta(seconds=2))

    assert cancelled is not None and cancelled.status is JobStatus.cancelled
    assert cancelled.result_message == "后台任务 test.echo 已取消。"
    assert cancelled.finished_at == _NOW + timedelta(seconds=1)
    assert again == cancelled
    assert await store.request_cancel("missing", _NOW) is None
    injected = [
        event for event in events.snapshot("target") if event.payload.get("job_id") == job.id
    ]
    assert len(injected) == 1
    assert injected[0].payload == {
        "role": "assistant",
        "text": "后台任务 test.echo 已取消。",
        "partial": False,
        "job_id": job.id,
        "job_kind": "test.echo",
        "job_status": "cancelled",
    }


async def test_running_cancel_request_does_not_beat_a_handler_that_already_completed() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "cancel-running")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    local_zone = timezone(timedelta(hours=8))

    requested = await store.request_cancel(
        job_id, (_NOW + timedelta(seconds=1)).astimezone(local_zone)
    )
    again = await store.request_cancel(job_id, _NOW + timedelta(seconds=2))
    assert requested is not None and requested.status is JobStatus.running
    assert requested.cancel_requested_at == _NOW + timedelta(seconds=1)
    assert again is not None
    assert again.cancel_requested_at == requested.cancel_requested_at
    succeeded = await store.succeed(
        lease,
        JobResult(data={"count": 1}, message="completed before checkpoint"),
        _NOW + timedelta(seconds=3),
    )
    assert succeeded.status is JobStatus.succeeded
    assert succeeded.cancel_requested_at == requested.cancel_requested_at
    assert succeeded.result == {"count": 1}
    assert await store.request_cancel(job_id, _NOW + timedelta(seconds=4)) == succeeded


async def test_observed_running_cancel_finishes_cancelled() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "cancel-observed")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    await store.request_cancel(job_id, _NOW + timedelta(seconds=1))
    assert await store.heartbeat(lease, _NOW + timedelta(seconds=2)) is True

    cancelled = await store.finish_cancelled(lease, _NOW + timedelta(seconds=3))
    assert cancelled.status is JobStatus.cancelled


async def test_finish_cancelled_requires_a_pending_request() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "cancel-not-requested")
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None
    with pytest.raises(JobValidationError, match="cancellation_not_requested"):
        await store.finish_cancelled(lease, _NOW + timedelta(seconds=1))


async def test_requeue_is_non_terminal_bounded_utc_and_does_not_inject() -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore(
        "web:local",
        events=events,
        limits=JobLimits(error_message_max_chars=4),
    )
    job, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target",
        idempotency_key="retry",
        max_attempts=3,
        now=_NOW,
    )
    lease = await store.claim(job.id, _NOW, 60)
    assert lease is not None
    local_zone = timezone(timedelta(hours=8))
    retry_at = (_NOW + timedelta(seconds=5)).astimezone(local_zone)
    updated_at = (_NOW + timedelta(seconds=1)).astimezone(local_zone)

    queued = await store.requeue(
        lease,
        JobError("provider_timeout", "safe public message"),
        retry_at,
        updated_at,
    )

    assert queued.status is JobStatus.queued
    assert queued.attempt == 1
    assert queued.next_attempt_at == _NOW + timedelta(seconds=5)
    assert queued.updated_at == _NOW + timedelta(seconds=1)
    assert queued.error_kind == "provider_timeout"
    assert queued.error_message == "safe"
    assert queued.lease_token is None
    assert queued.lease_expires_at is None
    assert queued.heartbeat_at is None
    assert queued.finished_at is None
    assert queued.injected_event_seq is None
    assert not any(e.payload.get("job_id") == job.id for e in events.snapshot("target"))


async def test_requeue_rejects_an_exhausted_attempt_without_mutating_the_job() -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, "retry-exhausted", max_attempts=1)
    lease = await store.claim(job_id, _NOW, 60)
    assert lease is not None

    with pytest.raises(JobValidationError, match="attempts_exhausted"):
        await store.requeue(
            lease,
            JobError("provider_timeout", "safe"),
            _NOW + timedelta(seconds=5),
            _NOW + timedelta(seconds=1),
        )

    record = await store.get(job_id)
    assert record is not None
    assert record.status is JobStatus.running
    assert record.lease_token == lease.token


@pytest.mark.parametrize(
    ("terminal", "expected_text"),
    [
        ("succeeded", "done"),
        ("failed", "后台任务 test.echo 失败：bad_input"),
        ("cancelled", "后台任务 test.echo 已取消。"),
    ],
)
async def test_terminal_finalizers_inject_exactly_once_and_freeze_the_job(
    terminal: str, expected_text: str
) -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore("web:local", events=events)
    job, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target",
        idempotency_key=f"terminal-{terminal}",
        max_attempts=3,
        now=_NOW,
    )
    lease = await store.claim(job.id, _NOW, 60)
    assert lease is not None

    if terminal == "succeeded":
        result = await store.succeed(lease, JobResult(data={"ok": True}, message="done"), _NOW)
    elif terminal == "failed":
        result = await store.fail_terminal(lease, JobError("bad_input", "safe"), _NOW)
    else:
        await store.request_cancel(job.id, _NOW)
        result = await store.finish_cancelled(lease, _NOW)

    assert result.status.value == terminal
    assert result.result_message == expected_text
    assert result.lease_token is None
    assert result.lease_expires_at is None
    assert result.heartbeat_at is None
    with pytest.raises(JobLeaseLostError):
        await store.succeed(
            lease,
            JobResult(data={}, message="again"),
            _NOW + timedelta(seconds=1),
        )
    assert await store.request_cancel(job.id, _NOW + timedelta(seconds=2)) == result
    assert await store.get(job.id) == result
    injected = [
        event for event in events.snapshot("target") if event.payload.get("job_id") == job.id
    ]
    assert len(injected) == 1
    assert injected[0].payload["role"] == "assistant"
    assert injected[0].payload["text"] == expected_text
    assert injected[0].payload["partial"] is False
    assert injected[0].payload["job_status"] == terminal


async def test_terminal_finalizer_bounds_record_error_and_injected_messages() -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore(
        "web:local",
        events=events,
        limits=JobLimits(result_message_max_chars=10, error_message_max_chars=4),
    )

    for terminal in ("succeeded", "failed", "cancelled"):
        job, _ = await store.enqueue_once(
            kind="test.echo",
            payload={},
            target_session_id="target",
            idempotency_key=f"bounded-{terminal}",
            max_attempts=3,
            now=_NOW,
        )
        lease = await store.claim(job.id, _NOW, 60)
        assert lease is not None
        if terminal == "succeeded":
            row = await store.succeed(
                lease,
                JobResult(data={"ok": True}, message="completed successfully"),
                _NOW,
            )
            full_message = "completed successfully"
        elif terminal == "failed":
            row = await store.fail_terminal(
                lease,
                JobError("provider_timeout", "safe public message"),
                _NOW,
            )
            full_message = "后台任务 test.echo 失败：provider_timeout"
            assert row.error_message == "safe"
        else:
            await store.request_cancel(job.id, _NOW)
            row = await store.finish_cancelled(lease, _NOW)
            full_message = "后台任务 test.echo 已取消。"

        injected = [
            event for event in events.snapshot("target") if event.payload.get("job_id") == job.id
        ]
        assert row.result_message == full_message[:10]
        assert len(injected) == 1
        assert injected[0].payload["text"] == row.result_message


async def test_terminal_text_normalizes_before_clipping() -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore(
        "web:local",
        events=events,
        limits=JobLimits(result_message_max_chars=1, error_message_max_chars=1),
    )

    success, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target",
        idempotency_key="clip-success",
        max_attempts=1,
        now=_NOW,
    )
    success_lease = await store.claim(success.id, _NOW, 60)
    assert success_lease is not None
    succeeded = await store.succeed(success_lease, JobResult(data={}, message=" safe"), _NOW)
    assert succeeded.result_message == "s"

    retry, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="clip-error",
        max_attempts=2,
        now=_NOW,
    )
    retry_lease = await store.claim(retry.id, _NOW, 60)
    assert retry_lease is not None
    queued = await store.requeue(
        retry_lease,
        JobError("provider_timeout", " safe"),
        _NOW + timedelta(seconds=5),
        _NOW + timedelta(seconds=1),
    )
    assert queued.error_message == "s"


async def test_fail_exhausted_uses_terminal_finalizer_once() -> None:
    events = InMemoryEventStore()
    await _session(events, "target", "web:local")
    store = InMemoryJobStore("web:local", events=events)
    job, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id="target",
        idempotency_key="exhausted",
        max_attempts=1,
        now=_NOW,
    )
    assert await store.claim(job.id, _NOW, 10) is not None
    assert await store.fail_exhausted(job.id, _NOW + timedelta(seconds=9)) is None
    local_zone = timezone(timedelta(hours=8))
    failed = await store.fail_exhausted(
        job.id, (_NOW + timedelta(seconds=10)).astimezone(local_zone)
    )

    assert failed is not None and failed.status is JobStatus.failed
    assert failed.error_kind == "attempts_exhausted"
    assert failed.finished_at == _NOW + timedelta(seconds=10)
    assert await store.fail_exhausted(job.id, _NOW + timedelta(seconds=11)) is None
    assert len([e for e in events.snapshot("target") if e.payload.get("job_id") == job.id]) == 1

    retryable_id = await _queued(store, "not-exhausted", max_attempts=2)
    assert await store.claim(retryable_id, _NOW, 10) is not None
    assert await store.fail_exhausted(retryable_id, _NOW + timedelta(seconds=10)) is None


async def test_failed_rerun_requires_a_new_enqueue_request_key() -> None:
    store = InMemoryJobStore("web:local")
    first, _ = await store.enqueue_once(
        kind="test.echo",
        payload={"version": 1},
        target_session_id=None,
        idempotency_key="request-1",
        max_attempts=1,
        now=_NOW,
    )
    lease = await store.claim(first.id, _NOW, 10)
    assert lease is not None
    await store.fail_terminal(lease, JobError("bad_input", "safe"), _NOW)

    duplicate, duplicate_created = await store.enqueue_once(
        kind="test.echo",
        payload={"version": 2},
        target_session_id=None,
        idempotency_key="request-1",
        max_attempts=1,
        now=_NOW + timedelta(seconds=1),
    )
    rerun, rerun_created = await store.enqueue_once(
        kind="test.echo",
        payload={"version": 2},
        target_session_id=None,
        idempotency_key="request-2",
        max_attempts=1,
        now=_NOW + timedelta(seconds=1),
    )

    assert duplicate_created is False
    assert duplicate.id == first.id
    assert duplicate.status is JobStatus.failed
    assert rerun_created is True
    assert rerun.id != first.id
    assert rerun.status is JobStatus.queued


@pytest.mark.parametrize(
    "operation",
    ["requeue", "succeed", "fail_terminal", "finish_cancelled"],
)
async def test_lifecycle_writes_require_a_current_non_expired_lease(
    operation: str,
) -> None:
    store = InMemoryJobStore("web:local")
    job_id = await _queued(store, f"expired-{operation}")
    lease = await store.claim(job_id, _NOW, 10)
    assert lease is not None
    expired_at = _NOW + timedelta(seconds=10)

    with pytest.raises(JobLeaseLostError):
        if operation == "requeue":
            await store.requeue(
                lease,
                JobError("provider_timeout", "safe"),
                expired_at + timedelta(seconds=5),
                expired_at,
            )
        elif operation == "succeed":
            await store.succeed(
                lease,
                JobResult(data={}, message="late"),
                expired_at,
            )
        elif operation == "fail_terminal":
            await store.fail_terminal(
                lease,
                JobError("bad_input", "safe"),
                expired_at,
            )
        else:
            await store.finish_cancelled(lease, expired_at)

    record = await store.get(job_id)
    assert record is not None
    assert record.status is JobStatus.running
    assert record.lease_token == lease.token


async def test_new_lifecycle_methods_reject_naive_timestamps() -> None:
    store = InMemoryJobStore("web:local")
    queued_id = await _queued(store, "naive-cancel")
    naive = datetime(2026, 7, 14, 9, 0)

    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.request_cancel(queued_id, naive)

    running_id = await _queued(store, "naive-running")
    lease = await store.claim(running_id, _NOW, 60)
    assert lease is not None
    error = JobError("provider_timeout", "safe")
    result = JobResult(data={}, message="done")

    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.requeue(lease, error, naive, _NOW)
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.requeue(lease, error, _NOW + timedelta(seconds=5), naive)
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.succeed(lease, result, naive)
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.fail_terminal(lease, error, naive)
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.finish_cancelled(lease, naive)
    with pytest.raises(JobValidationError, match="timezone_required"):
        await store.fail_exhausted("missing", naive)

    record = await store.get(running_id)
    assert record is not None
    assert record.status is JobStatus.running
    assert record.lease_token == lease.token
