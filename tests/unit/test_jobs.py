"""Strict durable-job contracts, limits and deterministic retry math."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import pytest

from keel_core.config import Settings
from keel_core.events import Event, EventType
from keel_core.jobs import (
    InMemoryJobStore,
    JobError,
    JobLease,
    JobLimits,
    JobResult,
    JobStatus,
    JobValidationError,
    PermanentJobError,
    RetryableJobError,
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
