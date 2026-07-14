"""Strict durable-job contracts, limits and deterministic retry math."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from keel_core.config import Settings
from keel_core.jobs import (
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
