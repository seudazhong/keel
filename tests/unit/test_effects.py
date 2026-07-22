"""Unit tests for the Effect domain (keel_core.effects): the state machine, canonical-args
digesting, and generic provider-ref extraction — the pure logic C4/C5 depend on."""

from __future__ import annotations

import pytest

from keel_core.effects import (
    EffectStatus,
    EffectTransitionError,
    canonical_args_or_digest,
    default_provider_ref,
    is_digest,
    is_retryable,
    is_terminal,
    validate_transition,
)

# --- Legal/illegal transitions -----------------------------------------------------

_LEGAL = [
    (EffectStatus.reserved, EffectStatus.executing),
    (EffectStatus.executing, EffectStatus.confirmed),
    (EffectStatus.executing, EffectStatus.unknown),
    (EffectStatus.executing, EffectStatus.failed),
    (EffectStatus.unknown, EffectStatus.reconciled_confirmed),
    (EffectStatus.unknown, EffectStatus.reconciled_absent),
    (EffectStatus.reconciled_absent, EffectStatus.executing),
    (EffectStatus.reconciled_absent, EffectStatus.reconciled_confirmed),
    (EffectStatus.failed, EffectStatus.executing),
    (EffectStatus.failed, EffectStatus.reconciled_confirmed),
]


@pytest.mark.parametrize("current,target", _LEGAL)
def test_legal_transitions_are_accepted(current: EffectStatus, target: EffectStatus) -> None:
    validate_transition(current, target)  # must not raise


_ILLEGAL = [
    # Unknown can NEVER become an ordinary failure or vanish back to reserved/executing.
    (EffectStatus.unknown, EffectStatus.failed),
    (EffectStatus.unknown, EffectStatus.executing),
    (EffectStatus.unknown, EffectStatus.reserved),
    (EffectStatus.unknown, EffectStatus.confirmed),
    # Terminal states accept nothing further.
    (EffectStatus.confirmed, EffectStatus.executing),
    (EffectStatus.confirmed, EffectStatus.failed),
    (EffectStatus.reconciled_confirmed, EffectStatus.executing),
    (EffectStatus.reconciled_confirmed, EffectStatus.unknown),
    # A fresh reservation cannot skip straight to a terminal/ambiguous state.
    (EffectStatus.reserved, EffectStatus.confirmed),
    (EffectStatus.reserved, EffectStatus.unknown),
    (EffectStatus.reserved, EffectStatus.failed),
    # Ordinary failure retry must re-enter execution, not become confirmed directly.
    (EffectStatus.failed, EffectStatus.confirmed),
    (EffectStatus.reconciled_absent, EffectStatus.confirmed),
]


@pytest.mark.parametrize("current,target", _ILLEGAL)
def test_illegal_transitions_are_rejected(current: EffectStatus, target: EffectStatus) -> None:
    with pytest.raises(EffectTransitionError):
        validate_transition(current, target)


def test_only_failed_and_reconciled_absent_and_reserved_are_retryable() -> None:
    for status in EffectStatus:
        expected = status in {
            EffectStatus.reserved,
            EffectStatus.failed,
            EffectStatus.reconciled_absent,
        }
        assert is_retryable(status) is expected
    # Headline C4 property: unknown is never retryable.
    assert is_retryable(EffectStatus.unknown) is False


def test_terminal_statuses() -> None:
    assert is_terminal(EffectStatus.confirmed)
    assert is_terminal(EffectStatus.reconciled_confirmed)
    assert not is_terminal(EffectStatus.unknown)
    assert not is_terminal(EffectStatus.failed)


# --- Canonical args / safe digest ---------------------------------------------------


def test_canonical_args_is_stable_sorted_json_when_no_secret_present() -> None:
    a = canonical_args_or_digest({"b": 1, "a": 2})
    b = canonical_args_or_digest({"a": 2, "b": 1})
    assert a == b
    assert not is_digest(a)
    assert '"a":2' in a and '"b":1' in a


@pytest.mark.parametrize(
    "key", ["token", "secret", "password", "refresh_token", "access_token", "api_key"]
)
def test_secret_shaped_keys_are_never_persisted_raw(key: str) -> None:
    value = canonical_args_or_digest({"to": "a@example.com", key: "super-secret-value"})
    assert is_digest(value)
    assert "super-secret-value" not in value


def test_nested_secret_key_is_also_digested() -> None:
    value = canonical_args_or_digest({"outer": {"inner": {"password": "hunter2"}}})
    assert is_digest(value)
    assert "hunter2" not in value


def test_digest_is_deterministic_for_same_args() -> None:
    args = {"token": "abc"}
    assert canonical_args_or_digest(args) == canonical_args_or_digest(dict(args))


# --- Generic provider-ref extraction -------------------------------------------------


def test_default_provider_ref_extracts_json_id() -> None:
    assert default_provider_ref('{"id": "evt_123", "summary": "x"}') == "evt_123"


def test_default_provider_ref_extracts_message_id_field() -> None:
    assert default_provider_ref('{"message_id": "msg_1"}') == "msg_1"


def test_default_provider_ref_extracts_key_value_style() -> None:
    assert default_provider_ref("sent (id=abc123)") == "abc123"


def test_default_provider_ref_returns_empty_when_unrecognized() -> None:
    assert default_provider_ref("no identity here") == ""
