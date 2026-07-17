"""Unit tests for retention policies, scheduling, and the data map (M3.5)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from keel_core.lifecycle import policies
from keel_core.lifecycle.datamap import DATA_MAP, ErasureTreatment
from keel_core.lifecycle.policies import (
    DEFAULT_RETENTION,
    RetentionClass,
    RetentionPolicy,
    is_expired,
    resolve_policy,
    retention_expires_at,
)
from keel_core.lifecycle.retention import (
    RetentionCandidate,
    next_expiry,
    select_expired,
)

_NOW = datetime(2026, 7, 17, tzinfo=UTC)


def test_user_content_is_permanent_by_default() -> None:
    for resource_class in (
        policies.SESSION,
        policies.EVENT,
        policies.MEMORY_BLOCK,
        policies.ARCHIVAL,
        policies.KNOWLEDGE,
        policies.CONNECTOR_TOKEN,
    ):
        policy = DEFAULT_RETENTION[resource_class]
        assert policy.is_permanent
        assert retention_expires_at(_NOW, policy) is None


def test_ephemeral_classes_have_finite_ttls() -> None:
    assert DEFAULT_RETENTION[policies.OAUTH_STATE].retention_class is RetentionClass.transient
    assert DEFAULT_RETENTION[policies.OAUTH_STATE].ttl_seconds == 3600
    assert DEFAULT_RETENTION[policies.WEBHOOK_DELIVERY].ttl_seconds == 86_400


def test_is_expired_and_expires_at() -> None:
    policy = DEFAULT_RETENTION[policies.OAUTH_STATE]
    created = _NOW
    assert retention_expires_at(created, policy) == created + timedelta(hours=1)
    assert not is_expired(created, policy, created + timedelta(minutes=59))
    assert is_expired(created, policy, created + timedelta(hours=2))


def test_resolve_policy_prefers_override() -> None:
    override = RetentionPolicy(policies.SESSION, RetentionClass.standard, 30 * 86_400)
    resolved = resolve_policy(policies.SESSION, overrides={policies.SESSION: override})
    assert resolved is override
    # Absent override falls back to the default (permanent).
    assert resolve_policy(policies.SESSION).is_permanent


def test_select_expired_ignores_permanent_and_picks_due() -> None:
    candidates = [
        RetentionCandidate(policies.OAUTH_STATE, "s-old", _NOW - timedelta(hours=2)),
        RetentionCandidate(policies.OAUTH_STATE, "s-new", _NOW - timedelta(minutes=1)),
        RetentionCandidate(policies.SESSION, "sess-keep", _NOW - timedelta(days=3650)),
    ]
    due = select_expired(candidates, _NOW)
    assert [c.resource_id for c in due] == ["s-old"]


def test_next_expiry_is_earliest_finite_horizon() -> None:
    candidates = [
        RetentionCandidate(policies.WEBHOOK_DELIVERY, "w", _NOW),
        RetentionCandidate(policies.OAUTH_STATE, "o", _NOW),
        RetentionCandidate(policies.SESSION, "keep", _NOW),
    ]
    assert next_expiry(candidates) == _NOW + timedelta(hours=1)
    assert next_expiry([RetentionCandidate(policies.SESSION, "keep", _NOW)]) is None


def test_data_map_every_class_has_a_default_policy() -> None:
    for entry in DATA_MAP:
        assert entry.resource_class in DEFAULT_RETENTION
        assert entry.retention is DEFAULT_RETENTION[entry.resource_class]


def test_data_map_scope_bound_tables_carry_scope_column() -> None:
    for entry in DATA_MAP:
        if entry.kind == "table" and entry.treatment in (
            ErasureTreatment.scope_bound,
            ErasureTreatment.session_scoped,
        ):
            assert entry.scope_column == "scope_id", entry.name


def test_webhook_deliveries_is_global_preserved() -> None:
    entry = next(e for e in DATA_MAP if e.name == "webhook_deliveries")
    assert entry.treatment is ErasureTreatment.global_preserved
    assert entry.scope_column is None
