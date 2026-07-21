"""Admission-fingerprint deployment-rollout compatibility (M3.6 web-routing rollback).

The admission fingerprint now folds the selected ``model`` into the immutable identity. A run
admitted by a *pre-model* binary stored a fingerprint that omitted the model entirely, so a
retry that reaches a freshly-deployed (model-aware) binary must still be recognized as the same
admission instead of being falsely rejected as a :class:`RunAdmissionConflict`. These tests pin
the four rollout cases:

1. an in-flight legacy row can be retried across the deploy (accepted);
2. a genuinely different content/binding is still a conflict (409);
3. a *new* (model-aware) row cannot be hijacked by a changed model;
4. the same model on a new row is an idempotent no-op.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from keel_core.agent_config_snapshot import AgentConfigSnapshot
from keel_core.runs import (
    InMemoryRunStore,
    RunAdmissionConflict,
    RunBudgetSpec,
    admission_fingerprint,
    legacy_admission_fingerprint,
    pre_snapshot_admission_fingerprint,
)

_SCOPE = "web:local"
_BINDING = dict(
    org_id="org-1",
    actor="user-1",
    agent_id="agent-1",
    session_id="sess-1",
    surface="web",
    content="triage",
)


def _current(*, model: str | None, snapshot_hash: str | None = None, **overrides: str) -> str:
    return admission_fingerprint(
        **{**_BINDING, **overrides},  # type: ignore[arg-type]
        model=model,
        snapshot_hash=snapshot_hash,
    )


def _pre_snapshot(*, model: str | None, **overrides: str) -> str:
    return pre_snapshot_admission_fingerprint(**{**_BINDING, **overrides}, model=model)  # type: ignore[arg-type]


def _legacy(**overrides: str) -> str:
    return legacy_admission_fingerprint(**{**_BINDING, **overrides})  # type: ignore[arg-type]


async def _create(
    store: InMemoryRunStore,
    *,
    run_id: str,
    fingerprint: str,
    legacy_fingerprint: str = "",
    pre_snapshot_fingerprint: str = "",
    idempotency_key: str = "k1",
    snapshot: AgentConfigSnapshot | None = None,
    **binding: str,
) -> tuple[str, bool]:
    # ``content`` is a fingerprint input only, not a stored run column.
    merged = {**_BINDING, **binding}
    merged.pop("content", None)
    record, created = await store.create(
        run_id=run_id,
        scope_id=_SCOPE,
        idempotency_key=idempotency_key,
        budget=RunBudgetSpec(),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        snapshot=snapshot or AgentConfigSnapshot(agent_id=binding.get("agent_id", "agent-1")),
        fingerprint=fingerprint,
        legacy_fingerprint=legacy_fingerprint,
        pre_snapshot_fingerprint=pre_snapshot_fingerprint,
        **merged,  # type: ignore[arg-type]
    )
    return record.id, created


async def test_legacy_row_retry_succeeds_after_model_upgrade() -> None:
    """A row admitted by a pre-model binary retries cleanly on the model-aware binary."""
    store = InMemoryRunStore()
    # Old binary stored the legacy (no-model) fingerprint.
    original, created = await _create(store, run_id="run-1", fingerprint=_legacy())
    assert created
    # New binary retries: model-aware current fingerprint + reconstructed legacy fallback.
    retried, created_again = await _create(
        store,
        run_id="run-2",
        fingerprint=_current(model="gpt-5"),
        legacy_fingerprint=_legacy(),
    )
    assert retried == original
    assert created_again is False


async def test_legacy_row_retry_with_changed_binding_still_conflicts() -> None:
    """The legacy fallback only accepts the exact original binding — evil content is a 409."""
    store = InMemoryRunStore()
    await _create(store, run_id="run-1", fingerprint=_legacy(content="do X"), content="do X")
    with pytest.raises(RunAdmissionConflict):
        await _create(
            store,
            run_id="run-2",
            fingerprint=_current(model="gpt-5", content="do EVIL"),
            legacy_fingerprint=_legacy(content="do EVIL"),
            content="do EVIL",
        )
    with pytest.raises(RunAdmissionConflict):
        await _create(
            store,
            run_id="run-3",
            fingerprint=_current(model="gpt-5", agent_id="agent-EVIL"),
            legacy_fingerprint=_legacy(agent_id="agent-EVIL"),
            agent_id="agent-EVIL",
        )


async def test_new_model_aware_row_changed_model_conflicts() -> None:
    """A changed model cannot hijack a row admitted by the model-aware binary."""
    store = InMemoryRunStore()
    await _create(store, run_id="run-1", fingerprint=_current(model="model-A"))
    with pytest.raises(RunAdmissionConflict):
        await _create(
            store,
            run_id="run-2",
            fingerprint=_current(model="model-B"),
            legacy_fingerprint=_legacy(),
        )


async def test_new_model_aware_row_same_model_is_idempotent() -> None:
    """The same model on a model-aware row is an idempotent no-op."""
    store = InMemoryRunStore()
    original, _ = await _create(store, run_id="run-1", fingerprint=_current(model="model-A"))
    retried, created_again = await _create(
        store,
        run_id="run-2",
        fingerprint=_current(model="model-A"),
        legacy_fingerprint=_legacy(),
    )
    assert retried == original
    assert created_again is False


def test_legacy_and_model_aware_fingerprints_are_structurally_distinct() -> None:
    """A model-aware hash always encodes the model field, so it can never equal the legacy one
    — the property that makes the legacy fallback safe against model-change hijack."""
    assert _legacy() != _current(model=None)
    assert _legacy() != _current(model="gpt-5")


# --------------------------------------------------------------------------------------------
# R1B: the Agent config snapshot hash joins the fingerprint (INVARIANTS.md C8).
# --------------------------------------------------------------------------------------------


def test_snapshot_aware_and_pre_snapshot_fingerprints_are_structurally_distinct() -> None:
    """A snapshot-aware hash always encodes ``snapshot_hash``, so it never equals the older
    (model-aware, pre-snapshot) or legacy (pre-model) forms — the property that makes both
    older compatibility forms safe against a changed-snapshot hijack."""
    assert _pre_snapshot(model="gpt-5") != _current(model="gpt-5", snapshot_hash="")
    assert _pre_snapshot(model="gpt-5") != _current(model="gpt-5", snapshot_hash="deadbeef")
    assert _legacy() != _current(model=None, snapshot_hash="deadbeef")


async def test_pre_snapshot_row_retry_succeeds_after_snapshot_upgrade() -> None:
    """A row admitted by a pre-snapshot (but model-aware) binary retries cleanly once the
    fleet is snapshot-aware — mirroring the legacy pre-model rollout compatibility."""
    store = InMemoryRunStore()
    original, created = await _create(
        store, run_id="run-1", fingerprint=_pre_snapshot(model="gpt-5")
    )
    assert created
    retried, created_again = await _create(
        store,
        run_id="run-2",
        fingerprint=_current(model="gpt-5", snapshot_hash="abc123"),
        pre_snapshot_fingerprint=_pre_snapshot(model="gpt-5"),
    )
    assert retried == original
    assert created_again is False


async def test_changed_snapshot_hash_conflicts_on_a_snapshot_aware_row() -> None:
    """A retry that reuses the idempotency key but presents a *different* snapshot hash (the
    Agent's version/persona/model/budget changed) is rejected — never silently repaired."""
    store = InMemoryRunStore()
    await _create(
        store, run_id="run-1", fingerprint=_current(model="gpt-5", snapshot_hash="abc123")
    )
    with pytest.raises(RunAdmissionConflict):
        await _create(
            store,
            run_id="run-2",
            fingerprint=_current(model="gpt-5", snapshot_hash="changed-hash"),
            pre_snapshot_fingerprint=_pre_snapshot(model="gpt-5"),
        )


async def test_same_snapshot_hash_is_idempotent() -> None:
    """The same snapshot hash on a retry of a snapshot-aware row is an idempotent no-op."""
    store = InMemoryRunStore()
    original, _ = await _create(
        store, run_id="run-1", fingerprint=_current(model="gpt-5", snapshot_hash="abc123")
    )
    retried, created_again = await _create(
        store,
        run_id="run-2",
        fingerprint=_current(model="gpt-5", snapshot_hash="abc123"),
        pre_snapshot_fingerprint=_pre_snapshot(model="gpt-5"),
    )
    assert retried == original
    assert created_again is False
