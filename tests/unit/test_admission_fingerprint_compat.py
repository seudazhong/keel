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

from keel_core.runs import (
    InMemoryRunStore,
    RunAdmissionConflict,
    RunBudgetSpec,
    admission_fingerprint,
    legacy_admission_fingerprint,
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


def _current(*, model: str | None, **overrides: str) -> str:
    return admission_fingerprint(**{**_BINDING, **overrides}, model=model)  # type: ignore[arg-type]


def _legacy(**overrides: str) -> str:
    return legacy_admission_fingerprint(**{**_BINDING, **overrides})  # type: ignore[arg-type]


async def _create(
    store: InMemoryRunStore,
    *,
    run_id: str,
    fingerprint: str,
    legacy_fingerprint: str = "",
    idempotency_key: str = "k1",
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
        fingerprint=fingerprint,
        legacy_fingerprint=legacy_fingerprint,
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
