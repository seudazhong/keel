"""Unit tests for the in-memory Effect ledger (keel_core.effect_store) — the durable
reserve/execute/confirm state machine C4/C5 depend on, and the cross-scope reconciliation
pointer it drives (keel_core.effect_outbox)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from keel_core.effect_outbox import EffectOutboxKind, InMemoryEffectReconciliationOutbox
from keel_core.effect_store import InMemoryEffectStore
from keel_core.effects import EffectConflictError, EffectStatus

SCOPE_A = "agent:org-a/agent-1"
SCOPE_B = "agent:org-b/agent-1"


def _store() -> InMemoryEffectStore:
    return InMemoryEffectStore()


async def _reserve(store: InMemoryEffectStore, *, scope_id: str = SCOPE_A, key: str = "k1"):
    return await store.create_or_get(
        scope_id=scope_id,
        org_id="org-a",
        agent_id="agent-1",
        actor_id="user-1",
        run_id="run-1",
        tool_name="email_send",
        provider="gmail",
        resource_id="",
        action_name="email_send",
        action_hash="hash-1",
        idempotency_key=key,
        args={"to": "a@example.com"},
    )


async def test_create_or_get_is_idempotent_on_identity() -> None:
    store = _store()
    first = await _reserve(store)
    second = await _reserve(store)
    assert first.id == second.id
    assert first.status is EffectStatus.reserved


async def test_duplicate_idempotency_key_with_different_action_hash_conflicts() -> None:
    store = _store()
    await _reserve(store)
    with pytest.raises(EffectConflictError):
        await store.create_or_get(
            scope_id=SCOPE_A,
            org_id="org-a",
            agent_id="agent-1",
            actor_id="user-1",
            run_id="run-1",
            tool_name="email_send",
            provider="gmail",
            resource_id="",
            action_name="email_send",
            action_hash="a-different-hash",
            idempotency_key="k1",
            args={"to": "b@example.com"},
        )


async def test_begin_execution_is_a_single_winner_under_concurrency() -> None:
    """Two concurrent callers racing the same Effect: exactly one wins the lease."""
    store = _store()
    effect = await _reserve(store)

    results = await asyncio.gather(
        store.begin_execution(SCOPE_A, effect.id, lease_owner="w1"),
        store.begin_execution(SCOPE_A, effect.id, lease_owner="w2"),
    )
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert winners[0].status is EffectStatus.executing
    assert winners[0].attempt == 1


async def test_confirm_requires_the_exact_lease_token() -> None:
    store = _store()
    effect = await _reserve(store)
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1")
    assert claimed is not None
    with pytest.raises(LookupError):
        await store.confirm(
            SCOPE_A, effect.id, lease_token="wrong-token", provider_ref="x", result="y"
        )
    confirmed = await store.confirm(
        SCOPE_A, effect.id, lease_token=claimed.lease_token or "", provider_ref="ref-1", result="ok"
    )
    assert confirmed.status is EffectStatus.confirmed
    assert confirmed.provider_ref == "ref-1"


async def test_ordinary_failure_is_retryable() -> None:
    store = _store()
    effect = await _reserve(store)
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1")
    assert claimed is not None
    failed = await store.mark_failed(
        SCOPE_A, effect.id, lease_token=claimed.lease_token or "", error="boom"
    )
    assert failed.status is EffectStatus.failed
    retried = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1")
    assert retried is not None
    assert retried.status is EffectStatus.executing
    assert retried.attempt == 2


async def test_ambiguous_outcome_becomes_unknown_and_cannot_be_retried() -> None:
    """Headline C4: a possible-success-before-response-loss blocks retry."""
    store = _store()
    effect = await _reserve(store)
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1")
    assert claimed is not None
    unknown = await store.mark_unknown(
        SCOPE_A, effect.id, lease_token=claimed.lease_token or "", error="timeout after send"
    )
    assert unknown.status is EffectStatus.unknown
    # begin_execution refuses: unknown is not in the retryable set.
    blocked = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w2")
    assert blocked is None
    current = await store.get(SCOPE_A, effect.id)
    assert current is not None and current.status is EffectStatus.unknown


async def test_crash_after_provider_success_before_confirm_becomes_unknown_on_reap() -> None:
    """A crash between the provider call succeeding and `confirm()` must recover to
    `unknown`, never silently back to a plain pending/retryable state (C4)."""
    store = _store()
    effect = await _reserve(store)
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1", lease_seconds=1)
    assert claimed is not None
    # Simulate a crash: no confirm() ever happens. After the lease's deadline, a
    # restarted worker reaps it.
    future = datetime.now(UTC) + timedelta(seconds=2)
    reaped = await store.reap_expired_lease(SCOPE_A, effect.id, now=future)
    assert reaped is not None
    assert reaped.status is EffectStatus.unknown
    # Never a "no-op" pending-retry outcome: begin_execution still refuses.
    assert await store.begin_execution(SCOPE_A, effect.id, lease_owner="w2") is None


async def test_reap_is_a_noop_before_the_lease_actually_expires() -> None:
    store = _store()
    effect = await _reserve(store)
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1", lease_seconds=60)
    assert claimed is not None
    reaped = await store.reap_expired_lease(SCOPE_A, effect.id, now=datetime.now(UTC))
    assert reaped is None
    current = await store.get(SCOPE_A, effect.id)
    assert current is not None and current.status is EffectStatus.executing


async def test_renewed_execution_lease_is_not_reaped_at_the_original_deadline() -> None:
    store = _store()
    effect = await _reserve(store)
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1", lease_seconds=1)
    assert claimed is not None and claimed.lease_expires_at is not None
    original_deadline = claimed.lease_expires_at
    assert await store.renew_execution_lease(
        SCOPE_A,
        effect.id,
        lease_token=claimed.lease_token or "",
        lease_seconds=60,
    )
    assert (
        await store.reap_expired_lease(
            SCOPE_A, effect.id, now=original_deadline + timedelta(seconds=1)
        )
        is None
    )


async def test_reconciliation_confirms_existing_mutation_exactly_once() -> None:
    store = _store()
    effect = await _reserve(store)
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1")
    assert claimed is not None
    await store.mark_unknown(SCOPE_A, effect.id, lease_token=claimed.lease_token or "", error="x")
    confirmed = await store.reconcile_confirmed(
        SCOPE_A, effect.id, provider_ref="msg-999", result="sent"
    )
    assert confirmed.status is EffectStatus.reconciled_confirmed
    assert confirmed.provider_ref == "msg-999"
    assert confirmed.reconciled_at is not None
    # Terminal: a second reconciliation call is illegal (no longer `unknown`).
    with pytest.raises(LookupError):
        await store.reconcile_confirmed(SCOPE_A, effect.id, provider_ref="x", result="y")


async def test_reconciliation_proves_absent_then_permits_exactly_one_controlled_retry() -> None:
    store = _store()
    effect = await _reserve(store)
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1")
    assert claimed is not None
    await store.mark_unknown(SCOPE_A, effect.id, lease_token=claimed.lease_token or "", error="x")
    absent = await store.reconcile_absent(SCOPE_A, effect.id)
    assert absent.status is EffectStatus.reconciled_absent

    retried = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w2")
    assert retried is not None
    assert retried.status is EffectStatus.executing
    confirmed = await store.confirm(
        SCOPE_A, effect.id, lease_token=retried.lease_token or "", provider_ref="r", result="ok"
    )
    assert confirmed.status is EffectStatus.confirmed


async def test_cross_scope_isolation() -> None:
    """Two scopes never see or affect each other's Effect, even with the same key."""
    store = _store()
    a = await _reserve(store, scope_id=SCOPE_A, key="shared-key")
    b = await _reserve(store, scope_id=SCOPE_B, key="shared-key")
    assert a.id != b.id
    assert await store.get(SCOPE_A, b.id) is None
    assert await store.get(SCOPE_B, a.id) is None
    # A caller cannot claim/confirm another scope's effect by id.
    assert await store.begin_execution(SCOPE_B, a.id, lease_owner="w") is None


async def test_incapable_provider_stays_unknown_after_failed_reconciliation_attempt() -> None:
    """An incapable provider must never be inferred confirmed/absent — it just stays
    `unknown` (surfaced for operator/user action, R1B requirement 6)."""
    store = _store()
    effect = await _reserve(store)
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1")
    assert claimed is not None
    unknown = await store.mark_unknown(
        SCOPE_A, effect.id, lease_token=claimed.lease_token or "", error="x"
    )
    assert unknown.status is EffectStatus.unknown
    # No reconciliation call at all is made for an incapable provider — the Effect just
    # remains observably `unknown` via a plain read.
    still_unknown = await store.get(SCOPE_A, effect.id)
    assert still_unknown is not None and still_unknown.status is EffectStatus.unknown


# --- Reconciliation-outbox pointer lifecycle (cross-scope worker discovery) --------


async def test_outbox_pointer_tracks_execution_then_reconciliation_then_clears() -> None:
    outbox = InMemoryEffectReconciliationOutbox()
    store = InMemoryEffectStore(outbox)
    effect = await _reserve(store)

    assert await outbox.get(effect.id) is None
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1", lease_seconds=5)
    assert claimed is not None
    pointer = await outbox.get(effect.id)
    assert pointer is not None and pointer.kind is EffectOutboxKind.lease_watch

    await store.mark_unknown(SCOPE_A, effect.id, lease_token=claimed.lease_token or "", error="x")
    pointer = await outbox.get(effect.id)
    assert pointer is not None and pointer.kind is EffectOutboxKind.reconcile

    await store.reconcile_confirmed(SCOPE_A, effect.id, provider_ref="r", result="ok")
    assert await outbox.get(effect.id) is None  # resolved: pointer retired


async def test_outbox_pointer_cleared_on_ordinary_confirm() -> None:
    outbox = InMemoryEffectReconciliationOutbox()
    store = InMemoryEffectStore(outbox)
    effect = await _reserve(store)
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1")
    assert claimed is not None
    assert await outbox.get(effect.id) is not None
    await store.confirm(
        SCOPE_A, effect.id, lease_token=claimed.lease_token or "", provider_ref="r", result="ok"
    )
    assert await outbox.get(effect.id) is None


async def test_outbox_claim_due_is_fenced_and_bounded() -> None:
    outbox = InMemoryEffectReconciliationOutbox()
    store = InMemoryEffectStore(outbox)
    effect = await _reserve(store)
    claimed = await store.begin_execution(SCOPE_A, effect.id, lease_owner="w1", lease_seconds=1)
    assert claimed is not None

    now = datetime.now(UTC) + timedelta(seconds=2)
    due = await outbox.claim_due(worker_id="reconciler-1", now=now, limit=10)
    assert len(due) == 1
    token = due[0].lease_token
    assert token is not None
    # A second worker cannot claim the same pointer while the lease is held.
    due_again = await outbox.claim_due(worker_id="reconciler-2", now=now, limit=10)
    assert due_again == []
    # Reschedule releases the lease and defers the next attempt.
    assert await outbox.reschedule(effect.id, lease_token=token, delay_seconds=30, now=now)
    later = await outbox.claim_due(worker_id="reconciler-2", now=now + timedelta(seconds=31))
    assert len(later) == 1
