"""Integration: the durable Effect ledger over Postgres (R1B, C4/C5) — migration
round-trip, RLS/FORCE RLS cross-scope isolation, real concurrent-execution races, crash
recovery via lease expiry, and provider reconciliation over the real schema."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.effect_outbox import EffectOutboxKind, PostgresEffectReconciliationOutbox
from keel_core.effect_store import PostgresEffectStore, purge_scope
from keel_core.effects import EffectConflictError, EffectStatus

pytestmark = pytest.mark.integration


def _scope() -> str:
    return f"agent:org-{uuid.uuid4().hex}/agent-1"


async def _reserve(store: PostgresEffectStore, scope_id: str, key: str = "k1"):
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
        args={"to": "a@example.com", "token": "shh"},
    )


async def test_migration_creates_effects_and_outbox_with_rls(migrated_db: AsyncEngine) -> None:
    async with migrated_db.connect() as conn:
        for table in ("effects", "effect_reconciliation_outbox"):
            exists = await conn.scalar(
                text("SELECT 1 FROM information_schema.tables WHERE table_name = :t"),
                {"t": table},
            )
            assert exists == 1
        forced = await conn.scalar(
            text("SELECT relforcerowsecurity FROM pg_class WHERE relname = 'effects'")
        )
        assert forced is True


async def test_create_or_get_is_idempotent_and_persists_across_instances(
    migrated_db: AsyncEngine,
) -> None:
    scope = _scope()
    outbox = PostgresEffectReconciliationOutbox(migrated_db)
    first_store = PostgresEffectStore(migrated_db, outbox)
    effect = await _reserve(first_store, scope)
    # A fresh store instance sharing the same durable backend sees the same row.
    second_store = PostgresEffectStore(migrated_db, outbox)
    same = await _reserve(second_store, scope)
    assert same.id == effect.id
    assert same.status is EffectStatus.reserved
    # Args contained a secret-shaped key -> a safe digest is persisted, never the secret.
    assert same.canonical_args.startswith("sha256:")
    assert "shh" not in same.canonical_args


async def test_duplicate_key_different_action_hash_conflicts(migrated_db: AsyncEngine) -> None:
    scope = _scope()
    store = PostgresEffectStore(migrated_db)
    await _reserve(store, scope)
    with pytest.raises(EffectConflictError):
        await store.create_or_get(
            scope_id=scope,
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


async def test_concurrent_begin_execution_has_a_single_winner(migrated_db: AsyncEngine) -> None:
    """Real concurrency (two independent connections racing the same row): exactly one
    caller's compare-and-set wins, matching the in-memory guarantee under Postgres."""
    scope = _scope()
    store = PostgresEffectStore(migrated_db)
    effect = await _reserve(store, scope)

    results = await asyncio.gather(
        *[store.begin_execution(scope, effect.id, lease_owner=f"worker-{i}") for i in range(8)]
    )
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert winners[0].attempt == 1


async def test_crash_after_provider_success_before_confirm_recovers_to_unknown(
    migrated_db: AsyncEngine,
) -> None:
    scope = _scope()
    outbox = PostgresEffectReconciliationOutbox(migrated_db)
    store = PostgresEffectStore(migrated_db, outbox)
    effect = await _reserve(store, scope)
    claimed = await store.begin_execution(scope, effect.id, lease_owner="w1", lease_seconds=1)
    assert claimed is not None
    pointer = await outbox.get(effect.id)
    assert pointer is not None and pointer.kind is EffectOutboxKind.lease_watch

    # Simulate the worker crashing before confirm(): a fresh store/process (or a
    # scheduled reaper) recovers the expired lease to `unknown`, never back to pending.
    future = datetime.now(UTC) + timedelta(seconds=2)
    reaped = await store.reap_expired_lease(scope, effect.id, now=future)
    assert reaped is not None and reaped.status is EffectStatus.unknown
    assert await store.begin_execution(scope, effect.id, lease_owner="w2") is None

    pointer = await outbox.get(effect.id)
    assert pointer is not None and pointer.kind is EffectOutboxKind.reconcile


async def test_reconciliation_confirms_exactly_once_and_clears_the_pointer(
    migrated_db: AsyncEngine,
) -> None:
    scope = _scope()
    outbox = PostgresEffectReconciliationOutbox(migrated_db)
    store = PostgresEffectStore(migrated_db, outbox)
    effect = await _reserve(store, scope)
    claimed = await store.begin_execution(scope, effect.id, lease_owner="w1")
    assert claimed is not None
    await store.mark_unknown(scope, effect.id, lease_token=claimed.lease_token or "", error="x")

    confirmed = await store.reconcile_confirmed(
        scope, effect.id, provider_ref="msg-1", result="sent"
    )
    assert confirmed.status is EffectStatus.reconciled_confirmed
    assert await outbox.get(effect.id) is None
    with pytest.raises(LookupError):
        await store.reconcile_confirmed(scope, effect.id, provider_ref="x", result="y")


async def test_reconciliation_proves_absent_then_one_controlled_retry(
    migrated_db: AsyncEngine,
) -> None:
    scope = _scope()
    store = PostgresEffectStore(migrated_db, PostgresEffectReconciliationOutbox(migrated_db))
    effect = await _reserve(store, scope)
    claimed = await store.begin_execution(scope, effect.id, lease_owner="w1")
    assert claimed is not None
    await store.mark_unknown(scope, effect.id, lease_token=claimed.lease_token or "", error="x")
    absent = await store.reconcile_absent(scope, effect.id)
    assert absent.status is EffectStatus.reconciled_absent

    retried = await store.begin_execution(scope, effect.id, lease_owner="w2")
    assert retried is not None and retried.status is EffectStatus.executing


async def test_cross_scope_isolation_under_rls(migrated_db: AsyncEngine) -> None:
    scope_a = _scope()
    scope_b = _scope()
    store = PostgresEffectStore(migrated_db)
    a = await _reserve(store, scope_a, key="shared")
    b = await _reserve(store, scope_b, key="shared")
    assert a.id != b.id
    assert await store.get(scope_a, b.id) is None
    assert await store.get(scope_b, a.id) is None
    assert await store.begin_execution(scope_b, a.id, lease_owner="w") is None


async def test_runtime_role_cannot_bypass_effect_scope_rls(migrated_db: AsyncEngine) -> None:
    """Adversarial: the non-owner ``keel_runtime`` role is bound by scope RLS + FORCE."""
    scope_a = _scope()
    scope_b = _scope()
    store = PostgresEffectStore(migrated_db)
    await _reserve(store, scope_a, key="k-a")
    await _reserve(store, scope_b, key="k-b")

    async with migrated_db.connect() as conn:
        has_role = await conn.scalar(text("SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime'"))
        if not has_role:
            pytest.skip("keel_runtime role not present (managed Postgres restricts CREATE ROLE)")
        await conn.execute(text("SET ROLE keel_runtime"))
        try:
            await conn.execute(text("SELECT set_config('app.scope_id', :s, false)"), {"s": scope_a})
            rows = (
                await conn.execute(
                    text("SELECT scope_id FROM effects WHERE scope_id IN (:a, :b)"),
                    {"a": scope_a, "b": scope_b},
                )
            ).fetchall()
            assert {r[0] for r in rows} == {scope_a}
        finally:
            await conn.execute(text("RESET ROLE"))


async def test_purge_scope_removes_effects_and_cascades_the_pointer(
    migrated_db: AsyncEngine,
) -> None:
    scope = _scope()
    outbox = PostgresEffectReconciliationOutbox(migrated_db)
    store = PostgresEffectStore(migrated_db, outbox)
    effect = await _reserve(store, scope)
    await store.begin_execution(scope, effect.id, lease_owner="w1")
    assert await outbox.get(effect.id) is not None

    removed = await purge_scope(migrated_db, scope)
    assert removed == 1
    assert await store.get(scope, effect.id) is None
    assert await outbox.get(effect.id) is None  # cascaded via the composite FK
