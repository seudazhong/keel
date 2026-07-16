"""Integration: M3.3 cloud-safety durable stores + runtime role RLS (Postgres)."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.oauth_state import PostgresOAuthStateStore
from keel_core.outbox import PostgresOutboundStore
from keel_core.secrets import EnvelopeCipher, KeyRing
from keel_core.tokens import PostgresTokenStore
from keel_core.webhooks import PostgresWebhookReplayStore

pytestmark = pytest.mark.integration


async def test_oauth_state_is_durable_and_one_time(migrated_db: AsyncEngine) -> None:
    store = PostgresOAuthStateStore(migrated_db, ttl_seconds=600)
    state = f"st-{uuid.uuid4().hex}"
    await store.put(state, "web:local", "gmail")
    consumed = await store.consume(state)
    assert consumed is not None and consumed.scope_id == "web:local"
    assert consumed.connector_id == "gmail"
    # One-time: a replayed callback finds nothing.
    assert await store.consume(state) is None


async def test_oauth_state_rejects_expired(migrated_db: AsyncEngine) -> None:
    store = PostgresOAuthStateStore(migrated_db, ttl_seconds=0)
    state = f"st-{uuid.uuid4().hex}"
    await store.put(state, "web:local", "gmail")
    assert await store.consume(state) is None  # already expired


async def test_webhook_replay_store_dedups(migrated_db: AsyncEngine) -> None:
    store = PostgresWebhookReplayStore(migrated_db, ttl_seconds=3600)
    did = f"d-{uuid.uuid4().hex}"
    assert await store.seen_before("telegram", did) is False  # first sighting
    assert await store.seen_before("telegram", did) is True  # replay dropped
    assert await store.seen_before("onebot", did) is False  # provider-namespaced


async def test_outbound_idempotency_is_durable_and_scope_isolated(
    migrated_db: AsyncEngine,
) -> None:
    scope_a = f"u:{uuid.uuid4().hex}"
    scope_b = f"u:{uuid.uuid4().hex}"
    store = PostgresOutboundStore(migrated_db)

    claim = await store.claim(scope_a, "email_send", "k1")
    assert claim.owner is True
    # A second claim (e.g. another worker / after restart) before finalize is a duplicate.
    assert (await store.claim(scope_a, "email_send", "k1")).owner is False
    await store.finalize(scope_a, "email_send", "k1", "sent id=1")
    replay = await store.claim(scope_a, "email_send", "k1")
    assert replay.owner is False and replay.result == "sent id=1"

    # A different scope reusing the same key is independent (scope-isolated claim).
    other = await store.claim(scope_b, "email_send", "k1")
    assert other.owner is True


async def test_outbound_claim_release_allows_retry(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    store = PostgresOutboundStore(migrated_db)
    assert (await store.claim(scope, "email_send", "k")).owner is True
    await store.release(scope, "email_send", "k")  # action failed -> drop pending claim
    assert (await store.claim(scope, "email_send", "k")).owner is True  # retry can claim


async def test_token_key_rotation_reencrypts_stale_rows(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    # Write under key id v1.
    old_ring = KeyRing({"v1": "old-secret"}, active_id="v1")
    await PostgresTokenStore(migrated_db, scope, old_ring).put("gmail", "refresh-token-abc")

    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": scope})
        key_id = (
            await conn.execute(
                text("SELECT key_id FROM connector_tokens WHERE scope_id = :s"), {"s": scope}
            )
        ).scalar_one()
    assert key_id == "v1"

    # Rotate: v2 becomes active, v1 kept for decrypt.
    new_ring = KeyRing({"v1": "old-secret", "v2": "new-secret"}, active_id="v2")
    store = PostgresTokenStore(migrated_db, scope, new_ring)
    # The token still decrypts before rotation (versioned decrypt via recorded key_id).
    assert await store.get("gmail") == "refresh-token-abc"

    rotated = await store.reencrypt_stale()
    assert rotated == 1

    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": scope})
        key_id = (
            await conn.execute(
                text("SELECT key_id FROM connector_tokens WHERE scope_id = :s"), {"s": scope}
            )
        ).scalar_one()
    assert key_id == "v2"  # re-wrapped onto the active key
    assert await store.get("gmail") == "refresh-token-abc"  # still decrypts after rotation

    # A ring without v1 can only decrypt because rotation moved the row to v2.
    v2_only = PostgresTokenStore(migrated_db, scope, KeyRing({"v2": "new-secret"}, active_id="v2"))
    assert await v2_only.get("gmail") == "refresh-token-abc"


async def test_legacy_cipher_rows_are_readable_and_rotatable(migrated_db: AsyncEngine) -> None:
    """A row written by the pre-M3.3 single-cipher path (key_id v1) still decrypts."""
    scope = f"u:{uuid.uuid4().hex}"
    await PostgresTokenStore(migrated_db, scope, EnvelopeCipher("legacy")).put("gmail", "tok")
    ring = KeyRing({"v1": "legacy", "v2": "new"}, active_id="v2")
    store = PostgresTokenStore(migrated_db, scope, ring)
    assert await store.get("gmail") == "tok"
    assert await store.reencrypt_stale() == 1
    assert await store.get("gmail") == "tok"


async def test_runtime_role_cannot_bypass_scope_rls(migrated_db: AsyncEngine) -> None:
    """Adversarial: the non-owner ``keel_runtime`` role is bound by scope RLS + FORCE.

    It sees only its own scope's ``connector_outbox`` rows, nothing with no scope set,
    and cannot read another scope even though it holds table grants.
    """
    scope_a = f"A:{uuid.uuid4().hex}"
    scope_b = f"B:{uuid.uuid4().hex}"
    store = PostgresOutboundStore(migrated_db)
    await store.claim(scope_a, "email_send", "k-a")
    await store.claim(scope_b, "email_send", "k-b")

    async with migrated_db.connect() as conn:
        has_role = await conn.scalar(text("SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime'"))
        if not has_role:
            pytest.skip("keel_runtime role not present (managed Postgres restricts CREATE ROLE)")

        await conn.execute(text("SET ROLE keel_runtime"))
        try:
            await conn.execute(text("SELECT set_config('app.scope_id', :s, false)"), {"s": scope_a})
            rows = (
                await conn.execute(
                    text("SELECT scope_id FROM connector_outbox WHERE scope_id IN (:a, :b)"),
                    {"a": scope_a, "b": scope_b},
                )
            ).fetchall()
            assert {r[0] for r in rows} == {scope_a}  # scope B invisible under RLS

            await conn.execute(text("SELECT set_config('app.scope_id', '', false)"))
            none_rows = (
                await conn.execute(
                    text("SELECT scope_id FROM connector_outbox WHERE scope_id IN (:a, :b)"),
                    {"a": scope_a, "b": scope_b},
                )
            ).fetchall()
            assert none_rows == []  # deny-by-default with no scope set
        finally:
            await conn.execute(text("RESET ROLE"))
