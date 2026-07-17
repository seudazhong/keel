"""Durable OAuth state + outbound idempotency (in-memory) unit tests (M3.3)."""

from __future__ import annotations

from keel_core.oauth_state import InMemoryOAuthStateStore
from keel_core.outbox import InMemoryOutboundStore


async def test_oauth_state_is_one_time() -> None:
    store = InMemoryOAuthStateStore(ttl_seconds=600)
    await store.put("state-abc", "web:local", "gmail")
    first = await store.consume("state-abc")
    assert first is not None and first.scope_id == "web:local" and first.connector_id == "gmail"
    # Consuming again returns nothing (single-use) — a replayed callback is rejected.
    assert await store.consume("state-abc") is None


async def test_oauth_state_rejects_unknown_and_expired() -> None:
    assert await InMemoryOAuthStateStore().consume("never-issued") is None
    expired = InMemoryOAuthStateStore(ttl_seconds=0)
    await expired.put("s", "web:local", "gmail")
    assert await expired.consume("s") is None  # already expired


async def test_outbound_store_claim_finalize_replay() -> None:
    store = InMemoryOutboundStore()
    claim = await store.claim("web:local", "email_send", "k1")
    assert claim.owner is True and claim.result is None
    # A second claim before finalize is a concurrent duplicate (not owner, no result).
    dup = await store.claim("web:local", "email_send", "k1")
    assert dup.owner is False and dup.result is None

    await store.finalize("web:local", "email_send", "k1", "sent id=1")
    replay = await store.claim("web:local", "email_send", "k1")
    assert replay.owner is False and replay.result == "sent id=1"


async def test_outbound_store_release_allows_reclaim() -> None:
    store = InMemoryOutboundStore()
    assert (await store.claim("s", "c", "k")).owner is True
    await store.release("s", "c", "k")  # action failed -> drop the claim
    assert (await store.claim("s", "c", "k")).owner is True  # can retry
