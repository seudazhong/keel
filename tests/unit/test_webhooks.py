"""Webhook verification + durable replay protection unit tests (M3.3)."""

from __future__ import annotations

import hashlib
import hmac

from keel_core.webhooks import (
    InMemoryWebhookReplayStore,
    onebot_delivery_id,
    telegram_delivery_id,
    verify_onebot_signature,
    verify_telegram_secret,
)


def _sign(secret: str, body: bytes) -> str:
    return "sha1=" + hmac.new(secret.encode(), body, hashlib.sha1).hexdigest()


def test_onebot_signature_accepts_valid_and_rejects_tampered() -> None:
    body = b'{"post_type":"message"}'
    good = _sign("s3cret", body)
    assert verify_onebot_signature("s3cret", body, good) is True
    # Wrong secret, tampered body, and malformed headers all fail.
    assert verify_onebot_signature("other", body, good) is False
    assert verify_onebot_signature("s3cret", body + b"x", good) is False
    assert verify_onebot_signature("s3cret", body, "deadbeef") is False
    assert verify_onebot_signature("s3cret", body, None) is False
    assert verify_onebot_signature("", body, good) is False


def test_telegram_secret_constant_time_compare() -> None:
    assert verify_telegram_secret("tok", "tok") is True
    assert verify_telegram_secret("tok", "nope") is False
    assert verify_telegram_secret("tok", None) is False
    assert verify_telegram_secret("", "tok") is False


def test_delivery_ids() -> None:
    assert onebot_delivery_id(b"a") != onebot_delivery_id(b"b")
    assert onebot_delivery_id(b"a") == onebot_delivery_id(b"a")
    assert telegram_delivery_id({"update_id": 42}) == "42"
    assert telegram_delivery_id({}) is None


async def test_in_memory_replay_store_drops_second_sighting() -> None:
    store = InMemoryWebhookReplayStore()
    assert await store.seen_before("onebot", "d1") is False  # first time
    assert await store.seen_before("onebot", "d1") is True  # replay
    assert await store.seen_before("telegram", "d1") is False  # namespaced by provider


async def test_in_memory_replay_store_expires() -> None:
    store = InMemoryWebhookReplayStore(ttl_seconds=0)
    assert await store.seen_before("onebot", "d1") is False
    # ttl=0 means the record is already expired on the next check -> not a replay.
    assert await store.seen_before("onebot", "d1") is False
