"""Authenticated, replay-safe IM webhook verification (WS-E/J, M3.3).

IM webhooks are an **unauthenticated public surface** unless proven otherwise: without a
signature check anyone who finds the URL can inject events, and without replay protection
a captured-and-resent request re-runs the agent. This module provides:

* constant-time signature/secret verification for OneBot (HMAC-SHA1 over the raw body)
  and Telegram (a shared secret header), and
* a durable :class:`WebhookReplayStore` that records each delivery id once (unique
  constraint) so a replayed delivery is recognised and dropped across restarts/replicas.

Verification runs **before** dispatch; the gateway rejects an invalid signature and
silently drops a duplicate.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


def _now() -> datetime:
    return datetime.now(UTC)


def verify_onebot_signature(secret: str, body: bytes, signature: str | None) -> bool:
    """Verify a OneBot ``X-Signature: sha1=<hex>`` HMAC over the raw request body.

    The digest is ``HMAC-SHA1(secret, body)`` per the OneBot v11 spec. Comparison is
    constant-time. Returns False for a missing/malformed header or a wrong digest.
    """
    if not secret or not signature:
        return False
    prefix, _, provided = signature.partition("=")
    if prefix != "sha1" or not provided:
        return False
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha1).hexdigest()
    return hmac.compare_digest(expected, provided.strip())


def verify_telegram_secret(secret: str, header_value: str | None) -> bool:
    """Constant-time compare Telegram's ``X-Telegram-Bot-Api-Secret-Token`` header."""
    if not secret or not header_value:
        return False
    return hmac.compare_digest(secret, header_value)


def onebot_delivery_id(body: bytes) -> str:
    """A stable dedup id for a OneBot delivery (SHA-256 of the raw body).

    OneBot events have no guaranteed unique id, so the body digest identifies an exact
    replay; distinct events hash differently.
    """
    return hashlib.sha256(body).hexdigest()


def telegram_delivery_id(payload: dict[str, Any]) -> str | None:
    """Telegram's monotonic ``update_id`` as the dedup id (None if absent)."""
    update_id = payload.get("update_id")
    return None if update_id is None else str(update_id)


@runtime_checkable
class WebhookReplayStore(Protocol):
    """Records a delivery id once; ``seen_before`` claims it and reports prior sightings."""

    async def seen_before(self, provider: str, delivery_id: str) -> bool: ...


class InMemoryWebhookReplayStore:
    """Non-durable replay store (tests / lite profile)."""

    def __init__(self, ttl_seconds: int = 86_400) -> None:
        self._ttl = ttl_seconds
        self._rows: dict[tuple[str, str], datetime] = {}

    async def seen_before(self, provider: str, delivery_id: str) -> bool:
        now = _now()
        # Opportunistic sweep so the map cannot grow unbounded.
        for key in [k for k, exp in self._rows.items() if exp <= now]:
            del self._rows[key]
        rk = (provider, delivery_id)
        if rk in self._rows:
            return True
        self._rows[rk] = now + timedelta(seconds=self._ttl)
        return False


class PostgresWebhookReplayStore:
    """Durable replay store over Postgres (``webhook_deliveries``).

    The primary key ``(provider, delivery_id)`` plus ``INSERT ... ON CONFLICT DO NOTHING``
    makes the first sighting atomic and correct across replicas: the caller that inserts
    the row is the first to see the delivery, everyone else gets ``True`` (replay).
    """

    def __init__(self, engine: AsyncEngine, ttl_seconds: int = 86_400) -> None:
        self._engine = engine
        self._ttl = ttl_seconds

    async def seen_before(self, provider: str, delivery_id: str) -> bool:
        expires = _now() + timedelta(seconds=self._ttl)
        async with self._engine.begin() as conn:
            inserted = (
                await conn.execute(
                    text(
                        "INSERT INTO webhook_deliveries (provider, delivery_id, expires_at) "
                        "VALUES (:provider, :did, :exp) "
                        "ON CONFLICT (provider, delivery_id) DO NOTHING "
                        "RETURNING delivery_id"
                    ),
                    {"provider": provider, "did": delivery_id, "exp": expires},
                )
            ).one_or_none()
        return inserted is None

    async def sweep_expired(self) -> int:
        """Delete expired delivery records (housekeeping). Returns the number removed."""
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text("DELETE FROM webhook_deliveries WHERE expires_at <= :now"), {"now": _now()}
            )
        return int(result.rowcount or 0)
