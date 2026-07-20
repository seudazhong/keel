"""Durable, expiring, one-time OAuth CSRF ``state`` store (WS-G / M3.3).

The in-browser connect flow mints a random ``state`` before redirecting to the provider
and must recognise exactly that value on the callback (CSRF defence). An in-memory set
is lost on restart and invisible to a second server replica, so a legitimate callback
after a redeploy — or against another worker — would be rejected. This store persists
each ``state`` with its scope + connector and an expiry, and :meth:`consume` deletes the
row as it returns it, so a ``state`` is accepted **once** and only within its TTL.

The random ``state`` is itself the capability, so the table is not scope-partitioned
(the callback has no scope context until the row is looked up).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


@dataclass(frozen=True)
class OAuthState:
    """The scope + connector a consumed ``state`` was minted for."""

    scope_id: str
    connector_id: str
    metadata: dict[str, str] = field(default_factory=dict, repr=False)


def _now() -> datetime:
    return datetime.now(UTC)


async def purge_scope(engine: AsyncEngine, scope_id: str) -> int:
    """Erase every pending OAuth CSRF state minted for a scope (idempotent).

    ``oauth_states`` is keyed by the random state (not scope-partitioned), but each row
    records the ``scope_id`` it was minted for, so scope erasure can drop them by scope.
    """
    async with engine.begin() as conn:
        result = await conn.execute(
            text("DELETE FROM oauth_states WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
    return int(result.rowcount or 0)


class InMemoryOAuthStateStore:
    """Non-durable one-time state store (tests / lite profile)."""

    def __init__(self, ttl_seconds: int = 600) -> None:
        self._ttl = ttl_seconds
        self._rows: dict[str, tuple[OAuthState, datetime]] = {}

    async def put(
        self,
        state: str,
        scope_id: str,
        connector_id: str,
        metadata: dict[str, str] | None = None,
    ) -> None:
        expires = _now() + timedelta(seconds=self._ttl)
        self._rows[state] = (OAuthState(scope_id, connector_id, metadata or {}), expires)

    async def consume(self, state: str) -> OAuthState | None:
        row = self._rows.pop(state, None)  # one-time: remove on read
        if row is None:
            return None
        value, expires = row
        if expires <= _now():
            return None
        return value


class PostgresOAuthStateStore:
    """Durable one-time state store over Postgres (``oauth_states``).

    :meth:`consume` uses ``DELETE ... RETURNING`` so recognising a ``state`` and
    invalidating it are one atomic step — a replayed callback finds nothing.
    """

    def __init__(self, engine: AsyncEngine, ttl_seconds: int = 600) -> None:
        self._engine = engine
        self._ttl = ttl_seconds

    async def put(
        self,
        state: str,
        scope_id: str,
        connector_id: str,
        metadata: dict[str, str] | None = None,
    ) -> None:
        expires = _now() + timedelta(seconds=self._ttl)
        encoded_metadata = json.dumps(metadata or {}, separators=(",", ":"), sort_keys=True)
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO oauth_states "
                    "(state, scope_id, connector_id, metadata, expires_at) "
                    "VALUES (:state, :scope, :cid, CAST(:metadata AS jsonb), :exp) "
                    "ON CONFLICT (state) DO UPDATE "
                    "SET scope_id = :scope, connector_id = :cid, "
                    "metadata = CAST(:metadata AS jsonb), expires_at = :exp"
                ),
                {
                    "state": state,
                    "scope": scope_id,
                    "cid": connector_id,
                    "metadata": encoded_metadata,
                    "exp": expires,
                },
            )

    async def consume(self, state: str) -> OAuthState | None:
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "DELETE FROM oauth_states WHERE state = :state "
                        "RETURNING scope_id, connector_id, metadata, expires_at"
                    ),
                    {"state": state},
                )
            ).one_or_none()
        if row is None or row.expires_at <= _now():
            return None
        raw_metadata = row.metadata
        if isinstance(raw_metadata, str):
            decoded = json.loads(raw_metadata)
            metadata = decoded if isinstance(decoded, dict) else {}
        else:
            metadata = raw_metadata if isinstance(raw_metadata, dict) else {}
        return OAuthState(
            str(row.scope_id),
            str(row.connector_id),
            {str(key): str(value) for key, value in metadata.items()},
        )

    async def sweep_expired(self) -> int:
        """Delete expired rows (housekeeping). Returns the number removed."""
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text("DELETE FROM oauth_states WHERE expires_at <= :now"), {"now": _now()}
            )
        return int(result.rowcount or 0)
