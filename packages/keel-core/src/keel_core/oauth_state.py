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

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


@dataclass(frozen=True)
class OAuthState:
    """The scope + connector a consumed ``state`` was minted for."""

    scope_id: str
    connector_id: str


def _now() -> datetime:
    return datetime.now(UTC)


class InMemoryOAuthStateStore:
    """Non-durable one-time state store (tests / lite profile)."""

    def __init__(self, ttl_seconds: int = 600) -> None:
        self._ttl = ttl_seconds
        self._rows: dict[str, tuple[OAuthState, datetime]] = {}

    async def put(self, state: str, scope_id: str, connector_id: str) -> None:
        expires = _now() + timedelta(seconds=self._ttl)
        self._rows[state] = (OAuthState(scope_id, connector_id), expires)

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

    async def put(self, state: str, scope_id: str, connector_id: str) -> None:
        expires = _now() + timedelta(seconds=self._ttl)
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO oauth_states (state, scope_id, connector_id, expires_at) "
                    "VALUES (:state, :scope, :cid, :exp) "
                    "ON CONFLICT (state) DO UPDATE "
                    "SET scope_id = :scope, connector_id = :cid, expires_at = :exp"
                ),
                {"state": state, "scope": scope_id, "cid": connector_id, "exp": expires},
            )

    async def consume(self, state: str) -> OAuthState | None:
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "DELETE FROM oauth_states WHERE state = :state "
                        "RETURNING scope_id, connector_id, expires_at"
                    ),
                    {"state": state},
                )
            ).one_or_none()
        if row is None or row.expires_at <= _now():
            return None
        return OAuthState(row.scope_id, row.connector_id)

    async def sweep_expired(self) -> int:
        """Delete expired rows (housekeeping). Returns the number removed."""
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text("DELETE FROM oauth_states WHERE expires_at <= :now"), {"now": _now()}
            )
        return int(result.rowcount or 0)
