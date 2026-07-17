"""Durable outbound-connector idempotency (G20, M3.3).

An outbound connector action (send email / post message) must fire **at most once**
even across a process restart or a second worker — an in-process cache is lost on
restart and invisible to other workers, so a retried run would re-send. The store here
records a claim keyed by ``(scope_id, connector_id, idempotency_key)`` with a unique
constraint: the first caller *claims* the key and performs the side effect, then
*finalizes* the recorded result; any later caller (this process or another) replays the
finalized result instead of re-sending. A claim whose action failed is *released* so a
later retry may claim it again.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


async def purge_scope(engine: AsyncEngine, scope_id: str) -> int:
    """Erase every outbound-idempotency claim for a scope (idempotent)."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        result = await conn.execute(
            text("DELETE FROM connector_outbox WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
    return int(result.rowcount or 0)


@dataclass(frozen=True)
class Claim:
    """The outcome of attempting to claim an idempotency key.

    ``owner`` is True when this caller won the claim and must perform the side effect,
    then call :meth:`finalize`. When False the key was already claimed: ``result`` holds
    the finalized output to replay, or is ``None`` when a concurrent claim is still
    in-flight (the caller must not re-send).
    """

    owner: bool
    result: str | None = None


@runtime_checkable
class OutboundIdempotencyStore(Protocol):
    """At-most-once claim/finalize for outbound connector actions."""

    async def claim(self, scope_id: str, connector_id: str, key: str) -> Claim: ...

    async def finalize(self, scope_id: str, connector_id: str, key: str, result: str) -> None: ...

    async def release(self, scope_id: str, connector_id: str, key: str) -> None: ...


class InMemoryOutboundStore:
    """Non-durable claim store (tests / single-process lite profile)."""

    def __init__(self) -> None:
        # (scope, connector, key) -> result | None  (None = claimed but not finalized)
        self._rows: dict[tuple[str, str, str], str | None] = {}

    async def claim(self, scope_id: str, connector_id: str, key: str) -> Claim:
        rk = (scope_id, connector_id, key)
        if rk in self._rows:
            return Claim(owner=False, result=self._rows[rk])
        self._rows[rk] = None
        return Claim(owner=True)

    async def finalize(self, scope_id: str, connector_id: str, key: str, result: str) -> None:
        self._rows[(scope_id, connector_id, key)] = result

    async def release(self, scope_id: str, connector_id: str, key: str) -> None:
        self._rows.pop((scope_id, connector_id, key), None)


class PostgresOutboundStore:
    """Durable, scope-bound claim store over Postgres (``connector_outbox``).

    The primary key ``(scope_id, connector_id, idempotency_key)`` plus
    ``INSERT ... ON CONFLICT DO NOTHING`` gives an atomic claim that is correct across
    workers and restarts. Row-Level Security (``app.scope_id``) keeps a scope's claims
    private, mirroring ``connector_tokens``.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def claim(self, scope_id: str, connector_id: str, key: str) -> Claim:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            inserted = (
                await conn.execute(
                    text(
                        "INSERT INTO connector_outbox "
                        "(scope_id, connector_id, idempotency_key, status) "
                        "VALUES (:scope, :cid, :key, 'pending') "
                        "ON CONFLICT (scope_id, connector_id, idempotency_key) DO NOTHING "
                        "RETURNING idempotency_key"
                    ),
                    {"scope": scope_id, "cid": connector_id, "key": key},
                )
            ).one_or_none()
            if inserted is not None:
                return Claim(owner=True)
            row = (
                await conn.execute(
                    text(
                        "SELECT status, result FROM connector_outbox "
                        "WHERE scope_id = :scope AND connector_id = :cid "
                        "AND idempotency_key = :key"
                    ),
                    {"scope": scope_id, "cid": connector_id, "key": key},
                )
            ).one_or_none()
        result = row.result if row is not None and row.status == "done" else None
        return Claim(owner=False, result=result)

    async def finalize(self, scope_id: str, connector_id: str, key: str, result: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            await conn.execute(
                text(
                    "UPDATE connector_outbox SET status = 'done', result = :result, "
                    "updated_at = now() WHERE scope_id = :scope AND connector_id = :cid "
                    "AND idempotency_key = :key"
                ),
                {"result": result, "scope": scope_id, "cid": connector_id, "key": key},
            )

    async def release(self, scope_id: str, connector_id: str, key: str) -> None:
        """Drop a still-pending claim so a later retry can re-claim (action failed)."""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            await conn.execute(
                text(
                    "DELETE FROM connector_outbox WHERE scope_id = :scope AND connector_id = :cid "
                    "AND idempotency_key = :key AND status = 'pending'"
                ),
                {"scope": scope_id, "cid": connector_id, "key": key},
            )
