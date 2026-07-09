"""Connector OAuth token store — encrypted, scope-bound (DESIGN-REVIEW G16/G18).

Tokens are envelope-encrypted (:mod:`keel_core.secrets`) and keyed by
``(scope_id, connector_id)``. Each store is bound to one scope: it only ever reads
or writes that scope's rows (application-layer isolation, ADR-0009), with Postgres
RLS as defense-in-depth. On scope deletion, :meth:`purge` revokes every token
(G18: revoke + purge). Plaintext tokens never touch the database.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.secrets import EnvelopeCipher
from keel_core.types import ScopeId

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


@dataclass(frozen=True)
class ConnectorTokenInfo:
    """A connector that has a stored token for a scope (status, not the secret)."""

    connector_id: str
    updated_at: datetime | None


async def list_connected(engine: AsyncEngine, scope_id: ScopeId) -> list[ConnectorTokenInfo]:
    """Connectors with a stored token for ``scope_id`` (no decryption — status only)."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        rows = (
            await conn.execute(
                text(
                    "SELECT connector_id, updated_at FROM connector_tokens "
                    "WHERE scope_id = :scope ORDER BY connector_id"
                ),
                {"scope": scope_id},
            )
        ).all()
    return [ConnectorTokenInfo(connector_id=r.connector_id, updated_at=r.updated_at) for r in rows]


async def delete_token(engine: AsyncEngine, scope_id: ScopeId, connector_id: str) -> bool:
    """Revoke (delete) a connector's stored token for a scope. Returns True if a row went."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        result = await conn.execute(
            text("DELETE FROM connector_tokens WHERE scope_id = :scope AND connector_id = :cid"),
            {"scope": scope_id, "cid": connector_id},
        )
    return bool(result.rowcount)


class InMemoryTokenStore:
    """Non-durable, scope-bound encrypted token store (tests / lite profile)."""

    def __init__(self, scope_id: ScopeId, cipher: EnvelopeCipher) -> None:
        self._scope_id = scope_id
        self._cipher = cipher
        self._rows: dict[tuple[str, str], str] = {}  # (scope, connector) -> ciphertext

    async def put(self, connector_id: str, secret: str) -> None:
        self._rows[(self._scope_id, connector_id)] = self._cipher.encrypt(secret)

    async def get(self, connector_id: str) -> str | None:
        ciphertext = self._rows.get((self._scope_id, connector_id))
        return None if ciphertext is None else self._cipher.decrypt(ciphertext)

    async def delete(self, connector_id: str) -> None:
        self._rows.pop((self._scope_id, connector_id), None)

    async def purge(self) -> None:
        for key in [k for k in self._rows if k[0] == self._scope_id]:
            del self._rows[key]


class PostgresTokenStore:
    """Durable, scope-bound, encrypted token store over Postgres."""

    def __init__(self, engine: AsyncEngine, scope_id: ScopeId, cipher: EnvelopeCipher) -> None:
        self._engine = engine
        self._scope_id = scope_id
        self._cipher = cipher

    async def put(self, connector_id: str, secret: str) -> None:
        ciphertext = self._cipher.encrypt(secret)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "INSERT INTO connector_tokens (scope_id, connector_id, ciphertext) "
                    "VALUES (:scope, :cid, :ct) "
                    "ON CONFLICT (scope_id, connector_id) DO UPDATE "
                    "SET ciphertext = :ct, updated_at = now()"
                ),
                {"scope": self._scope_id, "cid": connector_id, "ct": ciphertext},
            )

    async def get(self, connector_id: str) -> str | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "SELECT ciphertext FROM connector_tokens "
                        "WHERE scope_id = :scope AND connector_id = :cid"
                    ),
                    {"scope": self._scope_id, "cid": connector_id},
                )
            ).one_or_none()
        if row is None:
            return None
        return self._cipher.decrypt(row.ciphertext)

    async def delete(self, connector_id: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "DELETE FROM connector_tokens WHERE scope_id = :scope AND connector_id = :cid"
                ),
                {"scope": self._scope_id, "cid": connector_id},
            )

    async def purge(self) -> None:
        """Revoke every token for this scope (G18: revoke + purge on scope deletion)."""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text("DELETE FROM connector_tokens WHERE scope_id = :scope"),
                {"scope": self._scope_id},
            )
