"""Connector OAuth token store — encrypted, scope-bound (DESIGN-REVIEW G16/G18).

Tokens are envelope-encrypted (:mod:`keel_core.secrets`) and keyed by
``(scope_id, connector_id)``. Each store is bound to one scope: it only ever reads
or writes that scope's rows (application-layer isolation, ADR-0009), with Postgres
RLS as defense-in-depth. On scope deletion, :meth:`purge` revokes every token
(G18: revoke + purge). Plaintext tokens never touch the database.

Each row records the ``key_id`` that encrypted its ciphertext, so a :class:`KeyRing`
can decrypt across a rotation and :meth:`PostgresTokenStore.reencrypt_stale` can
re-wrap rows onto the active key without any downtime (M3.3).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.secrets import EnvelopeCipher, KeyRing
from keel_core.types import ScopeId

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


async def purge_scope(engine: AsyncEngine, scope_id: ScopeId) -> int:
    """Revoke every connector token for a scope without needing the envelope key.

    Mirrors :meth:`PostgresTokenStore.purge` but as a key-free module function the erasure
    coordinator can call (deletion never decrypts). Idempotent; returns rows removed.
    """
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        result = await conn.execute(
            text("DELETE FROM connector_tokens WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
    return int(result.rowcount or 0)


def _as_keyring(cipher: EnvelopeCipher | KeyRing) -> KeyRing:
    """Accept either a legacy single cipher or a versioned ring."""
    return cipher if isinstance(cipher, KeyRing) else KeyRing.from_cipher(cipher)


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

    def __init__(self, scope_id: ScopeId, cipher: EnvelopeCipher | KeyRing) -> None:
        self._scope_id = scope_id
        self._ring = _as_keyring(cipher)
        # (scope, connector) -> (key_id, ciphertext)
        self._rows: dict[tuple[str, str], tuple[str, str]] = {}

    async def put(self, connector_id: str, secret: str) -> None:
        enc = self._ring.encrypt(secret)
        self._rows[(self._scope_id, connector_id)] = (enc.key_id, enc.ciphertext)

    async def get(self, connector_id: str) -> str | None:
        row = self._rows.get((self._scope_id, connector_id))
        if row is None:
            return None
        key_id, ciphertext = row
        return self._ring.decrypt(key_id, ciphertext)

    async def delete(self, connector_id: str) -> None:
        self._rows.pop((self._scope_id, connector_id), None)

    async def purge(self) -> None:
        for key in [k for k in self._rows if k[0] == self._scope_id]:
            del self._rows[key]


class PostgresTokenStore:
    """Durable, scope-bound, encrypted token store over Postgres."""

    def __init__(
        self, engine: AsyncEngine, scope_id: ScopeId, cipher: EnvelopeCipher | KeyRing
    ) -> None:
        self._engine = engine
        self._scope_id = scope_id
        self._ring = _as_keyring(cipher)

    async def put(self, connector_id: str, secret: str) -> None:
        enc = self._ring.encrypt(secret)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "INSERT INTO connector_tokens "
                    "(scope_id, connector_id, ciphertext, key_id) "
                    "VALUES (:scope, :cid, :ct, :kid) "
                    "ON CONFLICT (scope_id, connector_id) DO UPDATE "
                    "SET ciphertext = :ct, key_id = :kid, updated_at = now()"
                ),
                {
                    "scope": self._scope_id,
                    "cid": connector_id,
                    "ct": enc.ciphertext,
                    "kid": enc.key_id,
                },
            )

    async def get(self, connector_id: str) -> str | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "SELECT ciphertext, key_id FROM connector_tokens "
                        "WHERE scope_id = :scope AND connector_id = :cid"
                    ),
                    {"scope": self._scope_id, "cid": connector_id},
                )
            ).one_or_none()
        if row is None:
            return None
        return self._ring.decrypt(row.key_id, row.ciphertext)

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

    async def reencrypt_stale(self) -> int:
        """Re-wrap this scope's tokens not on the active key onto it (M3.3 rotation).

        Reads each stale row, decrypts with its recorded ``key_id``, re-encrypts under
        the ring's active key, and writes back the new ciphertext + key id. Runs one row
        per transaction so a partial rotation is always consistent and resumable. Returns
        the number of rows rotated. Safe to run repeatedly (idempotent once on the active
        key).
        """
        active = self._ring.active_id
        rotated = 0
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            stale = (
                await conn.execute(
                    text(
                        "SELECT connector_id, ciphertext, key_id FROM connector_tokens "
                        "WHERE scope_id = :scope AND key_id <> :active"
                    ),
                    {"scope": self._scope_id, "active": active},
                )
            ).all()
            for row in stale:
                plaintext = self._ring.decrypt(row.key_id, row.ciphertext)
                enc = self._ring.encrypt(plaintext)
                await conn.execute(
                    text(
                        "UPDATE connector_tokens SET ciphertext = :ct, key_id = :kid, "
                        "updated_at = now() WHERE scope_id = :scope AND connector_id = :cid"
                    ),
                    {
                        "ct": enc.ciphertext,
                        "kid": enc.key_id,
                        "scope": self._scope_id,
                        "cid": row.connector_id,
                    },
                )
                rotated += 1
        return rotated
