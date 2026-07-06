"""Integration: encrypted, scope-bound connector token store over Postgres."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.secrets import EnvelopeCipher
from keel_core.tokens import PostgresTokenStore

pytestmark = pytest.mark.integration


async def test_token_round_trip_and_purge(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    store = PostgresTokenStore(migrated_db, scope, EnvelopeCipher("k"))
    await store.put("google", "refresh-token-123")
    assert await store.get("google") == "refresh-token-123"

    await store.put("google", "rotated-token-456")  # upsert
    assert await store.get("google") == "rotated-token-456"

    await store.purge()  # scope deletion revokes every token (G18)
    assert await store.get("google") is None


async def test_tokens_are_encrypted_at_rest(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    store = PostgresTokenStore(migrated_db, scope, EnvelopeCipher("k"))
    await store.put("google", "super-secret-token")

    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": scope})
        ciphertext = (
            await conn.execute(
                text("SELECT ciphertext FROM connector_tokens WHERE scope_id = :s"), {"s": scope}
            )
        ).scalar_one()
    assert "super-secret-token" not in ciphertext  # plaintext never hits the DB


async def test_token_store_is_scope_isolated(migrated_db: AsyncEngine) -> None:
    cipher = EnvelopeCipher("k")
    personal = f"u:{uuid.uuid4().hex}"
    group = f"g:{uuid.uuid4().hex}"
    await PostgresTokenStore(migrated_db, personal, cipher).put("google", "personal-secret")

    # A store bound to the group scope cannot read the personal scope's token.
    group_store = PostgresTokenStore(migrated_db, group, cipher)
    assert await group_store.get("google") is None
