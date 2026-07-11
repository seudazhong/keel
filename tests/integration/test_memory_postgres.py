"""Memory-block tests (real Postgres): versioning + scope isolation."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.memory import PostgresMemoryStore

pytestmark = pytest.mark.integration


async def test_memory_block_versioning(migrated_db: AsyncEngine) -> None:
    store = PostgresMemoryStore(migrated_db, f"u-{uuid.uuid4().hex}")

    assert await store.get("persona") is None
    assert await store.set("persona", "helpful") == 1
    assert await store.get("persona") == "helpful"
    assert await store.set("persona", "terse") == 2
    assert await store.get("persona") == "terse"

    assert await store.history("persona") == [(1, "helpful"), (2, "terse")]


async def test_memory_scope_isolation(migrated_db: AsyncEngine) -> None:
    a = PostgresMemoryStore(migrated_db, "A")
    b = PostgresMemoryStore(migrated_db, "B")

    await a.set("shared", "a-value")
    await b.set("shared", "b-value")

    assert await a.get("shared") == "a-value"
    assert await b.get("shared") == "b-value"  # same key, different scopes -> independent


async def test_blocks_returns_all_scope_blocks(migrated_db: AsyncEngine) -> None:
    store = PostgresMemoryStore(migrated_db, "u:blk1")
    await store.set("persona", "concise")
    await store.set("human", "name X")
    assert await store.blocks() == {"persona": "concise", "human": "name X"}
    assert await PostgresMemoryStore(migrated_db, "u:blk2").blocks() == {}  # scope-isolated
