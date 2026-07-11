"""Memory-block tests (real Postgres): versioning + scope isolation."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.memory import (
    MemoryAppendTool,
    MemoryReplaceTool,
    MemoryRethinkTool,
    PostgresMemoryStore,
)
from keel_core.protocols import ToolContext

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


def _ctx(scope: str) -> ToolContext:
    return ToolContext(scope_id=scope, session_id="s")


async def test_memory_append_creates_and_grows(migrated_db: AsyncEngine) -> None:
    tool = MemoryAppendTool(migrated_db, max_chars=100)
    assert (await tool.run({"block": "human", "content": "name is X"}, _ctx("u:ma"))).ok
    assert (await tool.run({"block": "human", "content": "likes tea"}, _ctx("u:ma"))).ok
    assert await PostgresMemoryStore(migrated_db, "u:ma").get("human") == "name is X\nlikes tea"


async def test_memory_append_over_cap_errors(migrated_db: AsyncEngine) -> None:
    result = await MemoryAppendTool(migrated_db, max_chars=5).run(
        {"block": "human", "content": "way too long"}, _ctx("u:mc")
    )
    assert not result.ok and "exceed" in result.output


async def test_memory_replace_missing_old_errors(migrated_db: AsyncEngine) -> None:
    result = await MemoryReplaceTool(migrated_db).run(
        {"block": "human", "old": "nope", "new": "x"}, _ctx("u:mr")
    )
    assert not result.ok and "not found" in result.output


async def test_memory_rethink_overwrites(migrated_db: AsyncEngine) -> None:
    store = PostgresMemoryStore(migrated_db, "u:mk")
    await store.set("persona", "old value")
    assert (
        await MemoryRethinkTool(migrated_db).run(
            {"block": "persona", "content": "new value"}, _ctx("u:mk")
        )
    ).ok
    assert await store.get("persona") == "new value"
