"""Fixtures for integration tests (live Postgres / Redis).

Each fixture skips its tests when the service is unreachable, so
``uv run pytest`` stays green on a bare checkout. CI provides the services and
runs these via ``-m integration``.
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_DEFAULT_PG = "postgresql+psycopg://keel:keel@localhost:5432/keel"
_DEFAULT_REDIS = "redis://localhost:6379/0"
_REPO_ROOT = Path(__file__).resolve().parents[2]


# psycopg's async mode does not support Windows' ProactorEventLoop. On Windows,
# force a SelectorEventLoop; Linux/CI already default to a selector loop, so the
# fixture is overridden only there (avoids pytest-asyncio's deprecation warning).
if sys.platform == "win32":

    @pytest.fixture
    def event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
        return asyncio.WindowsSelectorEventLoopPolicy()


@pytest_asyncio.fixture
async def pg_engine() -> AsyncIterator[AsyncEngine]:
    url = os.environ.get("KEEL_TEST_DATABASE_URL", _DEFAULT_PG)
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:
        await engine.dispose()
        pytest.skip("Postgres not available")
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[aioredis.Redis]:
    url = os.environ.get("KEEL_TEST_REDIS_URL", _DEFAULT_REDIS)
    client: aioredis.Redis = aioredis.Redis.from_url(url, decode_responses=True)
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        pytest.skip("Redis not available")
    yield client
    await client.aclose()


def _upgrade_head(url: str) -> None:
    """Run Alembic migrations to head against ``url`` (sync; call in a thread).

    Alembic uses a synchronous engine, so strip any async driver suffix
    (``+asyncpg``) from the URL before handing it to the config.
    """
    from alembic import command
    from alembic.config import Config

    sync_url = url.replace("+asyncpg", "")
    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", sync_url)
    command.upgrade(cfg, "head")


@pytest_asyncio.fixture
async def migrated_db(pg_engine: AsyncEngine) -> AsyncIterator[AsyncEngine]:
    """A Postgres engine with the schema migrated to head and a clean slate."""
    url = os.environ.get("KEEL_TEST_DATABASE_URL", _DEFAULT_PG)
    await asyncio.to_thread(_upgrade_head, url)
    async with pg_engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE message_embeddings, events, sessions, memory_blocks, "
                "memory_block_versions, connector_tokens, archival, schedules, approvals"
            )
        )
    yield pg_engine
