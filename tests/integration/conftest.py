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

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_DEFAULT_PG = "postgresql+psycopg://keel:keel@localhost:5432/keel"
_DEFAULT_REDIS = "redis://localhost:6379/0"


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
