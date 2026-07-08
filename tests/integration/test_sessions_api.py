"""Integration: session listing + the /v1/sessions endpoints."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.loop import admit
from keel_core.state import PostgresEventStore, list_sessions
from keel_server.api.v1 import router

pytestmark = pytest.mark.integration


async def test_list_sessions(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    sid = f"s:{uuid.uuid4().hex}"
    store = PostgresEventStore(migrated_db, scope)
    await admit(store, sid, scope, "hello from the test")

    summaries = await list_sessions(migrated_db, scope)
    assert len(summaries) == 1
    s = summaries[0]
    assert s.id == sid
    assert s.messages >= 1
    assert s.updated_at is not None
    assert s.title is not None and "hello from the test" in s.title

    assert await list_sessions(migrated_db, f"u:{uuid.uuid4().hex}") == []


@pytest_asyncio.fixture
async def sessions_client(
    migrated_db: AsyncEngine,
) -> AsyncIterator[tuple[httpx.AsyncClient, str, AsyncEngine]]:
    scope = f"u:{uuid.uuid4().hex}"
    app = FastAPI()
    app.include_router(router)
    app.state.engine = migrated_db
    app.state.durable_scope = scope
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, scope, migrated_db


async def test_sessions_endpoints(
    sessions_client: tuple[httpx.AsyncClient, str, AsyncEngine],
) -> None:
    client, scope, engine = sessions_client
    sid = f"s:{uuid.uuid4().hex}"
    await admit(PostgresEventStore(engine, scope), sid, scope, "hi there")

    listed = (await client.get("/v1/sessions")).json()
    assert any(row["id"] == sid for row in listed)

    history = (await client.get(f"/v1/sessions/{sid}/history")).json()
    assert any(e["type"] == "message.token" for e in history)
