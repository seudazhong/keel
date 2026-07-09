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


async def test_search_sessions(migrated_db: AsyncEngine) -> None:
    from keel_core.search import search_sessions

    scope = f"u:{uuid.uuid4().hex}"
    s1, s2 = f"s:{uuid.uuid4().hex}", f"s:{uuid.uuid4().hex}"
    await admit(
        PostgresEventStore(migrated_db, scope), s1, scope, "how do I organize invoices and expenses"
    )
    await admit(
        PostgresEventStore(migrated_db, scope), s2, scope, "schedule a meeting tomorrow afternoon"
    )

    hits = await search_sessions(migrated_db, scope, "invoices")
    assert hits and hits[0].id == s1
    assert "invoice" in hits[0].snippet.lower()
    assert all(h.id != s2 for h in hits)  # unrelated session not matched
    assert await search_sessions(migrated_db, scope, "") == []


async def test_search_endpoint(sessions_client: tuple[httpx.AsyncClient, str, AsyncEngine]) -> None:
    client, scope, engine = sessions_client
    sid = f"s:{uuid.uuid4().hex}"
    await admit(PostgresEventStore(engine, scope), sid, scope, "quarterly OKR review notes")
    rows = (await client.get("/v1/sessions/search", params={"q": "OKR"})).json()
    assert any(r["id"] == sid for r in rows)
    assert (await client.get("/v1/sessions/search", params={"q": ""})).json() == []
