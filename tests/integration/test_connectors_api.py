"""Integration: connector status listing + the /v1/connectors endpoint."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.secrets import EnvelopeCipher
from keel_core.tokens import PostgresTokenStore, list_connected
from keel_server.api.v1 import router

pytestmark = pytest.mark.integration


async def test_list_connected(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    cipher = EnvelopeCipher("k")
    await PostgresTokenStore(migrated_db, scope, cipher).put("gmail", "t1")
    await PostgresTokenStore(migrated_db, scope, cipher).put("calendar", "t2")

    infos = await list_connected(migrated_db, scope)
    assert [i.connector_id for i in infos] == ["calendar", "gmail"]  # ordered
    assert all(i.updated_at is not None for i in infos)

    assert await list_connected(migrated_db, f"u:{uuid.uuid4().hex}") == []  # scope-isolated


@pytest_asyncio.fixture
async def connectors_client() -> AsyncIterator[httpx.AsyncClient]:
    app = FastAPI()
    app.include_router(router)
    app.state.engine = None  # no durable store -> catalog with connected=False
    app.state.durable_scope = "web:local"
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_connectors_endpoint_lists_catalog(connectors_client: httpx.AsyncClient) -> None:
    rows = (await connectors_client.get("/v1/connectors")).json()
    gmail = next(r for r in rows if r["id"] == "gmail")
    assert gmail["connected"] is False
    assert gmail["scopes"] == ["gmail.readonly", "gmail.send"]
    assert gmail["name"] == "Gmail"


async def test_revoke_connector_endpoint(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    await PostgresTokenStore(migrated_db, scope, EnvelopeCipher("k")).put("gmail", "t")
    assert [i.connector_id for i in await list_connected(migrated_db, scope)] == ["gmail"]

    app = FastAPI()
    app.include_router(router)
    app.state.engine = migrated_db
    app.state.durable_scope = scope
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        assert (await client.delete("/v1/connectors/gmail")).json() == {"ok": True}
        assert (await client.delete("/v1/connectors/gmail")).json() == {"ok": False}  # already gone

    assert await list_connected(migrated_db, scope) == []
