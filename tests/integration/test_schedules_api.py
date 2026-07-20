"""Integration: schedule management store methods + /v1/schedules endpoints."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_scheduler.store import PostgresScheduleStore
from keel_server.api.v1 import router

pytestmark = pytest.mark.integration


async def _seed(engine: AsyncEngine, scope: str, schedule_id: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        await conn.execute(
            text(
                "INSERT INTO schedules (id, scope_id, agent_id, session_id, trigger_kind, "
                "spec, next_run_at, interval_s, enabled) VALUES "
                "(:id, :scope, 'digest', :session, 'interval', '86400', now(), 86400, true)"
            ),
            {"id": schedule_id, "scope": scope, "session": schedule_id},
        )


async def test_schedule_store_list_and_toggle(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    sid = f"digest:{scope}"
    await _seed(migrated_db, scope, sid)

    store = PostgresScheduleStore(migrated_db, scope)
    assert [r.id for r in await store.list_all()] == [sid]
    assert (await store.list_all())[0].enabled is True
    assert await store.set_enabled(sid, False) is True
    assert (await store.list_all())[0].enabled is False
    assert await store.set_enabled("nope", False) is False  # unknown id


@pytest_asyncio.fixture
async def schedules_client(
    migrated_db: AsyncEngine,
) -> AsyncIterator[tuple[httpx.AsyncClient, str, list[tuple[Any, ...]]]]:
    scope = f"u:{uuid.uuid4().hex}"
    enqueued: list[tuple[Any, ...]] = []

    async def enqueue(name: str, *args: object) -> None:
        enqueued.append((name, *args))

    app = FastAPI()
    app.include_router(router)
    app.state.engine = migrated_db
    app.state.durable_scope = scope
    app.state.enqueue = enqueue
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, scope, enqueued


async def test_schedules_endpoints(
    schedules_client: tuple[httpx.AsyncClient, str, list[tuple[Any, ...]]],
    migrated_db: AsyncEngine,
) -> None:
    client, scope, enqueued = schedules_client
    sid = f"digest:{scope}"
    await _seed(migrated_db, scope, sid)

    listed = (await client.get("/v1/schedules")).json()
    assert any(r["id"] == sid and r["enabled"] for r in listed)

    toggled = (await client.post(f"/v1/schedules/{sid}/toggle", json={"enabled": False})).json()
    assert toggled == {"ok": True, "enabled": False}

    run = await client.post(f"/v1/schedules/{sid}/run")
    assert run.status_code == 200
    assert run.json() == {"ok": True}
    assert enqueued == [("run_agent", sid, scope)]

    missing = await client.post("/v1/schedules/does-not-exist/run")
    assert missing.status_code == 404
