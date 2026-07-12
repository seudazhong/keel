"""Integration: /v1/memory proposals list/approve/reject + manual consolidation run."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation import MemoryProposalStore
from keel_core.memory import PostgresMemoryStore
from keel_server.api.v1 import router

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def proposals_client(
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


async def test_list_approve_reject_and_run(
    proposals_client: tuple[httpx.AsyncClient, str, list[tuple[Any, ...]]],
    migrated_db: AsyncEngine,
) -> None:
    client, scope, enqueued = proposals_client
    store = MemoryProposalStore(migrated_db, scope)
    memory = PostgresMemoryStore(migrated_db, scope)

    pid_human, created_human = await store.propose(
        block="human",
        proposed_value="likes tea",
        reason="stated in chat",
        confidence=0.9,
        source_event_ids=[1],
    )
    pid_persona, _ = await store.propose(
        block="persona",
        proposed_value="concise and warm",
        reason="tone feedback",
        confidence=0.8,
        source_event_ids=[2],
    )
    assert created_human is True

    pending = (await client.get("/v1/memory/proposals", params={"status": "pending"})).json()
    assert {p["id"] for p in pending} == {pid_human, pid_persona}
    assert all(p["status"] == "pending" for p in pending)

    approved = await client.post(f"/v1/memory/proposals/{pid_human}/approve")
    assert approved.status_code == 200
    assert approved.json() == {"ok": True, "status": "applied", "version": 1}
    assert await memory.get("human") == "likes tea"

    rejected = await client.post(f"/v1/memory/proposals/{pid_persona}/reject")
    assert rejected.status_code == 200
    assert rejected.json() == {"ok": True, "status": "rejected", "version": None}

    missing = await client.post("/v1/memory/proposals/does-not-exist/approve")
    assert missing.status_code == 404
    assert missing.json() == {"ok": False, "status": "not_found", "version": None}

    still_pending = (await client.get("/v1/memory/proposals", params={"status": "pending"})).json()
    assert still_pending == []

    ran = await client.post("/v1/memory/consolidation/run")
    assert ran.json() == {"ok": True}
    assert enqueued == [("run_agent", f"memory-consolidation:{scope}")]
