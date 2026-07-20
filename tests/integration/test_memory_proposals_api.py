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
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.consolidation import MemoryProposalStore
from keel_core.memory import PostgresMemoryStore
from keel_server.api.v1 import router
from keel_server.auth import parse_api_keys

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


@pytest_asyncio.fixture
async def keyed_proposals_client(
    migrated_db: AsyncEngine,
) -> AsyncIterator[tuple[httpx.AsyncClient, str]]:
    """Client fixture with keyed RBAC: 'vw' viewer key and 'op' operator key."""
    scope = f"u:{uuid.uuid4().hex}"
    enqueued: list[tuple[Any, ...]] = []

    async def enqueue(name: str, *args: object) -> None:
        enqueued.append((name, *args))

    app = FastAPI()
    app.include_router(router)
    app.state.engine = migrated_db
    app.state.durable_scope = scope
    app.state.enqueue = enqueue
    app.state.api_keys = parse_api_keys("vw:viewer,op:operator")
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, scope


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
    blocks = (await client.get("/v1/memory/blocks")).json()
    assert blocks == [{"key": "human", "value": "likes tea", "version": 1}]

    rejected = await client.post(f"/v1/memory/proposals/{pid_persona}/reject")
    assert rejected.status_code == 200
    assert rejected.json() == {"ok": True, "status": "rejected", "version": None}

    missing = await client.post("/v1/memory/proposals/does-not-exist/approve")
    assert missing.status_code == 404
    assert missing.json() == {"ok": False, "status": "not_found", "version": None}

    still_pending = (await client.get("/v1/memory/proposals", params={"status": "pending"})).json()
    assert still_pending == []

    schedule_id = f"memory-consolidation:{scope}"
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        await conn.execute(
            text(
                "INSERT INTO schedules "
                "(id, scope_id, agent_id, session_id, trigger_kind, spec, next_run_at, "
                "interval_s, enabled) VALUES "
                "(:id, :scope, 'memory-consolidator', :session, 'interval', '86400', "
                "now(), 86400, true)"
            ),
            {"id": schedule_id, "scope": scope, "session": schedule_id},
        )
    ran = await client.post("/v1/memory/consolidation/run")
    assert ran.status_code == 200
    assert ran.json() == {"ok": True}
    assert enqueued == [("run_agent", schedule_id, scope)]


async def test_approve_already_resolved_returns_409(
    proposals_client: tuple[httpx.AsyncClient, str, list[tuple[Any, ...]]],
    migrated_db: AsyncEngine,
) -> None:
    """Double resolution: second approve on an already-resolved proposal yields 409."""
    client, scope, _ = proposals_client
    store = MemoryProposalStore(migrated_db, scope)
    pid, _ = await store.propose(
        block="human",
        proposed_value="likes coffee",
        reason="stated in chat",
        confidence=0.9,
        source_event_ids=[1],
    )
    first = await client.post(f"/v1/memory/proposals/{pid}/approve")
    assert first.status_code == 200
    second = await client.post(f"/v1/memory/proposals/{pid}/approve")
    assert second.status_code == 409
    assert second.json()["status"] == "already_resolved"


async def test_approve_stale_returns_409(
    proposals_client: tuple[httpx.AsyncClient, str, list[tuple[Any, ...]]],
    migrated_db: AsyncEngine,
) -> None:
    """Stale: create proposal, mutate Core block version out-of-band, then approve -> 409."""
    client, scope, _ = proposals_client
    store = MemoryProposalStore(migrated_db, scope)
    memory = PostgresMemoryStore(migrated_db, scope)
    pid, _ = await store.propose(
        block="human",
        proposed_value="likes tea",
        reason="stated in chat",
        confidence=0.9,
        source_event_ids=[1],
    )
    # Advance the block's version so the proposal's expected_version is now stale.
    await memory.set("human", "written directly")
    stale = await client.post(f"/v1/memory/proposals/{pid}/approve")
    assert stale.status_code == 409
    assert stale.json()["status"] == "stale"
    # Core memory is untouched: the direct write remains.
    assert await memory.get("human") == "written directly"


async def test_rbac_viewer_read_operator_mutate(
    keyed_proposals_client: tuple[httpx.AsyncClient, str],
    migrated_db: AsyncEngine,
) -> None:
    """Keyed RBAC: viewer GET succeeds; approve/reject/manual-run require operator."""
    client, scope = keyed_proposals_client
    store = MemoryProposalStore(migrated_db, scope)
    pid, _ = await store.propose(
        block="human",
        proposed_value="likes tea",
        reason="stated in chat",
        confidence=0.9,
        source_event_ids=[1],
    )

    viewer = {"X-API-Key": "vw"}
    operator = {"X-API-Key": "op"}

    # Viewer can read the proposals list.
    resp = await client.get("/v1/memory/proposals", headers=viewer)
    assert resp.status_code == 200
    assert any(p["id"] == pid for p in resp.json())

    # Viewer is forbidden from approve, reject, and manual consolidation run.
    assert (
        await client.post(f"/v1/memory/proposals/{pid}/approve", headers=viewer)
    ).status_code == 403
    assert (
        await client.post(f"/v1/memory/proposals/{pid}/reject", headers=viewer)
    ).status_code == 403
    assert (await client.post("/v1/memory/consolidation/run", headers=viewer)).status_code == 403

    # Operator can reject the proposal.
    resp = await client.post(f"/v1/memory/proposals/{pid}/reject", headers=operator)
    assert resp.status_code == 200

    # Operator can trigger a manual consolidation run.
    schedule_id = f"memory-consolidation:{scope}"
    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        await conn.execute(
            text(
                "INSERT INTO schedules "
                "(id, scope_id, agent_id, session_id, trigger_kind, spec, next_run_at, "
                "interval_s, enabled) VALUES "
                "(:id, :scope, 'memory-consolidator', :session, 'interval', '86400', "
                "now(), 86400, true)"
            ),
            {"id": schedule_id, "scope": scope, "session": schedule_id},
        )
    resp = await client.post("/v1/memory/consolidation/run", headers=operator)
    assert resp.status_code == 200
