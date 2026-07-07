"""Web approvals: durable approvals API (list/approve/reject) over an in-memory store.

No live services — a minimal FastAPI app with the v1 router and an in-memory
ApprovalStore + a recording enqueue, driven via httpx's ASGI transport."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport

from keel_core.approvals import InMemoryApprovalStore
from keel_server.api.v1 import router

_Fixture = tuple[httpx.AsyncClient, InMemoryApprovalStore, list[tuple[Any, ...]]]


@pytest_asyncio.fixture
async def approvals_client() -> AsyncIterator[_Fixture]:
    approvals = InMemoryApprovalStore()
    enqueued: list[tuple[Any, ...]] = []

    async def enqueue(name: str, *args: object) -> None:
        enqueued.append((name, *args))

    app = FastAPI()
    app.include_router(router)
    app.state.durable_approvals = approvals
    app.state.durable_scope = "web:local"
    app.state.enqueue = enqueue
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, approvals, enqueued


async def _seed(approvals: InMemoryApprovalStore) -> str:
    return await approvals.create_pending(
        scope_id="web:local",
        run_id="r1",
        session_id="digest:web:local",
        tool="email.send",
        args={"to": "finance@external.example"},
        call_id="c1",
        idempotency_key="k1",
        reason="tainted",
        expires_at=datetime.now(UTC) + timedelta(hours=24),
    )


async def test_list_and_approve_enqueues_resume(approvals_client: _Fixture) -> None:
    client, approvals, enqueued = approvals_client
    aid = await _seed(approvals)

    listed = (await client.get("/v1/approvals?status=pending")).json()
    assert [a["id"] for a in listed] == [aid]
    assert listed[0]["tool"] == "email.send"

    approve = await client.post(f"/v1/approvals/{aid}/approve")
    assert approve.json() == {"ok": True}
    assert enqueued == [("resume_run", "digest:web:local", "r1", "web:local")]

    # Single-shot: approving again is a no-op and enqueues nothing further.
    again = await client.post(f"/v1/approvals/{aid}/approve")
    assert again.json() == {"ok": False}
    assert len(enqueued) == 1


async def test_reject_resolves_and_enqueues_resume(approvals_client: _Fixture) -> None:
    client, approvals, enqueued = approvals_client
    aid = await _seed(approvals)
    reject = await client.post(f"/v1/approvals/{aid}/reject")
    assert reject.json() == {"ok": True}
    assert enqueued == [("resume_run", "digest:web:local", "r1", "web:local")]
    rec = await approvals.get(aid)
    assert rec is not None and rec.status == "denied"
