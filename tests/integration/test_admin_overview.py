"""Integration: admin overview aggregate (counts + usage totals) + endpoint."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.admin import compute_overview
from keel_server.api.v1 import router

pytestmark = pytest.mark.integration


async def _seed(engine: AsyncEngine, scope: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id', :s, true)"), {"s": scope})
        await conn.execute(
            text("INSERT INTO sessions (id, scope_id, next_seq) VALUES (:sid, :s, 3)"),
            {"sid": f"sess:{scope}", "s": scope},
        )
        runs = (
            (
                1,
                {
                    "prompt_tokens": 100,
                    "completion_tokens": 40,
                    "cache_read_tokens": 10,
                    "cost_usd": 0.005,
                },
            ),
            (
                2,
                {
                    "prompt_tokens": 50,
                    "completion_tokens": 20,
                    "cache_read_tokens": 0,
                    "cost_usd": 0.002,
                },
            ),
        )
        for seq, usage in runs:
            await conn.execute(
                text(
                    "INSERT INTO events (session_id, scope_id, seq, type, version, run_id, ts, payload) "
                    "VALUES (:sid, :s, :seq, 'run.ended', 1, :rid, :ts, CAST(:p AS jsonb))"
                ),
                {
                    "sid": f"sess:{scope}",
                    "s": scope,
                    "seq": seq,
                    "rid": f"r{seq}",
                    "ts": datetime.now(UTC),
                    "p": json.dumps({"reason": "completed", "usage": usage}),
                },
            )
        for sid, enabled in ((f"a:{scope}", True), (f"b:{scope}", False)):
            await conn.execute(
                text(
                    "INSERT INTO schedules (id, scope_id, agent_id, session_id, trigger_kind, "
                    "spec, next_run_at, interval_s, enabled) VALUES "
                    "(:id, :s, 'digest', :sid, 'interval', '86400', now(), 86400, :en)"
                ),
                {"id": sid, "s": scope, "sid": sid, "en": enabled},
            )
        for aid, status in ((f"p:{scope}", "pending"), (f"g:{scope}", "granted")):
            await conn.execute(
                text(
                    "INSERT INTO approvals (id, scope_id, run_id, session_id, tool, args, call_id, "
                    "idempotency_key, reason, status, expires_at) VALUES "
                    "(:id, :s, 'r1', 's1', 'email_send', '{}'::jsonb, 'c1', :id, 'tainted', :st, "
                    "now() + interval '1 day')"
                ),
                {"id": aid, "s": scope, "st": status},
            )
        await conn.execute(
            text(
                "INSERT INTO connector_tokens (scope_id, connector_id, ciphertext) VALUES (:s, 'gmail', 'x')"
            ),
            {"s": scope},
        )


async def test_compute_overview_aggregates(migrated_db: AsyncEngine) -> None:
    scope = f"u:{uuid.uuid4().hex}"
    await _seed(migrated_db, scope)

    ov = await compute_overview(migrated_db, scope)
    assert ov["sessions"] == 1
    assert ov["schedules"] == {"total": 2, "enabled": 1}
    assert ov["approvals"] == {"pending": 1, "granted": 1, "denied": 0, "expired": 0}
    assert ov["connectors"] == 1
    usage = ov["usage"]
    assert usage["runs"] == 2
    assert usage["prompt_tokens"] == 150  # 100 + 50
    assert usage["completion_tokens"] == 60  # 40 + 20
    assert usage["cache_read_tokens"] == 10
    assert abs(usage["cost_usd"] - 0.007) < 1e-9


@pytest_asyncio.fixture
async def admin_client(migrated_db: AsyncEngine) -> AsyncIterator[tuple[httpx.AsyncClient, str]]:
    scope = f"u:{uuid.uuid4().hex}"
    app = FastAPI()
    app.include_router(router)
    app.state.engine = migrated_db
    app.state.durable_scope = scope
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, scope


async def test_admin_overview_endpoint(
    admin_client: tuple[httpx.AsyncClient, str], migrated_db: AsyncEngine
) -> None:
    client, scope = admin_client
    await _seed(migrated_db, scope)

    ov = (await client.get("/v1/admin/overview")).json()
    assert ov["sessions"] == 1
    assert ov["usage"]["runs"] == 2
    assert ov["usage"]["prompt_tokens"] == 150
    assert ov["connectors"] == 1
    assert ov["schedules"] == {"total": 2, "enabled": 1}
