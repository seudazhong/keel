"""End-to-end slice: scheduler tick -> run suspends -> HTTP approve -> resume sends once.

Assembles the REAL pieces against live Postgres — the event/approval/schedule/claim
stores, the ``/v1`` API over HTTP, the worker task functions, and ``due_tick``/CAS.
Only the LLM is scripted, so the whole durable chain is exercised deterministically."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.approvals import PostgresApprovalStore
from keel_core.digest import digest_session_id
from keel_core.events import EventType
from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.state import PostgresEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_scheduler.store import PostgresClaimStore, PostgresScheduleStore
from keel_server.api.v1 import router
from keel_worker.main import resume_run, run_agent, scheduler_tick

pytestmark = pytest.mark.integration


def _read_then_send() -> ScriptedProviderGateway:
    return ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c1", name="inbox_list", arguments={}),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c2",
                        name="email_send",
                        arguments={"to": "finance@external.example", "idempotency_key": "k"},
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
        ]
    )


async def test_slice_end_to_end(migrated_db: AsyncEngine) -> None:
    scope = f"e2e:{uuid.uuid4().hex[:8]}"
    sid = digest_session_id(scope)
    sched_id = f"digest:{scope}"
    due_at = datetime.now(UTC) - timedelta(seconds=1)  # already due

    async with migrated_db.begin() as conn:
        await conn.execute(text("select set_config('app.scope_id',:s,true)"), {"s": scope})
        await conn.execute(
            text(
                "insert into schedules(id,scope_id,agent_id,session_id,trigger_kind,spec,"
                "next_run_at,interval_s,enabled) values "
                "(:id,:s,'digest',:sess,'interval','86400',:t,86400,true)"
            ),
            {"id": sched_id, "s": scope, "sess": sid, "t": due_at},
        )

    store = PostgresEventStore(migrated_db, scope)
    approvals = PostgresApprovalStore(migrated_db, scope)
    schedules = PostgresScheduleStore(migrated_db, scope)
    claim = PostgresClaimStore(migrated_db, scope)
    sent: list[dict[str, object]] = []

    # 1) scheduler_tick enqueues run_agent (at-most-once cursor advance)
    tick_enqueued: list[tuple[Any, ...]] = []

    async def tick_enqueue(name: str, *args: object) -> None:
        tick_enqueued.append((name, *args))

    tick_ctx: dict[str, Any] = {
        "schedules": schedules,
        "claim": claim,
        "approvals": approvals,
        "enqueue": tick_enqueue,
    }
    assert await scheduler_tick(tick_ctx) == 1
    assert ("run_agent", sched_id) in tick_enqueued

    # 2) worker runs it against the real Postgres stores -> suspends at the tainted send
    run_ctx: dict[str, Any] = {
        "store": store,
        "approvals": approvals,
        "provider": _read_then_send(),
        "schedules": schedules,
        "sent": sent,
    }
    assert await run_agent(run_ctx, sched_id) == "suspended"
    pending = await approvals.list_pending(scope)
    assert len(pending) == 1 and sent == []
    approval_id, run_id = pending[0].id, pending[0].run_id

    # 3) the Approvals API (real app wired to the Postgres store) lists + approves it
    app = FastAPI()
    app.include_router(router)
    app.state.durable_approvals = approvals
    app.state.durable_scope = scope
    api_enqueued: list[tuple[Any, ...]] = []

    async def api_enqueue(name: str, *args: object) -> None:
        api_enqueued.append((name, *args))

    app.state.enqueue = api_enqueue
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        listed = (await client.get("/v1/approvals?status=pending")).json()
        assert [a["id"] for a in listed] == [approval_id]
        approve = await client.post(f"/v1/approvals/{approval_id}/approve")
        assert approve.json() == {"ok": True}
    assert api_enqueued == [("resume_run", sid, run_id, scope)]

    # 4) worker resumes from the durable log -> sends exactly once, run completes
    resume_ctx: dict[str, Any] = {
        "store": store,
        "approvals": approvals,
        "provider": ScriptedProviderGateway(
            [
                [
                    ProviderChunk(
                        delta="已发送。今日摘要：3 封重要邮件。",
                        finish_reason=FinishReason.end_turn,
                    )
                ]
            ]
        ),
        "sent": sent,
    }
    assert await resume_run(resume_ctx, sid, run_id, scope) == "completed"
    assert sent == [{"to": "finance@external.example", "idempotency_key": "k"}]

    # 5) the digest session in Postgres holds the full suspend/resume trace + the summary
    events = [e async for e in store.read(sid)]
    types = [e.type for e in events]
    assert EventType.run_suspended in types
    assert EventType.run_resumed in types
    assert EventType.run_ended in types
    assert any(
        e.type is EventType.message_token and "今日摘要" in str(e.payload.get("text", ""))
        for e in events
    )
