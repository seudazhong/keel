"""Worker task tests: run_agent (suspend), scheduler_tick (enqueue + expire), resume_run."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from keel_core.approvals import InMemoryApprovalStore
from keel_core.digest import digest_session_id
from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, StopReason
from keel_scheduler.atmostonce import InMemoryClaimStore
from keel_scheduler.store import InMemoryScheduleStore, ScheduleRow
from keel_worker.main import resume_run, run_agent, scheduler_tick

_NOW = datetime(2026, 7, 7, 9, 0, tzinfo=UTC)


def _row() -> ScheduleRow:
    return ScheduleRow(
        id="daily",
        scope_id="u:1",
        agent_id="digest",
        session_id=digest_session_id("u:1"),
        trigger_kind="interval",
        spec="86400",
        next_run_at=_NOW,
        interval_s=86400,
    )


def _read_then_send() -> ScriptedProviderGateway:
    return ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c1", name="inbox.list", arguments={}),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c2",
                        name="email.send",
                        arguments={"to": "finance@external.example", "idempotency_key": "k"},
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
        ]
    )


async def test_run_agent_suspends_and_records_pending() -> None:
    store, approvals = InMemoryEventStore(), InMemoryApprovalStore()
    ctx: dict[str, Any] = {
        "store": store,
        "approvals": approvals,
        "provider": _read_then_send(),
        "schedules": InMemoryScheduleStore([_row()]),
        "sent": [],
    }
    reason = await run_agent(ctx, "daily")
    assert reason == StopReason.suspended.value
    assert len(await approvals.list_pending("u:1")) == 1


async def test_run_agent_missing_schedule_is_a_noop() -> None:
    ctx: dict[str, Any] = {
        "store": InMemoryEventStore(),
        "approvals": InMemoryApprovalStore(),
        "provider": _read_then_send(),
        "schedules": InMemoryScheduleStore([]),
        "sent": [],
    }
    assert await run_agent(ctx, "nope") == "missing"


async def test_scheduler_tick_enqueues_due_and_expires_stale() -> None:
    approvals = InMemoryApprovalStore()
    await approvals.create_pending(
        scope_id="u:1",
        run_id="r1",
        session_id="digest:u:1",
        tool="email.send",
        args={},
        call_id="c",
        idempotency_key="k",
        reason="tainted",
        expires_at=_NOW - timedelta(minutes=1),  # already stale -> fail-closed
    )
    enqueued: list[tuple[Any, ...]] = []

    async def enqueue(name: str, *args: object) -> None:
        enqueued.append((name, *args))

    ctx: dict[str, Any] = {
        "schedules": InMemoryScheduleStore([_row()]),
        "claim": InMemoryClaimStore({"daily": _NOW}),
        "approvals": approvals,
        "enqueue": enqueue,
    }
    count = await scheduler_tick(ctx)
    assert count == 1
    assert ("run_agent", "daily") in enqueued
    assert any(e[0] == "resume_run" for e in enqueued)  # the expired approval is resumed to deny


async def test_resume_run_completes_after_grant() -> None:
    store, approvals = InMemoryEventStore(), InMemoryApprovalStore()
    sent: list[dict[str, object]] = []
    ctx: dict[str, Any] = {
        "store": store,
        "approvals": approvals,
        "provider": _read_then_send(),
        "schedules": InMemoryScheduleStore([_row()]),
        "sent": sent,
    }
    await run_agent(ctx, "daily")  # suspends
    aid = (await approvals.list_pending("u:1"))[0].id
    await approvals.resolve(aid, "granted", "me")
    run_id = store.snapshot(digest_session_id("u:1"))[-1].run_id

    ctx2: dict[str, Any] = {
        "store": store,
        "approvals": approvals,
        "provider": ScriptedProviderGateway(
            [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
        ),
        "sent": sent,
    }
    reason = await resume_run(ctx2, digest_session_id("u:1"), run_id, "u:1")
    assert reason == StopReason.completed.value
    assert sent == [{"to": "finance@external.example", "idempotency_key": "k"}]
