"""Postgres integration: the suspension checkpoint + approval batch are ONE atomic unit (M3.6).

Blocker 2 (true atomic suspension). The durable run's fenced checkpoint (source attempt + exact
batch id), the batch's ``tool.call`` events, its approval rows, and its ``approval.requested``
events must commit — or roll back — **together**, in a single transaction. There is no separate
``mark_checkpoint`` commit before the batch. Against a live Postgres these prove:

* a crash before the events commit rolls the checkpoint back too (the run stays cleanly
  ``running`` with no marker/batch — a reclaim then restarts fresh, nothing lost/partial);
* a lost/superseded lease aborts (and rolls back) the whole unit — no unfenced write;
* the happy path commits the checkpoint + rows + events atomically, so a reclaim always finds a
  complete batch bound to the checkpoint;
* driven through ``execute_run``, a mid-batch crash leaves the run resumable-fresh (resume=False)
  and a later attempt applies the approved effect exactly once.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

import keel_core.loop as loop_mod
from keel_core.agent_config_snapshot import AgentConfigSnapshot
from keel_core.agents import AgentSpec, Scope
from keel_core.approvals import PostgresApprovalStore
from keel_core.connectors import ConnectorTool
from keel_core.events import EventType
from keel_core.loop import ApprovalBinding, ToolRegistry, admit
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import ProviderChunk, ToolCall, ToolContext
from keel_core.run_service import (
    DurableRunService,
    _make_suspension_persister,
    execute_run,
)
from keel_core.runs import (
    PostgresRunStore,
    RunBudgetSpec,
    RunLease,
    RunStatus,
    RunSurface,
    action_hash,
)
from keel_core.state import PostgresEventStore, append_event_in_transaction
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, PermissionDecision, ScopeKind, TrustLevel

pytestmark = pytest.mark.integration

_SCOPE = "web:local"
_SESSION = "sess-1"
_RUN = "run-1"
_EXPIRES = datetime(2026, 7, 17, 12, 0, tzinfo=UTC) + timedelta(hours=24)


def _agent() -> AgentSpec:
    return AgentSpec(
        id="agent-1",
        name="A",
        model="test/model",
        scope=Scope(id=_SCOPE, kind=ScopeKind.personal, trust=TrustLevel.trusted),
    )


def _mail_tools(sent: list[dict[str, object]]) -> ToolRegistry:
    async def send(args: dict[str, object], ctx: ToolContext) -> str:
        sent.append(args)
        return "sent"

    return ToolRegistry(
        [ConnectorTool(name="email.send", description="", action=send, outbound=True)]
    )


def _ask() -> RuleBasedPermissionEngine:
    return RuleBasedPermissionEngine(
        [Rule("email.send", PermissionDecision.ask)], default=PermissionDecision.ask
    )


def _one_send(call_id: str, to: str) -> ScriptedProviderGateway:
    return ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id=call_id, name="email.send", arguments={"to": to}),
                    finish_reason=FinishReason.tool_use,
                )
            ]
        ]
    )


def _done() -> ScriptedProviderGateway:
    return ScriptedProviderGateway(
        [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
    )


async def _admit_and_claim(runs: PostgresRunStore, *, now: datetime, key: str = "k1") -> RunLease:
    await runs.admit(
        run_id=_RUN,
        scope_id=_SCOPE,
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id=_SESSION,
        surface=RunSurface.web.value,
        idempotency_key=key,
        budget=RunBudgetSpec(),
        expires_at=_EXPIRES,
        snapshot=AgentConfigSnapshot(agent_id="agent-1"),
        now=now,
    )
    await runs.mark_queued(_RUN, now=now)
    lease = await runs.claim(_RUN, worker_id="w1", now=now, lease_seconds=30)
    assert lease is not None
    return lease


async def _row_count(engine: AsyncEngine) -> int:
    store = PostgresApprovalStore(engine, _SCOPE)
    return len(await store.list_for_run(_RUN))


async def _event_types(engine: AsyncEngine) -> list[str]:
    async with engine.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": _SCOPE})
        rows = (
            (
                await conn.execute(
                    text(
                        "SELECT type FROM events WHERE scope_id = :s AND run_id = :r ORDER BY seq"
                    ),
                    {"s": _SCOPE, "r": _RUN},
                )
            )
            .scalars()
            .all()
        )
    return [str(r) for r in rows]


# ---- happy path: checkpoint + rows + events commit atomically -------------------------------
async def test_checkpoint_and_batch_commit_atomically(migrated_db: AsyncEngine) -> None:
    runs = PostgresRunStore(migrated_db, _SCOPE)
    events = PostgresEventStore(migrated_db, _SCOPE)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    t0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    lease = await _admit_and_claim(runs, now=t0)

    persister = _make_suspension_persister(runs, _SCOPE, lease)
    calls = [ToolCall(id="c1", name="email.send", arguments={"to": "z@x"})]
    created = await persister(
        store=events,
        approvals=approvals,
        calls=calls,
        asks=calls,
        session_id=_SESSION,
        scope_id=_SCOPE,
        run_id=_RUN,
        reason="first_use",
        expires_at=_EXPIRES,
        binding=ApprovalBinding(org_id="org-1", actor="user-1", run_attempt=lease.attempt),
        batch_id="batch-1",
    )
    assert len(created) == 1
    rec = await runs.get(_RUN)
    assert rec is not None and rec.suspend_checkpoint is True
    assert rec.checkpoint_attempt == lease.attempt and rec.checkpoint_batch_id == "batch-1"
    assert await _row_count(migrated_db) == 1
    assert await _event_types(migrated_db) == ["tool.call", "approval.requested"]
    approval = (await approvals.pending_for_run(_RUN))[0]
    assert approval.batch_id == "batch-1" and approval.run_attempt == lease.attempt


# ---- crash before the events commit rolls the CHECKPOINT back too ---------------------------
async def test_crash_before_event_rolls_back_checkpoint_and_batch(
    migrated_db: AsyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs = PostgresRunStore(migrated_db, _SCOPE)
    events = PostgresEventStore(migrated_db, _SCOPE)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    t0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    lease = await _admit_and_claim(runs, now=t0)

    async def crash_on_approval_event(conn: object, event: object, **kw: object) -> int:
        if getattr(event, "type", None) is EventType.approval_requested:
            raise RuntimeError("crash before approval.requested commits")
        return await append_event_in_transaction(conn, event, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(loop_mod, "append_event_in_transaction", crash_on_approval_event)

    persister = _make_suspension_persister(runs, _SCOPE, lease)
    calls = [ToolCall(id="c1", name="email.send", arguments={"to": "z@x"})]
    with pytest.raises(RuntimeError):
        await persister(
            store=events,
            approvals=approvals,
            calls=calls,
            asks=calls,
            session_id=_SESSION,
            scope_id=_SCOPE,
            run_id=_RUN,
            reason="first_use",
            expires_at=_EXPIRES,
            binding=ApprovalBinding(org_id="org-1", actor="user-1", run_attempt=lease.attempt),
            batch_id="batch-1",
        )

    # All-or-nothing: the checkpoint, the approval row, and EVERY batch event rolled back — the
    # run is cleanly ``running`` with no marker, so a reclaim restarts fresh (nothing partial).
    rec = await runs.get(_RUN)
    assert rec is not None and rec.status is RunStatus.running
    assert rec.suspend_checkpoint is False and rec.checkpoint_attempt == 0
    assert rec.checkpoint_batch_id == ""
    assert await _row_count(migrated_db) == 0
    assert await _event_types(migrated_db) == []


# ---- a lost/superseded lease aborts (rolls back) the whole unit — no unfenced write ---------
async def test_lost_lease_checkpoint_aborts_the_batch(migrated_db: AsyncEngine) -> None:
    runs = PostgresRunStore(migrated_db, _SCOPE)
    events = PostgresEventStore(migrated_db, _SCOPE)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    t0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    lease = await _admit_and_claim(runs, now=t0)

    stale = RunLease(
        run_id=_RUN,
        scope_id=_SCOPE,
        org_id="org-1",
        token="not-the-token",
        worker_id="w9",
        attempt=lease.attempt,
        agent_id="agent-1",
        session_id=_SESSION,
        lease_seconds=30,
    )
    persister = _make_suspension_persister(runs, _SCOPE, stale)
    calls = [ToolCall(id="c1", name="email.send", arguments={"to": "z@x"})]
    from keel_core.runs import RunLeaseLostError

    with pytest.raises(RunLeaseLostError):
        await persister(
            store=events,
            approvals=approvals,
            calls=calls,
            asks=calls,
            session_id=_SESSION,
            scope_id=_SCOPE,
            run_id=_RUN,
            reason="first_use",
            expires_at=_EXPIRES,
            binding=ApprovalBinding(org_id="org-1", actor="user-1", run_attempt=stale.attempt),
            batch_id="batch-1",
        )
    rec = await runs.get(_RUN)
    assert rec is not None and rec.suspend_checkpoint is False and rec.checkpoint_batch_id == ""
    assert await _row_count(migrated_db) == 0
    assert await _event_types(migrated_db) == []


# ---- driven through execute_run: a mid-batch crash reclaims FRESH, then applies once ---------
async def test_execute_run_midbatch_crash_reclaims_fresh_and_applies_once(
    migrated_db: AsyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs = PostgresRunStore(migrated_db, _SCOPE)
    events = PostgresEventStore(migrated_db, _SCOPE)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    enqueued: list[str] = []

    async def enqueue(run_id: str) -> None:
        enqueued.append(run_id)

    service = DurableRunService(
        run_store=runs,
        event_store=events,
        approvals=approvals,
        scope_id=_SCOPE,
        enqueue=enqueue,
        admit_fn=admit,
    )
    sent: list[dict[str, object]] = []
    t0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    lease = await _admit_and_claim(runs, now=t0)

    async def crash_on_approval_event(conn: object, event: object, **kw: object) -> int:
        if getattr(event, "type", None) is EventType.approval_requested:
            raise RuntimeError("crash mid-batch before approval.requested commits")
        return await append_event_in_transaction(conn, event, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(loop_mod, "append_event_in_transaction", crash_on_approval_event)
    with pytest.raises(RuntimeError):
        await execute_run(
            lease=lease,
            run_store=runs,
            event_store=events,
            approvals=approvals,
            agent=_agent(),
            provider=_one_send("c1", "z@x"),
            registry=_mail_tools(sent),
            permissions=_ask(),
            admit_fn=admit,
            resume=lease.resume,
            now=t0,
        )
    monkeypatch.undo()

    crashed = await runs.get(_RUN)
    assert crashed is not None and crashed.status is RunStatus.running
    assert crashed.suspend_checkpoint is False and crashed.checkpoint_batch_id == ""
    assert await _row_count(migrated_db) == 0

    # Reclaim after lease expiry: no durable checkpoint -> restart FRESH (resume=False).
    t1 = t0 + timedelta(seconds=60)
    lease2 = await runs.claim(_RUN, worker_id="w2", now=t1, lease_seconds=30)
    assert lease2 is not None and lease2.attempt == 2 and lease2.resume is False

    rec2 = await execute_run(
        lease=lease2,
        run_store=runs,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=_one_send("c1", "z@x"),
        registry=_mail_tools(sent),
        permissions=_ask(),
        admit_fn=admit,
        resume=lease2.resume,
        now=t1,
    )
    assert rec2.status is RunStatus.waiting_approval
    pend = await approvals.pending_for_run(_RUN)
    assert len(pend) == 1 and pend[0].run_attempt == 2
    fresh = await runs.get(_RUN)
    assert fresh is not None and fresh.checkpoint_batch_id != ""  # a real checkpoint now exists
    assert pend[0].batch_id == fresh.checkpoint_batch_id
    assert pend[0].action_hash == action_hash("email.send", {"to": "z@x"})

    assert await service.resolve_approval(
        pend[0].id, approved=True, resolved_by="user-1", actor="user-1", org_id="org-1"
    )
    t2 = t1 + timedelta(seconds=60)
    lease3 = await runs.claim(_RUN, worker_id="w3", now=t2, lease_seconds=30)
    assert lease3 is not None and lease3.resume is True
    rec3 = await execute_run(
        lease=lease3,
        run_store=runs,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=_done(),
        registry=_mail_tools(sent),
        permissions=_ask(),
        admit_fn=admit,
        resume=lease3.resume,
        now=t2,
    )
    assert rec3.status is RunStatus.completed
    assert sent == [{"to": "z@x"}]  # approved effect applied exactly once


# ---- reconstruction (older-build lost event) honours the exact checkpoint batch/attempt ------
async def test_resume_reconstructs_only_on_exact_checkpoint_batch(
    migrated_db: AsyncEngine,
) -> None:
    runs = PostgresRunStore(migrated_db, _SCOPE)
    events = PostgresEventStore(migrated_db, _SCOPE)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    enqueued: list[str] = []

    async def enqueue(run_id: str) -> None:
        enqueued.append(run_id)

    service = DurableRunService(
        run_store=runs,
        event_store=events,
        approvals=approvals,
        scope_id=_SCOPE,
        enqueue=enqueue,
        admit_fn=admit,
    )
    sent: list[dict[str, object]] = []
    t0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    lease = await _admit_and_claim(runs, now=t0)

    # Suspend cleanly (checkpoint + batch commit atomically), then release to waiting_approval.
    rec = await execute_run(
        lease=lease,
        run_store=runs,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=_one_send("c1", "z@x"),
        registry=_mail_tools(sent),
        permissions=_ask(),
        admit_fn=admit,
        resume=lease.resume,
        now=t0,
    )
    assert rec.status is RunStatus.waiting_approval
    pend = await approvals.pending_for_run(_RUN)
    assert len(pend) == 1
    approval_id = pend[0].id
    checkpoint = await runs.get(_RUN)
    assert checkpoint is not None and checkpoint.checkpoint_batch_id == pend[0].batch_id
    assert await service.resolve_approval(
        approval_id, approved=True, resolved_by="user-1", actor="user-1", org_id="org-1"
    )

    # Simulate an older-build crash on top: the approval row survived but its event is lost.
    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": _SCOPE})
        deleted = await conn.execute(
            text(
                "DELETE FROM events WHERE scope_id = :s AND run_id = :r "
                "AND type = 'approval.requested'"
            ),
            {"s": _SCOPE, "r": _RUN},
        )
    assert deleted.rowcount == 1

    t1 = t0 + timedelta(seconds=1)
    lease2 = await runs.claim(_RUN, worker_id="w2", now=t1, lease_seconds=30)
    assert lease2 is not None and lease2.resume is True
    rec2 = await execute_run(
        lease=lease2,
        run_store=runs,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=_done(),
        registry=_mail_tools(sent),
        permissions=_ask(),
        admit_fn=admit,
        resume=lease2.resume,
        now=t1,
    )
    assert rec2.status is RunStatus.completed
    assert sent == [{"to": "z@x"}]  # reconstructed from the durable row on the exact checkpoint
    assert "approval.requested" in await _event_types(migrated_db)  # audit event back-filled
