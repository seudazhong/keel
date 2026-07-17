"""Crash-injection coverage for the durable suspension/approval boundary (M3.6).

The dangerous window: an approval/suspension is durable but the run row is still
``running`` because the worker died before releasing to ``waiting_approval``. Without a
durable checkpoint marker the lease-expiry reclaim advances ``attempt`` with
``resume=False`` and restarts fresh — silently discarding the (later approved) action,
because the approval is now bound to a superseded attempt.

These tests inject a crash at each boundary and assert the invariant holds:
  1. before the marker            -> reclaim restarts fresh (no false resume), nothing lost;
  2. mid-batch (checkpoint written -> the checkpoint + rows + events roll back together
     but rows/events not durable)     (atomic unit), so reclaim restarts fresh — no partial
                                       checkpoint without its batch, no unapproved effect;
  3. after the atomic batch commit, -> reclaim resumes, the approval bound to the *source*
     before the waiting_approval        attempt still applies exactly once (THE bug);
     transition
  4. after decision / before        -> the reconciler redispatches; approved effect applies
     dispatch                          exactly once, never twice.

In every case: no silently-completed run with a missing approved effect, and no duplicate
effects. Exercised against the deterministic in-memory doubles (Postgres parity is proven
in tests/integration/test_runs_postgres.py).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from keel_core.agents import AgentSpec, Scope
from keel_core.approvals import InMemoryApprovalStore
from keel_core.connectors import ConnectorTool
from keel_core.events import EventType
from keel_core.loop import ToolRegistry, admit
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import ProviderChunk, ToolCall, ToolContext
from keel_core.run_service import DurableRunService, execute_run, reconcile_runs
from keel_core.runs import (
    InMemoryRunStore,
    RunCost,
    RunLease,
    RunRecord,
    RunStatus,
    RunSurface,
)
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, PermissionDecision, ScopeKind, TrustLevel

_SCOPE = "web:local"


def _agent() -> AgentSpec:
    return AgentSpec(
        id="agent-1",
        name="A",
        model="test/model",
        scope=Scope(id=_SCOPE, kind=ScopeKind.personal, trust=TrustLevel.trusted),
    )


class _CrashRunStore(InMemoryRunStore):
    """An in-memory run store that can crash at a chosen fenced write (process death)."""

    crash_on: str | None = None

    async def mark_checkpoint(
        self, lease: RunLease, *, batch_id: str = "", now: datetime | None = None
    ) -> bool:
        if self.crash_on == "mark_checkpoint":
            raise RuntimeError("crash: worker died before the checkpoint marker persisted")
        return await super().mark_checkpoint(lease, batch_id=batch_id, now=now)

    async def release(
        self,
        lease: RunLease,
        *,
        to_status: RunStatus,
        now: datetime | None = None,
        cost: RunCost | None = None,
    ) -> RunRecord:
        if self.crash_on == "release":
            raise RuntimeError("crash: worker died before the waiting_approval transition")
        return await super().release(lease, to_status=to_status, now=now, cost=cost)


class _CrashApprovalStore(InMemoryApprovalStore):
    """An in-memory approval store that can crash while persisting the approval batch."""

    crash_create: bool = False

    async def create_pending(self, **kwargs: object) -> str:
        if self.crash_create:
            raise RuntimeError("crash: worker died before the approval row/event persisted")
        return await super().create_pending(**kwargs)  # type: ignore[arg-type]


def _service(
    run_store: InMemoryRunStore,
    events: InMemoryEventStore,
    approvals: InMemoryApprovalStore,
    enqueued: list[str],
) -> DurableRunService:
    async def enqueue(run_id: str) -> None:
        enqueued.append(run_id)

    return DurableRunService(
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        scope_id=_SCOPE,
        enqueue=enqueue,
        admit_fn=admit,
    )


def _send_tool(sent: list[dict[str, object]]) -> ToolRegistry:
    async def send(args: dict[str, object], ctx: ToolContext) -> str:
        sent.append(args)
        return "sent"

    return ToolRegistry(
        [ConnectorTool(name="email.send", description="", action=send, outbound=True)]
    )


def _ask_permissions() -> RuleBasedPermissionEngine:
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


async def _admit(service: DurableRunService, key: str = "k1") -> str:
    admitted = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="triage",
        idempotency_key=key,
    )
    return admitted.run_id


async def _exec(
    lease: RunLease,
    run_store: InMemoryRunStore,
    events: InMemoryEventStore,
    approvals: InMemoryApprovalStore,
    sent: list[dict[str, object]],
    provider: ScriptedProviderGateway,
    now: datetime,
) -> RunStatus:
    record = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=provider,
        registry=_send_tool(sent),
        permissions=_ask_permissions(),
        admit_fn=admit,
        resume=lease.resume,
        now=now,
    )
    return record.status


# ---- Crash 1: before the checkpoint marker -------------------------------------------------
async def test_crash_before_marker_restarts_fresh_no_false_resume() -> None:
    run_store, events, approvals, enqueued = (
        _CrashRunStore(),
        InMemoryEventStore(),
        _CrashApprovalStore(),
        list[str](),
    )
    service = _service(run_store, events, approvals, enqueued)
    sent: list[dict[str, object]] = []
    run_id = await _admit(service)

    t0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    lease1 = await run_store.claim(run_id, worker_id="w1", now=t0, lease_seconds=30)
    assert lease1 is not None and lease1.attempt == 1

    # The worker dies the instant it begins to suspend — before any marker is durable.
    run_store.crash_on = "mark_checkpoint"
    with pytest.raises(RuntimeError):
        await _exec(lease1, run_store, events, approvals, sent, _one_send("c1", "z@x"), t0)

    crashed = await run_store.get(run_id)
    assert crashed is not None and crashed.status is RunStatus.running
    assert crashed.suspend_checkpoint is False and crashed.checkpoint_attempt == 0
    assert await approvals.pending_for_run(run_id) == []  # no approval ever persisted
    assert sent == []

    # Reclaim after lease expiry: with no durable suspension there is nothing to resume — the
    # run must restart fresh (resume=False), never falsely resume a non-existent checkpoint.
    run_store.crash_on = None
    t1 = t0 + timedelta(seconds=60)
    lease2 = await run_store.claim(run_id, worker_id="w2", now=t1, lease_seconds=30)
    assert lease2 is not None and lease2.attempt == 2 and lease2.resume is False

    # The fresh restart re-issues the send, re-raises the approval, and (once granted) sends
    # exactly once: nothing was silently lost or duplicated by the before-marker crash.
    status = await _exec(lease2, run_store, events, approvals, sent, _one_send("c1", "z@x"), t1)
    assert status is RunStatus.waiting_approval
    pend = await approvals.pending_for_run(run_id)
    assert len(pend) == 1 and pend[0].run_attempt == 2
    assert await service.resolve_approval(
        pend[0].id, approved=True, resolved_by="user-1", actor="user-1"
    )
    t2 = t1 + timedelta(seconds=60)
    lease3 = await run_store.claim(run_id, worker_id="w3", now=t2, lease_seconds=30)
    assert lease3 is not None and lease3.resume is True
    status = await _exec(lease3, run_store, events, approvals, sent, _done(), t2)
    assert status is RunStatus.completed
    assert sent == [{"to": "z@x"}]  # exactly once


# ---- Crash 2: mid-batch — checkpoint written, rows/events not (the atomic unit rolls back) --
async def test_crash_midbatch_rolls_back_checkpoint_and_restarts_fresh() -> None:
    run_store, events, approvals, enqueued = (
        _CrashRunStore(),
        InMemoryEventStore(),
        _CrashApprovalStore(),
        list[str](),
    )
    service = _service(run_store, events, approvals, enqueued)
    sent: list[dict[str, object]] = []
    run_id = await _admit(service)

    t0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    lease1 = await run_store.claim(run_id, worker_id="w1", now=t0, lease_seconds=30)
    assert lease1 is not None

    # The worker dies mid-batch: the checkpoint marker is applied, then the approval row/event
    # fails. Because the checkpoint + rows + events are ONE atomic unit, the whole thing rolls
    # back — no marker, no approval, no tool.call event survive (blocker 2 atomicity).
    approvals.crash_create = True
    with pytest.raises(RuntimeError):
        await _exec(lease1, run_store, events, approvals, sent, _one_send("c1", "z@x"), t0)

    crashed = await run_store.get(run_id)
    assert crashed is not None and crashed.status is RunStatus.running
    assert crashed.suspend_checkpoint is False and crashed.checkpoint_attempt == 0
    assert crashed.checkpoint_batch_id == ""
    assert await approvals.pending_for_run(run_id) == []  # approval never persisted
    assert [e for e in events.snapshot("sess-1") if e.type is EventType.tool_call] == []

    # Reclaim: with nothing durable there is no checkpoint to honour — the run RESTARTS FRESH
    # (resume=False), never falsely resuming a rolled-back checkpoint.
    approvals.crash_create = False
    run_store.crash_on = None
    t1 = t0 + timedelta(seconds=60)
    lease2 = await run_store.claim(run_id, worker_id="w2", now=t1, lease_seconds=30)
    assert lease2 is not None and lease2.attempt == 2 and lease2.resume is False

    # The fresh restart re-issues the send + re-raises the approval, then (once granted) sends
    # exactly once — nothing was silently lost or double-applied by the mid-batch crash.
    status = await _exec(lease2, run_store, events, approvals, sent, _one_send("c1", "z@x"), t1)
    assert status is RunStatus.waiting_approval
    pend = await approvals.pending_for_run(run_id)
    assert len(pend) == 1 and pend[0].run_attempt == 2
    assert await service.resolve_approval(
        pend[0].id, approved=True, resolved_by="user-1", actor="user-1"
    )
    t2 = t1 + timedelta(seconds=60)
    lease3 = await run_store.claim(run_id, worker_id="w3", now=t2, lease_seconds=30)
    assert lease3 is not None and lease3.resume is True
    status = await _exec(lease3, run_store, events, approvals, sent, _done(), t2)
    assert status is RunStatus.completed
    assert sent == [{"to": "z@x"}]  # exactly once


# ---- Crash 3: after the approval, before the waiting_approval transition (THE bug) ---------
async def test_crash_after_approval_before_waiting_transition_keeps_approval() -> None:
    run_store, events, approvals, enqueued = (
        _CrashRunStore(),
        InMemoryEventStore(),
        InMemoryApprovalStore(),
        list[str](),
    )
    service = _service(run_store, events, approvals, enqueued)
    sent: list[dict[str, object]] = []
    run_id = await _admit(service)

    t0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    lease1 = await run_store.claim(run_id, worker_id="w1", now=t0, lease_seconds=30)
    assert lease1 is not None and lease1.attempt == 1

    # The approval batch + event are durable, but the worker dies before releasing the run
    # to waiting_approval: the row is still ``running`` with an approval bound to attempt 1.
    run_store.crash_on = "release"
    with pytest.raises(RuntimeError):
        await _exec(lease1, run_store, events, approvals, sent, _one_send("c1", "z@x"), t0)

    crashed = await run_store.get(run_id)
    assert crashed is not None and crashed.status is RunStatus.running
    assert crashed.suspend_checkpoint is True and crashed.checkpoint_attempt == 1
    pend = await approvals.pending_for_run(run_id)
    assert len(pend) == 1 and pend[0].run_attempt == 1
    assert sent == []

    # Reclaim: the marker makes it RESUME. The approval is still pending, so the reclaimed
    # worker safely restores waiting_approval without any fresh model/tool work.
    run_store.crash_on = None
    t1 = t0 + timedelta(seconds=60)
    lease2 = await run_store.claim(run_id, worker_id="w2", now=t1, lease_seconds=30)
    assert lease2 is not None and lease2.attempt == 2 and lease2.resume is True
    status = await _exec(lease2, run_store, events, approvals, sent, _done(), t1)
    assert status is RunStatus.waiting_approval
    assert sent == []
    restored = await run_store.get(run_id)
    assert restored is not None and restored.checkpoint_attempt == 1  # source attempt preserved

    # The operator approves the approval bound to the SOURCE attempt (1) even though the lease
    # attempt has since advanced to 2 — the decision is NOT silently discarded.
    assert await service.resolve_approval(
        pend[0].id, approved=True, resolved_by="user-1", actor="user-1"
    )
    requeued = await run_store.get(run_id)
    assert requeued is not None and requeued.status is RunStatus.queued
    assert requeued.resume_requested

    # The final resume executes the approved send exactly once and completes.
    t2 = t1 + timedelta(seconds=60)
    lease3 = await run_store.claim(run_id, worker_id="w3", now=t2, lease_seconds=30)
    assert lease3 is not None and lease3.resume is True
    status = await _exec(lease3, run_store, events, approvals, sent, _done(), t2)
    assert status is RunStatus.completed
    assert sent == [{"to": "z@x"}]  # approved effect applied exactly once


async def test_stale_approval_from_older_checkpoint_is_rejected() -> None:
    """An approval bound to a different (older) checkpoint attempt is refused (fail closed)."""
    run_store, events, approvals, enqueued = (
        _CrashRunStore(),
        InMemoryEventStore(),
        InMemoryApprovalStore(),
        list[str](),
    )
    service = _service(run_store, events, approvals, enqueued)
    sent: list[dict[str, object]] = []
    run_id = await _admit(service)

    t0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    lease1 = await run_store.claim(run_id, worker_id="w1", now=t0, lease_seconds=30)
    assert lease1 is not None
    run_store.crash_on = "release"
    with pytest.raises(RuntimeError):
        await _exec(lease1, run_store, events, approvals, sent, _one_send("c1", "z@x"), t0)
    pend = await approvals.pending_for_run(run_id)
    assert len(pend) == 1

    # Forge a second, stale approval bound to a foreign attempt (99) for the same run/action.
    stale = await approvals.create_pending(
        scope_id=_SCOPE,
        run_id=run_id,
        session_id="sess-1",
        tool="email.send",
        args={"to": "z@x"},
        call_id="c1",
        idempotency_key="stale",
        reason="first_use",
        expires_at=t0 + timedelta(hours=1),
        org_id="org-1",
        actor="user-1",
        action_hash=pend[0].action_hash,
        run_attempt=99,
    )
    # The run's outstanding checkpoint is attempt 1; the stale attempt-99 decision is rejected.
    assert (
        await service.resolve_approval(stale, approved=True, resolved_by="user-1", actor="user-1")
        is False
    )
    still = await run_store.get(run_id)
    assert still is not None and still.status is RunStatus.running  # unchanged


# ---- Crash 4: after the decision, before the dispatch --------------------------------------
async def test_crash_after_decision_before_dispatch_applies_effect_once() -> None:
    run_store, events, approvals, enqueued = (
        InMemoryRunStore(),
        InMemoryEventStore(),
        InMemoryApprovalStore(),
        list[str](),
    )
    service = _service(run_store, events, approvals, enqueued)
    sent: list[dict[str, object]] = []
    run_id = await _admit(service)

    t0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    lease1 = await run_store.claim(run_id, worker_id="w1", now=t0, lease_seconds=30)
    assert lease1 is not None
    status = await _exec(lease1, run_store, events, approvals, sent, _one_send("c1", "z@x"), t0)
    assert status is RunStatus.waiting_approval
    pend = await approvals.pending_for_run(run_id)

    # The decision + requeue commit, but the post-commit dispatch dies before enqueue.
    async def crashing_enqueue(_run_id: str) -> None:
        raise RuntimeError("dispatch crashed after the atomic resolve+requeue commit")

    crashing = DurableRunService(
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        scope_id=_SCOPE,
        enqueue=crashing_enqueue,
        admit_fn=admit,
    )
    with pytest.raises(RuntimeError):
        await crashing.resolve_approval(
            pend[0].id, approved=True, resolved_by="user-1", actor="user-1"
        )
    queued = await run_store.get(run_id)
    assert queued is not None and queued.status is RunStatus.queued and queued.resume_requested
    assert (await approvals.get(pend[0].id)).status == "granted"  # type: ignore[union-attr]

    # The reconciler redispatches the committed-but-undispatched queued run.
    redispatched: list[str] = []

    async def _capture(rid: str) -> None:
        redispatched.append(rid)

    t1 = queued.updated_at + timedelta(minutes=1)
    result = await reconcile_runs(run_store=run_store, enqueue=_capture, now=t1)
    assert result.redispatched == 1 and redispatched == [run_id]

    # A worker claims + resumes: the approved send executes exactly once (never twice).
    t2 = t1 + timedelta(seconds=1)
    lease2 = await run_store.claim(run_id, worker_id="w2", now=t2, lease_seconds=30)
    assert lease2 is not None and lease2.resume is True
    status = await _exec(lease2, run_store, events, approvals, sent, _done(), t2)
    assert status is RunStatus.completed
    assert sent == [{"to": "z@x"}]  # exactly once
