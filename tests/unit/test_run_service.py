"""Unit tests for the durable run service: admission, worker execution, resume, recovery.

Uses the in-memory run/approval/event doubles + a scripted provider so the worker-owned
execution contract is exercised end-to-end without Postgres/Redis. (The concurrency +
isolation guarantees are proven against Postgres in tests/integration/test_runs_postgres.py.)
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from keel_core.agents import AgentSpec, Scope
from keel_core.approvals import InMemoryApprovalStore
from keel_core.connectors import ConnectorTool
from keel_core.events import EventType
from keel_core.loop import ToolRegistry, admit, admit_run, admit_steer
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.projections import project_messages
from keel_core.protocols import ProviderChunk, ToolCall, ToolContext, Usage
from keel_core.run_service import (
    DurableRunService,
    _ControlWatcher,
    execute_run,
    prompt_persisted_in_log,
    reconcile_runs,
)
from keel_core.runs import (
    InMemoryRunStore,
    RunBudgetSpec,
    RunControlKind,
    RunLease,
    RunStatus,
    RunSurface,
    action_hash,
)
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import (
    FinishReason,
    PermissionDecision,
    ScopeKind,
    TrustLevel,
)

_SCOPE = "web:local"


def _agent() -> AgentSpec:
    return AgentSpec(
        id="agent-1",
        name="A",
        model="test/model",
        scope=Scope(id=_SCOPE, kind=ScopeKind.personal, trust=TrustLevel.trusted),
    )


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


async def test_admit_is_exactly_once_and_enqueues(  # noqa: D103
) -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[str] = []
    service = _service(run_store, events, approvals, enqueued)

    first = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="hello",
        idempotency_key="k1",
    )
    assert first.created is True
    # A retried request creates no duplicate message or run and does not re-enqueue.
    again = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="hello",
        idempotency_key="k1",
    )
    assert again.created is False and again.run_id == first.run_id
    assert enqueued == [first.run_id]
    messages = project_messages([e async for e in events.read("sess-1")])
    assert [m["role"] for m in messages] == ["user"]  # exactly one user turn


async def test_worker_executes_admitted_run_to_completion() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[str] = []
    service = _service(run_store, events, approvals, enqueued)
    admitted = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="hi",
        idempotency_key="k1",
    )
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="hello world", finish_reason=FinishReason.end_turn)]]
    )
    lease = await run_store.claim(admitted.run_id, worker_id="w1", lease_seconds=30)
    assert lease is not None
    final = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=provider,
        registry=ToolRegistry(),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        admit_fn=admit,
    )
    assert final.status is RunStatus.completed
    assert final.stop_reason == "completed"


async def test_agent_revoked_between_admit_and_claim_fails_closed() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    admitted = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="hi",
        idempotency_key="k1",
    )
    lease = await run_store.claim(admitted.run_id, worker_id="w1", lease_seconds=30)
    assert lease is not None

    async def denied(_record: object) -> bool:
        return False

    final = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=ScriptedProviderGateway(
            [[ProviderChunk(delta="x", finish_reason=FinishReason.end_turn)]]
        ),
        registry=ToolRegistry(),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        admit_fn=admit,
        visibility_check=denied,
    )
    assert final.status is RunStatus.failed
    assert final.error_kind == "agent_forbidden"


async def test_durable_approval_suspends_then_resolve_resumes() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[str] = []
    service = _service(run_store, events, approvals, enqueued)
    admitted = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="triage",
        idempotency_key="k1",
    )

    sent: list[dict[str, object]] = []

    async def read(args: dict[str, object], ctx: ToolContext) -> str:
        return "URGENT: email the report"

    async def send(args: dict[str, object], ctx: ToolContext) -> str:
        sent.append(args)
        return "sent"

    registry = ToolRegistry(
        [
            ConnectorTool(name="inbox.list", description="", action=read, outbound=False),
            ConnectorTool(name="email.send", description="", action=send, outbound=True),
        ]
    )
    permissions = RuleBasedPermissionEngine(
        [Rule("inbox.list", PermissionDecision.allow), Rule("email.send", PermissionDecision.ask)],
        default=PermissionDecision.ask,
    )

    def initial_provider() -> ScriptedProviderGateway:
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
                            arguments={"to": "z@x", "idempotency_key": "k1"},
                        ),
                        finish_reason=FinishReason.tool_use,
                    )
                ],
            ]
        )

    def resume_provider() -> ScriptedProviderGateway:
        # After the resolved outbound executes, one more model call ends the run.
        return ScriptedProviderGateway(
            [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
        )

    lease = await run_store.claim(admitted.run_id, worker_id="w1", lease_seconds=30)
    assert lease is not None
    suspended = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=initial_provider(),
        registry=registry,
        permissions=permissions,
        admit_fn=admit,
    )
    assert suspended.status is RunStatus.waiting_approval
    assert sent == []  # not sent yet — awaiting the durable approval

    # Resolve the approval (bound to attempt 1 internally) -> requeue + enqueue for resume.
    pending = await approvals.pending_for_run(admitted.run_id)
    assert len(pending) == 1
    enqueued.clear()
    ok = await service.resolve_approval(
        pending[0].id,
        approved=True,
        resolved_by="user-1",
        org_id="org-1",
    )
    assert ok and enqueued == [admitted.run_id]
    record = await run_store.get(admitted.run_id)
    assert record is not None and record.status is RunStatus.queued

    # A second worker claims and resumes; the approved outbound now executes.
    resume_lease = await run_store.claim(admitted.run_id, worker_id="w2", lease_seconds=30)
    assert resume_lease is not None
    final = await execute_run(
        lease=resume_lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=resume_provider(),
        registry=registry,
        permissions=permissions,
        admit_fn=admit,
        resume=True,
    )
    assert final.status is RunStatus.completed
    assert sent == [{"to": "z@x", "idempotency_key": "k1"}]


async def test_reconcile_redispatches_reclaims_and_expires() -> None:
    run_store = InMemoryRunStore()
    now = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    # (a) admitted-but-undispatched
    await run_store.admit(
        run_id="r-undispatched",
        scope_id=_SCOPE,
        org_id="o",
        actor="u",
        agent_id="a",
        session_id="s1",
        surface=RunSurface.web.value,
        idempotency_key="k1",
        budget=run_store_budget(),
        expires_at=now + timedelta(hours=1),
        now=now,
    )
    # (b) running with an expired lease
    await run_store.admit(
        run_id="r-expired-lease",
        scope_id=_SCOPE,
        org_id="o",
        actor="u",
        agent_id="a",
        session_id="s2",
        surface=RunSurface.web.value,
        idempotency_key="k2",
        budget=run_store_budget(),
        expires_at=now + timedelta(hours=1),
        now=now,
    )
    await run_store.mark_queued("r-expired-lease", now=now)
    await run_store.claim("r-expired-lease", worker_id="w1", now=now, lease_seconds=30)
    # (c) past the admission deadline
    await run_store.admit(
        run_id="r-past-deadline",
        scope_id=_SCOPE,
        org_id="o",
        actor="u",
        agent_id="a",
        session_id="s3",
        surface=RunSurface.web.value,
        idempotency_key="k3",
        budget=run_store_budget(),
        expires_at=now - timedelta(seconds=1),
        now=now,
    )

    enqueued: list[str] = []

    async def enqueue(run_id: str) -> None:
        enqueued.append(run_id)

    result = await reconcile_runs(
        run_store=run_store, enqueue=enqueue, now=now + timedelta(seconds=60)
    )
    assert result.redispatched == 1
    assert result.reclaimed == 1
    assert result.expired == 1
    assert "r-undispatched" in enqueued and "r-expired-lease" in enqueued
    past = await run_store.get("r-past-deadline")
    assert past is not None and past.status is RunStatus.expired


def run_store_budget() -> RunBudgetSpec:
    return RunBudgetSpec(max_iterations=5, token_budget=1000)


async def test_interrupt_control_is_consumed_by_watcher() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    admitted = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="hi",
        idempotency_key="k1",
    )
    # A durable interrupt requested before the worker even claims must be honoured.
    assert await service.interrupt(admitted.run_id, requested_by="user-1")
    lease = await run_store.claim(admitted.run_id, worker_id="w1", lease_seconds=30)
    assert lease is not None
    # A provider that would loop forever if not interrupted (many tool-call turns).
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id=f"c{i}", name="noop", arguments={}),
                    finish_reason=FinishReason.tool_use,
                )
            ]
            for i in range(50)
        ]
    )

    async def noop(args: dict[str, object], ctx: ToolContext) -> str:
        return "ok"

    final = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=provider,
        registry=ToolRegistry([ConnectorTool(name="noop", description="", action=noop)]),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        admit_fn=admit,
        control_poll_seconds=0.01,
    )
    assert final.status is RunStatus.interrupted
    assert final.stop_reason == "interrupted"
    _ = EventType  # keep import used for readers cross-referencing the event vocabulary


# ----------------------------------------------------------------------------------------
# Helpers for the hardening tests (M3.6 review fixes).
# ----------------------------------------------------------------------------------------
def _tool_turns(n: int, name: str = "noop") -> ScriptedProviderGateway:
    return ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id=f"c{i}", name=name, arguments={}),
                    finish_reason=FinishReason.tool_use,
                )
            ]
            for i in range(n)
        ]
    )


class _RenewFailingStore:
    """Wraps a RunStore but forces ``renew`` to fail — models a lost/reclaimed lease."""

    def __init__(self, inner: InMemoryRunStore) -> None:
        self._inner = inner
        self.renew_calls = 0

    async def renew(self, lease: RunLease, *, lease_seconds: int, now: Any = None) -> bool:
        self.renew_calls += 1
        return False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _RenewCountingStore:
    """Wraps a RunStore counting ``renew`` calls (a live keeper keeps the lease alive)."""

    def __init__(self, inner: InMemoryRunStore) -> None:
        self._inner = inner
        self.renew_calls = 0

    async def renew(self, lease: RunLease, *, lease_seconds: int, now: Any = None) -> bool:
        self.renew_calls += 1
        return await self._inner.renew(lease, lease_seconds=lease_seconds, now=now)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


async def _admit_and_claim(
    service: DurableRunService,
    run_store: InMemoryRunStore,
    *,
    content: str = "hi",
    budget: RunBudgetSpec | None = None,
    worker_id: str = "w1",
    lease_seconds: int = 30,
) -> tuple[str, RunLease]:
    admitted = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content=content,
        idempotency_key="k1",
        budget=budget,
    )
    lease = await run_store.claim(admitted.run_id, worker_id=worker_id, lease_seconds=lease_seconds)
    assert lease is not None
    return admitted.run_id, lease


# ----- Item 1: lease renewal + effect fencing -------------------------------------------
async def test_lease_keeper_renews_a_long_running_run() -> None:
    inner = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(inner, events, approvals, [])
    run_id, lease = await _admit_and_claim(service, inner, lease_seconds=1)
    counting = _RenewCountingStore(inner)

    async def slow(args: dict[str, object], ctx: ToolContext) -> str:
        await asyncio.sleep(0.03)
        return "ok"

    provider = ScriptedProviderGateway(
        [
            *(
                [
                    ProviderChunk(
                        tool_call=ToolCall(id=f"c{i}", name="slow", arguments={}),
                        finish_reason=FinishReason.tool_use,
                    )
                ]
                for i in range(5)
            ),
            [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
        ]
    )
    final = await execute_run(
        lease=lease,
        run_store=counting,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=provider,
        registry=ToolRegistry([ConnectorTool(name="slow", description="", action=slow)]),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        admit_fn=admit,
        heartbeat_seconds=0.01,
    )
    assert final.status is RunStatus.completed
    assert counting.renew_calls >= 1  # the keeper renewed the lease well before expiry


async def test_lost_lease_fences_out_run_with_no_terminal_write() -> None:
    inner = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(inner, events, approvals, [])
    run_id, lease = await _admit_and_claim(service, inner)
    # A durable cancel is pending; it must remain pending after a fenced-out bail (re-honored).
    assert await service.cancel(run_id, requested_by="user-1")
    fencing = _RenewFailingStore(inner)

    async def slow(args: dict[str, object], ctx: ToolContext) -> str:
        await asyncio.sleep(0.03)
        return "ok"

    final = await execute_run(
        lease=lease,
        run_store=fencing,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=_tool_turns(50, name="slow"),
        registry=ToolRegistry([ConnectorTool(name="slow", description="", action=slow)]),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        admit_fn=admit,
        heartbeat_seconds=0.01,
        control_poll_seconds=0.5,
    )
    # Fenced out: no terminal write happened; the run row is still owned/running for the
    # reclaiming worker, and the durable cancel control is still pending to be re-honored.
    assert final.status is RunStatus.running
    record = await inner.get(run_id)
    assert record is not None and record.status is RunStatus.running
    assert [c.kind for c in await inner.peek_control(run_id)] == [RunControlKind.cancel]


async def test_two_worker_reclaim_fences_first_owner() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    admitted = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="hi",
        idempotency_key="k1",
    )
    t0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    first = await run_store.claim(admitted.run_id, worker_id="w1", now=t0, lease_seconds=30)
    assert first is not None
    # The lease lapses; a second worker reclaims (attempt advances) and completes the run.
    t1 = t0 + timedelta(seconds=40)
    second = await run_store.claim(admitted.run_id, worker_id="w2", now=t1, lease_seconds=30)
    assert second is not None and second.attempt == 2
    final = await execute_run(
        lease=second,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=ScriptedProviderGateway(
            [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
        ),
        registry=ToolRegistry(),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        admit_fn=admit,
    )
    assert final.status is RunStatus.completed
    # The fenced-out first owner can neither heartbeat nor terminalize.
    assert await run_store.heartbeat(first) is False


# ----- Item 2: atomic / recoverable admission -------------------------------------------
async def test_admit_repairs_incomplete_admission() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[str] = []
    service = _service(run_store, events, approvals, enqueued)
    # Simulate a crash after the row was created but before the prompt/queue/enqueue steps.
    await run_store.create(
        run_id="run-1",
        scope_id=_SCOPE,
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        idempotency_key="k1",
        budget=RunBudgetSpec(),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    # The idempotent retry must *repair* the row: persist the prompt, queue, and enqueue.
    result = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="the real prompt",
        idempotency_key="k1",
    )
    assert result.run_id == "run-1" and result.created is False
    record = await run_store.get("run-1")
    assert record is not None and record.status is RunStatus.queued
    assert enqueued == ["run-1"]
    messages = project_messages([e async for e in events.read("sess-1")])
    assert [m["role"] for m in messages] == ["user"]  # exactly one user turn, never prompt-less
    # A second repair is an idempotent no-op (no duplicate prompt, no re-enqueue).
    again = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="the real prompt",
        idempotency_key="k1",
    )
    assert again.created is False
    assert enqueued == ["run-1"]
    messages = project_messages([e async for e in events.read("sess-1")])
    assert [m["role"] for m in messages] == ["user"]


async def test_reconcile_never_dispatches_a_prompt_less_run() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    now = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    await run_store.create(
        run_id="run-1",
        scope_id=_SCOPE,
        org_id="o",
        actor="u",
        agent_id="a",
        session_id="s1",
        surface=RunSurface.web.value,
        idempotency_key="k1",
        budget=RunBudgetSpec(),
        expires_at=now + timedelta(hours=1),
        now=now,
    )
    enqueued: list[str] = []

    async def enqueue(run_id: str) -> None:
        enqueued.append(run_id)

    async def prompt_persisted(record: Any) -> bool:
        return await prompt_persisted_in_log(events, record.session_id, record.id)

    later = now + timedelta(seconds=60)
    result = await reconcile_runs(
        run_store=run_store, enqueue=enqueue, prompt_persisted=prompt_persisted, now=later
    )
    assert result.redispatched == 0 and enqueued == []  # prompt-less -> never dispatched
    # Once the prompt is durably admitted, reconciliation dispatches it exactly once.
    await admit(events, "s1", _SCOPE, "prompt")
    # tag the log so the marker check sees it (admit_run marks the admission turn)
    await admit_run(events, "s1", _SCOPE, "prompt", "run-1")
    result = await reconcile_runs(
        run_store=run_store, enqueue=enqueue, prompt_persisted=prompt_persisted, now=later
    )
    assert result.redispatched == 1 and enqueued == ["run-1"]


# ----- Item 3: approval routing + resume marker -----------------------------------------
async def _suspend_on_approval(
    service: DurableRunService,
    run_store: InMemoryRunStore,
    events: InMemoryEventStore,
    approvals: InMemoryApprovalStore,
) -> str:
    admitted = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="triage",
        idempotency_key="k1",
    )

    async def send(args: dict[str, object], ctx: ToolContext) -> str:
        return "sent"

    registry = ToolRegistry(
        [ConnectorTool(name="email.send", description="", action=send, outbound=True)]
    )
    permissions = RuleBasedPermissionEngine(
        [Rule("email.send", PermissionDecision.ask)], default=PermissionDecision.ask
    )
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c1", name="email.send", arguments={"to": "z@x"}),
                    finish_reason=FinishReason.tool_use,
                )
            ]
        ]
    )
    lease = await run_store.claim(admitted.run_id, worker_id="w1", lease_seconds=30)
    assert lease is not None
    suspended = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=provider,
        registry=registry,
        permissions=permissions,
        admit_fn=admit,
    )
    assert suspended.status is RunStatus.waiting_approval
    return admitted.run_id


async def test_resolve_approval_marks_resume_and_binds_org_state() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[str] = []
    service = _service(run_store, events, approvals, enqueued)
    run_id = await _suspend_on_approval(service, run_store, events, approvals)
    pending = await approvals.pending_for_run(run_id)
    assert len(pending) == 1

    # Cross-org resolution is denied (fail closed), leaving the run suspended.
    assert (
        await service.resolve_approval(
            pending[0].id, approved=True, resolved_by="attacker", org_id="org-OTHER"
        )
        is False
    )
    assert (await run_store.get(run_id)).status is RunStatus.waiting_approval  # type: ignore[union-attr]

    # The correct-org resolution requeues with an explicit resume marker + run_interactive job.
    enqueued.clear()
    assert (
        await service.resolve_approval(
            pending[0].id, approved=True, resolved_by="user-1", org_id="org-1"
        )
        is True
    )
    assert enqueued == [run_id]
    record = await run_store.get(run_id)
    assert record is not None and record.status is RunStatus.queued and record.resume_requested

    # The claiming worker learns resume atomically from the claim (never inferred from status).
    lease = await run_store.claim(run_id, worker_id="w2", lease_seconds=30)
    assert lease is not None and lease.resume is True
    record = await run_store.get(run_id)
    assert record is not None and record.resume_requested is False  # marker consumed on claim


async def test_resolve_approval_denies_when_run_not_waiting() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    # An approval whose run is not currently waiting_approval must fail closed.
    approval_id = await approvals.create_pending(
        scope_id=_SCOPE,
        run_id="ghost",
        session_id="sess-1",
        tool="email.send",
        args={},
        call_id="c1",
        idempotency_key="i1",
        reason="first_use",
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    assert await service.resolve_approval(approval_id, approved=True, resolved_by="u") is False


async def test_expire_approvals_resumes_suspended_run() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[str] = []
    service = _service(run_store, events, approvals, enqueued)
    run_id = await _suspend_on_approval(service, run_store, events, approvals)
    pending = await approvals.pending_for_run(run_id)
    # Force the approval past its deadline, then run the expiry sweep.
    approvals._rows[pending[0].id].expires_at = datetime.now(UTC) - timedelta(seconds=1)
    enqueued.clear()
    resumed = await service.expire_approvals()
    assert resumed == 1 and enqueued == [run_id]
    record = await run_store.get(run_id)
    assert record is not None and record.status is RunStatus.queued and record.resume_requested


# ----- Item 4: durable controls (claim/ack semantics) -----------------------------------
async def test_steer_is_acked_only_after_the_durable_turn_is_appended() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    run_id, lease = await _admit_and_claim(service, run_store)
    assert await service.steer(run_id, requested_by="user-1", text="use the staging server")
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c1", name="noop", arguments={}),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="ok", finish_reason=FinishReason.end_turn)],
        ]
    )

    async def noop(args: dict[str, object], ctx: ToolContext) -> str:
        return "ok"

    final = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=provider,
        registry=ToolRegistry([ConnectorTool(name="noop", description="", action=noop)]),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        admit_fn=admit,
        control_poll_seconds=0.01,
    )
    assert final.status is RunStatus.completed
    messages = project_messages([e async for e in events.read("sess-1")])
    assert any(
        m["role"] == "user" and "staging server" in str(m.get("content", "")) for m in messages
    )
    assert await run_store.peek_control(run_id) == []  # steer acked after the durable turn


# ----- Item 5: authoritative budgets ----------------------------------------------------
async def test_persisted_max_iterations_bounds_the_run() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    run_id, lease = await _admit_and_claim(
        service, run_store, budget=RunBudgetSpec(max_iterations=2)
    )
    assert lease.max_iterations == 2
    provider = _tool_turns(20)
    tool_runs = 0

    async def noop(args: dict[str, object], ctx: ToolContext) -> str:
        nonlocal tool_runs
        tool_runs += 1
        return "ok"

    final = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=provider,
        registry=ToolRegistry([ConnectorTool(name="noop", description="", action=noop)]),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        admit_fn=admit,
    )
    assert final.status is RunStatus.completed  # max_iterations is a named, non-error stop
    assert tool_runs == 2  # bounded by the persisted budget, not the 20 scripted turns


async def test_persisted_token_budget_bounds_the_run() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    run_id, lease = await _admit_and_claim(
        service, run_store, budget=RunBudgetSpec(max_iterations=20, token_budget=5)
    )
    tool_runs = 0

    async def noop(args: dict[str, object], ctx: ToolContext) -> str:
        nonlocal tool_runs
        tool_runs += 1
        return "ok"

    # Each turn reports 10 completion tokens (> the 5-token budget) so the run stops early.
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id=f"c{i}", name="noop", arguments={}),
                    finish_reason=FinishReason.tool_use,
                    usage=Usage(completion_tokens=10),
                )
            ]
            for i in range(20)
        ]
    )
    final = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=provider,
        registry=ToolRegistry([ConnectorTool(name="noop", description="", action=noop)]),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        admit_fn=admit,
    )
    assert final.status is RunStatus.completed  # budget_exhausted is a named, non-error stop
    assert tool_runs == 1  # one turn ran, then the token budget halted the loop


# ----- Races item 1: concurrent / synchronized admission --------------------------------
async def test_concurrent_admission_is_atomic_single_prompt_and_enqueue() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[str] = []
    service = _service(run_store, events, approvals, enqueued)

    async def one() -> Any:
        return await service.admit(
            org_id="org-1",
            actor="user-1",
            agent_id="agent-1",
            session_id="sess-1",
            surface=RunSurface.web.value,
            content="hello",
            idempotency_key="k1",
        )

    # N admitters race the same idempotency key (retries / racing processes).
    results = await asyncio.gather(*[one() for _ in range(6)])
    run_ids = {r.run_id for r in results}
    assert len(run_ids) == 1  # exactly one durable run
    assert sum(1 for r in results if r.created) == 1  # exactly one creator
    (run_id,) = run_ids
    # Exactly one durable user prompt and exactly one worker enqueue (no duplicate event/job).
    messages = project_messages([e async for e in events.read("sess-1")])
    assert [m["role"] for m in messages] == ["user"]
    assert enqueued == [run_id]


async def test_admit_repairs_after_prompt_persisted_before_queue() -> None:
    # Crash *after* the prompt was durably appended but *before* the queue/enqueue steps.
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    enqueued: list[str] = []
    service = _service(run_store, events, InMemoryApprovalStore(), enqueued)
    await run_store.create(
        run_id="run-1",
        scope_id=_SCOPE,
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        idempotency_key="k1",
        budget=RunBudgetSpec(),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    await admit_run(events, "sess-1", _SCOPE, "the real prompt", "run-1")
    await run_store.mark_prompt_persisted("run-1")
    # The retry repairs: queue + enqueue, never a duplicate prompt.
    result = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="the real prompt",
        idempotency_key="k1",
    )
    assert result.run_id == "run-1" and result.created is False
    record = await run_store.get("run-1")
    assert record is not None and record.status is RunStatus.queued
    assert enqueued == ["run-1"]
    messages = project_messages([e async for e in events.read("sess-1")])
    assert [m["role"] for m in messages] == ["user"]  # exactly one user turn


# ----- Races item 2: pre-effect fencing -------------------------------------------------
async def test_pre_effect_fence_blocks_tool_batch_after_provider_returns() -> None:
    # A cancel/lease-loss that trips *after* the provider returned but *before* the tool
    # batch must prevent every subsequent external effect (fail closed).
    from keel_core.loop import run as loop_run

    events = InMemoryEventStore()
    ran: list[int] = []

    async def effect(args: dict[str, object], ctx: ToolContext) -> str:
        ran.append(1)
        return "did it"

    calls = {"n": 0}

    def interrupt() -> bool:
        calls["n"] += 1
        # 1st call: top-of-loop (allow the provider turn). 2nd call: the pre-tool fence.
        return calls["n"] >= 2

    result = await loop_run(
        agent=_agent(),
        session_id="sess-1",
        store=events,
        provider=_tool_turns(1, name="do"),
        registry=ToolRegistry([ConnectorTool(name="do", description="", action=effect)]),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        interrupt=interrupt,
    )
    from keel_core.types import StopReason

    assert result.reason is StopReason.interrupted
    assert ran == []  # the external effect never ran — fenced before the batch


# ----- Races item 3: lease renewal exception fails closed -------------------------------
class _RenewRaisingStore:
    """Wraps a RunStore but makes ``renew`` *raise* — models a DB error during renewal."""

    def __init__(self, inner: InMemoryRunStore) -> None:
        self._inner = inner
        self.renew_calls = 0

    async def renew(self, lease: RunLease, *, lease_seconds: int, now: Any = None) -> bool:
        self.renew_calls += 1
        raise RuntimeError("renewal backend unavailable")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


async def test_renew_exception_marks_lease_lost_with_no_terminal_write() -> None:
    inner = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(inner, events, approvals, [])
    run_id, lease = await _admit_and_claim(service, inner)
    raising = _RenewRaisingStore(inner)

    async def slow(args: dict[str, object], ctx: ToolContext) -> str:
        await asyncio.sleep(0.03)
        return "ok"

    # execute_run returns normally (the renew exception is *not* re-raised / does not mask
    # the primary flow); the lease is marked lost so no terminal state is written.
    final = await execute_run(
        lease=lease,
        run_store=raising,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=_tool_turns(50, name="slow"),
        registry=ToolRegistry([ConnectorTool(name="slow", description="", action=slow)]),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        admit_fn=admit,
        heartbeat_seconds=0.01,
        control_poll_seconds=0.5,
    )
    assert raising.renew_calls >= 1
    assert final.status is RunStatus.running  # never falsely terminalized
    record = await inner.get(run_id)
    assert record is not None and record.status is RunStatus.running  # reclaimable, not lost work


# ----- Races item 4: steering idempotency -----------------------------------------------
async def test_steer_replay_after_crash_does_not_duplicate_message() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    service = _service(run_store, events, InMemoryApprovalStore(), [])
    run_id, _ = await _admit_and_claim(service, run_store)
    assert await service.steer(run_id, requested_by="user-1", text="use staging")
    control = (await run_store.peek_control(run_id))[0]
    # Simulate: a prior owner appended the durable steer turn, then crashed before acking.
    await admit_steer(events, "sess-1", _SCOPE, "use staging", run_id, control.id)
    # A reclaiming watcher re-drains the still-pending control: it must NOT append a duplicate
    # steering message, and must ack safely.
    watcher = _ControlWatcher(
        run_store=run_store,
        event_store=events,
        admit_fn=admit,
        run_id=run_id,
        session_id="sess-1",
        scope_id=_SCOPE,
    )
    await watcher._drain()
    messages = project_messages([e async for e in events.read("sess-1")])
    steers = [m for m in messages if m["role"] == "user" and "staging" in str(m.get("content", ""))]
    assert len(steers) == 1  # exactly one steer turn despite the replay
    assert await run_store.peek_control(run_id) == []  # acked exactly once, safely


# ----- Races item 5: approval binding to the current attempt -----------------------------
async def test_approval_binding_rejects_wrong_attempt_and_tampered_hash() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    admitted = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="hi",
        idempotency_key="k1",
    )
    run_id = admitted.run_id
    lease = await run_store.claim(run_id, worker_id="w1", lease_seconds=30)  # attempt 1
    assert lease is not None
    await run_store.release(lease, to_status=RunStatus.waiting_approval)
    args = {"to": "z@x"}

    async def _pending(*, run_attempt: int, hash_: str) -> str:
        return await approvals.create_pending(
            scope_id=_SCOPE,
            run_id=run_id,
            session_id="sess-1",
            tool="email.send",
            args=args,
            call_id="c1",
            idempotency_key=f"i-{run_attempt}-{hash_[:6]}",
            reason="first_use",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
            org_id="org-1",
            actor="user-1",
            action_hash=hash_,
            run_attempt=run_attempt,
        )

    good_hash = action_hash("email.send", args)
    # (a) A stale attempt-0 approval cannot resume this attempt-1 run.
    stale = await _pending(run_attempt=0, hash_=good_hash)
    assert (
        await service.resolve_approval(stale, approved=True, resolved_by="user-1", org_id="org-1")
        is False
    )
    assert (await run_store.get(run_id)).status is RunStatus.waiting_approval  # type: ignore[union-attr]
    # (b) A tampered action hash (not recomputable from the stored args) is rejected.
    tampered = await _pending(run_attempt=1, hash_="deadbeef")
    assert (
        await service.resolve_approval(
            tampered, approved=True, resolved_by="user-1", org_id="org-1"
        )
        is False
    )
    assert (await run_store.get(run_id)).status is RunStatus.waiting_approval  # type: ignore[union-attr]
    # (c) The correctly-bound decision (attempt 1, valid recomputed hash) is accepted.
    good = await _pending(run_attempt=1, hash_=good_hash)
    assert (
        await service.resolve_approval(good, approved=True, resolved_by="user-1", org_id="org-1")
        is True
    )
    record = await run_store.get(run_id)
    assert record is not None and record.status is RunStatus.queued and record.resume_requested


# ----- Races item 6: centralized expiry routing -----------------------------------------
async def test_expire_approvals_routes_durable_and_legacy_separately() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[str] = []
    service = _service(run_store, events, approvals, enqueued)
    # A durable interactive approval (suspended run).
    run_id = await _suspend_on_approval(service, run_store, events, approvals)
    dur_pending = await approvals.pending_for_run(run_id)
    approvals._rows[dur_pending[0].id].expires_at = datetime.now(UTC) - timedelta(seconds=1)
    # A legacy approval whose run has no durable run row.
    await approvals.create_pending(
        scope_id=_SCOPE,
        run_id="legacy-run",
        session_id="sched-sess",
        tool="email.send",
        args={},
        call_id="c",
        idempotency_key="legacy",
        reason="tainted",
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    legacy_calls: list[tuple[str, str, str]] = []

    async def legacy_resume(record: Any) -> None:
        legacy_calls.append((record.session_id, record.run_id, record.scope_id))

    enqueued.clear()
    resumed = await service.expire_approvals(legacy_resume=legacy_resume)
    assert resumed == 2
    assert enqueued == [run_id]  # durable interactive -> run_interactive resume
    assert legacy_calls == [("sched-sess", "legacy-run", _SCOPE)]  # legacy -> resume_run path


# ----- Races item 7: cumulative iteration budget + provider cost ------------------------
async def test_max_iterations_one_blocks_a_second_iteration_after_resume() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    enqueued: list[str] = []
    service = _service(run_store, events, approvals, enqueued)
    admitted = await service.admit(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="triage",
        idempotency_key="k1",
        budget=RunBudgetSpec(max_iterations=1),
    )
    tool_runs = 0

    async def send(args: dict[str, object], ctx: ToolContext) -> str:
        nonlocal tool_runs
        tool_runs += 1
        return "sent"

    registry = ToolRegistry(
        [ConnectorTool(name="email.send", description="", action=send, outbound=True)]
    )
    permissions = RuleBasedPermissionEngine(
        [Rule("email.send", PermissionDecision.ask)], default=PermissionDecision.ask
    )

    def ask_turn(call_id: str, to: str) -> ScriptedProviderGateway:
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

    lease = await run_store.claim(admitted.run_id, worker_id="w1", lease_seconds=30)
    assert lease is not None and lease.iterations_used == 0
    suspended = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=ask_turn("c1", "z@x"),
        registry=registry,
        permissions=permissions,
        admit_fn=admit,
    )
    assert suspended.status is RunStatus.waiting_approval
    record = await run_store.get(admitted.run_id)
    assert record is not None and record.iterations == 1  # the pending batch consumes the one iter

    pending = await approvals.pending_for_run(admitted.run_id)
    assert await service.resolve_approval(
        pending[0].id, approved=True, resolved_by="user-1", org_id="org-1"
    )
    resume_lease = await run_store.claim(admitted.run_id, worker_id="w2", lease_seconds=30)
    assert resume_lease is not None and resume_lease.iterations_used == 1
    # On resume the model would want ANOTHER tool turn, but max_iterations=1 is exhausted.
    final = await execute_run(
        lease=resume_lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=ask_turn("c2", "a@b"),
        registry=registry,
        permissions=permissions,
        admit_fn=admit,
        resume=True,
    )
    assert final.status is RunStatus.completed  # max_iterations is a named, non-error stop
    assert tool_runs == 1  # only the approved first tool ran — no second iteration executed


async def test_provider_reported_cost_is_propagated_to_the_run() -> None:
    run_store = InMemoryRunStore()
    events = InMemoryEventStore()
    approvals = InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    run_id, lease = await _admit_and_claim(service, run_store)
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    delta="done",
                    finish_reason=FinishReason.end_turn,
                    usage=Usage(prompt_tokens=2, completion_tokens=3, cost_usd=0.05),
                )
            ]
        ]
    )
    final = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=provider,
        registry=ToolRegistry(),
        permissions=RuleBasedPermissionEngine([], default=PermissionDecision.allow),
        admit_fn=admit,
    )
    assert final.status is RunStatus.completed
    assert final.cost_usd == 0.05  # provider-reported cost persisted on the run
