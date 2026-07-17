"""Unit tests for the durable run service: admission, worker execution, resume, recovery.

Uses the in-memory run/approval/event doubles + a scripted provider so the worker-owned
execution contract is exercised end-to-end without Postgres/Redis. (The concurrency +
isolation guarantees are proven against Postgres in tests/integration/test_runs_postgres.py.)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from keel_core.agents import AgentSpec, Scope
from keel_core.approvals import InMemoryApprovalStore
from keel_core.connectors import ConnectorTool
from keel_core.events import EventType
from keel_core.loop import ToolRegistry, admit
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.projections import project_messages
from keel_core.protocols import ProviderChunk, ToolCall, ToolContext
from keel_core.run_service import (
    DurableRunService,
    execute_run,
    reconcile_runs,
)
from keel_core.runs import (
    InMemoryRunStore,
    RunStatus,
    RunSurface,
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

    # Resolve the approval (bound to attempt 1) -> requeue + enqueue for resume.
    pending = await approvals.pending_for_run(admitted.run_id)
    assert len(pending) == 1
    enqueued.clear()
    ok = await service.resolve_approval(
        pending[0].id,
        approved=True,
        resolved_by="user-1",
        expected_action_hash=pending[0].action_hash,
        expected_run_attempt=1,
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


def run_store_budget():  # type: ignore[no-untyped-def]
    from keel_core.runs import RunBudgetSpec

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
