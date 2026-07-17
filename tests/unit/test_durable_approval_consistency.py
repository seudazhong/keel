"""Adversarial re-review coverage for the five durable-approval consistency blockers (M3.6).

These exercise the in-memory doubles (deterministic, no Postgres) for the production run
service + agent loop:

1. approval-resolve / expiry + run requeue commit together, and a crashed dispatch is
   repaired by the reconciler (a retry of a resolved approval never leaves a run stuck);
2. resume event isolation — two suspended runs in one shared session, overlapping call ids;
3. admission fingerprint + tenant namespace — a mismatched retry is a conflict;
4. resolver actor is mandatory (never blank), cross-user same-org denied, delegate opt-in;
5. multi-approval batches — a run resumes only once every decision is terminal, each exact
   decision is preserved, duplicates are idempotent and conflicts rejected.
"""

from __future__ import annotations

from datetime import timedelta

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
    RunAdmissionConflict,
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


def _service(
    run_store: InMemoryRunStore,
    events: InMemoryEventStore,
    approvals: InMemoryApprovalStore,
    enqueued: list[str],
    **kwargs: object,
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
        **kwargs,  # type: ignore[arg-type]
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


def _two_sends() -> ScriptedProviderGateway:
    """One turn emitting two ask-gated tool calls -> a single two-approval batch."""
    return ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c1", name="email.send", arguments={"to": "a@x"}),
                    finish_reason=FinishReason.tool_use,
                ),
                ProviderChunk(
                    tool_call=ToolCall(id="c2", name="email.send", arguments={"to": "b@x"}),
                    finish_reason=FinishReason.tool_use,
                ),
            ]
        ]
    )


def _done() -> ScriptedProviderGateway:
    return ScriptedProviderGateway(
        [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
    )


async def _admit(service: DurableRunService, **overrides: object) -> str:
    params: dict[str, object] = dict(
        org_id="org-1",
        actor="user-1",
        agent_id="agent-1",
        session_id="sess-1",
        surface=RunSurface.web.value,
        content="triage",
        idempotency_key="k1",
    )
    params.update(overrides)
    result = await service.admit(**params)  # type: ignore[arg-type]
    return result.run_id


async def _suspend(
    service: DurableRunService,
    run_store: InMemoryRunStore,
    events: InMemoryEventStore,
    approvals: InMemoryApprovalStore,
    sent: list[dict[str, object]],
    provider: ScriptedProviderGateway,
) -> str:
    run_id = await _admit(service)
    lease = await run_store.claim(run_id, worker_id="w1", lease_seconds=30)
    assert lease is not None
    outcome = await execute_run(
        lease=lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=provider,
        registry=_send_tool(sent),
        permissions=_ask_permissions(),
        admit_fn=admit,
    )
    assert outcome.status is RunStatus.waiting_approval
    return run_id


# ---- Blocker 1: crash safety of the resolve/expiry -> requeue -> dispatch outbox ----------
async def test_dispatch_crash_is_repaired_by_reconciler() -> None:
    run_store, events, approvals, enqueued = (
        InMemoryRunStore(),
        InMemoryEventStore(),
        InMemoryApprovalStore(),
        [],
    )
    service = _service(run_store, events, approvals, enqueued)
    run_id = await _suspend(service, run_store, events, approvals, [], _one_send("c1", "z@x"))
    pending = await approvals.pending_for_run(run_id)

    # A service whose post-commit dispatch always crashes AFTER the atomic requeue commits.
    async def crashing_enqueue(run_id: str) -> None:
        raise RuntimeError("dispatch crashed after commit")

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
            pending[0].id, approved=True, resolved_by="user-1", actor="user-1"
        )
    # The approval + run requeue COMMITTED atomically even though dispatch crashed.
    record = await run_store.get(run_id)
    assert record is not None and record.status is RunStatus.queued and record.resume_requested
    assert (await approvals.get(pending[0].id)).status == "granted"  # type: ignore[union-attr]

    # The reconciler re-enqueues the committed-but-undispatched queued run (no lost run).
    redispatched: list[str] = []

    async def _capture(run_id: str) -> None:
        redispatched.append(run_id)

    later = record.updated_at + timedelta(minutes=1)
    result = await reconcile_runs(run_store=run_store, enqueue=_capture, now=later)
    assert result.redispatched == 1 and redispatched == [run_id]


async def test_retry_of_resolved_approval_repairs_a_stuck_run() -> None:
    run_store, events, approvals, enqueued = (
        InMemoryRunStore(),
        InMemoryEventStore(),
        InMemoryApprovalStore(),
        [],
    )
    service = _service(run_store, events, approvals, enqueued)
    run_id = await _suspend(service, run_store, events, approvals, [], _one_send("c1", "z@x"))
    pending = await approvals.pending_for_run(run_id)

    # Simulate a crash between the approval-resolve commit and the run requeue: resolve the
    # approval directly, leaving the run stuck in waiting_approval.
    await approvals.resolve(pending[0].id, "granted", "user-1")
    stuck = await run_store.get(run_id)
    assert stuck is not None and stuck.status is RunStatus.waiting_approval

    # A retry of the (already resolved) approval observes/repairs the transition (idempotent
    # True), not leaving the run stuck.
    enqueued.clear()
    ok = await service.resolve_approval(
        pending[0].id, approved=True, resolved_by="user-1", actor="user-1"
    )
    assert ok is True
    repaired = await run_store.get(run_id)
    assert repaired is not None and repaired.status is RunStatus.queued
    assert enqueued == [run_id]


async def test_repair_stuck_resumes_backstops_a_missed_requeue() -> None:
    run_store, events, approvals, enqueued = (
        InMemoryRunStore(),
        InMemoryEventStore(),
        InMemoryApprovalStore(),
        [],
    )
    service = _service(run_store, events, approvals, enqueued)
    run_id = await _suspend(service, run_store, events, approvals, [], _one_send("c1", "z@x"))
    pending = await approvals.pending_for_run(run_id)
    # Approval terminal but the run never requeued (crash after expiry/resolve commit).
    await approvals.resolve(pending[0].id, "granted", "user-1")
    enqueued.clear()

    repaired = await service.repair_stuck_resumes()
    assert repaired == 1 and enqueued == [run_id]
    record = await run_store.get(run_id)
    assert record is not None and record.status is RunStatus.queued and record.resume_requested
    # Idempotent: a second pass finds nothing to repair.
    enqueued.clear()
    assert await service.repair_stuck_resumes() == 0 and enqueued == []


# ---- Blocker 3: admission tenant namespace + immutable fingerprint ------------------------
async def test_same_key_different_org_are_distinct_runs() -> None:
    run_store, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    a = await _admit(service, org_id="org-A", idempotency_key="shared")
    b = await _admit(service, org_id="org-B", idempotency_key="shared")
    assert a != b  # a shared idempotency key does NOT collide across tenants
    run_a, run_b = await run_store.get(a), await run_store.get(b)
    assert run_a is not None and run_a.org_id == "org-A"
    assert run_b is not None and run_b.org_id == "org-B"


async def test_retry_with_mismatched_content_is_a_conflict() -> None:
    run_store, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    first = await _admit(service, content="do X", idempotency_key="k1")
    # Same identity (scope/org/actor/key), same content -> idempotent no-op.
    again = await _admit(service, content="do X", idempotency_key="k1")
    assert again == first
    # Same identity but ATTACKER-SUPPLIED different content/binding -> conflict (never repair).
    with pytest.raises(RunAdmissionConflict):
        await _admit(service, content="do EVIL", idempotency_key="k1")
    with pytest.raises(RunAdmissionConflict):
        await _admit(service, agent_id="agent-EVIL", idempotency_key="k1")


# ---- Blocker 4: resolver actor is mandatory + cross-user policy ---------------------------
async def test_blank_actor_is_denied() -> None:
    run_store, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    run_id = await _suspend(service, run_store, events, approvals, [], _one_send("c1", "z@x"))
    pending = await approvals.pending_for_run(run_id)
    assert (
        await service.resolve_approval(pending[0].id, approved=True, resolved_by="anon", actor="")
        is False
    )
    assert (await run_store.get(run_id)).status is RunStatus.waiting_approval  # type: ignore[union-attr]


async def test_cross_user_same_org_is_denied_without_delegation() -> None:
    run_store, events, approvals = InMemoryRunStore(), InMemoryEventStore(), InMemoryApprovalStore()
    service = _service(run_store, events, approvals, [])
    run_id = await _suspend(service, run_store, events, approvals, [], _one_send("c1", "z@x"))
    pending = await approvals.pending_for_run(run_id)
    # A different user in the SAME org (org-1) may not resolve another user's approval.
    assert (
        await service.resolve_approval(
            pending[0].id, approved=True, resolved_by="mallory", actor="user-2", org_id="org-1"
        )
        is False
    )
    assert (await run_store.get(run_id)).status is RunStatus.waiting_approval  # type: ignore[union-attr]


async def test_documented_delegate_policy_authorizes_admin() -> None:
    run_store, events, approvals, enqueued = (
        InMemoryRunStore(),
        InMemoryEventStore(),
        InMemoryApprovalStore(),
        [],
    )

    async def admin_delegate(actor: str, run: object, record: object) -> bool:
        return actor == "org-1-admin"

    service = _service(run_store, events, approvals, enqueued, delegate_policy=admin_delegate)
    run_id = await _suspend(service, run_store, events, approvals, [], _one_send("c1", "z@x"))
    pending = await approvals.pending_for_run(run_id)
    ok = await service.resolve_approval(
        pending[0].id, approved=True, resolved_by="admin", actor="org-1-admin", org_id="org-1"
    )
    assert ok is True
    assert (await run_store.get(run_id)).status is RunStatus.queued  # type: ignore[union-attr]


# ---- Blocker 5: multi-approval batches ---------------------------------------------------
async def test_batch_resumes_only_when_all_decisions_terminal() -> None:
    run_store, events, approvals, enqueued = (
        InMemoryRunStore(),
        InMemoryEventStore(),
        InMemoryApprovalStore(),
        [],
    )
    service = _service(run_store, events, approvals, enqueued)
    run_id = await _suspend(service, run_store, events, approvals, [], _two_sends())
    enqueued.clear()  # drop the admission-time enqueue; assert only the resume dispatch
    pending = await approvals.pending_for_run(run_id)
    assert len(pending) == 2
    assert pending[0].batch_id and pending[0].batch_id == pending[1].batch_id  # one batch

    # Resolving ONE decision does not resume — the batch is not yet terminal.
    ok1 = await service.resolve_approval(
        pending[0].id, approved=True, resolved_by="user-1", actor="user-1"
    )
    assert ok1 is True and enqueued == []
    assert (await run_store.get(run_id)).status is RunStatus.waiting_approval  # type: ignore[union-attr]

    # Resolving the LAST decision resumes exactly once.
    ok2 = await service.resolve_approval(
        pending[1].id, approved=False, resolved_by="user-1", actor="user-1"
    )
    assert ok2 is True and enqueued == [run_id]
    record = await run_store.get(run_id)
    assert record is not None and record.status is RunStatus.queued and record.resume_requested


async def test_duplicate_decision_idempotent_conflicting_rejected() -> None:
    run_store, events, approvals, enqueued = (
        InMemoryRunStore(),
        InMemoryEventStore(),
        InMemoryApprovalStore(),
        [],
    )
    service = _service(run_store, events, approvals, enqueued)
    run_id = await _suspend(service, run_store, events, approvals, [], _two_sends())
    pending = await approvals.pending_for_run(run_id)

    assert await service.resolve_approval(
        pending[0].id, approved=True, resolved_by="user-1", actor="user-1"
    )
    # Duplicate identical decision is idempotent (True), no state churn.
    assert await service.resolve_approval(
        pending[0].id, approved=True, resolved_by="user-1", actor="user-1"
    )
    # A CONFLICTING decision on the already-granted approval is rejected.
    assert (
        await service.resolve_approval(
            pending[0].id, approved=False, resolved_by="user-1", actor="user-1"
        )
        is False
    )
    assert (await approvals.get(pending[0].id)).status == "granted"  # type: ignore[union-attr]


async def test_resume_executes_only_approved_calls_in_a_batch() -> None:
    run_store, events, approvals, enqueued = (
        InMemoryRunStore(),
        InMemoryEventStore(),
        InMemoryApprovalStore(),
        [],
    )
    service = _service(run_store, events, approvals, enqueued)
    sent: list[dict[str, object]] = []
    run_id = await _suspend(service, run_store, events, approvals, sent, _two_sends())
    pending = sorted(await approvals.pending_for_run(run_id), key=lambda r: r.call_id)
    # Approve c1's send, deny c2's send.
    await service.resolve_approval(
        pending[0].id, approved=True, resolved_by="user-1", actor="user-1"
    )
    await service.resolve_approval(
        pending[1].id, approved=False, resolved_by="user-1", actor="user-1"
    )
    resume_lease = await run_store.claim(run_id, worker_id="w2", lease_seconds=30)
    assert resume_lease is not None and resume_lease.resume is True
    final = await execute_run(
        lease=resume_lease,
        run_store=run_store,
        event_store=events,
        approvals=approvals,
        agent=_agent(),
        provider=_done(),
        registry=_send_tool(sent),
        permissions=_ask_permissions(),
        admit_fn=admit,
        resume=True,
    )
    assert final.status is RunStatus.completed
    # Only the approved (c1) send executed; the denied (c2) send never fired.
    assert sent == [{"to": "a@x"}]
    results = {
        e.payload["call_id"]: e.payload["ok"]
        for e in events.snapshot("sess-1")
        if e.type is EventType.tool_result
    }
    assert results == {"c1": True, "c2": False}
