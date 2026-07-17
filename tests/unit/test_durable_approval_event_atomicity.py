"""Crash-boundary coverage for the durable approval-row / approval-event atomicity gap (M3.6).

The gap: a suspended tool batch persisted its approval *row* but crashed before its
``approval.requested`` *event* committed. On resume the event-derived call -> approval map
missed that call, so a **granted** approval was silently **denied** (or, worse, a still-pending
one implicitly denied). These exercise the in-memory doubles by simulating the lost event
(deleting it from the event log after suspension) and asserting resume reconstructs the
association from the durable approval row — honouring the exact decision, never silently
denying, and back-filling the missing event for audit consistency.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from keel_core.agents import AgentSpec, Scope
from keel_core.approvals import ApprovalRecord, InMemoryApprovalStore
from keel_core.connectors import ConnectorTool
from keel_core.events import EventType
from keel_core.loop import ToolRegistry, admit_system, resume, run
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import ProviderChunk, ToolCall, ToolContext
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, PermissionDecision, ScopeKind, StopReason, TrustLevel

_SCOPE = "u:1"
_EXPIRES = datetime(2026, 7, 7, 9, 0, tzinfo=UTC) + timedelta(hours=24)


def _agent() -> AgentSpec:
    return AgentSpec(
        id="a1",
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


def _send(call_id: str, to: str) -> ScriptedProviderGateway:
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


def _drop_approval_events(store: InMemoryEventStore, session_id: str) -> int:
    """Simulate a crash that lost the approval.requested events (row committed, event not)."""
    bucket = store._events.get(session_id, [])
    kept = [e for e in bucket if e.type is not EventType.approval_requested]
    dropped = len(bucket) - len(kept)
    store._events[session_id] = kept
    return dropped


async def _suspend(
    store: InMemoryEventStore,
    approvals: InMemoryApprovalStore,
    sent: list[dict[str, object]],
    provider: ScriptedProviderGateway,
    *,
    run_id: str = "run-1",
) -> str:
    await admit_system(store, "s1", _SCOPE, "seed")
    result = await run(
        agent=_agent(),
        session_id="s1",
        store=store,
        provider=provider,
        registry=_mail_tools(sent),
        permissions=_ask(),
        approvals=approvals,
        run_id=run_id,
        expires_at=_EXPIRES,
    )
    assert result.reason is StopReason.suspended
    return result.run_id


# ---- row committed, event lost: a GRANTED approval must still execute (never denied) ----
async def test_missing_event_granted_approval_is_reconstructed_and_executes() -> None:
    store, approvals = InMemoryEventStore(), InMemoryApprovalStore()
    sent: list[dict[str, object]] = []
    run_id = await _suspend(store, approvals, sent, _send("c1", "z@x"))
    record = (await approvals.list_pending(_SCOPE))[0]
    aid = record.id
    await approvals.resolve(aid, "granted", "reviewer")

    # The approval.requested event never committed (crash boundary); only the row survived.
    assert _drop_approval_events(store, "s1") == 1

    result = await resume(
        agent=_agent(),
        session_id="s1",
        run_id=run_id,
        store=store,
        provider=_done(),
        registry=_mail_tools(sent),
        permissions=_ask(),
        approvals=approvals,
        reconstruct_attempt=record.run_attempt,
        reconstruct_batch_id=record.batch_id,
    )
    assert result.reason is StopReason.completed
    assert sent == [{"to": "z@x"}]  # honoured the granted approval — no silent denial
    # Audit repair: the missing approval.requested event was back-filled.
    requested = [
        e
        for e in store.snapshot("s1")
        if e.type is EventType.approval_requested and e.payload.get("call_id") == "c1"
    ]
    assert len(requested) == 1 and requested[0].payload["approval_id"] == aid


# ---- row committed, event lost, still pending: never implicitly denied (re-suspend) ----
async def test_missing_event_pending_approval_re_suspends_not_denied() -> None:
    store, approvals = InMemoryEventStore(), InMemoryApprovalStore()
    sent: list[dict[str, object]] = []
    run_id = await _suspend(store, approvals, sent, _send("c1", "z@x"))
    record = (await approvals.list_pending(_SCOPE))[0]
    aid = record.id
    assert _drop_approval_events(store, "s1") == 1  # event lost while still pending

    result = await resume(
        agent=_agent(),
        session_id="s1",
        run_id=run_id,
        store=store,
        provider=_done(),
        registry=_mail_tools(sent),
        permissions=_ask(),
        approvals=approvals,
        reconstruct_attempt=record.run_attempt,
        reconstruct_batch_id=record.batch_id,
    )
    assert result.reason is StopReason.suspended  # re-suspended, not denied
    assert sent == []
    assert (await approvals.get(aid)).status == "pending"  # type: ignore[union-attr]


# ---- row committed, event lost, denied: the exact decision is preserved (no execute) ----
async def test_missing_event_denied_approval_is_not_executed() -> None:
    store, approvals = InMemoryEventStore(), InMemoryApprovalStore()
    sent: list[dict[str, object]] = []
    run_id = await _suspend(store, approvals, sent, _send("c1", "z@x"))
    record = (await approvals.list_pending(_SCOPE))[0]
    aid = record.id
    await approvals.resolve(aid, "denied", "reviewer")
    assert _drop_approval_events(store, "s1") == 1

    result = await resume(
        agent=_agent(),
        session_id="s1",
        run_id=run_id,
        store=store,
        provider=_done(),
        registry=_mail_tools(sent),
        permissions=_ask(),
        approvals=approvals,
        reconstruct_attempt=record.run_attempt,
        reconstruct_batch_id=record.batch_id,
    )
    assert result.reason is StopReason.completed
    assert sent == []
    denied = [
        e
        for e in store.snapshot("s1")
        if e.type is EventType.tool_result and e.payload.get("call_id") == "c1"
    ]
    assert denied and denied[0].payload["ok"] is False


# ---- partial multi-approval batch: only the row-with-lost-event is reconstructed ---------
async def test_partial_batch_missing_one_event_reconstructs_both() -> None:
    store, approvals = InMemoryEventStore(), InMemoryApprovalStore()
    sent: list[dict[str, object]] = []
    run_id = await _suspend(store, approvals, sent, _two_sends())
    pending = sorted(await approvals.list_pending(_SCOPE), key=lambda r: r.call_id)
    assert len(pending) == 2

    # Drop only c2's approval.requested event (c1's event survives). Grant both rows.
    bucket = store._events["s1"]
    store._events["s1"] = [
        e
        for e in bucket
        if not (e.type is EventType.approval_requested and e.payload.get("call_id") == "c2")
    ]
    for record in pending:
        await approvals.resolve(record.id, "granted", "reviewer")

    result = await resume(
        agent=_agent(),
        session_id="s1",
        run_id=run_id,
        store=store,
        provider=_done(),
        registry=_mail_tools(sent),
        permissions=_ask(),
        approvals=approvals,
        reconstruct_attempt=pending[0].run_attempt,
        reconstruct_batch_id=pending[0].batch_id,
    )
    assert result.reason is StopReason.completed
    assert sorted(str(a["to"]) for a in sent) == ["a@x", "b@x"]  # both approved sends fired once


# ---- injection guard: an ambiguous duplicate candidate is rejected (fail closed) ---------
async def test_duplicate_candidate_row_is_not_adopted() -> None:
    store, approvals = InMemoryEventStore(), InMemoryApprovalStore()
    sent: list[dict[str, object]] = []
    run_id = await _suspend(store, approvals, sent, _send("c1", "z@x"))
    original = (await approvals.list_pending(_SCOPE))[0]
    await approvals.resolve(original.id, "granted", "reviewer")
    _drop_approval_events(store, "s1")

    # A second (injected) approval row for the SAME call with the SAME action -> ambiguous.
    approvals._rows["injected"] = ApprovalRecord(
        id="injected",
        scope_id=_SCOPE,
        run_id=run_id,
        session_id="s1",
        tool=original.tool,
        args=dict(original.args),
        call_id=original.call_id,
        idempotency_key="x",
        reason="first_use",
        status="granted",
        created_at=datetime.now(UTC),
        expires_at=_EXPIRES,
        action_hash=original.action_hash,
        batch_id=original.batch_id,
    )

    result = await resume(
        agent=_agent(),
        session_id="s1",
        run_id=run_id,
        store=store,
        provider=_done(),
        registry=_mail_tools(sent),
        permissions=_ask(),
        approvals=approvals,
    )
    # Ambiguous candidates are never adopted: the call is denied rather than executed twice.
    assert result.reason is StopReason.completed
    assert sent == []
