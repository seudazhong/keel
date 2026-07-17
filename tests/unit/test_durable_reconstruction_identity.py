"""Adversarial coverage for resume's exact reconstruction identity (M3.6 blocker 1).

When an ``approval.requested`` event was lost to an older-build crash (the row committed but
its event did not), resume rebuilds the call -> approval association from the durable approval
rows. That repair is **exact**: a durable row is adopted for a suspended call only when its
``run_id``, ``session_id``, ``call_id``, recomputed ``action_hash`` and — when the run's
suspension checkpoint recorded them — its source ``run_attempt`` + ``batch_id`` all match. A
foreign/older/newer attempt or batch, or a mismatched session/call/hash, fails closed (the call
is denied, never executed on a stale/injected row). A legitimate older-build *checkpoint* (it
never persisted a batch id, so ``reconstruct_batch_id`` is ``None``) is still repaired on the
remaining exact fields — but only against an equally batch-less row (a true old-build approval
row). A batch-less checkpoint never wildcards the batch constraint: a row that carries a real,
non-empty batch id is always foreign to it and is rejected, fail closed — an empty legacy
checkpoint can never be tricked into adopting some other (possibly unrelated) batch's approval.

These drive ``loop.resume`` directly against the in-memory doubles, passing the checkpoint's
``reconstruct_attempt`` / ``reconstruct_batch_id`` expectations explicitly.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from keel_core.agents import AgentSpec, Scope
from keel_core.approvals import ApprovalRecord, InMemoryApprovalStore
from keel_core.connectors import ConnectorTool
from keel_core.events import EventType
from keel_core.loop import ApprovalBinding, ToolRegistry, admit_system, resume, run
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import ProviderChunk, ToolCall, ToolContext
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, PermissionDecision, ScopeKind, StopReason, TrustLevel

_SCOPE = "u:1"
_SESSION = "s1"
_RUN = "run-1"
_ATTEMPT = 1
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


def _done() -> ScriptedProviderGateway:
    return ScriptedProviderGateway(
        [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
    )


def _drop_approval_events(store: InMemoryEventStore) -> int:
    """Simulate the older-build crash: the approval.requested event never committed."""
    bucket = store._events.get(_SESSION, [])
    kept = [e for e in bucket if e.type is not EventType.approval_requested]
    dropped = len(bucket) - len(kept)
    store._events[_SESSION] = kept
    return dropped


async def _suspend_granted() -> tuple[InMemoryEventStore, InMemoryApprovalStore, ApprovalRecord]:
    """Suspend a single-send run, grant its approval, then lose the approval.requested event."""
    store, approvals = InMemoryEventStore(), InMemoryApprovalStore()
    await admit_system(store, _SESSION, _SCOPE, "seed")
    result = await run(
        agent=_agent(),
        session_id=_SESSION,
        store=store,
        provider=_send("c1", "z@x"),
        registry=_mail_tools([]),
        permissions=_ask(),
        approvals=approvals,
        run_id=_RUN,
        expires_at=_EXPIRES,
        binding=ApprovalBinding(org_id="org-1", actor="user-1", run_attempt=_ATTEMPT),
    )
    assert result.reason is StopReason.suspended
    record = (await approvals.list_pending(_SCOPE))[0]
    await approvals.resolve(record.id, "granted", "reviewer")
    assert _drop_approval_events(store) == 1
    return store, approvals, record


async def _resume(
    store: InMemoryEventStore,
    approvals: InMemoryApprovalStore,
    sent: list[dict[str, object]],
    *,
    reconstruct_attempt: int | None,
    reconstruct_batch_id: str | None,
) -> StopReason:
    result = await resume(
        agent=_agent(),
        session_id=_SESSION,
        run_id=_RUN,
        store=store,
        provider=_done(),
        registry=_mail_tools(sent),
        permissions=_ask(),
        approvals=approvals,
        reconstruct_attempt=reconstruct_attempt,
        reconstruct_batch_id=reconstruct_batch_id,
    )
    return result.reason


# ---- exact match (new build): source attempt + batch id both agree -> adopted, sends once ----
async def test_exact_attempt_and_batch_reconstructs_and_executes() -> None:
    store, approvals, record = await _suspend_granted()
    sent: list[dict[str, object]] = []
    reason = await _resume(
        store, approvals, sent, reconstruct_attempt=_ATTEMPT, reconstruct_batch_id=record.batch_id
    )
    assert reason is StopReason.completed
    assert sent == [{"to": "z@x"}]  # honoured the grant, exactly once


# ---- legitimate older-build repair: checkpoint AND row never stored a batch id (both empty,
# a true old-build row) -> still reconstructs on the remaining exact fields -----------------
async def test_older_build_repair_without_batch_id_still_reconstructs() -> None:
    store, approvals, record = await _suspend_granted()
    approvals._rows[record.id] = replace(record, batch_id="")
    sent: list[dict[str, object]] = []
    reason = await _resume(
        store, approvals, sent, reconstruct_attempt=_ATTEMPT, reconstruct_batch_id=None
    )
    assert reason is StopReason.completed
    assert sent == [{"to": "z@x"}]  # attempt + hash + call + session sufficed


# ---- adversarial: legacy/batch-less checkpoint (reconstruct_batch_id=None) must NEVER
# wildcard-match a row that carries a real, non-empty (foreign) batch id -> fail closed --------
async def test_empty_legacy_checkpoint_never_wildcards_foreign_batch_row() -> None:
    store, approvals, record = await _suspend_granted()
    assert record.batch_id  # the row genuinely carries a real batch id from this suspension
    sent: list[dict[str, object]] = []
    reason = await _resume(
        store, approvals, sent, reconstruct_attempt=_ATTEMPT, reconstruct_batch_id=None
    )
    assert reason is StopReason.completed
    assert sent == []  # foreign non-empty batch on a batch-less checkpoint -> never adopted


# ---- foreign source attempt: an older/newer attempt is refused (fail closed) ------------------
async def test_foreign_attempt_is_rejected() -> None:
    store, approvals, record = await _suspend_granted()
    sent: list[dict[str, object]] = []
    reason = await _resume(
        store, approvals, sent, reconstruct_attempt=99, reconstruct_batch_id=record.batch_id
    )
    assert reason is StopReason.completed
    assert sent == []  # foreign attempt -> never adopted -> denied


# ---- foreign batch id: a different batch is refused (fail closed) -----------------------------
async def test_foreign_batch_is_rejected() -> None:
    store, approvals, _record = await _suspend_granted()
    sent: list[dict[str, object]] = []
    reason = await _resume(
        store, approvals, sent, reconstruct_attempt=_ATTEMPT, reconstruct_batch_id="not-the-batch"
    )
    assert reason is StopReason.completed
    assert sent == []  # foreign batch -> never adopted -> denied


# ---- foreign call id: the only durable row is for a different call -> no candidate ------------
async def test_foreign_call_id_is_rejected() -> None:
    store, approvals, record = await _suspend_granted()
    approvals._rows[record.id] = replace(record, call_id="different-call", status="granted")
    sent: list[dict[str, object]] = []
    reason = await _resume(
        store, approvals, sent, reconstruct_attempt=_ATTEMPT, reconstruct_batch_id=record.batch_id
    )
    assert reason is StopReason.completed
    assert sent == []  # no row for the suspended call -> denied


# ---- foreign session: a row bound to another session is never adopted -------------------------
async def test_foreign_session_is_rejected() -> None:
    store, approvals, record = await _suspend_granted()
    approvals._rows[record.id] = replace(record, session_id="other-session", status="granted")
    sent: list[dict[str, object]] = []
    reason = await _resume(
        store, approvals, sent, reconstruct_attempt=_ATTEMPT, reconstruct_batch_id=record.batch_id
    )
    assert reason is StopReason.completed
    assert sent == []  # cross-session row -> denied


# ---- foreign action hash: a tampered/replayed action is never adopted -------------------------
async def test_foreign_action_hash_is_rejected() -> None:
    store, approvals, record = await _suspend_granted()
    approvals._rows[record.id] = replace(record, action_hash="deadbeef", status="granted")
    sent: list[dict[str, object]] = []
    reason = await _resume(
        store, approvals, sent, reconstruct_attempt=_ATTEMPT, reconstruct_batch_id=record.batch_id
    )
    assert reason is StopReason.completed
    assert sent == []  # action-hash mismatch -> denied


# ---- foreign run id: a row bound to another run is never adopted ------------------------------
async def test_foreign_run_id_is_rejected() -> None:
    store, approvals, record = await _suspend_granted()
    approvals._rows[record.id] = replace(record, run_id="other-run", status="granted")
    sent: list[dict[str, object]] = []
    reason = await _resume(
        store, approvals, sent, reconstruct_attempt=_ATTEMPT, reconstruct_batch_id=record.batch_id
    )
    assert reason is StopReason.completed
    assert sent == []  # cross-run row -> denied


# ---- ambiguous batch-matched duplicates: two candidate rows -> fail closed --------------------
async def test_ambiguous_duplicate_matched_rows_are_rejected() -> None:
    store, approvals, record = await _suspend_granted()
    approvals._rows["injected"] = replace(record, id="injected", idempotency_key="x")
    sent: list[dict[str, object]] = []
    reason = await _resume(
        store, approvals, sent, reconstruct_attempt=_ATTEMPT, reconstruct_batch_id=record.batch_id
    )
    assert reason is StopReason.completed
    assert sent == []  # >1 candidate for one call -> never adopted -> denied
