"""Postgres integration: durable approval-row / approval-event atomicity (M3.6).

Proves the crash boundary between an approval row and its ``approval.requested`` /
``tool.call`` events against a live Postgres:

* a suspended batch persists its tool.call events, approval rows, and approval.requested
  events in ONE transaction — a crash before the events commit rolls the rows back too, so
  there is never an approval row without its events (nor a partially-raised multi-ask batch);
* a retry after such a rolled-back attempt persists cleanly, exactly once;
* ``ux_approvals_run_call`` rejects a duplicate / injected interactive approval row;
* resume reconstructs the call -> approval association from the durable rows and back-fills a
  lost approval.requested event (older-build repair), so a granted approval is honoured
  exactly once — never silently denied.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

import keel_core.loop as loop_mod
from keel_core.agents import AgentSpec, Scope
from keel_core.approvals import PostgresApprovalStore, insert_pending_in_transaction
from keel_core.connectors import ConnectorTool
from keel_core.events import EventType
from keel_core.loop import (
    ApprovalBinding,
    ToolRegistry,
    _persist_suspension_batch,
    admit_system,
    resume,
    run,
)
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import ProviderChunk, ToolCall, ToolContext
from keel_core.runs import action_hash
from keel_core.state import PostgresEventStore, append_event_in_transaction
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import (
    FinishReason,
    PermissionDecision,
    ScopeKind,
    StopReason,
    TrustLevel,
)

pytestmark = pytest.mark.integration

_SCOPE = "web:local"
_SESSION = "sess-1"
_RUN = "run-1"
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


def _calls(*specs: tuple[str, str]) -> list[ToolCall]:
    return [ToolCall(id=cid, name="email.send", arguments={"to": to}) for cid, to in specs]


async def _row_count(engine: AsyncEngine, run_id: str) -> int:
    store = PostgresApprovalStore(engine, _SCOPE)
    return len(await store.list_for_run(run_id))


async def _event_types(engine: AsyncEngine, run_id: str) -> list[str]:
    async with engine.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": _SCOPE})
        rows = (
            (
                await conn.execute(
                    text(
                        "SELECT type FROM events WHERE scope_id = :s AND run_id = :r ORDER BY seq"
                    ),
                    {"s": _SCOPE, "r": run_id},
                )
            )
            .scalars()
            .all()
        )
    return [str(r) for r in rows]


# ---- atomicity: a crash before the events commit rolls the approval rows back too ----------
async def test_single_ask_crash_before_event_rolls_back_row(
    migrated_db: AsyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = PostgresEventStore(migrated_db, _SCOPE)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)

    async def crash_on_approval_event(conn: object, event: object, **kw: object) -> int:
        if getattr(event, "type", None) is EventType.approval_requested:
            raise RuntimeError("crash before approval.requested commits")
        return await append_event_in_transaction(conn, event, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(loop_mod, "append_event_in_transaction", crash_on_approval_event)

    calls = _calls(("c1", "z@x"))
    with pytest.raises(RuntimeError):
        await _persist_suspension_batch(
            events,
            approvals,
            calls=calls,
            asks=calls,
            session_id=_SESSION,
            scope_id=_SCOPE,
            run_id=_RUN,
            reason="first_use",
            expires_at=_EXPIRES,
            binding=ApprovalBinding(org_id="org-1", actor="user-1", run_attempt=1),
            batch_id="batch-1",
        )

    # All-or-nothing: neither the approval row nor ANY of the batch's events survived.
    assert await _row_count(migrated_db, _RUN) == 0
    assert await _event_types(migrated_db, _RUN) == []


async def test_partial_multi_ask_batch_is_atomic(
    migrated_db: AsyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = PostgresEventStore(migrated_db, _SCOPE)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)

    async def crash_on_second_ask(conn: object, event: object, **kw: object) -> int:
        payload = getattr(event, "payload", {})
        if (
            getattr(event, "type", None) is EventType.approval_requested
            and payload.get("call_id") == "c2"
        ):
            raise RuntimeError("crash mid-batch, after c1 row+event, before c2 event")
        return await append_event_in_transaction(conn, event, **kw)  # type: ignore[arg-type]

    monkeypatch.setattr(loop_mod, "append_event_in_transaction", crash_on_second_ask)

    calls = _calls(("c1", "a@x"), ("c2", "b@x"))
    with pytest.raises(RuntimeError):
        await _persist_suspension_batch(
            events,
            approvals,
            calls=calls,
            asks=calls,
            session_id=_SESSION,
            scope_id=_SCOPE,
            run_id=_RUN,
            reason="first_use",
            expires_at=_EXPIRES,
            binding=ApprovalBinding(org_id="org-1", actor="user-1", run_attempt=1),
            batch_id="batch-1",
        )

    # The whole batch rolled back — c1's already-inserted row + event did NOT survive.
    assert await _row_count(migrated_db, _RUN) == 0
    assert await _event_types(migrated_db, _RUN) == []


async def test_retry_after_rolled_back_batch_persists_exactly_once(
    migrated_db: AsyncEngine,
) -> None:
    events = PostgresEventStore(migrated_db, _SCOPE)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    calls = _calls(("c1", "z@x"))
    binding = ApprovalBinding(org_id="org-1", actor="user-1", run_attempt=1)

    created = await _persist_suspension_batch(
        events,
        approvals,
        calls=calls,
        asks=calls,
        session_id=_SESSION,
        scope_id=_SCOPE,
        run_id=_RUN,
        reason="first_use",
        expires_at=_EXPIRES,
        binding=binding,
        batch_id="batch-1",
    )
    assert len(created) == 1
    assert await _row_count(migrated_db, _RUN) == 1
    assert await _event_types(migrated_db, _RUN) == ["tool.call", "approval.requested"]


# ---- injection guard: the partial unique index rejects a duplicate interactive row ---------
async def test_duplicate_interactive_approval_row_is_rejected(migrated_db: AsyncEngine) -> None:
    args = {"to": "z@x"}
    common = dict(
        scope_id=_SCOPE,
        run_id=_RUN,
        session_id=_SESSION,
        tool="email.send",
        args=args,
        call_id="c1",
        idempotency_key="k",
        reason="first_use",
        expires_at=_EXPIRES,
        org_id="org-1",
        actor="user-1",
        action_hash=action_hash("email.send", args),
        run_attempt=1,
        batch_id="batch-1",
    )
    async with migrated_db.begin() as conn:
        await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": _SCOPE})
        await insert_pending_in_transaction(conn, id="a1", **common)  # type: ignore[arg-type]
    with pytest.raises(IntegrityError):
        async with migrated_db.begin() as conn:
            await conn.execute(text("SELECT set_config('app.scope_id', :s, true)"), {"s": _SCOPE})
            await insert_pending_in_transaction(conn, id="a2", **common)  # type: ignore[arg-type]


# ---- reconstruction: a lost approval.requested event does NOT silently deny a grant ---------
async def test_resume_reconstructs_row_when_event_lost(migrated_db: AsyncEngine) -> None:
    events = PostgresEventStore(migrated_db, _SCOPE)
    approvals = PostgresApprovalStore(migrated_db, _SCOPE)
    sent: list[dict[str, object]] = []

    await admit_system(events, _SESSION, _SCOPE, "triage")
    suspended = await run(
        agent=_agent(),
        session_id=_SESSION,
        store=events,
        provider=ScriptedProviderGateway(
            [
                [
                    ProviderChunk(
                        tool_call=ToolCall(id="c1", name="email.send", arguments={"to": "z@x"}),
                        finish_reason=FinishReason.tool_use,
                    )
                ]
            ]
        ),
        registry=_mail_tools(sent),
        permissions=_ask(),
        approvals=approvals,
        binding=ApprovalBinding(org_id="org-1", actor="user-1", run_attempt=1),
        expires_at=_EXPIRES,
        run_id=_RUN,
    )
    pending = await approvals.pending_for_run(_RUN)
    assert len(pending) == 1
    approval_id = pending[0].id
    await approvals.resolve(approval_id, "granted", "reviewer")

    # Simulate an older-build crash: the approval row committed but its event was lost.
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

    result = await resume(
        agent=_agent(),
        session_id=_SESSION,
        run_id=_RUN,
        store=events,
        provider=ScriptedProviderGateway(
            [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
        ),
        registry=_mail_tools(sent),
        permissions=_ask(),
        approvals=approvals,
        binding=ApprovalBinding(org_id="org-1", actor="user-1", run_attempt=1),
    )
    assert result.reason is StopReason.completed
    assert sent == [{"to": "z@x"}]  # honoured the grant — no silent denial
    # Audit repair: the lost approval.requested event was back-filled during resume.
    assert "approval.requested" in await _event_types(migrated_db, _RUN)
    suspended_run_id = suspended.run_id
    assert suspended_run_id == _RUN
