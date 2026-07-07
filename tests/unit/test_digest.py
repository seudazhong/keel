"""Digest agent + fake connectors tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from keel_core.approvals import InMemoryApprovalStore
from keel_core.digest import (
    DIGEST_INSTRUCTION,
    build_digest_agent,
    digest_permissions,
    digest_registry,
    digest_session_id,
)
from keel_core.loop import admit_system, run
from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, StopReason


def test_agent_and_session_shape() -> None:
    agent = build_digest_agent("u:1")
    assert agent.scope.id == "u:1"
    assert set(agent.toolset) == {"inbox.list", "email.send"}
    assert digest_session_id("u:1") == "digest:u:1"
    assert "triage" in DIGEST_INSTRUCTION.lower()


async def test_digest_run_suspends_on_send() -> None:
    store, approvals, sent = InMemoryEventStore(), InMemoryApprovalStore(), []
    provider = ScriptedProviderGateway(
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
    sid = digest_session_id("u:1")
    await admit_system(store, sid, "u:1", DIGEST_INSTRUCTION)
    result = await run(
        agent=build_digest_agent("u:1"),
        session_id=sid,
        store=store,
        provider=provider,
        registry=digest_registry(sent),
        permissions=digest_permissions(),
        approvals=approvals,
        expires_at=datetime.now(UTC) + timedelta(hours=24),
    )
    assert result.reason is StopReason.suspended
    assert sent == []
    assert len(await approvals.list_pending("u:1")) == 1
