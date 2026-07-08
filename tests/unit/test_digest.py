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
from keel_core.protocols import ProviderChunk, ToolCall, ToolContext
from keel_core.state import InMemoryEventStore
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import ContentTaint, FinishReason, StopReason


def test_agent_and_session_shape() -> None:
    agent = build_digest_agent("u:1")
    assert agent.scope.id == "u:1"
    assert set(agent.toolset) == {"inbox_list", "email_send"}
    assert digest_session_id("u:1") == "digest:u:1"
    assert "triage" in DIGEST_INSTRUCTION.lower()


def test_tool_names_are_provider_valid() -> None:
    import re

    # OpenAI/Anthropic function names: ^[a-zA-Z0-9_-]{1,64}$ (no dots) — else 400 Bad Request.
    names = [str(s["function"]["name"]) for s in digest_registry().schemas()]
    assert names
    for name in names:
        assert re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", name), name


async def test_default_registry_uses_fake_inbox() -> None:
    tool = digest_registry().get("inbox_list")
    assert tool is not None
    result = await tool.run({}, ToolContext(scope_id="u:1", session_id="digest:u:1"))
    assert "zhangwei@example.com" in result.output
    assert result.taint is ContentTaint.tainted


async def test_registry_honors_injected_inbox_action() -> None:
    async def real_inbox(args: dict[str, object], ctx: ToolContext) -> str:
        return "REAL INBOX"

    tool = digest_registry(inbox_action=real_inbox).get("inbox_list")
    assert tool is not None
    result = await tool.run({}, ToolContext(scope_id="u:1", session_id="digest:u:1"))
    assert result.output == "REAL INBOX"
    assert result.taint is ContentTaint.tainted  # inbound is always tainted (G17)


async def test_registry_honors_injected_send_action() -> None:
    calls: list[dict[str, object]] = []

    async def real_send(args: dict[str, object], ctx: ToolContext) -> str:
        calls.append(args)
        return "sent (id=abc)"

    sent: list[dict[str, object]] = []
    tool = digest_registry(sent, send_action=real_send).get("email_send")
    assert tool is not None
    result = await tool.run(
        {"to": "me@example.com", "idempotency_key": "k"},
        ToolContext(scope_id="u:1", session_id="digest:u:1"),
    )
    assert result.output == "sent (id=abc)"
    assert calls == [{"to": "me@example.com", "idempotency_key": "k"}]
    assert sent == []  # the fake in-memory outbox is bypassed


async def test_digest_run_suspends_on_send() -> None:
    store, approvals, sent = InMemoryEventStore(), InMemoryApprovalStore(), []
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c1", name="inbox_list", arguments={}),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c2",
                        name="email_send",
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
