"""Integration: web runtime over live Postgres + Redis (fan-out, SSE tail, approvals).

Exercises the full server-side spine deterministically with a scripted provider:
a run streams events to a durable store *and* a Redis stream, and a mutating tool
pauses on an HTTP-resolved approval before executing.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.events import EventType
from keel_core.protocols import ProviderChunk, ToolCall
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason
from keel_server.runtime import AgentRuntime

pytestmark = pytest.mark.integration


async def test_web_run_streams_over_redis(
    migrated_db: AsyncEngine, redis_client: aioredis.Redis, tmp_path: Path
) -> None:
    provider = ScriptedProviderGateway(
        [[ProviderChunk(delta="hello from the web", finish_reason=FinishReason.end_turn)]]
    )
    runtime = AgentRuntime(
        redis_client=redis_client,
        engine=migrated_db,
        model="test/model",
        workspace=tmp_path,
        provider=provider,
    )
    session_id = f"web-run-{uuid.uuid4().hex}"
    await runtime.admit_and_run(session_id, "hi")

    types: list[EventType] = []
    assistant = ""
    async for event in runtime.tail(session_id, 0):
        types.append(event.type)
        if event.type is EventType.message_token and event.payload.get("role") == "assistant":
            assistant = str(event.payload.get("text", ""))
        if event.type is EventType.run_ended:
            break

    assert EventType.run_started in types
    assert types[-1] is EventType.run_ended
    assert "hello from the web" in assistant

    # The same events are durable in Postgres (not just fanned out to Redis).
    durable = [event.type async for event in runtime._durable().read(session_id)]
    assert EventType.run_ended in durable


async def test_web_approval_gates_a_mutating_tool(
    migrated_db: AsyncEngine, redis_client: aioredis.Redis, tmp_path: Path
) -> None:
    provider = ScriptedProviderGateway(
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c1", name="write", arguments={"path": "out.txt", "content": "hi"}
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
        ]
    )
    runtime = AgentRuntime(
        redis_client=redis_client,
        engine=migrated_db,
        model="test/model",
        workspace=tmp_path,
        provider=provider,
    )
    session_id = f"web-approve-{uuid.uuid4().hex}"
    await runtime.admit_and_run(session_id, "write a file")

    approval_id: str | None = None
    tool_ok: bool | None = None
    async for event in runtime.tail(session_id, 0):
        if event.type is EventType.approval_requested:
            approval_id = str(event.payload["approval_id"])
            assert runtime.resolve_approval(approval_id, True) is True  # allow the write
        elif event.type is EventType.tool_result:
            tool_ok = bool(event.payload.get("ok"))
        elif event.type is EventType.run_ended:
            break

    assert approval_id is not None  # the mutating tool asked before running
    assert tool_ok is True
    assert (tmp_path / "out.txt").read_text(encoding="utf-8") == "hi"  # approved -> executed
