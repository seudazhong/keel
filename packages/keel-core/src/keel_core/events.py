"""Event vocabulary v0 (event-sourced state; ARCHITECTURE §8.3, §9).

Every run emits a stream of typed events. The append-only log is the source of
truth for resume, replay, live streaming and audit.

Schema evolution (DESIGN-REVIEW G3): each event carries an integer ``version``
per ``type``; an upcaster registry (M1) migrates old payloads on read so
projections stay rebuildable from v0.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from .types import RunId, ScopeId, SessionId, StopReason


class EventType(StrEnum):
    """The v0 event vocabulary emitted by the agent loop."""

    run_started = "run.started"
    run_ended = "run.ended"
    turn_started = "turn.started"
    turn_ended = "turn.ended"
    message_token = "message.token"
    message_thinking = "message.thinking"
    tool_call = "tool.call"
    tool_result = "tool.result"
    approval_requested = "approval.requested"
    approval_resolved = "approval.resolved"
    error = "error"


class Event(BaseModel):
    """Append-only event envelope.

    ``seq`` is monotonic per session and is the replay cursor used by the
    ``after=`` query. ``scope_id`` is carried on every event (ADR-0009) so the
    stream is scope-filterable end to end.
    """

    type: EventType
    version: int = 1
    seq: int
    session_id: SessionId
    scope_id: ScopeId
    run_id: RunId | None = None
    ts: datetime
    payload: dict[str, Any] = Field(default_factory=dict)


# --- A few typed payloads for load-bearing events (illustrative for v0) ---


class RunEndedPayload(BaseModel):
    """Payload for ``run.ended`` — carries the named termination reason."""

    reason: StopReason


class ToolCallPayload(BaseModel):
    """Payload for ``tool.call``."""

    tool: str
    call_id: str
    args: dict[str, Any] = Field(default_factory=dict)


class ToolResultPayload(BaseModel):
    """Payload for ``tool.result`` (model-facing output is bounded elsewhere)."""

    call_id: str
    ok: bool
    spill_path: str | None = None
