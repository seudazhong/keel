"""Projections (WS-D) — fold the event log into read models.

M1 ships one computed projection: the chat message list the loop feeds to the
provider. It reconstructs proper **tool-calling threads** (an assistant message
carrying ``tool_calls`` followed by ``role: tool`` results linked by
``tool_call_id``) so a real provider accepts the follow-up turn — without this a
tool-use turn produces a malformed request the provider rejects. Persisted
projection tables (with upcasters, DESIGN-REVIEW G3) are a later refinement; the
event log stays the source of truth.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from keel_core.events import Event, EventType


def project_messages(events: Iterable[Event]) -> list[dict[str, Any]]:
    """Fold events into provider-ready messages, threading tool calls to results.

    An assistant turn that requested tools becomes one assistant message with a
    ``tool_calls`` array; each ``tool.result`` becomes a ``role: tool`` message
    keyed by ``tool_call_id`` (OpenAI/LiteLLM format). The assistant message always
    precedes its tool results, as the API requires.
    """
    messages: list[dict[str, Any]] = []
    pending_assistant: dict[str, Any] | None = None

    def flush() -> None:
        nonlocal pending_assistant
        if pending_assistant is not None:
            messages.append(pending_assistant)
            pending_assistant = None

    for event in events:
        role = event.payload.get("role")
        if event.type is EventType.message_token and role in ("user", "assistant", "system"):
            if event.payload.get("partial"):
                continue  # streaming-only delta; the whole message lands at turn end
            if role in ("user", "system"):
                flush()
                messages.append({"role": role, "content": str(event.payload.get("text", ""))})
            else:  # assistant text — may be joined by tool calls in the same turn
                flush()
                pending_assistant = {
                    "role": "assistant",
                    "content": str(event.payload.get("text", "")),
                }
        elif event.type is EventType.tool_call:
            if pending_assistant is None:
                pending_assistant = {"role": "assistant", "content": ""}
            pending_assistant.setdefault("tool_calls", []).append(
                {
                    "id": str(event.payload.get("call_id", "")),
                    "type": "function",
                    "function": {
                        "name": str(event.payload.get("tool", "")),
                        "arguments": json.dumps(event.payload.get("args", {})),
                    },
                }
            )
        elif event.type is EventType.tool_result:
            flush()  # the assistant tool_calls message must precede the tool results
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": str(event.payload.get("call_id", "")),
                    "content": str(event.payload.get("output", "")),
                }
            )

    flush()
    return messages
