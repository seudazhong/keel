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
    pending_assistant_is_job = False
    deferred_assistants: list[dict[str, Any]] = []
    open_tool_call_ids: set[str] = set()
    active_run_ids: set[str] = set()

    def append_message(message: dict[str, Any]) -> None:
        is_plain_assistant = message.get("role") == "assistant" and "tool_calls" not in message
        previous = messages[-1] if messages else None
        if (
            is_plain_assistant
            and previous is not None
            and previous.get("role") == "assistant"
            and "tool_calls" not in previous
        ):
            previous["content"] = (
                f"{str(previous.get('content', ''))}\n\n{str(message.get('content', ''))}"
            )
            return
        messages.append(message)

    def flush() -> None:
        nonlocal pending_assistant, pending_assistant_is_job
        if pending_assistant is not None:
            for tool_call in pending_assistant.get("tool_calls", []):
                call_id = str(tool_call.get("id", ""))
                if call_id:
                    open_tool_call_ids.add(call_id)
            append_message(pending_assistant)
            pending_assistant = None
            pending_assistant_is_job = False

    def defer_pending_job() -> None:
        nonlocal pending_assistant, pending_assistant_is_job
        if pending_assistant is not None and pending_assistant_is_job:
            deferred_assistants.append(pending_assistant)
            pending_assistant = None
            pending_assistant_is_job = False

    def flush_deferred() -> None:
        if pending_assistant is not None or open_tool_call_ids or active_run_ids:
            return
        while deferred_assistants:
            append_message(deferred_assistants.pop(0))

    for event in events:
        role = event.payload.get("role")
        if event.type is EventType.run_started and event.run_id is not None:
            if pending_assistant_is_job:
                defer_pending_job()
            else:
                flush()
            if active_run_ids and event.run_id not in active_run_ids:
                active_run_ids.clear()
            active_run_ids.add(event.run_id)
        elif event.type in (EventType.run_ended, EventType.run_suspended):
            flush()
            if event.run_id is not None:
                active_run_ids.discard(event.run_id)
            flush_deferred()
        elif event.type is EventType.run_resumed and event.run_id is not None:
            if pending_assistant_is_job:
                defer_pending_job()
            else:
                flush()
            if active_run_ids and event.run_id not in active_run_ids:
                active_run_ids.clear()
            active_run_ids.add(event.run_id)
        elif event.type is EventType.message_token and role in ("user", "assistant", "system"):
            if event.payload.get("partial"):
                continue  # streaming-only delta; the whole message lands at turn end
            if role in ("user", "system"):
                flush()
                flush_deferred()
                append_message({"role": role, "content": str(event.payload.get("text", ""))})
            else:  # assistant text — may be joined by tool calls in the same turn
                message = {
                    "role": "assistant",
                    "content": str(event.payload.get("text", "")),
                }
                if event.payload.get("job_id") and (
                    pending_assistant is not None or open_tool_call_ids or active_run_ids
                ):
                    deferred_assistants.append(message)
                    continue
                flush()
                flush_deferred()
                pending_assistant = message
                pending_assistant_is_job = bool(event.payload.get("job_id"))
        elif event.type is EventType.tool_call:
            defer_pending_job()
            if pending_assistant is None:
                pending_assistant = {"role": "assistant", "content": ""}
                pending_assistant_is_job = False
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
            call_id = str(event.payload.get("call_id", ""))
            append_message(
                {
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": str(event.payload.get("output", "")),
                }
            )
            open_tool_call_ids.discard(call_id)
            flush_deferred()

    flush()
    flush_deferred()
    return messages
