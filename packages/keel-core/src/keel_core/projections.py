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
    deferred_messages: list[dict[str, Any]] = []
    open_tool_call_ids: set[str] = set()
    open_tool_thread_start: int | None = None
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

    def flush_pending() -> None:
        nonlocal pending_assistant, open_tool_thread_start
        if pending_assistant is None:
            return
        tool_calls = pending_assistant.get("tool_calls", [])
        if tool_calls:
            open_tool_thread_start = len(messages)
            for tool_call in tool_calls:
                call_id = str(tool_call.get("id", ""))
                if call_id:
                    open_tool_call_ids.add(call_id)
        append_message(pending_assistant)
        pending_assistant = None

    def flush_all_deferred() -> None:
        if pending_assistant is not None or open_tool_call_ids or active_run_ids:
            return
        while deferred_messages:
            append_message(deferred_messages.pop(0))

    def flush_deferred_before_run() -> None:
        if pending_assistant is not None or open_tool_call_ids:
            return
        boundary = -1
        for index, message in enumerate(deferred_messages):
            if message.get("role") in ("user", "system"):
                boundary = index
        if boundary < 0:
            return
        ready = deferred_messages[: boundary + 1]
        del deferred_messages[: boundary + 1]
        for message in ready:
            append_message(message)

    def reconcile_incomplete_tool_thread() -> None:
        nonlocal pending_assistant, open_tool_thread_start
        content = ""
        if pending_assistant is not None and pending_assistant.get("tool_calls"):
            content = str(pending_assistant.get("content", ""))
            pending_assistant = None
        elif open_tool_thread_start is not None:
            content = str(messages[open_tool_thread_start].get("content", ""))
            del messages[open_tool_thread_start:]
        open_tool_call_ids.clear()
        open_tool_thread_start = None
        if content.strip():
            append_message({"role": "assistant", "content": content})

    def start_run(run_id: str) -> None:
        if active_run_ids and run_id not in active_run_ids:
            reconcile_incomplete_tool_thread()
            active_run_ids.clear()
        flush_pending()
        flush_deferred_before_run()
        active_run_ids.add(run_id)

    for event in events:
        role = event.payload.get("role")
        if event.type is EventType.run_started and event.run_id is not None:
            start_run(event.run_id)
        elif event.type is EventType.run_resumed and event.run_id is not None:
            start_run(event.run_id)
        elif event.type is EventType.run_suspended:
            flush_pending()
            if event.run_id is not None:
                active_run_ids.discard(event.run_id)
            flush_all_deferred()
        elif event.type is EventType.run_ended:
            if open_tool_call_ids or (
                pending_assistant is not None and pending_assistant.get("tool_calls")
            ):
                reconcile_incomplete_tool_thread()
            else:
                flush_pending()
            if event.run_id is not None:
                active_run_ids.discard(event.run_id)
            flush_all_deferred()
        elif event.type is EventType.message_token and role in ("user", "assistant", "system"):
            if event.payload.get("partial"):
                continue  # streaming-only delta; the whole message lands at turn end
            message = {"role": role, "content": str(event.payload.get("text", ""))}
            if role == "assistant" and event.payload.get("job_id"):
                deferred_messages.append(message)
            elif role in ("user", "system"):
                if active_run_ids or open_tool_call_ids:
                    deferred_messages.append(message)
                else:
                    flush_pending()
                    flush_all_deferred()
                    append_message(message)
            elif open_tool_call_ids:
                deferred_messages.append(message)
            else:
                flush_pending()
                if not active_run_ids:
                    flush_all_deferred()
                pending_assistant = message
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
            flush_pending()  # the assistant tool_calls message must precede the tool results
            call_id = str(event.payload.get("call_id", ""))
            append_message(
                {
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": str(event.payload.get("output", "")),
                }
            )
            open_tool_call_ids.discard(call_id)
            if not open_tool_call_ids:
                open_tool_thread_start = None
                flush_all_deferred()

    flush_pending()
    flush_all_deferred()
    return messages
