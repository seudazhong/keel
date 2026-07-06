"""Projections (WS-D) — fold the event log into read models.

M1-α ships one computed projection: the chat message list the loop feeds to the
provider. Persisted projection tables (with upcasters, DESIGN-REVIEW G3) are a
later refinement; the event log stays the source of truth.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from keel_core.events import Event, EventType


def project_messages(events: Iterable[Event]) -> list[dict[str, Any]]:
    """Fold events into an ordered list of ``{role, content}`` messages."""
    messages: list[dict[str, Any]] = []
    for event in events:
        role = event.payload.get("role")
        if role in ("user", "assistant"):
            if event.payload.get("partial"):
                continue  # streaming-only delta; the whole message is emitted at turn end
            messages.append({"role": str(role), "content": str(event.payload.get("text", ""))})
        elif event.type == EventType.tool_result:
            messages.append({"role": "tool", "content": str(event.payload)})
    return messages
