"""OneBot (QQ) IM gateway (WS-E/J).

An untrusted surface: group/DM messages arrive over the OneBot v11 protocol, are
mapped to a ``platform:type:id`` session (and an **untrusted** scope), gated by
**wake rules** (only respond when @-mentioned or command-prefixed in groups; always
in DMs) and a **per-chat rate limit**, then run on the shared agent spine with the
**safe (read-only) toolset** — untrusted input can never reach write/shell tools.
The reply is sent back through an injected ``send`` callable (real transport = the
OneBot HTTP API).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from keel_core import ProviderGateway
from keel_server.gateway.base import (
    ImRunner,
    InboundMessage,
    RateLimiter,
    SendFn,
    WakeDecision,
)

__all__ = [
    "OneBotEvent",
    "OneBotGateway",
    "RateLimiter",
    "WakeDecision",
    "session_key",
    "wake_rule",
]


class OneBotEvent(BaseModel):
    """A OneBot v11 event (message events are the ones we act on)."""

    post_type: str = ""
    message_type: str = ""  # "group" | "private"
    user_id: int | None = None
    group_id: int | None = None
    self_id: int | None = None
    raw_message: str = ""
    message: Any = ""

    @property
    def text(self) -> str:
        """The message as plain text (OneBot may send a string or CQ array)."""
        if self.raw_message:
            return self.raw_message
        return self.message if isinstance(self.message, str) else ""


def session_key(event: OneBotEvent) -> str:
    """``platform:type:id`` — one conversation per group or per DM peer."""
    if event.message_type == "group" and event.group_id is not None:
        return f"qq:group:{event.group_id}"
    return f"qq:private:{event.user_id}"


_CQ_AT = re.compile(r"\[CQ:at,qq=(\d+)\]")


def wake_rule(
    event: OneBotEvent, *, self_id: int | None, prefixes: tuple[str, ...]
) -> WakeDecision:
    """Decide whether to respond and strip the wake token from the text.

    DMs always wake. Groups wake only on an @-mention of the bot or a command prefix,
    so the bot stays quiet in normal group chatter.
    """
    text = event.text
    if event.message_type != "group":
        return WakeDecision(True, text.strip())

    if self_id is not None and f"[CQ:at,qq={self_id}]" in text:
        return WakeDecision(True, _CQ_AT.sub("", text).strip())
    for prefix in prefixes:
        if text.strip().startswith(prefix):
            return WakeDecision(True, text.strip()[len(prefix) :].strip())
    return WakeDecision(False)


@dataclass
class OneBotGateway:
    """Route OneBot messages through wake rules + rate limiting to the safe agent."""

    provider: ProviderGateway
    send: SendFn
    workspace: Path = field(default_factory=lambda: Path("."))
    self_id: int | None = None
    prefixes: tuple[str, ...] = ("/keel",)
    model: str = "gpt-4o-mini"
    rate_limiter: RateLimiter = field(default_factory=RateLimiter)
    _runner: ImRunner = field(init=False)

    def __post_init__(self) -> None:
        self._runner = ImRunner(
            provider=self.provider,
            send=self.send,
            workspace=self.workspace,
            model=self.model,
            rate_limiter=self.rate_limiter,
        )

    async def handle(self, payload: dict[str, Any]) -> None:
        """Process one OneBot event: wake -> rate-limit -> run (safe) -> reply."""
        event = OneBotEvent.model_validate(payload)
        if event.post_type != "message":
            return
        decision = wake_rule(event, self_id=self.self_id, prefixes=self.prefixes)
        if not decision.woke or not decision.text:
            return
        await self._runner.dispatch(
            InboundMessage(
                session_key=session_key(event),
                text=decision.text,
                is_group=event.message_type == "group",
            )
        )
