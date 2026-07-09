"""Telegram Bot IM adapter (WS-E/J).

Reuses the shared untrusted :class:`~keel_server.gateway.base.ImRunner`: a Telegram
webhook update is parsed + wake-gated into an ``InboundMessage``, then run on the safe
(read-only) agent. DMs always wake; groups wake only on an @-mention of the bot or a
command prefix. The reply is sent through an injected ``send`` (real transport = the
Telegram Bot ``sendMessage`` API).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from keel_core import ProviderGateway
from keel_server.gateway.base import ImRunner, InboundMessage, RateLimiter, SendFn


class TelegramChat(BaseModel):
    id: int = 0
    type: str = "private"  # private | group | supergroup | channel


class TelegramMessage(BaseModel):
    text: str = ""
    chat: TelegramChat = Field(default_factory=TelegramChat)


class TelegramUpdate(BaseModel):
    """A Telegram Bot API update (we act on ``message`` updates with text)."""

    message: TelegramMessage | None = None


def telegram_session_key(chat: TelegramChat) -> str:
    """``tg:type:id`` — one conversation per group (chat) or per DM peer."""
    kind = "private" if chat.type == "private" else "group"
    return f"tg:{kind}:{chat.id}"


def parse_telegram_inbound(
    payload: dict[str, Any], *, bot_username: str | None, prefixes: tuple[str, ...]
) -> InboundMessage | None:
    """Parse + wake-gate a Telegram update into an ``InboundMessage`` (None = ignore).

    DMs always wake. Groups/supergroups wake only on an ``@bot_username`` mention or a
    command prefix, with the wake token stripped from the text.
    """
    message = TelegramUpdate.model_validate(payload).message
    if message is None:
        return None
    text = message.text.strip()
    if not text:
        return None
    key = telegram_session_key(message.chat)
    if message.chat.type == "private":
        return InboundMessage(session_key=key, text=text, is_group=False)

    mention = f"@{bot_username}" if bot_username else None
    if mention and mention in text:
        return InboundMessage(
            session_key=key, text=text.replace(mention, "").strip(), is_group=True
        )
    for prefix in prefixes:
        if text.startswith(prefix):
            return InboundMessage(session_key=key, text=text[len(prefix) :].strip(), is_group=True)
    return None


@dataclass
class TelegramGateway:
    """Route Telegram updates through wake rules + rate limiting to the safe agent."""

    provider: ProviderGateway
    send: SendFn
    workspace: Path = field(default_factory=lambda: Path("."))
    bot_username: str | None = None
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
        """Process one Telegram update: parse -> wake -> rate-limit -> run (safe) -> reply."""
        message = parse_telegram_inbound(
            payload, bot_username=self.bot_username, prefixes=self.prefixes
        )
        await self._runner.dispatch(message)
