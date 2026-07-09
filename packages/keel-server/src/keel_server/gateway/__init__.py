"""IM gateways hosted by the server (OneBot/QQ + Telegram; a shared safe-agent core)."""

from __future__ import annotations

from keel_server.gateway.base import ImRunner, InboundMessage, RateLimiter, SendFn, WakeDecision
from keel_server.gateway.onebot import OneBotEvent, OneBotGateway, session_key, wake_rule
from keel_server.gateway.telegram import (
    TelegramGateway,
    TelegramUpdate,
    parse_telegram_inbound,
    telegram_session_key,
)

__all__ = [
    "ImRunner",
    "InboundMessage",
    "OneBotEvent",
    "OneBotGateway",
    "RateLimiter",
    "SendFn",
    "TelegramGateway",
    "TelegramUpdate",
    "WakeDecision",
    "parse_telegram_inbound",
    "session_key",
    "telegram_session_key",
    "wake_rule",
]
