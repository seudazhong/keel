"""IM gateways hosted by the server (OneBot/QQ; more adapters in M2)."""

from __future__ import annotations

from keel_server.gateway.onebot import (
    OneBotEvent,
    OneBotGateway,
    RateLimiter,
    WakeDecision,
    session_key,
    wake_rule,
)

__all__ = [
    "OneBotEvent",
    "OneBotGateway",
    "RateLimiter",
    "WakeDecision",
    "session_key",
    "wake_rule",
]
