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
import time
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from keel_core import (
    AgentSpec,
    Event,
    EventType,
    InMemoryEventStore,
    ProviderGateway,
    Rule,
    RuleBasedPermissionEngine,
    Scope,
    ScopeKind,
    ToolRegistry,
    TrustLevel,
    admit,
    run,
)
from keel_core.protocols import Tool
from keel_core.tools import GlobTool, GrepTool, LsTool, ReadTool
from keel_core.types import PermissionDecision

# The safe toolset for untrusted surfaces (FR-X6): read-only, no write/edit/shell.
_SAFE_TOOLS = ("read", "ls", "glob", "grep")

SendFn = Callable[[str, str], Awaitable[None]]  # (session_key, text) -> None


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


@dataclass
class WakeDecision:
    woke: bool
    text: str = ""


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


class RateLimiter:
    """Per-key sliding-window limiter (G10): at most ``limit`` events per ``window`` s."""

    def __init__(self, limit: int = 5, window: float = 60.0) -> None:
        self._limit = limit
        self._window = window
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str, *, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        hits = self._hits[key]
        while hits and now - hits[0] > self._window:
            hits.popleft()
        if len(hits) >= self._limit:
            return False
        hits.append(now)
        return True


def _safe_tools(workspace: Path) -> list[Tool]:
    tools: list[Tool] = [
        ReadTool(workspace),
        LsTool(workspace),
        GlobTool(workspace),
        GrepTool(workspace),
    ]
    return tools


def _safe_permissions() -> RuleBasedPermissionEngine:
    """Allow only the safe read-only tools; everything else is denied (fail-closed)."""
    rules = [Rule(name, PermissionDecision.allow) for name in _SAFE_TOOLS]
    return RuleBasedPermissionEngine(rules, default=PermissionDecision.deny)


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
    _store: InMemoryEventStore = field(default_factory=InMemoryEventStore, init=False)

    def _agent(self, key: str) -> AgentSpec:
        kind = ScopeKind.group if key.startswith("qq:group:") else ScopeKind.personal
        # Untrusted surface: the scope is untrusted and carries only the safe toolset.
        scope = Scope(id=key, kind=kind, trust=TrustLevel.untrusted)
        return AgentSpec(
            id="im", name="Keel IM", model=self.model, scope=scope, toolset=list(_SAFE_TOOLS)
        )

    async def handle(self, payload: dict[str, Any]) -> None:
        """Process one OneBot event: wake -> rate-limit -> run (safe) -> reply."""
        event = OneBotEvent.model_validate(payload)
        if event.post_type != "message":
            return
        key = session_key(event)
        decision = wake_rule(event, self_id=self.self_id, prefixes=self.prefixes)
        if not decision.woke or not decision.text:
            return
        if not self.rate_limiter.allow(key):
            return  # drop silently to avoid amplifying spam
        reply = await self._run(key, decision.text)
        if reply:
            await self.send(key, reply)

    async def _run(self, key: str, text: str) -> str:
        agent = self._agent(key)
        await admit(self._store, key, agent.scope.id, text)
        parts: list[str] = []

        def collect(event: Event) -> None:
            if (
                event.type is EventType.message_token
                and event.payload.get("role") == "assistant"
                and not event.payload.get("partial")
            ):
                parts.append(str(event.payload.get("text", "")))

        await run(
            agent=agent,
            session_id=key,
            store=self._store,
            provider=self.provider,
            registry=ToolRegistry(_safe_tools(self.workspace)),
            permissions=_safe_permissions(),
            on_event=collect,
        )
        return "\n".join(p for p in parts if p)
