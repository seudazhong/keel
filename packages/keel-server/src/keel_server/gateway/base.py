"""Shared IM adapter core: the safe-agent runner + rate limiter (WS-E/J).

Every IM surface is **untrusted**: an inbound message maps to an untrusted, per-
conversation scope and runs with the read-only **safe toolset**, so external text can
never reach write/shell tools. Platform adapters (OneBot, Telegram, …) parse their own
payloads + wake rules into an :class:`InboundMessage`; :class:`ImRunner` owns the shared
pipeline (rate-limit -> run on the safe agent -> reply through the injected ``send``).
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

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
from keel_core.tools import (
    ExecutionEnvironment,
    GlobTool,
    GrepTool,
    LsTool,
    ReadTool,
    UnavailableExecutionEnvironment,
)
from keel_core.types import PermissionDecision

# The safe toolset for untrusted surfaces (FR-X6): read-only, no write/edit/shell.
_SAFE_TOOLS = ("read", "ls", "glob", "grep")

SendFn = Callable[[str, str], Awaitable[None]]  # (session_key, text) -> None


@dataclass
class WakeDecision:
    """Whether to respond, plus the message text with any wake token stripped."""

    woke: bool
    text: str = ""


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


def safe_tools(environment: ExecutionEnvironment) -> list[Tool]:
    return [
        ReadTool(environment),
        LsTool(environment),
        GlobTool(environment),
        GrepTool(environment),
    ]


def safe_permissions() -> RuleBasedPermissionEngine:
    """Allow only the safe read-only tools; everything else is denied (fail-closed)."""
    rules = [Rule(name, PermissionDecision.allow) for name in _SAFE_TOOLS]
    return RuleBasedPermissionEngine(rules, default=PermissionDecision.deny)


@dataclass
class InboundMessage:
    """A normalized, woke IM message ready to run (adapter output)."""

    session_key: str  # ``platform:type:id`` — one conversation per group or DM peer
    text: str  # message text with the wake token already stripped
    is_group: bool = False


@dataclass
class ImRunner:
    """The shared IM pipeline: rate-limit -> run on the safe agent -> reply."""

    provider: ProviderGateway
    send: SendFn
    workspace: Path = field(default_factory=lambda: Path("."))
    execution_environment: ExecutionEnvironment = field(
        default_factory=UnavailableExecutionEnvironment
    )
    model: str = "gpt-4o-mini"
    rate_limiter: RateLimiter = field(default_factory=RateLimiter)
    _store: InMemoryEventStore = field(default_factory=InMemoryEventStore, init=False)

    def agent(self, message: InboundMessage) -> AgentSpec:
        kind = ScopeKind.group if message.is_group else ScopeKind.personal
        # Untrusted surface: the scope is untrusted and carries only the safe toolset.
        scope = Scope(id=message.session_key, kind=kind, trust=TrustLevel.untrusted)
        return AgentSpec(
            id="im", name="Keel IM", model=self.model, scope=scope, toolset=list(_SAFE_TOOLS)
        )

    async def dispatch(self, message: InboundMessage | None) -> None:
        """Run a parsed inbound message (or no-op for None / empty / rate-limited)."""
        if message is None or not message.text:
            return
        if not self.rate_limiter.allow(message.session_key):
            return  # drop silently to avoid amplifying spam
        reply = await self._run(message)
        if reply:
            await self.send(message.session_key, reply)

    async def _run(self, message: InboundMessage) -> str:
        agent = self.agent(message)
        await admit(self._store, message.session_key, agent.scope.id, message.text)
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
            session_id=message.session_key,
            store=self._store,
            provider=self.provider,
            registry=ToolRegistry(safe_tools(self.execution_environment)),
            permissions=safe_permissions(),
            on_event=collect,
        )
        return "\n".join(p for p in parts if p)
