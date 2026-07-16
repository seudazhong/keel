"""Wire the agent spine into a runnable client (WS-E, M1 step 5).

Assembles a :class:`~keel_core.agents.AgentSpec`, an event store, the LiteLLM
provider gateway, the built-in toolset and a permission profile into a
:class:`ChatSession`. :class:`StreamRenderer` turns the loop's live-observation
seams (``on_event``/``on_delta``) into terminal output, so the CLI streams tokens
and tool activity as they happen — the first fully usable end-to-end path.
"""

from __future__ import annotations

import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from keel_core import (
    AgentSpec,
    EventStore,
    InMemoryEventStore,
    LiteLLMGateway,
    PermissionEngine,
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
from keel_core.events import Event, EventType
from keel_core.tools import (
    EditTool,
    GlobTool,
    GrepTool,
    LsTool,
    ReadTool,
    ShellTool,
    UnsafeLocalDevExecutionEnvironment,
    WriteTool,
)
from keel_core.tools.executor import ApproveFn
from keel_core.types import PermissionDecision

Writer = Callable[[str], None]

# Interactive default: reads are free, mutations ask (the permission gate resolves
# asks via an approver). ``--allow-all`` swaps in a permissive engine.
_READ_ONLY = ("read", "ls", "glob", "grep")
_MUTATING = ("write", "edit", "shell")


def build_tools(workspace: Path) -> list[Any]:
    """Instantiate the built-in toolset confined to ``workspace``."""
    environment = UnsafeLocalDevExecutionEnvironment(workspace)
    return [
        ReadTool(environment),
        WriteTool(environment),
        EditTool(environment),
        LsTool(environment),
        GlobTool(environment),
        GrepTool(environment),
        ShellTool(environment),
    ]


def default_permissions() -> RuleBasedPermissionEngine:
    """Allow read-only tools, ask before mutating ones (fail-closed default)."""
    rules = [Rule(name, PermissionDecision.allow) for name in _READ_ONLY]
    rules += [Rule(name, PermissionDecision.ask) for name in _MUTATING]
    return RuleBasedPermissionEngine(rules, default=PermissionDecision.ask)


def allow_all_permissions() -> RuleBasedPermissionEngine:
    """Allow every tool (the ``--allow-all`` profile)."""
    return RuleBasedPermissionEngine([Rule("*", PermissionDecision.allow)])


def build_provider() -> ProviderGateway:
    """Construct the live provider gateway (seam tests monkeypatch)."""
    return LiteLLMGateway()


def _preview(value: object, limit: int = 60) -> str:
    text = str(value).replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _stdout(text: str) -> None:
    sys.stdout.write(text)
    sys.stdout.flush()


def _stderr(text: str) -> None:
    sys.stderr.write(text)
    sys.stderr.flush()


def _noop(text: str) -> None:  # pragma: no cover - trivial
    return None


@dataclass
class StreamRenderer:
    """Render the loop's live event/delta seams to a pair of writers.

    ``write_out`` receives assistant text; ``write_meta`` receives tool/status
    lines. Assistant text is rendered from ``on_delta`` (token-live); the matching
    ``message.token`` event only closes the line, so text is never printed twice.
    """

    write_out: Writer = _stdout
    write_meta: Writer = _stderr
    output_parts: list[str] = field(default_factory=list)
    _streaming: bool = False

    def on_delta(self, text: str) -> None:
        self.write_out(text)
        self.output_parts.append(text)
        self._streaming = True

    def on_event(self, event: Event) -> None:
        if event.type is EventType.message_token:
            if event.payload.get("role") != "assistant":
                return
            if self._streaming:
                self.write_out("\n")
                self._streaming = False
            else:  # text arrived without deltas (defensive): render it once
                text = str(event.payload.get("text", ""))
                if text:
                    self.write_out(text + "\n")
                    self.output_parts.append(text)
        elif event.type is EventType.tool_call:
            tool = event.payload.get("tool", "?")
            self.write_meta(f"  -> {tool}({_preview(event.payload.get('args', {}))})\n")
        elif event.type is EventType.tool_result:
            ok = bool(event.payload.get("ok"))
            mark = "ok" if ok else "err"
            self.write_meta(f"  <- {mark}: {_preview(event.payload.get('output', ''))}\n")
        elif event.type is EventType.error:
            self.write_meta(f"  ! error: {_preview(event.payload.get('message', ''), 300)}\n")

    @property
    def output(self) -> str:
        """The full assistant text produced so far (for headless/JSON output)."""
        return "".join(self.output_parts)


@dataclass
class ChatSession:
    """A running conversation: agent spine + renderer bound to one session id."""

    agent: AgentSpec
    store: EventStore
    provider: ProviderGateway
    registry: ToolRegistry
    permissions: PermissionEngine
    renderer: StreamRenderer
    session_id: str
    approve: ApproveFn | None = None

    async def send(self, prompt: str) -> Any:
        """Admit user input (I2) then run the loop, streaming output live."""
        self.renderer.output_parts.clear()
        self.renderer._streaming = False
        await admit(self.store, self.session_id, self.agent.scope.id, prompt)
        return await run(
            agent=self.agent,
            session_id=self.session_id,
            store=self.store,
            provider=self.provider,
            registry=self.registry,
            permissions=self.permissions,
            approve=self.approve,
            on_event=self.renderer.on_event,
            on_delta=self.renderer.on_delta,
        )


def build_session(
    *,
    model: str,
    workspace: Path,
    allow_all: bool = False,
    approve: ApproveFn | None = None,
    provider: ProviderGateway | None = None,
    store: EventStore | None = None,
    session_id: str | None = None,
    scope_id: str = "cli:local",
    write_out: Writer = _stdout,
    write_meta: Writer = _stderr,
) -> ChatSession:
    """Assemble a :class:`ChatSession` from CLI options."""
    scope = Scope(id=scope_id, kind=ScopeKind.personal, trust=TrustLevel.trusted)
    agent = AgentSpec(
        id="cli",
        name="Keel CLI",
        model=model,
        scope=scope,
        toolset=list(_READ_ONLY + _MUTATING),
    )
    tools = build_tools(workspace)
    permissions = allow_all_permissions() if allow_all else default_permissions()
    return ChatSession(
        agent=agent,
        store=store or InMemoryEventStore(),
        provider=provider or build_provider(),
        registry=ToolRegistry(tools),
        permissions=permissions,
        renderer=StreamRenderer(write_out=write_out, write_meta=write_meta),
        session_id=session_id or uuid.uuid4().hex,
        approve=approve,
    )
