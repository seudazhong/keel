"""`keel-core` Protocols — the frozen seams surfaces/impls code against (P2/P3).

M0 freezes signatures only; **no behaviour**. Implementations land in M1 behind
these Protocols:

- ``Tool``            — one tool interface (P3): built-ins, MCP, sub-agents look identical.
- ``EventStore``      — append-only event log + replayable reads (``after=``).
- ``ProviderGateway`` — LLM provider seam (streaming, cache key).
- ``PermissionEngine``— allow/ask/deny (deny > ask > allow; default ask).
- ``PromptAssembler`` — byte-stable prompt prefix + stable cache key.
- ``ScopeGuard``      — per-scope data isolation seam (ADR-0009 / G16).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from .agents import AgentSpec
from .events import Event
from .types import (
    ContentTaint,
    FinishReason,
    PermissionDecision,
    ScopeId,
    SessionId,
    TrustLevel,
)

# --- Supporting contract models ------------------------------------------------


class ToolContext(BaseModel):
    """Ambient context handed to every tool invocation."""

    scope_id: ScopeId
    session_id: SessionId
    trust: TrustLevel = TrustLevel.untrusted


class ToolResult(BaseModel):
    """Tool output. ``output`` is model-facing (bounded); ``display`` user-facing.

    Full output spills to ``spill_path`` (FR-T5). ``taint`` propagates content
    trust for the confused-deputy guard (G17).
    """

    ok: bool
    output: str = ""
    display: str | None = None
    spill_path: str | None = None
    taint: ContentTaint = ContentTaint.clean


class ProviderRequest(BaseModel):
    """A normalized provider request."""

    model: str
    messages: list[dict[str, Any]] = Field(default_factory=list)
    tools: list[dict[str, Any]] = Field(default_factory=list)
    prompt_cache_key: str | None = None


class ToolCall(BaseModel):
    """A tool invocation requested by the model."""

    id: str
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class ProviderChunk(BaseModel):
    """A streamed provider delta.

    ``tool_call`` carries a requested tool invocation; ``finish_reason`` is set on
    the terminal chunk of a turn. The loop opens the tool gate only when
    ``finish_reason == tool_use`` (stop-reason-gated invariant).
    """

    delta: str = ""
    thinking: str = ""
    tool_call: ToolCall | None = None
    finish_reason: FinishReason | None = None


class PromptBundle(BaseModel):
    """Assembled prompt. ``prefix`` is byte-stable/cache-friendly (invariant).

    ``cache_key`` is derived only from the stable prefix; volatile content
    (memory, live context) must live in ``suffix``, never the prefix.
    """

    prefix: str
    suffix: str
    cache_key: str


# --- Protocols (seams) ---------------------------------------------------------


@runtime_checkable
class Tool(Protocol):
    """One tool interface for built-ins, MCP tools, skills and sub-agents (P3)."""

    name: str
    description: str

    def input_schema(self) -> dict[str, Any]:
        """Return the JSON Schema for this tool's arguments."""
        ...

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        """Execute the tool and return a bounded result."""
        ...


class EventStore(Protocol):
    """Append-only event log with replayable reads."""

    async def append(self, event: Event) -> None:
        """Durably append one event."""
        ...

    def read(self, session_id: SessionId, after: int | None = None) -> AsyncIterator[Event]:
        """Stream events for a session with ``seq > after`` (replay cursor)."""
        ...


class ProviderGateway(Protocol):
    """LLM provider seam — borrow plumbing (LiteLLM), own policy."""

    def stream(self, request: ProviderRequest) -> AsyncIterator[ProviderChunk]:
        """Stream a completion as normalized chunks (async generator)."""
        ...


class PermissionEngine(Protocol):
    """Fail-closed permission verdicts (default ask)."""

    def evaluate(self, tool: str, args: dict[str, Any], ctx: ToolContext) -> PermissionDecision:
        """Return the decision for a tool call in a given context."""
        ...


class PromptAssembler(Protocol):
    """Assemble a layered prompt with a byte-stable, cache-friendly prefix."""

    def assemble(self, agent: AgentSpec, history: Sequence[Event]) -> PromptBundle:
        """Build the prompt bundle for the next model call."""
        ...


class ScopeGuard(Protocol):
    """Per-scope data-isolation seam (ADR-0009 / DESIGN-REVIEW G16).

    Every scoped repository routes access through ``enforce``; a cross-scope
    attempt raises ``CrossScopeError`` and is audited. This is the enforcement
    home of the per-scope data-isolation invariant.
    """

    def enforce(self, actor_scope: ScopeId, resource_scope: ScopeId) -> None:
        """Deny (and audit) if ``actor_scope`` may not access ``resource_scope``."""
        ...
