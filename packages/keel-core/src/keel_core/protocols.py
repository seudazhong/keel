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
from .citations import Citation as Citation
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
    # Trust of the *content* seen so far this run (G17). A trusted scope may still
    # ingest tainted content (an email body, a web page); outbound actions gate on it.
    content_taint: ContentTaint = ContentTaint.clean
    # Stable provider tool-call id for durable idempotency. The executor fills this per call.
    tool_call_id: str | None = None
    # R1B: the exact resolved idempotency key ConnectorTool reserved this Effect under
    # (set by ConnectorTool itself before invoking an outbound action — never by the
    # model/caller). Lets an action build a deterministic provider-side reconciliation
    # identity (e.g. Gmail's Message-ID) from the *same* key regardless of whether the
    # model supplied one or it was derived from ``tool_call_id`` — without polluting the
    # action's own ``args`` dict (which many callers compare/record verbatim).
    idempotency_key: str | None = None
    # R1B durable Effect ledger (C4/C5) audit columns — best-effort, additive. ``run_id``
    # is the loop's own run identifier; ``org_id``/``actor_id`` come from the run's
    # ``ApprovalBinding`` when one was supplied (blank for a legacy/binding-less run — the
    # Effect ledger still isolates correctly on ``scope_id`` alone). ``agent_id`` is parsed
    # from the derived per-Agent ``scope_id`` (blank for the local-preview scope).
    run_id: str | None = None
    org_id: str = ""
    actor_id: str = ""

    @property
    def agent_id(self) -> str:
        from keel_core.scoping import parse_agent_scope

        parsed = parse_agent_scope(self.scope_id)
        return parsed[1] if parsed is not None else ""

    @property
    def resolved_org_id(self) -> str:
        """``org_id`` when explicitly set (an ``ApprovalBinding``-carrying run), else
        best-effort parsed from the derived per-Agent ``scope_id`` (R1B audit columns —
        never used for authorization, only traceability)."""
        if self.org_id:
            return self.org_id
        from keel_core.scoping import parse_agent_scope

        parsed = parse_agent_scope(self.scope_id)
        return parsed[0] if parsed is not None else ""


class ToolResult(BaseModel):
    """Tool output. ``output`` is model-facing (bounded); ``display`` user-facing.

    Full output spills to ``spill_path`` (FR-T5). ``taint`` propagates content
    trust for the confused-deputy guard (G17). ``effect_id``/``effect_status``/
    ``provider_ref`` (R1B) surface the durable Effect ledger record an outbound
    connector action reserved/executed, never a secret — only carried by
    :class:`~keel_core.connectors.ConnectorTool`'s outbound path.
    """

    ok: bool
    output: str = ""
    display: str | None = None
    spill_path: str | None = None
    taint: ContentTaint = ContentTaint.clean
    citations: list[Citation] = Field(default_factory=list)
    effect_id: str | None = None
    effect_status: str | None = None
    provider_ref: str | None = None


class ProviderRequest(BaseModel):
    """A normalized provider request."""

    model: str
    messages: list[dict[str, Any]] = Field(default_factory=list)
    tools: list[dict[str, Any]] = Field(default_factory=list)
    prompt_cache_key: str | None = None
    # Optional hard cap on generated (output) tokens for this turn. ``None`` leaves the
    # provider default in place; a caller that must bound generation (e.g. the read-only
    # review engine) sets it so an unbounded completion can never be requested.
    max_output_tokens: int | None = None


class ToolCall(BaseModel):
    """A tool invocation requested by the model."""

    id: str
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class Usage(BaseModel):
    """Token + cost accounting for a provider turn (WS-H)."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    cache_read_tokens: int = 0  # prompt-cache hits (cheaper); ADR-0003 / NFR-8
    cost_usd: float = 0.0

    def __add__(self, other: Usage) -> Usage:
        return Usage(
            prompt_tokens=self.prompt_tokens + other.prompt_tokens,
            completion_tokens=self.completion_tokens + other.completion_tokens,
            cache_read_tokens=self.cache_read_tokens + other.cache_read_tokens,
            cost_usd=self.cost_usd + other.cost_usd,
        )


class ProviderChunk(BaseModel):
    """A streamed provider delta.

    ``tool_call`` carries a requested tool invocation; ``finish_reason`` is set on
    the terminal chunk of a turn. The loop opens the tool gate only when
    ``finish_reason == tool_use`` (stop-reason-gated invariant). ``usage`` is set on
    a trailing accounting chunk (token + cost totals for the turn).
    """

    delta: str = ""
    thinking: str = ""
    tool_call: ToolCall | None = None
    finish_reason: FinishReason | None = None
    usage: Usage | None = None


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
