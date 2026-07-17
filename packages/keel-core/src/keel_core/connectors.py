"""Connectors — external accounts surfaced to the loop as scoped tools (WS-G).

ADR-0009 makes connectors first-class, but they must NOT open a second path into
the loop (P3, one tool interface / G19). So a connector action is just a
:class:`~keel_core.protocols.Tool`; the subsystem owns only auth, scoping,
provenance (taint), and outbound safety:

- **Inbound** actions (read email / fetch a page) tag their output as
  ``ContentTaint.tainted`` (G17).
- **Outbound** actions (send email / post) are **idempotent** (G20) and gated by
  the confused-deputy guard.
- :class:`ConfusedDeputyEngine` escalates an outbound action to **ask** once the
  run has ingested tainted content — a trusted agent can't be tricked by a
  malicious email into an unapproved send (G17, the headline threat of ADR-0009).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Protocol, runtime_checkable

from keel_core.events import Event, EventType
from keel_core.outbox import InMemoryOutboundStore, OutboundIdempotencyStore
from keel_core.protocols import PermissionEngine, ToolContext, ToolResult
from keel_core.types import ContentTaint, PermissionDecision

# A connector action: given call args + context, do the side effect and return text.
ActionFn = Callable[[dict[str, Any], ToolContext], Awaitable[str]]


@runtime_checkable
class Connector(Protocol):
    """An external account bound to a scope. Owns auth/lifecycle, never a tool path."""

    id: str
    required_scopes: tuple[str, ...]


class ConnectorTool:
    """Surface one connector action as a :class:`~keel_core.protocols.Tool` (P3).

    ``outbound`` marks a side-effecting action (send/post) — the confused-deputy
    guard watches these. Inbound results are tainted so downstream outbound actions
    can be gated. Outbound calls are idempotent on ``idempotency_key`` (G20) via an
    injected :class:`~keel_core.outbox.OutboundIdempotencyStore`; the default in-memory
    store keeps single-process at-most-once, while a durable store (Postgres) makes it
    safe across restarts and workers.
    """

    def __init__(
        self,
        *,
        name: str,
        description: str,
        action: ActionFn,
        outbound: bool = False,
        input_schema: dict[str, Any] | None = None,
        idempotency_store: OutboundIdempotencyStore | None = None,
    ) -> None:
        self.name = name
        self.description = description
        self.outbound = outbound
        self.writes = outbound  # executor schedules outbound actions like writes
        self._action = action
        self._schema = input_schema or {"type": "object"}
        self._idempotency = idempotency_store or InMemoryOutboundStore()

    def input_schema(self) -> dict[str, Any]:
        return self._schema

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        if self.outbound:
            return await self._run_outbound(args, ctx)
        # Inbound: external content is untrusted -> taint it (G17).
        output = await self._action(args, ctx)
        return ToolResult(ok=True, output=output, taint=ContentTaint.tainted)

    async def _run_outbound(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        key = str(args.get("idempotency_key", ""))
        if not key:
            output = await self._action(args, ctx)
            return ToolResult(ok=True, output=output, taint=ContentTaint.clean)
        claim = await self._idempotency.claim(ctx.scope_id, self.name, key)
        if not claim.owner:
            # Replay a finalized result; a still-pending concurrent claim replays empty
            # rather than re-sending (at-most-once wins over completeness).
            return ToolResult(ok=True, output=claim.result or "", taint=ContentTaint.clean)
        try:
            output = await self._action(args, ctx)
        except Exception:
            # Release the claim so a later retry may re-send; the side effect did not land.
            await self._idempotency.release(ctx.scope_id, self.name, key)
            raise
        await self._idempotency.finalize(ctx.scope_id, self.name, key, output)
        return ToolResult(ok=True, output=output, taint=ContentTaint.clean)


def taint_from_events(events: Iterable[Event]) -> ContentTaint:
    """Tainted if prior connector tool output or admitted external input is tainted."""
    for event in events:
        if event.type in {EventType.tool_result, EventType.message_token} and event.payload.get(
            "taint"
        ) == str(ContentTaint.tainted):
            return ContentTaint.tainted
    return ContentTaint.clean


class ConfusedDeputyEngine:
    """Wrap a permission engine to gate outbound actions on tainted content (G17).

    Once the run has ingested tainted content, any **outbound** connector action is
    escalated to at least ``ask`` (most-restrictive wins), so a human must approve —
    tainted input alone can never trigger an unapproved send. Everything else defers
    to the wrapped engine.
    """

    def __init__(self, base: PermissionEngine, outbound_tools: Iterable[str]) -> None:
        self._base = base
        self._outbound = frozenset(outbound_tools)

    def evaluate(self, tool: str, args: dict[str, Any], ctx: ToolContext) -> PermissionDecision:
        decision = self._base.evaluate(tool, args, ctx)
        if tool in self._outbound and ctx.content_taint is ContentTaint.tainted:
            # Escalate to ask (deny > ask > allow); a pre-existing deny stays deny.
            if decision is PermissionDecision.allow:
                return PermissionDecision.ask
        return decision
