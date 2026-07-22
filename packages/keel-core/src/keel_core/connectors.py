"""Connectors — external accounts surfaced to the loop as scoped tools (WS-G).

ADR-0009 makes connectors first-class, but they must NOT open a second path into
the loop (P3, one tool interface / G19). So a connector action is just a
:class:`~keel_core.protocols.Tool`; the subsystem owns only auth, scoping,
provenance (taint), and outbound safety:

- **Inbound** actions (read email / fetch a page) tag their output as
  ``ContentTaint.tainted`` (G17).
- **Outbound** actions (send email / post) are durable, at-most-once, and gated by
  the confused-deputy guard. Every outbound call goes through the generic Effect
  ledger (:mod:`keel_core.effect_store`, R1B, invariants C4/C5) instead of the
  narrower ``connector_outbox`` claim/finalize/release triple
  (:mod:`keel_core.outbox`): a possible provider success followed by response loss
  becomes ``unknown`` and blocks retry until reconciliation — it can never collapse
  into an ordinary failure or have its claim silently deleted.
- :class:`ConfusedDeputyEngine` escalates an outbound action to **ask** once the
  run has ingested tainted content — a trusted agent can't be tricked by a
  malicious email into an unapproved send (G17, the headline threat of ADR-0009).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Protocol, runtime_checkable

from keel_core.effect_store import EffectStore, InMemoryEffectStore
from keel_core.effects import EffectRecord, EffectStatus, default_provider_ref
from keel_core.events import Event, EventType
from keel_core.protocols import PermissionEngine, ToolContext, ToolResult
from keel_core.runs import action_hash
from keel_core.types import ContentTaint, PermissionDecision

# A connector action: given call args + context, do the side effect and return text.
ActionFn = Callable[[dict[str, Any], ToolContext], Awaitable[str]]

# Bounded execution-lease window: long enough for a slow provider round trip, short
# enough that a crashed owner's lease expires (and is reaped to `unknown`, C4) promptly.
_DEFAULT_LEASE_SECONDS = 120
logger = logging.getLogger("keel.connectors")


class ConnectorActionUserError(Exception):
    """A connector failure whose message is safe and actionable for the end user.

    Always an *ordinary* failure (validation, auth, a provider rejection observed
    before any mutation could have landed) — the Effect goes to ``failed``
    (retryable), never ``unknown``."""


class ProviderAmbiguousError(Exception):
    """A connector action's provider request may have been accepted before the
    response was lost (timeout/connection reset during or after transmission).

    Raising this from an :data:`ActionFn` is the **only** way an outbound action may
    signal ambiguity — :class:`ConnectorTool` marks the Effect ``unknown`` rather than
    ``failed`` and blocks any further retry until reconciliation proves whether the
    mutation landed (C4). An action must never raise this for an error it can prove
    happened before the provider could have been reached (that is an ordinary
    :class:`ConnectorActionUserError`/generic failure instead) — silently inferring
    ambiguity for everything would defeat ordinary retry for the common case."""


@runtime_checkable
class Connector(Protocol):
    """An external account bound to a scope. Owns auth/lifecycle, never a tool path."""

    id: str
    required_scopes: tuple[str, ...]


def _resource_id_from_args(args: dict[str, Any]) -> str:
    """Best-effort, generic resource identity for audit (never part of Effect identity)."""
    for key in ("resource_id", "calendar_id", "channel", "repo", "to"):
        value = args.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


class ConnectorTool:
    """Surface one connector action as a :class:`~keel_core.protocols.Tool` (P3).

    ``outbound`` marks a side-effecting action (send/post) — the confused-deputy
    guard watches these. Inbound results are tainted so downstream outbound actions
    can be gated. Outbound calls with an ``idempotency_key`` are reserved, executed,
    and confirmed exactly once through the injected :class:`~keel_core.effect_store.
    EffectStore` (C4/C5); the default in-memory store keeps single-process at-most-once
    semantics, while a durable store (Postgres) makes it safe across restarts and
    workers. ``provider`` identifies the Effect's provider column (defaults to the tool's
    own name, matching the pre-Effect ``connector_outbox`` claim key).
    """

    def __init__(
        self,
        *,
        name: str,
        description: str,
        action: ActionFn,
        outbound: bool = False,
        idempotency_required: bool = False,
        input_schema: dict[str, Any] | None = None,
        effect_store: EffectStore | None = None,
        provider: str | None = None,
        lease_seconds: int = _DEFAULT_LEASE_SECONDS,
    ) -> None:
        self.name = name
        self.description = description
        self.outbound = outbound
        self.writes = outbound  # executor schedules outbound actions like writes
        self._idempotency_required = idempotency_required
        self._action = action
        self._schema = input_schema or {"type": "object"}
        self._effects = effect_store or InMemoryEffectStore()
        self._provider = provider or name
        self._lease_seconds = lease_seconds
        self._owner_id = uuid.uuid4().hex

    def input_schema(self) -> dict[str, Any]:
        return self._schema

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        try:
            if self.outbound:
                return await self._run_outbound(args, ctx)
            # Inbound: external content is untrusted -> taint it (G17).
            output = await self._action(args, ctx)
            return ToolResult(ok=True, output=output, taint=ContentTaint.tainted)
        except ConnectorActionUserError as exc:
            return ToolResult(ok=False, output=str(exc), taint=ContentTaint.clean)

    def _reflect(self, effect: EffectRecord) -> ToolResult | None:
        """A ``ToolResult`` for a status that must NOT (re)execute the provider action.

        ``None`` means the effect is eligible for a (fresh or retried) execution
        attempt (``reserved``/``failed``/``reconciled_absent``, handled by the caller
        after it wins :meth:`~keel_core.effect_store.EffectStore.begin_execution`)."""
        if effect.status in (EffectStatus.confirmed, EffectStatus.reconciled_confirmed):
            return ToolResult(
                ok=True,
                output=effect.result,
                taint=ContentTaint.clean,
                effect_id=effect.id,
                effect_status=effect.status.value,
                provider_ref=effect.provider_ref or None,
            )
        if effect.status is EffectStatus.unknown:
            return ToolResult(
                ok=False,
                output=(
                    f"{self.name}: the prior attempt's outcome is unknown (a possible "
                    "provider success followed by response loss). It will not be "
                    "retried automatically; provider reconciliation must resolve it first."
                ),
                taint=ContentTaint.clean,
                effect_id=effect.id,
                effect_status=effect.status.value,
            )
        if effect.status is EffectStatus.executing:
            # Another caller currently owns execution (or a not-yet-reaped stale lease);
            # never fire a second provider mutation. Replay empty (matches the
            # pre-Effect concurrent-claim semantics: completeness loses to at-most-once).
            return ToolResult(
                ok=True,
                output="",
                taint=ContentTaint.clean,
                effect_id=effect.id,
                effect_status=effect.status.value,
            )
        return None

    async def _keep_execution_lease(
        self, scope_id: str, effect_id: str, lease_token: str, stop: asyncio.Event
    ) -> None:
        interval = max(1, min(30, self._lease_seconds // 3))
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except TimeoutError:
                try:
                    renewed = await self._effects.renew_execution_lease(
                        scope_id,
                        effect_id,
                        lease_token=lease_token,
                        lease_seconds=self._lease_seconds,
                    )
                except Exception:
                    logger.warning(
                        "effect lease renewal failed scope=%s effect=%s",
                        scope_id,
                        effect_id,
                        exc_info=True,
                    )
                    return
                if not renewed:
                    return

    async def _reflect_after_lease_loss(
        self,
        ctx: ToolContext,
        effect_id: str,
        *,
        output: str | None = None,
        provider_ref: str = "",
    ) -> ToolResult:
        current = await self._effects.get(ctx.scope_id, effect_id)
        if (
            current is not None
            and output is not None
            and current.status
            in (
                EffectStatus.unknown,
                EffectStatus.reconciled_absent,
                EffectStatus.failed,
            )
        ):
            try:
                current = await self._effects.record_late_confirmation(
                    ctx.scope_id,
                    effect_id,
                    provider_ref=provider_ref,
                    result=output,
                )
            except LookupError:
                current = await self._effects.get(ctx.scope_id, effect_id)
        if current is not None:
            if output is not None and current.status in (
                EffectStatus.executing,
                EffectStatus.confirmed,
                EffectStatus.reconciled_confirmed,
                EffectStatus.failed,
            ):
                logger_method = (
                    logger.warning
                    if current.status in (EffectStatus.confirmed, EffectStatus.reconciled_confirmed)
                    and current.provider_ref == provider_ref
                    else logger.critical
                )
                logger_method(
                    "late provider success raced Effect state scope=%s effect=%s status=%s "
                    "provider_ref=%s current_provider_ref=%s",
                    ctx.scope_id,
                    effect_id,
                    current.status.value,
                    provider_ref,
                    current.provider_ref,
                )
            if output is not None and current.status in (
                EffectStatus.executing,
                EffectStatus.failed,
            ):
                return ToolResult(
                    ok=False,
                    output=(
                        f"{self.name}: a late provider success raced Effect state; "
                        "operator reconciliation is required."
                    ),
                    taint=ContentTaint.clean,
                    effect_id=current.id,
                    effect_status=current.status.value,
                    provider_ref=provider_ref or None,
                )
            reflected = self._reflect(current)
            if reflected is not None:
                return reflected
            return ToolResult(
                ok=False,
                output=f"{self.name}: effect ownership changed; reconciliation is required.",
                taint=ContentTaint.clean,
                effect_id=current.id,
                effect_status=current.status.value,
                provider_ref=current.provider_ref or None,
            )
        return ToolResult(
            ok=False,
            output=f"{self.name}: effect state is unavailable after lease loss.",
            taint=ContentTaint.clean,
            effect_id=effect_id,
        )

    async def _run_outbound(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        key = str(args.get("idempotency_key", ""))
        if not key and ctx.tool_call_id:
            key = f"{ctx.session_id}:{ctx.tool_call_id}"
        if not key:
            if self._idempotency_required:
                raise ValueError(f"{self.name} requires an idempotency_key")
            # No stable identity to reserve a durable Effect against — best-effort,
            # single-shot execution (unchanged from the pre-Effect behavior).
            output = await self._action(args, ctx)
            return ToolResult(ok=True, output=output, taint=ContentTaint.clean)

        effect = await self._effects.create_or_get(
            scope_id=ctx.scope_id,
            org_id=ctx.resolved_org_id,
            agent_id=ctx.agent_id,
            actor_id=ctx.actor_id,
            run_id=ctx.run_id or "",
            tool_name=self.name,
            provider=self._provider,
            resource_id=_resource_id_from_args(args),
            action_name=self.name,
            action_hash=action_hash(self.name, args),
            idempotency_key=key,
            args=args,
        )
        reflected = self._reflect(effect)
        if reflected is not None:
            return reflected

        claimed = await self._effects.begin_execution(
            ctx.scope_id,
            effect.id,
            lease_owner=self._owner_id,
            lease_seconds=self._lease_seconds,
        )
        if claimed is None:
            # Lost the race (or the ambient state moved between create_or_get and here):
            # never execute a second mutation — reflect whatever is current.
            current = await self._effects.get(ctx.scope_id, effect.id)
            if current is not None:
                reflected = self._reflect(current)
                if reflected is not None:
                    return reflected
            return ToolResult(ok=True, output="", taint=ContentTaint.clean, effect_id=effect.id)

        # The exact resolved idempotency key is always available to the action via
        # ``ctx.idempotency_key`` (even when the model omitted it and ConnectorTool
        # derived it from the tool-call id), so a provider can build a deterministic
        # reconciliation identity from it (e.g. Gmail's Message-ID header,
        # keel_core.gmail.gmail_message_id) — never from the (possibly per-attempt)
        # ``tool_call_id`` alone. ``args`` is passed through unchanged (never polluted
        # with a synthetic key) so an action/test that records/compares it verbatim sees
        # exactly what the caller supplied.
        call_ctx = ctx.model_copy(update={"idempotency_key": key})
        lease_token = claimed.lease_token or ""
        stop_lease = asyncio.Event()
        lease_keeper = asyncio.create_task(
            self._keep_execution_lease(ctx.scope_id, effect.id, lease_token, stop_lease)
        )

        async def stop_keeper() -> None:
            stop_lease.set()
            await lease_keeper

        try:
            output = await self._action(args, call_ctx)
        except ProviderAmbiguousError as exc:
            await stop_keeper()
            try:
                updated = await self._effects.mark_unknown(
                    ctx.scope_id, effect.id, lease_token=lease_token, error=str(exc)
                )
            except LookupError:
                return await self._reflect_after_lease_loss(ctx, effect.id)
            return ToolResult(
                ok=False,
                output=(
                    f"{self.name}: outcome unknown after a possible provider timeout; "
                    "reconciliation is required before any retry."
                ),
                taint=ContentTaint.clean,
                effect_id=updated.id,
                effect_status=updated.status.value,
            )
        except asyncio.CancelledError:
            await stop_keeper()
            raise
        except Exception as exc:
            # An ordinary failure (validation/auth/provider-before-send): retryable,
            # never `unknown`. Re-raised so ConnectorActionUserError still reaches the
            # `run()` handler above and any other exception the tool executor's
            # generic (never-crash-the-run) handling.
            await stop_keeper()
            try:
                await self._effects.mark_failed(
                    ctx.scope_id, effect.id, lease_token=lease_token, error=str(exc)
                )
            except LookupError:
                return await self._reflect_after_lease_loss(ctx, effect.id)
            raise

        await stop_keeper()
        provider_ref = default_provider_ref(output)
        try:
            confirmed = await self._effects.confirm(
                ctx.scope_id,
                effect.id,
                lease_token=lease_token,
                provider_ref=provider_ref,
                result=output,
            )
        except LookupError:
            return await self._reflect_after_lease_loss(
                ctx, effect.id, output=output, provider_ref=provider_ref
            )
        return ToolResult(
            ok=True,
            output=output,
            taint=ContentTaint.clean,
            effect_id=confirmed.id,
            effect_status=confirmed.status.value,
            provider_ref=provider_ref or None,
        )


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
