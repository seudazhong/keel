"""Worker-side R1B Effect reconciliation: bounded, fenced, provider-capability-routed.

Owns exactly the background half of the durable Effect ledger (C4/C5) — the durable
reserve/execute/confirm state machine itself lives in :mod:`keel_core.effect_store`; this
module only *discovers and drives forward* the Effects that need attention across every
scope, via the global (non-RLS) :class:`~keel_core.effect_outbox.EffectReconciliationOutbox`
pointer:

* ``lease_watch`` pointers — an execution lease is outstanding. Once its deadline passes
  (the owning worker crashed before ``confirm``/``mark_unknown``/``mark_failed``), reap it
  to ``unknown`` — never back to a plain pending/retryable state (the request may have
  already reached the provider, C4).
* ``reconcile`` pointers — the Effect is ``unknown``. Resolve the owning provider's
  reconciliation capability (:meth:`~keel_core.connector_contracts.ConnectorProvider.
  build_reconciler`) and ask it to prove confirmed/absent. An **incapable** provider (no
  reconciliation seam) is never guessed at — the pointer is rescheduled with bounded
  backoff and the Effect stays ``unknown``, surfaced to an operator via the API instead.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings, get_settings
from keel_core.connector_contracts import (
    ConnectorActionContext,
    ConnectorReconciliationOutcome,
    ConnectorReconciliationRequest,
    reconciliation_result,
)
from keel_core.connector_credentials import ConnectorCredentialStore
from keel_core.connector_registry import ConnectorProviderUnavailableError, ConnectorRegistry
from keel_core.connector_repository import PostgresConnectorRepository
from keel_core.effect_outbox import (
    EffectOutboxEntry,
    EffectOutboxKind,
    EffectReconciliationOutbox,
)
from keel_core.effect_store import EffectStore
from keel_core.secrets import SecretsError, keyring_from_settings
from keel_core.tokens import PostgresTokenStore

logger = logging.getLogger("keel.worker.effects")

EFFECT_RECONCILE_LIMIT = 100
EFFECT_RECONCILE_LEASE_SECONDS = 120
# Bounded exponential backoff for an `incapable` provider (never gives up — the Effect
# stays `unknown` and is surfaced via the API for an operator/user decision, R1B
# requirement 6/7 — but a hopeless per-tick retry storm is still bounded).
_INCAPABLE_BACKOFF_BASE_SECONDS = 30
_INCAPABLE_BACKOFF_MAX_SECONDS = 3600
_LEASE_NOT_YET_DUE_DELAY_SECONDS = 15
_ORPHAN_RESCHEDULE_SECONDS = 300


def _now() -> datetime:
    return datetime.now(UTC)


def _backoff_seconds(attempts: int) -> int:
    growth = _INCAPABLE_BACKOFF_BASE_SECONDS * (2 ** max(attempts, 0))
    return int(min(growth, _INCAPABLE_BACKOFF_MAX_SECONDS))


async def _reconciliation_context(
    engine: AsyncEngine,
    settings: Settings,
    scope_id: str,
) -> ConnectorActionContext | None:
    """Build the minimal per-scope context a provider needs to build a reconciler.

    Mirrors :func:`keel_core.connector_actions.build_connector_actions`'s credential
    construction, but only what :meth:`ConnectorProvider.build_reconciler` needs
    (credentials) — no action wiring, no health tracking."""
    if not settings.secret_key and not settings.secret_keys:
        return None
    try:
        keyring = keyring_from_settings(settings)
    except SecretsError:
        return None
    repository = PostgresConnectorRepository(engine, scope_id)
    token_store = PostgresTokenStore(engine, scope_id, keyring)
    credential_store = ConnectorCredentialStore(token_store)
    return ConnectorActionContext.with_repository(
        scope_id,
        repository,
        credential_store=token_store,
        envelope_credential_store=credential_store,
    )


@dataclass
class EffectReconciler:
    """Fenced, bounded reconciler over the global Effect reconciliation pointer index.

    All dependencies are injected so a unit test can drive it with in-memory doubles and
    an integration test with Postgres. One tick leases a bounded batch of due pointers
    under a random lease token and reconciles each independently; one pointer's failure
    defers only that pointer (never crashes the tick)."""

    outbox: EffectReconciliationOutbox
    effect_store_factory: Callable[[str], EffectStore]
    registry: ConnectorRegistry
    context_factory: Callable[[str], Awaitable[ConnectorActionContext | None]]
    worker_id: str
    batch_limit: int = EFFECT_RECONCILE_LIMIT
    claim_lease_seconds: int = EFFECT_RECONCILE_LEASE_SECONDS
    clock: Callable[[], datetime] = field(default=lambda: _now())

    async def run(self, *, now: datetime | None = None) -> int:
        """Reconcile one bounded batch of due pointers; returns the number handled."""
        moment = now or self.clock()
        claimed = await self.outbox.claim_due(
            worker_id=self.worker_id,
            now=moment,
            limit=self.batch_limit,
            lease_seconds=self.claim_lease_seconds,
        )
        handled = 0
        for entry in claimed:
            try:
                if await self._reconcile_entry(entry, now=moment):
                    handled += 1
            except Exception:  # noqa: BLE001 - one pointer's failure must not crash the tick
                logger.warning(
                    "effect reconcile deferred one pointer kind=%s provider=%s",
                    entry.kind.value,
                    entry.provider,
                    exc_info=True,
                )
                await self._defer(entry, delay_seconds=_LEASE_NOT_YET_DUE_DELAY_SECONDS, now=moment)
        if handled:
            logger.info("effect reconciler handled %d pointer(s)", handled)
        return handled

    async def _defer(self, entry: EffectOutboxEntry, *, delay_seconds: int, now: datetime) -> None:
        if entry.lease_token is None:
            return
        try:
            await self.outbox.reschedule(
                entry.effect_id, lease_token=entry.lease_token, delay_seconds=delay_seconds, now=now
            )
        except Exception:  # noqa: BLE001 - best-effort release after an already-logged failure
            logger.debug("effect reconcile lease release failed after deferral")

    async def _reconcile_entry(self, entry: EffectOutboxEntry, *, now: datetime) -> bool:
        token = entry.lease_token
        if token is None:
            return False
        store = self.effect_store_factory(entry.scope_id)
        if entry.kind is EffectOutboxKind.lease_watch:
            return await self._reap_one(entry, store, token=token, now=now)
        return await self._reconcile_one(entry, store, token=token, now=now)

    async def _reap_one(
        self, entry: EffectOutboxEntry, store: EffectStore, *, token: str, now: datetime
    ) -> bool:
        reaped = await store.reap_expired_lease(entry.scope_id, entry.effect_id, now=now)
        if reaped is not None:
            # The store already re-pointed the outbox row to `reconcile` in the same
            # transaction — this tick's claim on the (now-superseded) pointer is done.
            return True
        # Not actually due yet (a race between the outbox's mirrored deadline and the
        # effect row) — release the lease and check again shortly.
        await self.outbox.reschedule(
            entry.effect_id,
            lease_token=token,
            delay_seconds=_LEASE_NOT_YET_DUE_DELAY_SECONDS,
            now=now,
        )
        return False

    async def _reconcile_one(
        self, entry: EffectOutboxEntry, store: EffectStore, *, token: str, now: datetime
    ) -> bool:
        effect = await store.get(entry.scope_id, entry.effect_id)
        if effect is None:
            # Orphaned pointer (scope purged / effect otherwise removed): retire it.
            await self.outbox.complete(entry.effect_id, lease_token=token)
            return True
        if effect.status.value != "unknown":
            # Already resolved by some other path — retire the stale pointer.
            await self.outbox.complete(entry.effect_id, lease_token=token)
            return True

        context = await self.context_factory(entry.scope_id)
        reconciler = None
        if context is not None:
            try:
                provider = self.registry.create(entry.provider)
            except (KeyError, ConnectorProviderUnavailableError):
                provider = None
            if provider is not None:
                reconciler = provider.build_reconciler(context)

        if reconciler is None:
            # Incapable (no reconciliation seam, or credentials unavailable): never
            # inferred confirmed/absent. Bounded backoff; the Effect stays `unknown`.
            await self.outbox.reschedule(
                entry.effect_id,
                lease_token=token,
                delay_seconds=_backoff_seconds(entry.attempts),
                now=now,
            )
            return False

        request = ConnectorReconciliationRequest(
            scope_id=entry.scope_id,
            connector_id=entry.provider,
            action_name=effect.action_name,
            idempotency_key=effect.idempotency_key,
            resource_id=effect.resource_id,
            provider_ref=effect.provider_ref,
            canonical_args=effect.canonical_args,
            unknown_since=effect.updated_at,
        )
        resolution = reconciliation_result(await reconciler.reconcile(request))
        try:
            if resolution.outcome is ConnectorReconciliationOutcome.confirmed:
                await store.reconcile_confirmed(
                    entry.scope_id,
                    effect.id,
                    provider_ref=resolution.provider_ref or effect.provider_ref,
                    result=resolution.result or effect.result,
                )
                return True
            if resolution.outcome is ConnectorReconciliationOutcome.absent:
                await store.reconcile_absent(entry.scope_id, effect.id)
                return True
        except LookupError:
            # Another reconciler won the compare-and-set while this provider lookup was in flight.
            await self.outbox.complete(entry.effect_id, lease_token=token)
            return True
        # incapable (the provider itself reports it cannot prove this one, e.g. missing
        # per-effect identity): same bounded-backoff posture as a missing reconciler.
        await self.outbox.reschedule(
            entry.effect_id,
            lease_token=token,
            delay_seconds=_backoff_seconds(entry.attempts),
            now=now,
        )
        return False


def build_effect_reconciler(
    *,
    engine: AsyncEngine,
    outbox: EffectReconciliationOutbox,
    effect_store_factory: Callable[[str], EffectStore],
    registry: ConnectorRegistry,
    settings: Settings | None = None,
    worker_id: str | None = None,
) -> EffectReconciler:
    resolved_settings = settings or get_settings()

    async def context_factory(scope_id: str) -> ConnectorActionContext | None:
        return await _reconciliation_context(engine, resolved_settings, scope_id)

    return EffectReconciler(
        outbox=outbox,
        effect_store_factory=effect_store_factory,
        registry=registry,
        context_factory=context_factory,
        worker_id=worker_id or f"effects:{uuid.uuid4().hex[:12]}",
    )


async def reconcile_effects_tick(ctx: dict[str, Any]) -> int:
    """Cron entrypoint: run one bounded reconcile pass over the global Effect pointer.

    Returns ``0`` (no-op) on a worker where the Effect ledger is unwired (no reconciler
    on ``ctx`` — e.g. the in-memory/lite profile with no durable engine)."""
    reconciler: EffectReconciler | None = ctx.get("effect_reconciler")
    if reconciler is None:
        return 0
    return await reconciler.run()


__all__ = [
    "EFFECT_RECONCILE_LEASE_SECONDS",
    "EFFECT_RECONCILE_LIMIT",
    "EffectReconciler",
    "build_effect_reconciler",
    "reconcile_effects_tick",
]
