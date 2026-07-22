"""Unit tests for the worker's Effect reconciliation tick (keel_worker.effects_reconciliation):
bounded backoff, provider-capability routing, lease-expiry reaping, and one-tick fencing —
all driven with in-memory doubles (no Postgres required)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from keel_core.connector_contracts import (
    ConnectorActionContext,
    ConnectorReconciliationOutcome,
    ConnectorReconciliationRequest,
    ConnectorReconciliationResult,
)
from keel_core.effect_outbox import EffectOutboxKind, InMemoryEffectReconciliationOutbox
from keel_core.effect_store import InMemoryEffectStore
from keel_core.effects import EffectStatus
from keel_worker.effects_reconciliation import EffectReconciler

SCOPE = "agent:org-a/agent-1"


class _FakeReconciler:
    def __init__(
        self, outcome: ConnectorReconciliationResult | ConnectorReconciliationOutcome
    ) -> None:
        self.outcome = outcome
        self.requests: list[ConnectorReconciliationRequest] = []

    async def reconcile(
        self, request: ConnectorReconciliationRequest
    ) -> ConnectorReconciliationResult | ConnectorReconciliationOutcome:
        self.requests.append(request)
        return self.outcome


class _FakeProvider:
    def __init__(self, reconciler: _FakeReconciler | None) -> None:
        self._reconciler = reconciler

    def build_reconciler(self, context: ConnectorActionContext) -> _FakeReconciler | None:
        return self._reconciler


class _FakeRegistry:
    def __init__(self, providers: dict[str, _FakeProvider]) -> None:
        self._providers = providers

    def create(self, connector_id: str) -> _FakeProvider:
        if connector_id not in self._providers:
            raise KeyError(connector_id)
        return self._providers[connector_id]


async def _reserve_and_make_unknown(
    store: InMemoryEffectStore, outbox: InMemoryEffectReconciliationOutbox, *, key: str = "k1"
):
    effect = await store.create_or_get(
        scope_id=SCOPE,
        org_id="org-a",
        agent_id="agent-1",
        actor_id="user-1",
        run_id="run-1",
        tool_name="email_send",
        provider="gmail",
        resource_id="",
        action_name="email_send",
        action_hash="hash-1",
        idempotency_key=key,
        args={"to": "a@example.com"},
    )
    claimed = await store.begin_execution(SCOPE, effect.id, lease_owner="w1")
    assert claimed is not None
    await store.mark_unknown(SCOPE, effect.id, lease_token=claimed.lease_token or "", error="x")
    return effect


def _reconciler(
    outbox: InMemoryEffectReconciliationOutbox,
    store: InMemoryEffectStore,
    registry: _FakeRegistry,
    *,
    incapable: bool = False,
) -> EffectReconciler:
    async def context_factory(scope_id: str) -> ConnectorActionContext | None:
        if incapable:
            return None
        return ConnectorActionContext(scope_id)

    return EffectReconciler(
        outbox=outbox,
        effect_store_factory=lambda scope_id: store,
        registry=registry,
        context_factory=context_factory,
        worker_id="test-worker",
    )


async def test_reconciliation_confirms_and_retires_the_pointer() -> None:
    outbox = InMemoryEffectReconciliationOutbox()
    store = InMemoryEffectStore(outbox)
    effect = await _reserve_and_make_unknown(store, outbox)
    fake = _FakeReconciler(
        ConnectorReconciliationResult(
            ConnectorReconciliationOutcome.confirmed,
            provider_ref="gmail-msg-1",
            result='{"id":"gmail-msg-1"}',
        )
    )
    registry = _FakeRegistry({"gmail": _FakeProvider(fake)})

    handled = await _reconciler(outbox, store, registry).run(now=datetime.now(UTC))

    assert handled == 1
    current = await store.get(SCOPE, effect.id)
    assert current is not None and current.status is EffectStatus.reconciled_confirmed
    assert current.provider_ref == "gmail-msg-1"
    assert current.result == '{"id":"gmail-msg-1"}'
    assert await outbox.get(effect.id) is None
    assert len(fake.requests) == 1
    assert fake.requests[0].idempotency_key == "k1"


async def test_reconciliation_proves_absent_and_permits_retry() -> None:
    outbox = InMemoryEffectReconciliationOutbox()
    store = InMemoryEffectStore(outbox)
    effect = await _reserve_and_make_unknown(store, outbox)
    registry = _FakeRegistry(
        {"gmail": _FakeProvider(_FakeReconciler(ConnectorReconciliationOutcome.absent))}
    )

    handled = await _reconciler(outbox, store, registry).run(now=datetime.now(UTC))

    assert handled == 1
    current = await store.get(SCOPE, effect.id)
    assert current is not None and current.status is EffectStatus.reconciled_absent
    retried = await store.begin_execution(SCOPE, effect.id, lease_owner="w2")
    assert retried is not None and retried.status is EffectStatus.executing


async def test_incapable_provider_stays_unknown_with_bounded_backoff() -> None:
    outbox = InMemoryEffectReconciliationOutbox()
    store = InMemoryEffectStore(outbox)
    effect = await _reserve_and_make_unknown(store, outbox)
    registry = _FakeRegistry({})  # no provider registered -> KeyError -> incapable

    now = datetime.now(UTC)
    handled = await _reconciler(outbox, store, registry).run(now=now)

    assert handled == 0
    current = await store.get(SCOPE, effect.id)
    assert current is not None and current.status is EffectStatus.unknown  # never inferred
    pointer = await outbox.get(effect.id)
    assert pointer is not None and pointer.due_at > now  # rescheduled with backoff
    assert pointer.attempts == 1


async def test_incapable_context_also_stays_unknown() -> None:
    """No credentials available at all (context_factory returns None) is incapable too."""
    outbox = InMemoryEffectReconciliationOutbox()
    store = InMemoryEffectStore(outbox)
    effect = await _reserve_and_make_unknown(store, outbox)
    fake = _FakeProvider(_FakeReconciler(ConnectorReconciliationOutcome.confirmed))
    registry = _FakeRegistry({"gmail": fake})

    handled = await _reconciler(outbox, store, registry, incapable=True).run(now=datetime.now(UTC))

    assert handled == 0
    current = await store.get(SCOPE, effect.id)
    assert current is not None and current.status is EffectStatus.unknown


async def test_lease_expiry_is_reaped_to_unknown_not_pending_retry() -> None:
    """Worker-restart recovery (R1B requirement 8): an expired execution lease becomes
    `unknown`, never a silent pending-retry state."""
    outbox = InMemoryEffectReconciliationOutbox()
    store = InMemoryEffectStore(outbox)
    effect = await store.create_or_get(
        scope_id=SCOPE,
        org_id="org-a",
        agent_id="agent-1",
        actor_id="user-1",
        run_id="run-1",
        tool_name="email_send",
        provider="gmail",
        resource_id="",
        action_name="email_send",
        action_hash="hash-1",
        idempotency_key="k2",
        args={"to": "a@example.com"},
    )
    claimed = await store.begin_execution(SCOPE, effect.id, lease_owner="w1", lease_seconds=1)
    assert claimed is not None
    registry = _FakeRegistry({})

    future = datetime.now(UTC) + timedelta(seconds=5)
    handled = await _reconciler(outbox, store, registry).run(now=future)

    assert handled == 1  # the lease_watch pointer was successfully reaped
    current = await store.get(SCOPE, effect.id)
    assert current is not None and current.status is EffectStatus.unknown
    assert await store.begin_execution(SCOPE, effect.id, lease_owner="w2") is None


async def test_orphaned_pointer_with_no_effect_is_retired() -> None:
    outbox = InMemoryEffectReconciliationOutbox()
    store = InMemoryEffectStore(outbox)
    await outbox.upsert_in_connection(
        None,
        effect_id="ghost",
        scope_id=SCOPE,
        org_id="org-a",
        provider="gmail",
        kind=EffectOutboxKind.reconcile,
        due_at=datetime.now(UTC),
    )
    fake = _FakeProvider(_FakeReconciler(ConnectorReconciliationOutcome.confirmed))
    registry = _FakeRegistry({"gmail": fake})

    handled = await _reconciler(outbox, store, registry).run(now=datetime.now(UTC))

    assert handled == 1
    assert await outbox.get("ghost") is None
