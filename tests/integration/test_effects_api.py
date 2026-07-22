"""Integration: the durable Effect ledger REST API (R1B, C4/C5) — list/get/reconcile/retry."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.connector_contracts import (
    ConnectorReconciliationOutcome,
    ConnectorReconciliationResult,
)
from keel_core.effect_store import PostgresEffectStore
from keel_core.effects import EffectStatus
from keel_server.api.effects import router

pytestmark = pytest.mark.integration


def _scope() -> str:
    return f"agent:org-{uuid.uuid4().hex}/agent-1"


async def _reserve(store: PostgresEffectStore, scope_id: str, key: str = "k1"):
    return await store.create_or_get(
        scope_id=scope_id,
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


@pytest_asyncio.fixture
async def effects_client(migrated_db: AsyncEngine) -> AsyncIterator[tuple[httpx.AsyncClient, str]]:
    scope = _scope()
    app = FastAPI()
    app.include_router(router)
    app.state.engine = migrated_db
    app.state.durable_scope = scope
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, scope


async def test_list_and_get_effect(
    migrated_db: AsyncEngine, effects_client: tuple[httpx.AsyncClient, str]
) -> None:
    client, scope = effects_client
    store = PostgresEffectStore(migrated_db)
    effect = await _reserve(store, scope)

    listed = (await client.get("/v1/effects")).json()
    assert [row["id"] for row in listed] == [effect.id]
    assert listed[0]["status"] == "reserved"

    got = await client.get(f"/v1/effects/{effect.id}")
    assert got.status_code == 200
    assert got.json()["idempotency_key"] == "k1"


async def test_get_missing_effect_is_404(
    effects_client: tuple[httpx.AsyncClient, str],
) -> None:
    client, _scope = effects_client
    response = await client.get("/v1/effects/does-not-exist")
    assert response.status_code == 404


async def test_list_filters_by_status(
    migrated_db: AsyncEngine, effects_client: tuple[httpx.AsyncClient, str]
) -> None:
    client, scope = effects_client
    store = PostgresEffectStore(migrated_db)
    reserved = await _reserve(store, scope, key="k1")
    executing_effect = await _reserve(store, scope, key="k2")
    await store.begin_execution(scope, executing_effect.id, lease_owner="w1")

    only_reserved = (await client.get("/v1/effects", params={"status": "reserved"})).json()
    assert [row["id"] for row in only_reserved] == [reserved.id]

    only_executing = (await client.get("/v1/effects", params={"status": "executing"})).json()
    assert [row["id"] for row in only_executing] == [executing_effect.id]


async def test_reconcile_rejects_non_unknown_effect(
    migrated_db: AsyncEngine, effects_client: tuple[httpx.AsyncClient, str]
) -> None:
    client, scope = effects_client
    store = PostgresEffectStore(migrated_db)
    effect = await _reserve(store, scope)

    response = await client.post(f"/v1/effects/{effect.id}/reconcile")
    assert response.status_code == 409


async def test_reconcile_confirms_via_provider_reconciler(
    migrated_db: AsyncEngine,
    effects_client: tuple[httpx.AsyncClient, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, scope = effects_client
    store = PostgresEffectStore(migrated_db)
    effect = await _reserve(store, scope)
    claimed = await store.begin_execution(scope, effect.id, lease_owner="w1")
    assert claimed is not None
    await store.mark_unknown(scope, effect.id, lease_token=claimed.lease_token or "", error="x")

    class _FakeReconciler:
        async def reconcile(self, request: object) -> ConnectorReconciliationResult:
            assert getattr(request, "unknown_since", None) is not None
            return ConnectorReconciliationResult(
                ConnectorReconciliationOutcome.confirmed,
                provider_ref="provider-123",
                result='{"id":"provider-123"}',
            )

    class _FakeProvider:
        def build_reconciler(self, context: object) -> _FakeReconciler:
            return _FakeReconciler()

    import keel_server.api.effects as effects_api

    async def fake_context(request: object, scope_id: str) -> object:
        return object()

    monkeypatch.setattr(effects_api, "_reconciliation_context", fake_context)

    class _FakeRegistry:
        def create(self, connector_id: str) -> _FakeProvider:
            return _FakeProvider()

    monkeypatch.setattr(effects_api, "get_connector_registry", lambda: _FakeRegistry())

    response = await client.post(f"/v1/effects/{effect.id}/reconcile")
    assert response.status_code == 200
    assert response.json()["status"] == EffectStatus.reconciled_confirmed.value
    assert response.json()["provider_ref"] == "provider-123"


async def test_reconcile_race_returns_conflict_instead_of_500(
    migrated_db: AsyncEngine,
    effects_client: tuple[httpx.AsyncClient, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, scope = effects_client
    store = PostgresEffectStore(migrated_db)
    effect = await _reserve(store, scope)
    claimed = await store.begin_execution(scope, effect.id, lease_owner="w1")
    assert claimed is not None
    await store.mark_unknown(scope, effect.id, lease_token=claimed.lease_token or "", error="x")

    class _FakeReconciler:
        async def reconcile(self, request: object) -> ConnectorReconciliationOutcome:
            await store.reconcile_absent(scope, effect.id)
            return ConnectorReconciliationOutcome.confirmed

    class _FakeProvider:
        def build_reconciler(self, context: object) -> _FakeReconciler:
            return _FakeReconciler()

    import keel_server.api.effects as effects_api

    async def fake_context(request: object, scope_id: str) -> object:
        return object()

    monkeypatch.setattr(effects_api, "_reconciliation_context", fake_context)

    class _FakeRegistry:
        def create(self, connector_id: str) -> _FakeProvider:
            return _FakeProvider()

    monkeypatch.setattr(effects_api, "get_connector_registry", lambda: _FakeRegistry())

    response = await client.post(f"/v1/effects/{effect.id}/reconcile")
    assert response.status_code == 409
    assert "already resolved" in response.json()["detail"]


async def test_retry_eligibility_reports_blocked_while_unknown(
    migrated_db: AsyncEngine, effects_client: tuple[httpx.AsyncClient, str]
) -> None:
    client, scope = effects_client
    store = PostgresEffectStore(migrated_db)
    effect = await _reserve(store, scope)
    claimed = await store.begin_execution(scope, effect.id, lease_owner="w1")
    assert claimed is not None
    await store.mark_unknown(scope, effect.id, lease_token=claimed.lease_token or "", error="x")

    response = await client.post(f"/v1/effects/{effect.id}/retry")
    assert response.status_code == 200
    body = response.json()
    assert body["retryable"] is False
    assert "unknown" in body["detail"]


async def test_retry_eligibility_true_after_ordinary_failure(
    migrated_db: AsyncEngine, effects_client: tuple[httpx.AsyncClient, str]
) -> None:
    client, scope = effects_client
    store = PostgresEffectStore(migrated_db)
    effect = await _reserve(store, scope)
    claimed = await store.begin_execution(scope, effect.id, lease_owner="w1")
    assert claimed is not None
    await store.mark_failed(scope, effect.id, lease_token=claimed.lease_token or "", error="boom")

    response = await client.post(f"/v1/effects/{effect.id}/retry")
    assert response.status_code == 200
    body = response.json()
    assert body["retryable"] is True


async def test_cross_scope_effect_is_not_visible(
    migrated_db: AsyncEngine, effects_client: tuple[httpx.AsyncClient, str]
) -> None:
    client, scope = effects_client
    other_scope = _scope()
    store = PostgresEffectStore(migrated_db)
    foreign = await _reserve(store, other_scope, key="foreign")

    assert (await client.get(f"/v1/effects/{foreign.id}")).status_code == 404
    listed = (await client.get("/v1/effects")).json()
    assert foreign.id not in [row["id"] for row in listed]
