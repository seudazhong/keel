"""Webhook scope routing: high-entropy route token -> scope-bound connector ingress (finding).

An inbound provider webhook carries no Keel auth headers, so the generated webhook URL embeds a
high-entropy route token that the ingress resolves against a global routing capability table to
find the exact org/Agent scope + binding — never the app-global ``web:local`` scope. These tests
cover the route store, the service minting/persisting/removing a route on setup/revoke, and the
router resolving the token (cross-provider/scope mismatch denied, cloud requires the routed
capability, local tokenless webhooks stay backward-compatible).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport

from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAuthKind,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCapability,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorIngressRequest,
    ConnectorIngressResponse,
    ConnectorIngressResult,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorSetupArtifact,
    ConnectorSetupArtifactKind,
    ConnectorSetupResult,
)
from keel_core.connector_registry import ConnectorRegistration, ConnectorRegistry
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.connector_service import ConnectorService
from keel_core.connector_webhook_routes import (
    InMemoryConnectorWebhookRouteStore,
    mint_route_token,
)
from keel_server.api.connectors import router

_SCOPE = "agent:orga/ag1"


class RoutingProvider(BaseConnectorProvider):
    manifest = ConnectorManifest(
        id="routing_fixture",
        name="Routing fixture",
        description="webhook routing fixture",
        auth_kind=ConnectorAuthKind.secret,
        capabilities=(ConnectorCapability.webhook,),
    )

    async def setup(
        self, context: ConnectorOperationContext, values: dict[str, str]
    ) -> ConnectorSetupResult:
        return ConnectorSetupResult(
            ConnectorBindingDraft(display_name="Routing fixture"),
            artifacts=(
                ConnectorSetupArtifact(
                    ConnectorSetupArtifactKind.url,
                    "Webhook URL",
                    f"{context.callback_base_url}/webhook",
                ),
            ),
        )

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        return ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC))

    async def ingress(
        self, context: ConnectorOperationContext, request: ConnectorIngressRequest
    ) -> ConnectorIngressResult:
        # Echo the scope the delivery was routed into so tests can assert it.
        return ConnectorIngressResult(
            ConnectorIngressResponse(
                status_code=202,
                content_type="application/json",
                headers={"x-routed-scope": context.scope_id},
                body=b'{"ok":true}',
            )
        )

    async def revoke(self, context: ConnectorOperationContext) -> None:
        return None


# --- Route store -----------------------------------------------------------------------------


async def test_route_store_put_resolve_and_delete() -> None:
    store = InMemoryConnectorWebhookRouteStore()
    token = mint_route_token()
    await store.put(token, _SCOPE, "routing_fixture", "binding-1", "connected")
    route = await store.resolve(token)
    assert route is not None
    assert (route.scope_id, route.connector_id, route.binding_id) == (
        _SCOPE,
        "routing_fixture",
        "binding-1",
    )
    assert await store.resolve("nope") is None
    await store.delete_for_connector(_SCOPE, "routing_fixture")
    assert await store.resolve(token) is None


async def test_route_store_keeps_one_route_per_connector() -> None:
    store = InMemoryConnectorWebhookRouteStore()
    first = mint_route_token()
    second = mint_route_token()
    await store.put(first, _SCOPE, "routing_fixture", "binding-1", "connected")
    await store.put(second, _SCOPE, "routing_fixture", "binding-1", "connected")
    # The prior token is replaced (setup re-run rotates the route).
    assert await store.resolve(first) is None
    assert (await store.resolve(second)) is not None


def _service(store: InMemoryConnectorWebhookRouteStore) -> ConnectorService:
    registry = ConnectorRegistry(
        (ConnectorRegistration(RoutingProvider.manifest, RoutingProvider, "tests.routing"),)
    )
    return ConnectorService(
        registry,
        InMemoryConnectorRepository(_SCOPE),
        webhook_route_store=store,
    )


async def test_setup_mints_route_and_webhook_url_contains_token() -> None:
    store = InMemoryConnectorWebhookRouteStore()
    service = _service(store)
    outcome = await service.setup(
        "routing_fixture",
        {},
        callback_base_url="https://keel.example/v1/connectors/routing_fixture",
    )
    webhook_url = outcome.artifacts[0].value
    # The webhook URL the provider registers embeds the routed token segment.
    assert "/r/" in webhook_url and webhook_url.endswith("/webhook")
    token = webhook_url.split("/r/")[1].split("/webhook")[0]
    route = await store.resolve(token)
    assert route is not None
    assert route.scope_id == _SCOPE and route.connector_id == "routing_fixture"
    assert route.binding_id == outcome.binding.id


async def test_revoke_removes_route() -> None:
    store = InMemoryConnectorWebhookRouteStore()
    service = _service(store)
    outcome = await service.setup(
        "routing_fixture",
        {},
        callback_base_url="https://keel.example/v1/connectors/routing_fixture",
    )
    token = outcome.artifacts[0].value.split("/r/")[1].split("/webhook")[0]
    assert await store.resolve(token) is not None
    await service.revoke("routing_fixture")
    assert await store.resolve(token) is None


# --- HTTP routing ----------------------------------------------------------------------------


@pytest_asyncio.fixture
async def routed_app() -> AsyncIterator[tuple[httpx.AsyncClient, FastAPI]]:
    app = FastAPI()
    app.include_router(router)
    app.state.durable_scope = _SCOPE
    app.state.connector_registry = ConnectorRegistry(
        (ConnectorRegistration(RoutingProvider.manifest, RoutingProvider, "tests.routing"),)
    )
    app.state.connector_repository = InMemoryConnectorRepository(_SCOPE)
    app.state.connector_webhook_route_store = InMemoryConnectorWebhookRouteStore()
    app.state.auth_required = False
    await app.state.connector_repository.upsert_binding(
        "routing_fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, app


async def test_routed_webhook_resolves_scope_and_ingests(
    routed_app: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, app = routed_app
    token = mint_route_token()
    await app.state.connector_webhook_route_store.put(
        token, _SCOPE, "routing_fixture", "binding-1", "connected"
    )
    resp = await client.post(f"/v1/connectors/routing_fixture/r/{token}/webhook", content=b"{}")
    assert resp.status_code == 202
    assert resp.headers["x-routed-scope"] == _SCOPE


async def test_routed_webhook_unknown_token_is_opaque_404(
    routed_app: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, _app = routed_app
    resp = await client.post(
        "/v1/connectors/routing_fixture/r/not-a-real-token/webhook", content=b"{}"
    )
    assert resp.status_code == 404


async def test_routed_webhook_cross_provider_mismatch_denied(
    routed_app: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, app = routed_app
    token = mint_route_token()
    # The token belongs to routing_fixture; a request for a different connector must be denied.
    await app.state.connector_webhook_route_store.put(
        token, _SCOPE, "routing_fixture", "binding-1", "connected"
    )
    resp = await client.post(f"/v1/connectors/other_connector/r/{token}/webhook", content=b"{}")
    assert resp.status_code == 404


async def test_cloud_mode_rejects_tokenless_legacy_webhook(
    routed_app: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, app = routed_app
    app.state.auth_required = True  # cloud mode requires the routed capability
    resp = await client.post("/v1/connectors/routing_fixture/webhook", content=b"{}")
    assert resp.status_code == 404


async def test_local_mode_allows_tokenless_legacy_webhook(
    routed_app: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, _app = routed_app  # auth_required=False (local preview)
    resp = await client.post("/v1/connectors/routing_fixture/webhook", content=b"{}")
    assert resp.status_code == 202
