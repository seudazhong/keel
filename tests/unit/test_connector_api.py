"""Generic connector API dispatch without provider literals."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport

from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAuthAction,
    ConnectorAuthenticationError,
    ConnectorAuthKind,
    ConnectorAuthStart,
    ConnectorBinding,
    ConnectorBindingDraft,
    ConnectorCallbackParameter,
    ConnectorCapability,
    ConnectorCursor,
    ConnectorCursorUpdate,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorIngressResult,
    ConnectorManifest,
    ConnectorResource,
    ConnectorResourceDraft,
    ConnectorSetupArtifact,
    ConnectorSetupArtifactKind,
    ConnectorSetupField,
    ConnectorSetupResult,
    ConnectorSyncResult,
    ConnectorTargetField,
    ConnectorTargetKind,
)
from keel_core.connector_credentials import ConnectorCredentialStore, CredentialEnvelope
from keel_core.connector_registry import ConnectorRegistration, ConnectorRegistry
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.jobs import InMemoryJobStore
from keel_core.oauth_state import InMemoryOAuthStateStore
from keel_core.secrets import EnvelopeCipher
from keel_core.tokens import InMemoryTokenStore
from keel_server.api.connectors import router

_REVOKED: list[str] = []


class ManualProvider(BaseConnectorProvider):
    manifest = ConnectorManifest(
        id="manual",
        name="Manual",
        description="Manual fixture",
        auth_kind=ConnectorAuthKind.secret,
        capabilities=(
            ConnectorCapability.resources,
            ConnectorCapability.sync,
            ConnectorCapability.webhook,
        ),
        setup_fields=(ConnectorSetupField("api_key", "API key", secret=True),),
        resource_label="Projects",
        target_fields=(
            ConnectorTargetField(
                ConnectorTargetKind.knowledge,
                "Knowledge Base",
            ),
        ),
        setup_action_label="Store credential",
    )

    async def setup(self, values: dict[str, str]) -> ConnectorSetupResult:
        return ConnectorSetupResult(
            ConnectorBindingDraft(display_name="Manual"),
            CredentialEnvelope("secret", {"api_key": values["api_key"]}),
            (
                ConnectorSetupArtifact(
                    ConnectorSetupArtifactKind.secret,
                    "Generated webhook secret",
                    "shown-once",
                ),
            ),
        )

    async def list_resources(
        self, binding: ConnectorBinding, credential: CredentialEnvelope | None
    ) -> tuple[ConnectorResourceDraft, ...]:
        return (ConnectorResourceDraft("project-1", "project", "Project one"),)

    async def sync(
        self,
        binding: ConnectorBinding,
        resources: tuple[ConnectorResource, ...],
        cursors: tuple[ConnectorCursor, ...],
        credential: CredentialEnvelope | None,
    ) -> ConnectorSyncResult:
        return ConnectorSyncResult(
            cursor_updates=(
                ConnectorCursorUpdate("items", "next", resource_id=resources[0].id),
                ConnectorCursorUpdate("global", "global-next"),
            )
        )

    async def health(
        self, binding: ConnectorBinding, credential: CredentialEnvelope | None
    ) -> ConnectorHealth:
        return ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC))

    async def ingress(
        self, headers: dict[str, str], body: bytes, binding: ConnectorBinding
    ) -> ConnectorIngressResult:
        if headers.get("x-signature") != "valid":
            raise ConnectorAuthenticationError("invalid signature")
        return ConnectorIngressResult("delivery-1")

    async def revoke(self, credential: CredentialEnvelope | None) -> None:
        _REVOKED.append("manual")


class OAuthProvider(BaseConnectorProvider):
    manifest = ConnectorManifest(
        id="oauth_fixture",
        name="OAuth fixture",
        description="OAuth fixture",
        auth_kind=ConnectorAuthKind.oauth,
        capabilities=(ConnectorCapability.read,),
        auth_action=ConnectorAuthAction(
            callback_parameters=(ConnectorCallbackParameter("ticket"),)
        ),
    )

    async def begin_auth(self, callback_url: str) -> ConnectorAuthStart:
        return ConnectorAuthStart("https://provider.invalid/authorize?state=state-1", "state-1")

    async def complete_auth(
        self, callback_url: str, parameters: dict[str, str]
    ) -> ConnectorSetupResult:
        assert parameters == {"state": "state-1", "ticket": "ticket-1"}
        return ConnectorSetupResult(
            ConnectorBindingDraft(display_name="OAuth fixture"),
            CredentialEnvelope("oauth", {"refresh_token": "encrypted-at-rest"}),
        )


@pytest_asyncio.fixture
async def connector_client() -> AsyncIterator[tuple[httpx.AsyncClient, FastAPI]]:
    _REVOKED.clear()
    app = FastAPI()
    app.include_router(router)
    app.state.durable_scope = "scope:test"
    app.state.connector_registry = ConnectorRegistry(
        (
            ConnectorRegistration(ManualProvider.manifest, ManualProvider, "tests.manual"),
            ConnectorRegistration(OAuthProvider.manifest, OAuthProvider, "tests.oauth"),
        )
    )
    app.state.connector_repository = InMemoryConnectorRepository("scope:test")
    app.state.connector_credentials = ConnectorCredentialStore(
        InMemoryTokenStore("scope:test", EnvelopeCipher("key"))
    )
    app.state.jobs = InMemoryJobStore("scope:test")
    app.state.oauth_state_store = InMemoryOAuthStateStore()

    async def validate_target(kind: ConnectorTargetKind, target_id: str) -> bool:
        return kind is ConnectorTargetKind.knowledge and target_id == "kb-1"

    app.state.connector_target_validator = validate_target
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, app


async def test_generic_catalog_setup_resources_sync_health_and_revoke(
    connector_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, app = connector_client
    catalog = (await client.get("/v1/connectors")).json()
    assert [item["id"] for item in catalog] == ["manual", "oauth_fixture"]
    assert catalog[0]["setup_fields"][0]["secret"] is True

    setup = await client.post(
        "/v1/connectors/manual/setup",
        json={"values": {"api_key": "must-not-echo"}},
    )
    assert setup.status_code == 200
    assert setup.headers["cache-control"] == "no-store"
    assert "must-not-echo" not in setup.text
    assert setup.json()["artifacts"][0]["value"] == "shown-once"
    assert "shown-once" not in (await client.get("/v1/connectors")).text

    resources = (await client.get("/v1/connectors/manual/resources")).json()
    assert resources[0]["external_id"] == "project-1"
    assert (
        await client.put(
            "/v1/connectors/manual/resources",
            json={"external_ids": ["project-1"]},
        )
    ).status_code == 200
    missing_target = await client.post("/v1/connectors/manual/sync", json={})
    assert missing_target.status_code == 503
    configured = await client.put(
        "/v1/connectors/manual/targets",
        json={"targets": {"knowledge": "kb-1"}},
    )
    assert configured.json()["targets"] == {"knowledge": "kb-1"}
    sync = await client.post("/v1/connectors/manual/sync", json={})
    assert sync.status_code == 200 and sync.json()["status"] == "queued"
    health = await client.get("/v1/connectors/manual/health")
    assert health.json()["status"] == "healthy"

    first = await client.post(
        "/v1/connectors/manual/webhook",
        content=b"body",
        headers={"X-Signature": "valid"},
    )
    replay = await client.post(
        "/v1/connectors/manual/webhook",
        content=b"body",
        headers={"X-Signature": "valid"},
    )
    assert first.json()["accepted"] is True
    assert replay.json()["replayed"] is True
    rejected = await client.post(
        "/v1/connectors/manual/webhook",
        content=b"body",
        headers={"X-Signature": "invalid"},
    )
    assert rejected.status_code == 401

    revoked = await client.delete("/v1/connectors/manual")
    assert revoked.json() == {"ok": True}
    assert _REVOKED == ["manual"]
    assert await app.state.connector_repository.get_binding("manual") is None


async def test_generic_oauth_callback_dispatch(
    connector_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, app = connector_client
    start = await client.get("/v1/connectors/oauth_fixture/connect", follow_redirects=False)
    assert start.status_code in {302, 307}
    callback = await client.get(
        "/v1/connectors/oauth_fixture/callback",
        params={"state": "state-1", "ticket": "ticket-1"},
    )
    assert callback.status_code == 200
    assert callback.headers["cache-control"] == "no-store"
    binding = await app.state.connector_repository.get_binding("oauth_fixture")
    assert binding is not None and binding.status.value == "connected"
