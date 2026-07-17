"""Generic connector API dispatch without provider literals."""

from __future__ import annotations

import hashlib
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
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCallbackParameter,
    ConnectorCapability,
    ConnectorCursorUpdate,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorIngressFailure,
    ConnectorIngressRequest,
    ConnectorIngressResponse,
    ConnectorIngressResult,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorResourceDraft,
    ConnectorResourceResult,
    ConnectorSetupArtifact,
    ConnectorSetupArtifactKind,
    ConnectorSetupField,
    ConnectorSetupResult,
    ConnectorStateUpdate,
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

    async def setup(
        self, context: ConnectorOperationContext, values: dict[str, str]
    ) -> ConnectorSetupResult:
        assert context.callback_base_url == "http://test/v1/connectors/manual"
        return ConnectorSetupResult(
            ConnectorBindingDraft(display_name="Manual"),
            CredentialEnvelope("secret", {"api_key": values["api_key"]}),
            (
                ConnectorSetupArtifact(
                    ConnectorSetupArtifactKind.url,
                    "Webhook URL",
                    f"{context.callback_base_url}/webhook",
                ),
            ),
        )

    async def list_resources(self, context: ConnectorOperationContext) -> ConnectorResourceResult:
        return ConnectorResourceResult(
            (ConnectorResourceDraft("project-1", "project", "Project one"),)
        )

    async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
        return ConnectorSyncResult(
            state=ConnectorStateUpdate(
                cursor_updates=(
                    ConnectorCursorUpdate("items", "next", resource_id=context.resources[0].id),
                    ConnectorCursorUpdate("global", "global-next"),
                ),
            )
        )

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        return ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC))

    async def ingress(
        self, context: ConnectorOperationContext, request: ConnectorIngressRequest
    ) -> ConnectorIngressResult:
        assert context.credential is not None
        assert context.credential.values["api_key"] == "must-not-echo"
        assert request.method in {"GET", "POST"}
        assert request.public_url.startswith("http://test/v1/connectors/manual/webhook")
        if request.query.get("validationToken") == ("graph-challenge",):
            return ConnectorIngressResult(
                ConnectorIngressResponse(
                    status_code=200,
                    content_type="text/plain",
                    headers={"cache-control": "no-store"},
                    body=b"graph-challenge",
                )
            )
        signature = request.headers.get("x-signature")
        if signature not in {"valid", "retryable-failure"}:
            raise ConnectorAuthenticationError("invalid signature")
        if signature == "retryable-failure":
            return ConnectorIngressResult(
                ConnectorIngressResponse(
                    status_code=503,
                    content_type="application/json",
                    headers={"retry-after": "30"},
                    body=b'{"retry":true}',
                ),
                delivery_id="delivery-failure",
                payload_hash=hashlib.sha256(request.body).hexdigest(),
                failure=ConnectorIngressFailure(
                    "provider_processing_failed",
                    "Verified delivery processing failed.",
                    retryable=True,
                ),
            )
        return ConnectorIngressResult(
            ConnectorIngressResponse(
                status_code=202,
                content_type="application/json",
                headers={"x-provider-result": "accepted"},
                body=b'{"provider":"accepted"}',
            ),
            delivery_id="delivery-1",
            payload_hash=hashlib.sha256(request.body).hexdigest(),
        )

    async def revoke(self, context: ConnectorOperationContext) -> None:
        assert context.binding is not None
        assert context.resources
        assert context.targets
        assert context.cursors == ()
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

    async def begin_auth(
        self, context: ConnectorOperationContext, callback_url: str
    ) -> ConnectorAuthStart:
        return ConnectorAuthStart("https://provider.invalid/authorize?state=state-1", "state-1")

    async def complete_auth(
        self,
        context: ConnectorOperationContext,
        callback_url: str,
        parameters: dict[str, str],
    ) -> ConnectorSetupResult:
        assert context.binding is not None
        assert context.binding.status is ConnectorBindingStatus.authorizing
        assert parameters == {"state": "state-1", "ticket": "ticket-1"}
        return ConnectorSetupResult(
            ConnectorBindingDraft(display_name="OAuth fixture"),
            CredentialEnvelope("oauth", {"refresh_token": "encrypted-at-rest"}),
        )


class StagedProvider(BaseConnectorProvider):
    manifest = ConnectorManifest(
        id="staged",
        name="Staged",
        description="Staged fixture",
        auth_kind=ConnectorAuthKind.app_credentials,
        capabilities=(ConnectorCapability.read,),
        setup_fields=(
            ConnectorSetupField("client_id", "Client ID"),
            ConnectorSetupField("client_secret", "Client secret", secret=True),
        ),
        auth_action=ConnectorAuthAction(
            label="Authorize staged app",
            callback_parameters=(ConnectorCallbackParameter("ticket"),),
            requires_setup=True,
            help_text="Save app credentials, then authorize in the browser.",
        ),
    )

    async def setup(
        self, context: ConnectorOperationContext, values: dict[str, str]
    ) -> ConnectorSetupResult:
        return ConnectorSetupResult(
            ConnectorBindingDraft(display_name="Staged app"),
            CredentialEnvelope(
                "app_credentials",
                {
                    "client_id": values["client_id"],
                    "client_secret": values["client_secret"],
                },
            ),
            status=ConnectorBindingStatus.configured,
        )

    async def begin_auth(
        self, context: ConnectorOperationContext, callback_url: str
    ) -> ConnectorAuthStart:
        assert context.binding is not None
        assert context.binding.status is ConnectorBindingStatus.configured
        assert context.credential is not None
        assert context.credential.values["client_secret"] == "staged-secret"
        return ConnectorAuthStart(
            "https://provider.invalid/install?state=staged-state",
            "staged-state",
        )

    async def complete_auth(
        self,
        context: ConnectorOperationContext,
        callback_url: str,
        parameters: dict[str, str],
    ) -> ConnectorSetupResult:
        assert context.binding is not None
        assert context.binding.status is ConnectorBindingStatus.authorizing
        assert context.credential is not None
        assert parameters == {"state": "staged-state", "ticket": "installed"}
        return ConnectorSetupResult(
            ConnectorBindingDraft(
                display_name="Staged app",
                external_account_id="installation-1",
            ),
            CredentialEnvelope(
                "app_credentials",
                {
                    **context.credential.values,
                    "installation_token": "encrypted-installation-context",
                },
            ),
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
            ConnectorRegistration(StagedProvider.manifest, StagedProvider, "tests.staged"),
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
    assert [item["id"] for item in catalog] == ["manual", "oauth_fixture", "staged"]
    assert catalog[0]["setup_fields"][0]["secret"] is True

    setup = await client.post(
        "/v1/connectors/manual/setup",
        json={"values": {"api_key": "must-not-echo"}},
    )
    assert setup.status_code == 200
    assert setup.headers["cache-control"] == "no-store"
    assert "must-not-echo" not in setup.text
    assert setup.json()["artifacts"][0]["value"] == ("http://test/v1/connectors/manual/webhook")

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
    assert first.status_code == 202
    assert first.headers["x-provider-result"] == "accepted"
    assert first.json() == {"provider": "accepted"}
    assert replay.status_code == 202
    retryable_failure = await client.post(
        "/v1/connectors/manual/webhook",
        content=b"verified-failure",
        headers={"X-Signature": "retryable-failure"},
    )
    assert retryable_failure.status_code == 503
    assert retryable_failure.headers["retry-after"] == "30"
    assert retryable_failure.json() == {"retry": True}
    binding = await app.state.connector_repository.get_binding("manual")
    assert binding is not None
    delivery_health = await app.state.connector_repository.get_delivery_health("manual", binding.id)
    assert delivery_health is not None
    assert delivery_health.summary == "Verified delivery processing failed."
    challenge = await client.post(
        "/v1/connectors/manual/webhook",
        params={"validationToken": "graph-challenge"},
    )
    assert challenge.status_code == 200
    assert challenge.headers["content-type"] == "text/plain"
    assert challenge.text == "graph-challenge"
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


async def test_staged_setup_remains_configured_until_contextual_callback(
    connector_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, app = connector_client
    blocked = await client.get("/v1/connectors/staged/connect", follow_redirects=False)
    assert blocked.status_code == 400
    setup = await client.post(
        "/v1/connectors/staged/setup",
        json={
            "values": {
                "client_id": "staged-client",
                "client_secret": "staged-secret",
            }
        },
    )
    assert setup.status_code == 200
    assert setup.json()["status"] == "configured"
    assert "staged-secret" not in setup.text
    catalog = {item["id"]: item for item in (await client.get("/v1/connectors")).json()}
    assert catalog["staged"]["configured"] is True
    assert catalog["staged"]["connected"] is False
    assert catalog["staged"]["next_action"]["kind"] == "authorize"

    start = await client.get("/v1/connectors/staged/connect", follow_redirects=False)
    assert start.status_code in {302, 307}
    authorizing = await app.state.connector_repository.get_binding("staged")
    assert authorizing.status is ConnectorBindingStatus.authorizing
    callback = await client.get(
        "/v1/connectors/staged/callback",
        params={"state": "staged-state", "ticket": "installed"},
    )
    assert callback.status_code == 200
    binding = await app.state.connector_repository.get_binding("staged")
    assert binding.status is ConnectorBindingStatus.connected
    assert binding.external_account_id == "installation-1"
    stored = await app.state.connector_credentials.get("staged")
    assert stored is not None
    assert stored.values["client_secret"] == "staged-secret"
    assert stored.values["installation_token"] == "encrypted-installation-context"


async def test_unavailable_provider_can_be_explicitly_forgotten_locally(
    connector_client: tuple[httpx.AsyncClient, FastAPI],
) -> None:
    client, app = connector_client
    manifest = ConnectorManifest(
        id="broken",
        name="Broken",
        description="Broken optional provider",
        auth_kind=ConnectorAuthKind.oauth,
        capabilities=(ConnectorCapability.read,),
    )

    class BrokenProvider(BaseConnectorProvider):
        pass

    BrokenProvider.manifest = manifest

    def unavailable() -> None:
        raise ModuleNotFoundError("missing optional", name="broken_sdk")

    app.state.connector_registry = ConnectorRegistry(
        (
            ConnectorRegistration(
                manifest,
                BrokenProvider,
                "tests.broken",
                availability=unavailable,
            ),
        )
    )
    await app.state.connector_repository.upsert_binding(
        "broken",
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
    )
    await app.state.connector_credentials.put(
        "broken",
        CredentialEnvelope("oauth", {"refresh_token": "retained"}),
    )

    failed = await client.delete("/v1/connectors/broken")
    assert failed.status_code == 502
    assert await app.state.connector_repository.get_binding("broken") is not None
    assert await app.state.connector_credentials.get("broken") is not None

    forgotten = await client.delete("/v1/connectors/broken/local")
    assert forgotten.json() == {"ok": True}
    assert await app.state.connector_repository.get_binding("broken") is None
    assert await app.state.connector_credentials.get("broken") is None
