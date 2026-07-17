"""Generic webhook provider behavior."""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from keel_core.connector_contracts import (
    ConnectorAuthenticationError,
    ConnectorBinding,
    ConnectorBindingStatus,
    ConnectorChange,
    ConnectorIngressRequest,
    ConnectorOperationContext,
    ConnectorSetupArtifactKind,
    ConnectorTargetKind,
)
from keel_core.connector_credentials import ConnectorCredentialStore
from keel_core.connector_providers.webhook import (
    WEBHOOK_MAX_BODY_BYTES,
    WebhookProvider,
    manifest,
)
from keel_core.connector_registry import ConnectorRegistration, ConnectorRegistry
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.connector_service import ConnectorChangeSink, ConnectorService
from keel_core.secrets import EnvelopeCipher
from keel_core.tokens import InMemoryTokenStore
from keel_core.types import ContentTaint

NOW = datetime(2026, 7, 18, 0, 0, tzinfo=UTC)


class Sink(ConnectorChangeSink):
    def __init__(self) -> None:
        self.changes: list[ConnectorChange] = []

    async def apply(self, change: ConnectorChange) -> None:
        self.changes.append(change)


def _signed_request(
    secret: str,
    endpoint_id: str,
    *,
    body: bytes = b'{"message":"hello"}',
    delivery_id: str = "delivery-1",
    timestamp: datetime = NOW,
    content_type: str = "application/json",
) -> ConnectorIngressRequest:
    timestamp_text = str(int(timestamp.timestamp()))
    signed = f"{timestamp_text}.{delivery_id}.".encode() + body
    signature = "v1=" + hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    return ConnectorIngressRequest(
        "POST",
        {"endpoint": (endpoint_id,)},
        {
            "content-type": content_type,
            "x-keel-webhook-id": delivery_id,
            "x-keel-webhook-timestamp": timestamp_text,
            "x-keel-webhook-signature": signature,
        },
        body,
        f"https://keel.example/v1/connectors/webhook/webhook?endpoint={endpoint_id}",
    )


async def _configured_service() -> tuple[
    ConnectorService,
    InMemoryConnectorRepository,
    ConnectorCredentialStore,
    Sink,
    str,
    str,
]:
    tokens = iter(("endpoint-token", "signing-secret-one", "signing-secret-two"))
    provider = WebhookProvider(clock=lambda: NOW, token_urlsafe=lambda size: next(tokens))
    registry = ConnectorRegistry(
        (ConnectorRegistration(manifest, lambda: provider, "tests.webhook"),)
    )
    repository = InMemoryConnectorRepository("scope:webhook")
    credentials = ConnectorCredentialStore(
        InMemoryTokenStore("scope:webhook", EnvelopeCipher("test-key"))
    )
    sink = Sink()
    service = ConnectorService(
        registry,
        repository,
        credentials=credentials,
        change_sink=sink,
    )
    outcome = await service.setup(
        "webhook",
        {},
        callback_base_url="https://keel.example/v1/connectors/webhook",
    )
    secret_artifacts = [
        item for item in outcome.artifacts if item.kind is ConnectorSetupArtifactKind.secret
    ]
    assert len(secret_artifacts) == 1
    stored = await credentials.get("webhook")
    assert stored is not None
    return (
        service,
        repository,
        credentials,
        sink,
        str(stored.values["endpoint_id"]),
        secret_artifacts[0].value,
    )


async def test_setup_returns_unique_endpoint_and_secret_once_then_rotates_locally() -> None:
    service, repository, credentials, _, endpoint_id, first_secret = await _configured_service()
    binding = await repository.get_binding("webhook")
    assert binding is not None
    assert binding.external_account_id == endpoint_id
    assert first_secret not in repr(binding)
    assert first_secret not in str(binding.metadata)

    rotated = await service.setup(
        "webhook",
        {},
        callback_base_url="https://keel.example/v1/connectors/webhook",
    )
    stored = await credentials.get("webhook")
    assert stored is not None
    assert stored.values["endpoint_id"] == endpoint_id
    assert stored.values["signing_secret"] == "signing-secret-two"
    assert first_secret not in str(rotated.binding.metadata)
    assert [
        item.value
        for item in rotated.artifacts
        if item.kind is ConnectorSetupArtifactKind.secret
    ] == ["signing-secret-two"]


async def test_valid_signature_is_tainted_and_replay_safe() -> None:
    service, _, _, sink, endpoint_id, secret = await _configured_service()
    request = _signed_request(secret, endpoint_id)
    first = await service.ingress("webhook", request)
    replay = await service.ingress("webhook", request)
    assert (first.accepted, first.changes, first.response.status_code) == (True, 1, 202)
    assert (replay.accepted, replay.changes) == (False, 0)
    assert len(sink.changes) == 1
    change = sink.changes[0]
    assert change.taint is ContentTaint.tainted
    assert change.event is not None
    assert change.event.payload["body"] == {"message": "hello"}

    changed = _signed_request(secret, endpoint_id, body=b'{"message":"changed"}')
    with pytest.raises(ValueError, match="different payload"):
        await service.ingress("webhook", changed)


@pytest.mark.parametrize(
    ("mutate", "message"),
    (
        (
            lambda request: ConnectorIngressRequest(
                request.method,
                request.query,
                {**request.headers, "x-keel-webhook-signature": "v1=" + "0" * 64},
                request.body,
                request.public_url,
            ),
            "signature",
        ),
        (
            lambda request: ConnectorIngressRequest(
                request.method,
                {"endpoint": ("wrong",)},
                request.headers,
                request.body,
                request.public_url,
            ),
            "endpoint",
        ),
    ),
)
async def test_rejects_invalid_authentication(
    mutate: Callable[[ConnectorIngressRequest], ConnectorIngressRequest],
    message: str,
) -> None:
    service, _, _, sink, endpoint_id, secret = await _configured_service()
    request = _signed_request(secret, endpoint_id)
    with pytest.raises(ConnectorAuthenticationError, match=message):
        await service.ingress("webhook", mutate(request))
    assert sink.changes == []


async def test_rejects_expired_oversized_and_wrong_content_type() -> None:
    service, _, _, sink, endpoint_id, secret = await _configured_service()
    with pytest.raises(ConnectorAuthenticationError, match="expired"):
        await service.ingress(
            "webhook",
            _signed_request(
                secret,
                endpoint_id,
                timestamp=NOW - timedelta(minutes=6),
            ),
        )
    with pytest.raises(ValueError, match="size limit"):
        await service.ingress(
            "webhook",
            _signed_request(
                secret,
                endpoint_id,
                body=b" " * (WEBHOOK_MAX_BODY_BYTES + 1),
            ),
        )
    with pytest.raises(ValueError, match="content type"):
        await service.ingress(
            "webhook",
            _signed_request(secret, endpoint_id, content_type="text/plain"),
        )
    assert sink.changes == []


async def test_health_and_revoke_are_local_and_explicit() -> None:
    service, repository, credentials, _, _, _ = await _configured_service()
    health = await service.health("webhook")
    assert health.status.value == "healthy"
    assert await service.revoke("webhook")
    assert await repository.get_binding("webhook") is None
    assert await credentials.get("webhook") is None


def test_webhook_context_rejects_cross_scope_binding() -> None:
    binding = ConnectorBinding(
        id="binding",
        scope_id="scope:other",
        connector_id="webhook",
        status=ConnectorBindingStatus.connected,
    )
    with pytest.raises(ValueError, match="crosses"):
        ConnectorOperationContext(
            "scope:expected",
            "webhook",
            binding=binding,
        )
    assert manifest.target_fields[0].kind is ConnectorTargetKind.trigger_session
