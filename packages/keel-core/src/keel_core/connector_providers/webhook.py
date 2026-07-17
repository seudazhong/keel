"""Generic signed webhook connector."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import NoReturn
from urllib.parse import urlencode

from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAuthenticationError,
    ConnectorAuthKind,
    ConnectorBindingDraft,
    ConnectorCapability,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorEvent,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorIngressRequest,
    ConnectorIngressResponse,
    ConnectorIngressResult,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorProvenance,
    ConnectorSetupArtifact,
    ConnectorSetupArtifactKind,
    ConnectorSetupResult,
    ConnectorTargetField,
    ConnectorTargetKind,
)
from keel_core.connector_credentials import CredentialEnvelope

WEBHOOK_MAX_BODY_BYTES = 60_000
WEBHOOK_TIMESTAMP_WINDOW_SECONDS = 300
_DELIVERY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

manifest = ConnectorManifest(
    id="webhook",
    name="Webhook",
    description="Receive HMAC-authenticated JSON events at a unique endpoint.",
    icon="🪝",
    auth_kind=ConnectorAuthKind.webhook,
    capabilities=(ConnectorCapability.webhook,),
    setup_action_label="Create or rotate webhook",
    target_fields=(
        ConnectorTargetField(
            ConnectorTargetKind.trigger_session,
            "Trigger session",
            help_text="Session that receives verified webhook events.",
        ),
    ),
)


def _header(headers: Mapping[str, str], name: str) -> str:
    expected = name.lower()
    return next((value for key, value in headers.items() if key.lower() == expected), "")


def _content_type(value: str) -> bool:
    parts = [part.strip().lower() for part in value.split(";")]
    if not parts or len(parts) > 2 or parts[0] != "application/json":
        return False
    return all(part == "charset=utf-8" for part in parts[1:])


def _credential(context: ConnectorOperationContext) -> tuple[str, str]:
    credential = context.credential
    if credential is None or credential.kind != "webhook_hmac":
        raise ConnectorAuthenticationError("webhook signing credentials are unavailable")
    endpoint_id = credential.values.get("endpoint_id")
    signing_secret = credential.values.get("signing_secret")
    if not isinstance(endpoint_id, str) or not isinstance(signing_secret, str):
        raise ConnectorAuthenticationError("webhook signing credentials are invalid")
    return endpoint_id, signing_secret


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"unsupported JSON constant: {value}")


def _json_payload(body: bytes) -> object:
    try:
        decoded = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("webhook JSON body must be UTF-8") from exc
    try:
        payload = json.loads(
            decoded,
            parse_constant=_reject_json_constant,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("webhook body must contain valid JSON") from exc
    if (
        len(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        > 55_000
    ):
        raise ValueError("normalized webhook payload is too large")
    return payload


class WebhookProvider(BaseConnectorProvider):
    manifest = manifest

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
        token_urlsafe: Callable[[int], str] = secrets.token_urlsafe,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))
        self._token_urlsafe = token_urlsafe

    async def setup(
        self, context: ConnectorOperationContext, values: dict[str, str]
    ) -> ConnectorSetupResult:
        if context.callback_base_url is None:
            raise RuntimeError("public connector callback URL is unavailable")
        endpoint_id: str | None = None
        if context.credential is not None and context.credential.kind == "webhook_hmac":
            current = context.credential.values.get("endpoint_id")
            if isinstance(current, str) and current:
                endpoint_id = current
        endpoint_id = endpoint_id or self._token_urlsafe(24)
        signing_secret = self._token_urlsafe(48)
        endpoint = f"{context.callback_base_url}/webhook?{urlencode({'endpoint': endpoint_id})}"
        return ConnectorSetupResult(
            binding=ConnectorBindingDraft(
                display_name="Generic webhook",
                external_account_id=endpoint_id,
                metadata={"signature_version": "v1"},
            ),
            credential=CredentialEnvelope(
                "webhook_hmac",
                {
                    "endpoint_id": endpoint_id,
                    "signing_secret": signing_secret,
                },
            ),
            artifacts=(
                ConnectorSetupArtifact(
                    ConnectorSetupArtifactKind.url,
                    "Webhook endpoint",
                    endpoint,
                ),
                ConnectorSetupArtifact(
                    ConnectorSetupArtifactKind.secret,
                    "Signing secret",
                    signing_secret,
                ),
                ConnectorSetupArtifact(
                    ConnectorSetupArtifactKind.instruction,
                    "Signature scheme",
                    "HMAC-SHA256: v1=hex(HMAC(secret, "
                    "timestamp + '.' + delivery_id + '.' + raw_body)); "
                    "send X-Keel-Webhook-Timestamp, X-Keel-Webhook-Id, and "
                    "X-Keel-Webhook-Signature.",
                ),
            ),
        )

    async def ingress(
        self, context: ConnectorOperationContext, request: ConnectorIngressRequest
    ) -> ConnectorIngressResult:
        if context.binding is None:
            raise ValueError("webhook connector binding is missing")
        endpoint_id, signing_secret = _credential(context)
        endpoints = request.query.get("endpoint", ())
        if len(endpoints) != 1 or not hmac.compare_digest(endpoints[0], endpoint_id):
            raise ConnectorAuthenticationError("webhook endpoint is invalid")
        if request.method.upper() != "POST":
            raise ValueError("webhook ingress requires POST")
        if not _content_type(_header(request.headers, "content-type")):
            raise ValueError("webhook content type must be application/json with optional UTF-8")
        if len(request.body) > WEBHOOK_MAX_BODY_BYTES:
            raise ValueError("webhook body exceeds the provider size limit")
        delivery_id = _header(request.headers, "x-keel-webhook-id").strip()
        if not _DELIVERY_ID.fullmatch(delivery_id):
            raise ValueError("webhook delivery id is invalid")
        timestamp_text = _header(request.headers, "x-keel-webhook-timestamp").strip()
        if not timestamp_text.isascii() or not timestamp_text.isdecimal():
            raise ConnectorAuthenticationError("webhook timestamp is invalid")
        try:
            timestamp = int(timestamp_text)
        except ValueError as exc:
            raise ConnectorAuthenticationError("webhook timestamp is invalid") from exc
        now = self._clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        if abs(int(now.timestamp()) - timestamp) > WEBHOOK_TIMESTAMP_WINDOW_SECONDS:
            raise ConnectorAuthenticationError("webhook timestamp is expired")
        supplied = _header(request.headers, "x-keel-webhook-signature").strip()
        if not supplied.startswith("v1=") or len(supplied) != 67:
            raise ConnectorAuthenticationError("webhook signature is invalid")
        signed = f"{timestamp_text}.{delivery_id}.".encode("ascii") + request.body
        expected = "v1=" + hmac.new(
            signing_secret.encode("utf-8"),
            signed,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(supplied, expected):
            raise ConnectorAuthenticationError("webhook signature is invalid")
        payload = _json_payload(request.body)
        payload_hash = hashlib.sha256(request.body).hexdigest()
        provenance = ConnectorProvenance(
            connector_id=manifest.id,
            binding_id=context.binding.id,
            external_resource_id=delivery_id,
            source_url=request.public_url,
            revision=payload_hash,
            event_id=delivery_id,
        )
        event = ConnectorEvent(
            "webhook.received",
            provenance,
            {
                "delivery_id": delivery_id,
                "timestamp": timestamp,
                "content_type": "application/json",
                "body": payload,
            },
        )
        return ConnectorIngressResult(
            ConnectorIngressResponse(
                status_code=202,
                content_type="application/json",
                body=b'{"accepted":true}',
            ),
            delivery_id=delivery_id,
            payload_hash=payload_hash,
            changes=(
                ConnectorChange(
                    ConnectorChangeKind.event,
                    provenance,
                    event=event,
                ),
            ),
        )

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        try:
            _credential(context)
        except ConnectorAuthenticationError as exc:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                self._clock(),
                str(exc),
            )
        return ConnectorHealth(ConnectorHealthStatus.healthy, self._clock())


def factory() -> WebhookProvider:
    return WebhookProvider()


__all__ = [
    "WEBHOOK_MAX_BODY_BYTES",
    "WEBHOOK_TIMESTAMP_WINDOW_SECONDS",
    "WebhookProvider",
    "factory",
    "manifest",
]
