"""Gmail manifest and provider factory."""

from __future__ import annotations

from datetime import UTC, datetime

from keel_core.config import get_settings
from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAction,
    ConnectorActionApproval,
    ConnectorActionContext,
    ConnectorActionIdempotency,
    ConnectorActionManifest,
    ConnectorActionSemantics,
    ConnectorAuthAction,
    ConnectorAuthKind,
    ConnectorAuthStart,
    ConnectorBindingDraft,
    ConnectorCallbackParameter,
    ConnectorCapability,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorSetupResult,
)
from keel_core.connector_credentials import CredentialEnvelope
from keel_core.gmail import (
    GMAIL_CONNECTOR_ID,
    GMAIL_SCOPES,
    make_gmail_inbox_action,
    make_gmail_send_action,
)

INBOX_ACTION = ConnectorActionManifest(
    name="inbox_list",
    description="List recent inbox messages.",
    input_schema={"type": "object", "properties": {}},
    semantics=ConnectorActionSemantics.read,
)
SEND_ACTION = ConnectorActionManifest(
    name="email_send",
    description="Send an email.",
    input_schema={
        "type": "object",
        "properties": {
            "to": {"type": "string"},
            "subject": {"type": "string"},
            "body": {"type": "string"},
            "idempotency_key": {"type": "string"},
        },
    },
    semantics=ConnectorActionSemantics.outbound,
    idempotency=ConnectorActionIdempotency.optional,
    approval=ConnectorActionApproval.tainted,
)

manifest = ConnectorManifest(
    id=GMAIL_CONNECTOR_ID,
    name="Gmail",
    description="Read Gmail messages and send approved email.",
    icon="✉️",
    auth_kind=ConnectorAuthKind.oauth,
    capabilities=(ConnectorCapability.read, ConnectorCapability.write),
    scopes=GMAIL_SCOPES,
    auth_action=ConnectorAuthAction(callback_parameters=(ConnectorCallbackParameter("code"),)),
    actions=(INBOX_ACTION, SEND_ACTION),
)


def enabled() -> bool:
    return get_settings().gmail_enabled


def availability() -> None:
    import google_auth_oauthlib.flow  # noqa: F401
    import googleapiclient.discovery  # noqa: F401


def _flow(redirect_uri: str) -> object:
    from google_auth_oauthlib.flow import Flow

    settings = get_settings()
    return Flow.from_client_secrets_file(
        settings.gmail_client_secrets_path,
        scopes=list(GMAIL_SCOPES),
        redirect_uri=redirect_uri,
    )


class GmailProvider(BaseConnectorProvider):
    manifest = manifest

    def enabled(self) -> bool:
        return enabled()

    async def begin_auth(self, callback_url: str) -> ConnectorAuthStart:
        flow = _flow(callback_url)
        auth_url, state = flow.authorization_url(  # type: ignore[attr-defined]
            access_type="offline",
            prompt="consent",
            include_granted_scopes="true",
        )
        return ConnectorAuthStart(str(auth_url), str(state))

    async def complete_auth(
        self, callback_url: str, parameters: dict[str, str]
    ) -> ConnectorSetupResult:
        code = parameters.get("code", "").strip()
        if not code:
            raise ValueError("missing authorization code")
        flow = _flow(callback_url)
        flow.fetch_token(code=code)  # type: ignore[attr-defined]
        credentials = flow.credentials  # type: ignore[attr-defined]
        raw = credentials.to_json()
        import json

        values = json.loads(raw)
        if not isinstance(values, dict):
            raise ValueError("Gmail credentials did not serialize to an object")
        return ConnectorSetupResult(
            binding=ConnectorBindingDraft(display_name="Gmail"),
            credential=CredentialEnvelope(kind="oauth", values=values),
        )

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        if not self.enabled():
            return ConnectorHealth(
                ConnectorHealthStatus.degraded,
                datetime.now(UTC),
                "Gmail is configured but disabled.",
            )
        if context.credential is None:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                datetime.now(UTC),
                "Gmail credentials are missing.",
            )
        return ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC))

    async def revoke(self, context: ConnectorOperationContext) -> None:
        """Preserve Gmail's existing local encrypted-token revoke behavior."""
        return None

    def build_actions(self, context: ConnectorActionContext) -> tuple[ConnectorAction, ...]:
        if context.credential_store is None:
            raise RuntimeError("encrypted connector credential storage is unavailable")
        settings = get_settings()
        actions = [
            ConnectorAction(
                INBOX_ACTION,
                make_gmail_inbox_action(
                    context.credential_store,
                    settings.gmail_max_messages,
                ),
            )
        ]
        if settings.gmail_send_enabled:
            actions.append(
                ConnectorAction(
                    SEND_ACTION,
                    make_gmail_send_action(context.credential_store),
                )
            )
        return tuple(actions)


def factory() -> GmailProvider:
    return GmailProvider()


__all__ = ["GmailProvider", "availability", "enabled", "factory", "manifest"]
