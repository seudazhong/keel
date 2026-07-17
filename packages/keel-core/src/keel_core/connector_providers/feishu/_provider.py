"""Combined Feishu Workspace and IM provider implementation."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAction,
    ConnectorActionApproval,
    ConnectorActionContext,
    ConnectorActionIdempotency,
    ConnectorActionManifest,
    ConnectorActionSemantics,
    ConnectorAuthKind,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCapability,
    ConnectorCredentialUpdate,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorIngressRequest,
    ConnectorIngressResult,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorResourceResult,
    ConnectorSetupArtifact,
    ConnectorSetupArtifactKind,
    ConnectorSetupField,
    ConnectorSetupResult,
    ConnectorStateUpdate,
    ConnectorSyncResult,
    ConnectorTargetField,
    ConnectorTargetKind,
    ConnectorUnsupportedError,
)

from ._auth import (
    FeishuCredential,
    bot_identity,
    granted_scopes,
    issue_tenant_token,
    refresh_credential,
    tenant_identity,
)
from ._client import FeishuApiError, FeishuClient, HttpFeishuClient
from ._im import build_reply_action, handle_ingress
from ._workspace import discover_resources, sync_workspace

FEISHU_CONNECTOR_ID = "feishu"
FEISHU_REQUIRED_SCOPES = (
    "docx:document:readonly",
    "drive:drive:readonly",
    "wiki:wiki:readonly",
    "im:chat:readonly",
    "im:message:readonly",
    "im:message:send_as_bot",
)

REPLY_ACTION = ConnectorActionManifest(
    name="feishu_reply",
    description="Reply to an authorized Feishu chat message or thread.",
    input_schema={
        "type": "object",
        "required": ["chat_id", "message_id", "text", "idempotency_key"],
        "properties": {
            "chat_id": {"type": "string"},
            "message_id": {"type": "string"},
            "thread_id": {"type": "string"},
            "text": {"type": "string"},
            "idempotency_key": {"type": "string"},
        },
        "additionalProperties": False,
    },
    semantics=ConnectorActionSemantics.outbound,
    idempotency=ConnectorActionIdempotency.required,
    approval=ConnectorActionApproval.tainted,
)

manifest = ConnectorManifest(
    id=FEISHU_CONNECTOR_ID,
    name="Feishu",
    description="Sync Feishu Docs, Wiki, and Drive into Knowledge and receive approved bot events.",
    icon="🐦",
    auth_kind=ConnectorAuthKind.app_credentials,
    capabilities=(
        ConnectorCapability.read,
        ConnectorCapability.write,
        ConnectorCapability.sync,
        ConnectorCapability.webhook,
        ConnectorCapability.resources,
    ),
    scopes=FEISHU_REQUIRED_SCOPES,
    setup_fields=(
        ConnectorSetupField("app_id", "App ID"),
        ConnectorSetupField("app_secret", "App secret", secret=True),
        ConnectorSetupField(
            "tenant_key",
            "Tenant key",
            help_text="The tenant authorized to install this self-built app.",
        ),
        ConnectorSetupField("verification_token", "Event verification token", secret=True),
        ConnectorSetupField("encrypt_key", "Event encryption key", secret=True),
    ),
    setup_action_label="Verify and save",
    resource_label="Docs, Wiki spaces, Drive folders, and chats",
    target_fields=(
        ConnectorTargetField(
            ConnectorTargetKind.knowledge,
            "Knowledge Base",
            required=True,
            help_text="All selected Workspace content is normalized into this Knowledge Base.",
        ),
        ConnectorTargetField(
            ConnectorTargetKind.trigger_session,
            "Bot trigger session",
            required=False,
            help_text="Target session for accepted direct messages and bot mentions.",
        ),
        ConnectorTargetField(
            ConnectorTargetKind.trigger_routine,
            "Bot trigger routine",
            required=False,
            help_text="Optional routine target when no trigger session is selected.",
        ),
    ),
    actions=(REPLY_ACTION,),
    default_sync_cadence_seconds=300,
)


def _health_from_api_error(error: FeishuApiError) -> ConnectorHealth:
    message = str(error).lower()
    if any(term in message for term in ("uninstall", "not installed", "revoked", "unauthorized")):
        summary = "Feishu app is uninstalled or tenant authorization was revoked."
    elif error.status_code in {401, 403} or "forbidden" in message:
        summary = "Feishu tenant authorization is invalid or no longer grants access."
    else:
        summary = f"Feishu API health check failed (code={error.code})."
    return ConnectorHealth(
        ConnectorHealthStatus.error,
        datetime.now(UTC),
        summary,
        retryable=error.status_code >= 500,
    )


def _authorization_pending(error: FeishuApiError) -> bool:
    message = str(error).lower()
    return any(
        term in message
        for term in ("not installed", "install", "tenant authorization", "unauthorized tenant")
    )


class FeishuProvider(BaseConnectorProvider):
    manifest = manifest

    def __init__(self, client_factory: Callable[[], FeishuClient] = HttpFeishuClient) -> None:
        self._client_factory = client_factory

    async def setup(
        self,
        context: ConnectorOperationContext,
        values: dict[str, str],
    ) -> ConnectorSetupResult:
        client = self._client_factory()
        app_id = values["app_id"].strip()
        app_secret = values["app_secret"].strip()
        expected_tenant = values["tenant_key"].strip()
        callback = (
            f"{context.callback_base_url.rstrip('/')}/webhook"
            if context.callback_base_url
            else "Configure the generic Feishu connector webhook URL."
        )
        try:
            token, expires_at = await issue_tenant_token(client, app_id, app_secret)
        except FeishuApiError as exc:
            if not _authorization_pending(exc):
                raise
            staged = FeishuCredential(
                app_id=app_id,
                app_secret=app_secret,
                tenant_key=expected_tenant,
                verification_token=values["verification_token"].strip(),
                encrypt_key=values["encrypt_key"].strip(),
                tenant_access_token="",
                token_expires_at=datetime.fromtimestamp(0, UTC),
                bot_open_id="",
                bot_name="",
            )
            return ConnectorSetupResult(
                binding=ConnectorBindingDraft(
                    display_name=f"Feishu — {expected_tenant}",
                    external_account_id=app_id,
                    external_tenant_id=expected_tenant,
                    metadata={
                        "authorization_state": "pending_tenant_install",
                        "webhook_url": callback,
                        "webhook_status": "pending",
                    },
                ),
                credential=staged.envelope(),
                artifacts=(
                    ConnectorSetupArtifact(
                        ConnectorSetupArtifactKind.instruction,
                        "Tenant authorization",
                        "Install and authorize the self-built app, then save setup again.",
                    ),
                ),
                status=ConnectorBindingStatus.authorizing,
            )
        actual_tenant, tenant_name = await tenant_identity(client, token)
        if actual_tenant != expected_tenant:
            raise ValueError(
                f"Feishu tenant mismatch: expected {expected_tenant!r}, got {actual_tenant!r}"
            )
        scopes = await granted_scopes(client, token)
        missing = set(FEISHU_REQUIRED_SCOPES) - scopes
        if missing:
            raise ValueError(f"Feishu app is missing permissions: {', '.join(sorted(missing))}")
        bot_open_id, bot_name = await bot_identity(client, token)
        credential = FeishuCredential(
            app_id=app_id,
            app_secret=app_secret,
            tenant_key=actual_tenant,
            verification_token=values["verification_token"].strip(),
            encrypt_key=values["encrypt_key"].strip(),
            tenant_access_token=token,
            token_expires_at=expires_at,
            bot_open_id=bot_open_id,
            bot_name=bot_name,
        )
        return ConnectorSetupResult(
            binding=ConnectorBindingDraft(
                display_name=f"Feishu — {tenant_name}",
                external_account_id=app_id,
                external_tenant_id=actual_tenant,
                metadata={
                    "tenant_name": tenant_name,
                    "bot_name": bot_name,
                    "webhook_url": callback,
                    "webhook_status": "configured",
                },
            ),
            credential=credential.envelope(),
            artifacts=(
                ConnectorSetupArtifact(
                    ConnectorSetupArtifactKind.url,
                    "Event callback URL",
                    callback,
                ),
                ConnectorSetupArtifact(
                    ConnectorSetupArtifactKind.instruction,
                    "Feishu event",
                    "Subscribe to im.message.receive_v1 and enable encrypted event delivery.",
                ),
            ),
        )

    async def list_resources(
        self,
        context: ConnectorOperationContext,
    ) -> ConnectorResourceResult:
        credential = FeishuCredential.from_envelope(context.credential)
        if credential.needs_refresh():
            raise RuntimeError("Feishu tenant token needs CAS rotation; run connector sync first")
        return await discover_resources(self._client_factory(), credential.tenant_access_token)

    async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
        credential = FeishuCredential.from_envelope(context.credential)
        client = self._client_factory()
        refreshed, changed = await refresh_credential(client, credential)
        workspace = await sync_workspace(client, refreshed.tenant_access_token, context)
        credential_update = (
            ConnectorCredentialUpdate(
                refreshed.envelope(),
                expected_version=context.credential_version,
            )
            if changed
            else None
        )
        return ConnectorSyncResult(
            changes=workspace.changes,
            state=ConnectorStateUpdate(
                credential=credential_update,
                binding_status=ConnectorBindingStatus.connected,
                cursor_updates=workspace.cursor_updates,
            ),
        )

    async def ingress(
        self,
        context: ConnectorOperationContext,
        request: ConnectorIngressRequest,
    ) -> ConnectorIngressResult:
        return handle_ingress(context, request, FeishuCredential.from_envelope(context.credential))

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        try:
            credential = FeishuCredential.from_envelope(context.credential)
        except (TypeError, ValueError) as exc:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                datetime.now(UTC),
                f"Feishu credentials are invalid: {exc}",
            )
        if context.binding is None:
            return ConnectorHealth(
                ConnectorHealthStatus.unconfigured,
                datetime.now(UTC),
                "Feishu is not configured.",
            )
        if context.binding.external_tenant_id != credential.tenant_key:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                datetime.now(UTC),
                "Feishu credential tenant does not match the configured binding.",
            )
        if not credential.verification_token or not credential.encrypt_key:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                datetime.now(UTC),
                "Feishu webhook verification or encryption keys are missing.",
            )
        client = self._client_factory()
        token = credential.tenant_access_token
        expired = credential.needs_refresh()
        try:
            if expired:
                token, _ = await issue_tenant_token(
                    client,
                    credential.app_id,
                    credential.app_secret,
                )
            actual_tenant, _ = await tenant_identity(client, token)
            if actual_tenant != credential.tenant_key:
                return ConnectorHealth(
                    ConnectorHealthStatus.error,
                    datetime.now(UTC),
                    "Feishu tenant authorization changed to a different tenant.",
                )
            scopes = await granted_scopes(client, token)
        except FeishuApiError as exc:
            return _health_from_api_error(exc)
        missing = set(FEISHU_REQUIRED_SCOPES) - scopes
        if missing:
            return ConnectorHealth(
                ConnectorHealthStatus.degraded,
                datetime.now(UTC),
                f"Feishu app permissions shrank; missing: {', '.join(sorted(missing))}.",
            )
        if context.delivery_health is not None:
            delivery = context.delivery_health
            return ConnectorHealth(
                (
                    ConnectorHealthStatus.degraded
                    if delivery.retryable
                    else ConnectorHealthStatus.error
                ),
                datetime.now(UTC),
                (
                    f"Feishu webhook has {delivery.unresolved_count} unresolved delivery "
                    f"failure(s): {delivery.summary}"
                ),
                retryable=delivery.retryable,
            )
        if context.binding.status is ConnectorBindingStatus.authorizing:
            return ConnectorHealth(
                ConnectorHealthStatus.degraded,
                datetime.now(UTC),
                "Feishu tenant authorization is available; save setup again to connect.",
            )
        if expired:
            return ConnectorHealth(
                ConnectorHealthStatus.degraded,
                datetime.now(UTC),
                "Feishu tenant token is expired; the next sync will rotate it with credential CAS.",
                retryable=True,
            )
        return ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC))

    async def revoke(self, context: ConnectorOperationContext) -> None:
        raise ConnectorUnsupportedError(
            "Feishu self-built apps cannot revoke their own tenant installation; "
            "uninstall the app in Feishu, then use local retain or local purge disconnect."
        )

    def build_actions(self, context: ConnectorActionContext) -> tuple[ConnectorAction, ...]:
        return (build_reply_action(REPLY_ACTION, context, self._client_factory),)


def factory() -> FeishuProvider:
    return FeishuProvider()


__all__ = [
    "FEISHU_CONNECTOR_ID",
    "FEISHU_REQUIRED_SCOPES",
    "FeishuProvider",
    "REPLY_ACTION",
    "factory",
    "manifest",
]
