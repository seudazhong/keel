"""Scope-bound connector actions shared by interactive and scheduled runs."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.config import Settings
from keel_core.connector_contracts import (
    ConnectorAction,
    ConnectorActionContext,
    ConnectorBindingStatus,
    ConnectorHealth,
    ConnectorHealthStatus,
)
from keel_core.connector_credentials import ConnectorCredentialStore
from keel_core.connector_registry import ConnectorRegistry, get_connector_registry
from keel_core.connector_repository import PostgresConnectorRepository
from keel_core.connectors import ActionFn, ConnectorActionUserError
from keel_core.outbox import PostgresOutboundStore
from keel_core.protocols import ToolContext
from keel_core.secrets import SecretsError, keyring_from_settings
from keel_core.tokens import PostgresTokenStore

logger = logging.getLogger("keel.connector_actions")


async def _record_health_best_effort(
    repository: Any,
    connector_id: str,
    binding_id: str,
    health: ConnectorHealth,
) -> None:
    try:
        await repository.record_health(connector_id, binding_id, health)
    except Exception:
        logger.warning(
            "connector health update failed connector=%s binding=%s",
            connector_id,
            binding_id,
            exc_info=True,
        )


def _tracked_action(
    action: ConnectorAction,
    connector_id: str,
    repository: Any,
) -> ActionFn:
    async def invoke(arguments: dict[str, Any], tool_context: ToolContext) -> str:
        binding = await repository.get_binding(connector_id)
        try:
            result = await action.action(arguments, tool_context)
        except Exception as exc:
            if binding is not None:
                message = (
                    str(exc)
                    if isinstance(exc, ConnectorActionUserError)
                    else f"{connector_id} action failed. Reconnect or check its configuration."
                )
                await _record_health_best_effort(
                    repository,
                    connector_id,
                    binding.id,
                    ConnectorHealth(
                        ConnectorHealthStatus.error,
                        datetime.now(UTC),
                        message,
                    ),
                )
            raise
        if binding is not None:
            await _record_health_best_effort(
                repository,
                connector_id,
                binding.id,
                ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC)),
            )
        return result

    return invoke


async def build_connector_actions(
    *,
    engine: AsyncEngine | None,
    settings: Settings,
    scope_id: str,
    registry: ConnectorRegistry | None = None,
    repository: Any | None = None,
    credential_store: Any | None = None,
    envelope_credential_store: Any | None = None,
) -> tuple[ConnectorAction, ...]:
    """Build enabled connector tools against one exact scope."""
    registry = registry or get_connector_registry()
    if repository is None or getattr(repository, "scope_id", scope_id) != scope_id:
        if engine is None:
            return ()
        repository = PostgresConnectorRepository(engine, scope_id)
    if credential_store is None:
        if engine is None:
            return ()
        if not settings.secret_key and not settings.secret_keys:
            return ()
        try:
            keyring = keyring_from_settings(settings)
        except SecretsError:
            return ()
        token_store = PostgresTokenStore(engine, scope_id, keyring)
        credential_store = token_store
        envelope_credential_store = ConnectorCredentialStore(token_store)
    context = ConnectorActionContext.with_repository(
        scope_id,
        repository,
        credential_store=credential_store,
        envelope_credential_store=envelope_credential_store,
        idempotency_store=PostgresOutboundStore(engine) if engine is not None else None,
    )
    owners = {
        action.name: manifest.id for manifest in registry.manifests() for action in manifest.actions
    }
    bindings = {binding.connector_id: binding for binding in await repository.list_bindings()}
    active_connector_ids = {
        connector_id
        for connector_id, binding in bindings.items()
        if binding.status
        in {
            ConnectorBindingStatus.connected,
            ConnectorBindingStatus.degraded,
        }
    }
    if credential_store is not None and hasattr(credential_store, "get"):
        for manifest in registry.manifests():
            if manifest.id in bindings:
                continue
            if await credential_store.get(manifest.id) is not None:
                active_connector_ids.add(manifest.id)
    return tuple(
        ConnectorAction(
            action.manifest,
            _tracked_action(action, owners[action.manifest.name], repository),
        )
        for action in registry.build_actions(context)
        if owners[action.manifest.name] in active_connector_ids
    )


__all__ = ["build_connector_actions"]
