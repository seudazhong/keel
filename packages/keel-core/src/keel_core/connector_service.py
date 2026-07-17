"""Connector orchestration for setup, sync, ingress, health, and revocation."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from keel_core.connector_contracts import (
    ConnectorBinding,
    ConnectorBindingStatus,
    ConnectorCapability,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorManifest,
    ConnectorResourceDraft,
    ConnectorSetupResult,
)
from keel_core.connector_credentials import ConnectorCredentialStore, CredentialEnvelope
from keel_core.connector_registry import ConnectorRegistry
from keel_core.connector_repository import ConnectorRepository
from keel_core.jobs import CancelMode, JobRecord, JobStore
from keel_core.knowledge.models import (
    CreateKnowledgeDocumentCommand,
    DeleteKnowledgeCommand,
    KnowledgeSourceType,
    UpdateKnowledgeDocumentCommand,
)
from keel_core.types import ContentTaint

CONNECTOR_SYNC_JOB_KIND = "connector.sync"
CONNECTOR_SYNC_MAX_ATTEMPTS = 3


@runtime_checkable
class ConnectorChangeSink(Protocol):
    async def apply(self, change: ConnectorChange) -> None: ...


class CallbackConnectorChangeSink:
    """Route normalized changes to durable Knowledge/trigger callbacks."""

    def __init__(
        self,
        *,
        upsert: Callable[[ConnectorChange], Awaitable[None]] | None = None,
        delete: Callable[[ConnectorChange], Awaitable[None]] | None = None,
        event: Callable[[ConnectorChange], Awaitable[None]] | None = None,
    ) -> None:
        self._callbacks = {"upsert": upsert, "delete": delete, "event": event}

    async def apply(self, change: ConnectorChange) -> None:
        callback = self._callbacks[change.kind.value]
        if callback is None:
            raise RuntimeError(
                f"no durable connector change sink is configured for {change.kind.value}"
            )
        await callback(change)


@runtime_checkable
class ConnectorKnowledgeService(Protocol):
    async def create_document(
        self,
        kb_id: str,
        command: CreateKnowledgeDocumentCommand,
        idempotency_key: str,
    ) -> Any: ...

    async def update_document(
        self,
        kb_id: str,
        document_id: str,
        command: UpdateKnowledgeDocumentCommand,
        idempotency_key: str,
    ) -> Any: ...

    async def delete_document(
        self,
        kb_id: str,
        document_id: str,
        command: DeleteKnowledgeCommand,
        idempotency_key: str,
    ) -> Any: ...


class DurableConnectorChangeSink:
    """Apply knowledge changes and durably admit trigger events using binding metadata."""

    def __init__(
        self,
        repository: ConnectorRepository,
        *,
        knowledge: ConnectorKnowledgeService | None = None,
        admit_event: Callable[[str, str, str], Awaitable[None]] | None = None,
    ) -> None:
        self._repository = repository
        self._knowledge = knowledge
        self._admit_event = admit_event

    async def apply(self, change: ConnectorChange) -> None:
        binding = await self._repository.get_binding(change.provenance.connector_id)
        if binding is None or binding.id != change.provenance.binding_id:
            raise RuntimeError("connector change binding is unavailable")
        if change.kind is ConnectorChangeKind.event:
            await self._apply_event(binding, change)
            return
        if self._knowledge is None:
            raise RuntimeError("connector Knowledge change sink is unavailable")
        kb_id = binding.metadata.get("knowledge_base_id")
        if not isinstance(kb_id, str) or not kb_id:
            raise RuntimeError("connector binding does not select a knowledge_base_id")
        resources = await self._repository.list_resources(change.provenance.connector_id)
        resource = next(
            (
                item
                for item in resources
                if item.external_id == change.provenance.external_resource_id
            ),
            None,
        )
        document_id = None if resource is None else resource.config.get("knowledge_document_id")
        if not isinstance(document_id, str):
            document_id = None
        key = _change_idempotency_key(change)
        if change.kind is ConnectorChangeKind.delete:
            if document_id is not None:
                await self._knowledge.delete_document(
                    kb_id,
                    document_id,
                    DeleteKnowledgeCommand(),
                    key,
                )
            return
        if not change.title or not change.content:
            raise ValueError("connector knowledge upserts require title and content")
        source_type = (
            KnowledgeSourceType.markdown
            if change.mime_type == "text/markdown"
            else KnowledgeSourceType.text
        )
        if document_id is None:
            result = await self._knowledge.create_document(
                kb_id,
                CreateKnowledgeDocumentCommand(
                    title=change.title,
                    source_type=source_type,
                    content=change.content,
                    source_uri=change.provenance.source_url,
                ),
                key,
            )
            document_id = str(result.document.id)
        else:
            await self._knowledge.update_document(
                kb_id,
                document_id,
                UpdateKnowledgeDocumentCommand(
                    title=change.title,
                    source_type=source_type,
                    content=change.content,
                    source_uri=change.provenance.source_url,
                ),
                key,
            )
        await self._repository.upsert_resources(
            change.provenance.connector_id,
            binding.id,
            (
                ConnectorResourceDraft(
                    external_id=change.provenance.external_resource_id,
                    kind="knowledge_document",
                    display_name=change.title,
                    url=change.provenance.source_url,
                    selected=True,
                    config={
                        "knowledge_document_id": document_id,
                        "revision": change.provenance.revision,
                    },
                ),
            ),
        )

    async def _apply_event(self, binding: ConnectorBinding, change: ConnectorChange) -> None:
        if self._admit_event is None:
            raise RuntimeError("connector trigger admission sink is unavailable")
        session_id = binding.metadata.get("trigger_session_id")
        if not isinstance(session_id, str) or not session_id:
            raise RuntimeError("connector binding does not select a trigger_session_id")
        if change.event is None:
            raise ValueError("connector event changes require a normalized event")
        content = json.dumps(
            {
                "source": "external_connector",
                "taint": ContentTaint.tainted.value,
                "type": change.event.type,
                "connector_id": change.provenance.connector_id,
                "binding_id": change.provenance.binding_id,
                "external_resource_id": change.provenance.external_resource_id,
                "source_url": change.provenance.source_url,
                "revision": change.provenance.revision,
                "event_id": change.provenance.event_id,
                "payload": change.event.payload,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if len(content.encode("utf-8")) > 65_536:
            raise ValueError("normalized connector event exceeds 65536 UTF-8 bytes")
        await self._admit_event(session_id, content, _change_idempotency_key(change))


class ConnectorService:
    def __init__(
        self,
        registry: ConnectorRegistry,
        repository: ConnectorRepository,
        *,
        credentials: ConnectorCredentialStore | None = None,
        jobs: JobStore | None = None,
        dispatch_job: Callable[[str, str], Awaitable[None]] | None = None,
        change_sink: ConnectorChangeSink | None = None,
        delete_credential: Callable[[str], Awaitable[bool]] | None = None,
        purge_outbound: Callable[[str], Awaitable[int]] | None = None,
    ) -> None:
        if jobs is not None and jobs.scope_id != repository.scope_id:
            raise ValueError("connector repository and job store must use the same scope")
        self.registry = registry
        self.repository = repository
        self.credentials = credentials
        self.jobs = jobs
        self._dispatch_job = dispatch_job
        self._change_sink = change_sink or CallbackConnectorChangeSink()
        self._delete_credential = delete_credential
        self._purge_outbound = purge_outbound

    async def catalog(
        self, legacy_connected: dict[str, datetime | None] | None = None
    ) -> list[dict[str, Any]]:
        bindings = {item.connector_id: item for item in await self.repository.list_bindings()}
        legacy = legacy_connected or {}
        rows: list[dict[str, Any]] = []
        for manifest in self.registry.manifests():
            provider_enabled = self.registry.create(manifest.id).enabled()
            binding = bindings.get(manifest.id)
            connected = (
                binding is not None
                and binding.status
                in {ConnectorBindingStatus.connected, ConnectorBindingStatus.configured}
            ) or manifest.id in legacy
            updated_at = (
                binding.updated_at if binding is not None else legacy.get(manifest.id)
            )
            rows.append(
                {
                    **manifest_to_dict(manifest),
                    "enabled": provider_enabled,
                    "operational": connected and provider_enabled,
                    "connected": connected,
                    "updated_at": updated_at.isoformat() if updated_at else None,
                    "binding": binding_to_dict(binding) if binding is not None else None,
                    "health": (
                        ConnectorHealthStatus.error.value
                        if binding is not None and binding.status is ConnectorBindingStatus.error
                        else (
                            ConnectorHealthStatus.healthy.value
                            if connected
                            else ConnectorHealthStatus.unconfigured.value
                        )
                    ),
                }
            )
        return rows

    async def save_setup(
        self, connector_id: str, result: ConnectorSetupResult
    ) -> ConnectorBinding:
        stored = False
        if result.credential is not None:
            if self.credentials is None:
                raise RuntimeError("encrypted connector credential storage is unavailable")
            await self.credentials.put(connector_id, result.credential)
            stored = True
        try:
            return await self.repository.upsert_binding(
                connector_id,
                result.binding,
                ConnectorBindingStatus.connected,
            )
        except Exception:
            if stored and self.credentials is not None:
                await self.credentials.delete(connector_id)
            raise

    async def setup(self, connector_id: str, values: dict[str, str]) -> ConnectorBinding:
        provider = self.registry.create(connector_id)
        expected = {item.id: item for item in provider.manifest.setup_fields}
        unknown = set(values) - set(expected)
        if unknown:
            raise ValueError(f"unknown connector setup fields: {', '.join(sorted(unknown))}")
        missing = [
            item.id
            for item in expected.values()
            if item.required and not values.get(item.id, "").strip()
        ]
        if missing:
            raise ValueError(f"missing connector setup fields: {', '.join(sorted(missing))}")
        return await self.save_setup(connector_id, await provider.setup(dict(values)))

    async def refresh_resources(self, connector_id: str) -> list[dict[str, Any]]:
        provider = self.registry.create(connector_id)
        if not provider.enabled():
            raise RuntimeError(f"connector {connector_id!r} is disabled")
        binding = await self._required_binding(connector_id)
        if ConnectorCapability.resources in provider.manifest.capabilities:
            credential = await self._credential(connector_id)
            drafts = await provider.list_resources(binding, credential)
            await self.repository.upsert_resources(connector_id, binding.id, drafts)
        return [
            resource_to_dict(item)
            for item in await self.repository.list_resources(connector_id)
        ]

    async def select_resources(self, connector_id: str, external_ids: set[str]) -> int:
        await self._required_binding(connector_id)
        known = {
            item.external_id for item in await self.repository.list_resources(connector_id)
        }
        unknown = external_ids - known
        if unknown:
            raise ValueError(f"unknown connector resources: {', '.join(sorted(unknown))}")
        return await self.repository.select_resources(connector_id, external_ids)

    async def enqueue_sync(
        self, connector_id: str, *, idempotency_key: str | None = None
    ) -> JobRecord:
        provider = self.registry.create(connector_id)
        if not provider.enabled():
            raise RuntimeError(f"connector {connector_id!r} is disabled")
        if ConnectorCapability.sync not in provider.manifest.capabilities:
            raise ValueError(f"connector {connector_id!r} does not support sync")
        binding = await self._required_binding(connector_id)
        if self.jobs is None:
            raise RuntimeError("durable connector jobs are unavailable")
        key = idempotency_key.strip() if idempotency_key else uuid.uuid4().hex
        job, created = await self.jobs.enqueue_once(
            kind=CONNECTOR_SYNC_JOB_KIND,
            payload={"connector_id": connector_id, "binding_id": binding.id},
            target_session_id=None,
            idempotency_key=f"{connector_id}:{key}",
            max_attempts=CONNECTOR_SYNC_MAX_ATTEMPTS,
            cancel_mode=CancelMode.cooperative,
        )
        if created and self._dispatch_job is not None:
            await self._dispatch_job(self.repository.scope_id, job.id)
        return job

    async def sync(self, connector_id: str, binding_id: str | None = None) -> int:
        provider = self.registry.create(connector_id)
        if not provider.enabled():
            raise RuntimeError(f"connector {connector_id!r} is disabled")
        binding = await self._required_binding(connector_id)
        if binding_id is not None and binding.id != binding_id:
            raise ValueError("connector binding changed before sync execution")
        resources = tuple(
            await self.repository.list_resources(connector_id, selected_only=True)
        )
        cursor = await self.repository.get_cursor(
            connector_id, binding.id, "default", resource_id=None
        )
        credential = await self._credential(connector_id)
        try:
            result = await provider.sync(binding, resources, cursor, credential)
            for change in result.changes:
                self._validate_change(connector_id, binding.id, change)
                await self._change_sink.apply(change)
            if result.cursor is not None:
                await self.repository.put_cursor(
                    connector_id,
                    binding.id,
                    result.cursor_stream,
                    result.cursor,
                )
            await self.repository.record_health(
                connector_id,
                ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC)),
            )
            return len(result.changes)
        except Exception as exc:
            await self.repository.record_health(
                connector_id,
                ConnectorHealth(
                    ConnectorHealthStatus.error,
                    datetime.now(UTC),
                    f"{type(exc).__name__}: connector sync failed",
                    retryable=True,
                ),
            )
            raise

    async def health(self, connector_id: str) -> ConnectorHealth:
        provider = self.registry.create(connector_id)
        binding = await self._required_binding(connector_id)
        health = await provider.health(binding, await self._credential(connector_id))
        await self.repository.record_health(connector_id, health)
        return health

    async def ingress(
        self, connector_id: str, headers: dict[str, str], body: bytes
    ) -> tuple[bool, int]:
        provider = self.registry.create(connector_id)
        if not provider.enabled():
            raise RuntimeError(f"connector {connector_id!r} is disabled")
        binding = await self._required_binding(connector_id)
        result = await provider.ingress(headers, body, binding)
        payload_hash = hashlib.sha256(body).hexdigest()
        claimed = await self.repository.claim_delivery(
            connector_id, binding.id, result.delivery_id, payload_hash
        )
        if not claimed:
            return False, 0
        try:
            for change in result.changes:
                self._validate_change(connector_id, binding.id, change)
                await self._change_sink.apply(change)
        except Exception as exc:
            await self.repository.finish_delivery(
                connector_id,
                result.delivery_id,
                error_code=type(exc).__name__,
                error_summary="connector delivery processing failed",
            )
            raise
        await self.repository.finish_delivery(connector_id, result.delivery_id)
        return True, len(result.changes)

    async def revoke(self, connector_id: str, *, purge: bool = False) -> bool:
        provider = self.registry.create(connector_id)
        credential = await self._credential(connector_id)
        binding = await self.repository.get_binding(connector_id)
        await provider.revoke(credential)
        deleted = False
        if self.credentials is not None:
            await self.credentials.delete(connector_id)
            deleted = credential is not None
        elif self._delete_credential is not None:
            deleted = await self._delete_credential(connector_id)
        removed = await self.repository.delete_connector(connector_id)
        outbound = 0
        if purge and self._purge_outbound is not None:
            outbound = await self._purge_outbound(connector_id)
        return deleted or removed > 0 or outbound > 0 or binding is not None

    async def _required_binding(self, connector_id: str) -> ConnectorBinding:
        binding = await self.repository.get_binding(connector_id)
        if binding is None:
            raise LookupError(f"connector {connector_id!r} is not configured")
        return binding

    async def _credential(self, connector_id: str) -> CredentialEnvelope | None:
        if self.credentials is None:
            return None
        return await self.credentials.get(connector_id)

    @staticmethod
    def _validate_change(
        connector_id: str, binding_id: str, change: ConnectorChange
    ) -> None:
        if change.taint is not ContentTaint.tainted:
            raise ValueError("external connector changes must be tainted")
        if change.provenance.connector_id != connector_id:
            raise ValueError("connector change provenance has the wrong connector id")
        if change.provenance.binding_id != binding_id:
            raise ValueError("connector change provenance has the wrong binding id")
        if not change.provenance.external_resource_id:
            raise ValueError("connector change provenance requires an external resource id")
        if change.kind is ConnectorChangeKind.event and change.event is None:
            raise ValueError("connector event changes require a normalized event")


def _change_idempotency_key(change: ConnectorChange) -> str:
    raw = "|".join(
        (
            change.kind.value,
            change.provenance.connector_id,
            change.provenance.binding_id,
            change.provenance.external_resource_id,
            change.provenance.revision or "",
            change.provenance.event_id or "",
        )
    )
    return f"connector:{hashlib.sha256(raw.encode()).hexdigest()}"


def manifest_to_dict(manifest: ConnectorManifest) -> dict[str, Any]:
    return {
        "id": manifest.id,
        "name": manifest.name,
        "description": manifest.description,
        "icon": manifest.icon,
        "kind": manifest.auth_kind.value,
        "auth_kind": manifest.auth_kind.value,
        "capabilities": [item.value for item in manifest.capabilities],
        "scopes": [scope.rsplit("/", 1)[-1] for scope in manifest.scopes],
        "setup_fields": [
            {
                "id": item.id,
                "label": item.label,
                "required": item.required,
                "secret": item.secret,
                "input_type": item.input_type,
                "help_text": item.help_text,
            }
            for item in manifest.setup_fields
        ],
        "resource_label": manifest.resource_label,
    }


def binding_to_dict(binding: ConnectorBinding) -> dict[str, Any]:
    return {
        "id": binding.id,
        "status": binding.status.value,
        "display_name": binding.display_name,
        "external_account_id": binding.external_account_id,
        "external_tenant_id": binding.external_tenant_id,
        "metadata": binding.metadata,
        "last_success_at": (
            binding.last_success_at.isoformat() if binding.last_success_at else None
        ),
        "error_code": binding.error_code,
        "error_summary": binding.error_summary,
    }


def resource_to_dict(resource: Any) -> dict[str, Any]:
    return {
        "id": resource.id,
        "external_id": resource.external_id,
        "kind": resource.kind,
        "display_name": resource.display_name,
        "url": resource.url,
        "selected": resource.selected,
        "config": resource.config,
    }


__all__ = [
    "CONNECTOR_SYNC_JOB_KIND",
    "CONNECTOR_SYNC_MAX_ATTEMPTS",
    "CallbackConnectorChangeSink",
    "ConnectorChangeSink",
    "ConnectorKnowledgeService",
    "ConnectorService",
    "DurableConnectorChangeSink",
    "binding_to_dict",
    "manifest_to_dict",
    "resource_to_dict",
]
