"""Connector orchestration for setup, sync, ingress, health, and revocation."""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, runtime_checkable

from keel_core.connector_contracts import (
    ConnectorAuthStart,
    ConnectorBinding,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorBindingTarget,
    ConnectorCapability,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorIngressFailure,
    ConnectorIngressRequest,
    ConnectorIngressResponse,
    ConnectorItem,
    ConnectorItemDraft,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorProvider,
    ConnectorRenewalResult,
    ConnectorResourceRefreshMode,
    ConnectorScheduleOperation,
    ConnectorSetupArtifact,
    ConnectorSetupResult,
    ConnectorStateUpdate,
    ConnectorTargetKind,
)
from keel_core.connector_credentials import ConnectorCredentialStore, CredentialEnvelope
from keel_core.connector_registry import ConnectorRegistry
from keel_core.connector_repository import (
    ConnectorRepository,
    ConnectorScheduleLease,
    ConnectorScheduleLeaseLostError,
    next_schedule_time,
)
from keel_core.connector_schedule_index import ConnectorScheduleIndex
from keel_core.connector_webhook_routes import ConnectorWebhookRouteStore, mint_route_token
from keel_core.job_dispatch import JobDispatchOutbox
from keel_core.jobs import CancelMode, JobRecord, JobStore, retry_delay_seconds
from keel_core.knowledge.models import (
    CreateKnowledgeDocumentCommand,
    DeleteKnowledgeCommand,
    KnowledgeSourceType,
    UpdateKnowledgeDocumentCommand,
)
from keel_core.types import ContentTaint

CONNECTOR_SYNC_JOB_KIND = "connector.sync"
CONNECTOR_RENEW_JOB_KIND = "connector.renew"
CONNECTOR_SYNC_MAX_ATTEMPTS = 3
CONNECTOR_RENEW_MAX_ATTEMPTS = 3
logger = logging.getLogger("keel.connectors")


@runtime_checkable
class ConnectorChangeSink(Protocol):
    async def apply(self, change: ConnectorChange) -> None: ...


@runtime_checkable
class ConnectorPurgeSink(Protocol):
    async def handoff_purge(
        self, binding: ConnectorBinding, items: tuple[ConnectorItem, ...]
    ) -> int: ...


TargetValidator = Callable[[ConnectorTargetKind, str], Awaitable[bool]]
TriggerTargetResolver = Callable[[ConnectorTargetKind, str], Awaitable[str]]


@dataclass(frozen=True, slots=True)
class ConnectorSetupOutcome:
    binding: ConnectorBinding
    artifacts: tuple[ConnectorSetupArtifact, ...] = ()


@dataclass(frozen=True, slots=True)
class ConnectorIngressOutcome:
    response: ConnectorIngressResponse
    accepted: bool
    changes: int


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
    """Apply changes through typed binding targets and durable item mappings."""

    def __init__(
        self,
        repository: ConnectorRepository,
        *,
        knowledge: ConnectorKnowledgeService | None = None,
        admit_event: Callable[[str, str, str], Awaitable[None]] | None = None,
        resolve_trigger: TriggerTargetResolver | None = None,
    ) -> None:
        self._repository = repository
        self._knowledge = knowledge
        self._admit_event = admit_event
        self._resolve_trigger = resolve_trigger

    async def apply(self, change: ConnectorChange) -> None:
        binding = await self._repository.get_binding(change.provenance.connector_id)
        if binding is None or binding.id != change.provenance.binding_id:
            raise RuntimeError("connector change binding is unavailable")
        if change.kind is ConnectorChangeKind.event:
            await self._apply_event(binding, change)
            return
        if self._knowledge is None:
            raise RuntimeError("connector Knowledge change sink is unavailable")
        kb_id = await self._target(binding, ConnectorTargetKind.knowledge)
        items = await self._repository.list_items(change.provenance.connector_id)
        item = next(
            (item for item in items if item.external_id == change.provenance.external_resource_id),
            None,
        )
        document_id = (
            item.destination_id
            if item is not None and item.destination_kind is ConnectorTargetKind.knowledge
            else None
        )
        mapped_kb_id = (
            item.destination_target_id
            if item is not None and item.destination_kind is ConnectorTargetKind.knowledge
            else None
        )
        key = _change_idempotency_key(change)
        if change.kind is ConnectorChangeKind.delete:
            if document_id is not None and mapped_kb_id is not None:
                await self._knowledge.delete_document(
                    mapped_kb_id,
                    document_id,
                    DeleteKnowledgeCommand(),
                    key,
                )
            await self._repository.delete_item(
                change.provenance.connector_id,
                binding.id,
                change.provenance.external_resource_id,
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
        await self._repository.upsert_items(
            change.provenance.connector_id,
            binding.id,
            (
                ConnectorItemDraft(
                    external_id=change.provenance.external_resource_id,
                    kind="knowledge_document",
                    display_name=change.title,
                    url=change.provenance.source_url,
                    destination_kind=ConnectorTargetKind.knowledge,
                    destination_target_id=kb_id,
                    destination_id=document_id,
                    config={"revision": change.provenance.revision},
                ),
            ),
        )

    async def _apply_event(self, binding: ConnectorBinding, change: ConnectorChange) -> None:
        if self._admit_event is None:
            raise RuntimeError("connector trigger admission sink is unavailable")
        session_id = await self._trigger_session(binding)
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

    async def handoff_purge(
        self, binding: ConnectorBinding, items: tuple[ConnectorItem, ...]
    ) -> int:
        if self._knowledge is None:
            raise RuntimeError("connector Knowledge purge service is unavailable")
        documents = tuple(
            item
            for item in items
            if item.destination_kind is ConnectorTargetKind.knowledge
            and item.destination_target_id is not None
            and item.destination_id is not None
        )
        if not documents:
            return 0
        for item in documents:
            assert item.destination_id is not None
            assert item.destination_target_id is not None
            await self._knowledge.delete_document(
                item.destination_target_id,
                item.destination_id,
                DeleteKnowledgeCommand(),
                _purge_idempotency_key(binding, item),
            )
        return len(documents)

    async def _target(self, binding: ConnectorBinding, kind: ConnectorTargetKind) -> str:
        targets = await self._repository.list_targets(binding.connector_id)
        target = next((item.target_id for item in targets if item.kind is kind), None)
        if target is None:
            raise RuntimeError(f"connector binding does not select a {kind.value} target")
        return target

    async def _trigger_session(self, binding: ConnectorBinding) -> str:
        targets = await self._repository.list_targets(binding.connector_id)
        by_kind = {item.kind: item.target_id for item in targets}
        session_id = by_kind.get(ConnectorTargetKind.trigger_session)
        if session_id is not None:
            return session_id
        routine_id = by_kind.get(ConnectorTargetKind.trigger_routine)
        if routine_id is None:
            raise RuntimeError(
                "connector binding does not select a trigger session or routine target"
            )
        if self._resolve_trigger is None:
            raise RuntimeError("connector trigger routine resolver is unavailable")
        return await self._resolve_trigger(ConnectorTargetKind.trigger_routine, routine_id)


class ConnectorService:
    def __init__(
        self,
        registry: ConnectorRegistry,
        repository: ConnectorRepository,
        *,
        credentials: ConnectorCredentialStore | None = None,
        jobs: JobStore | None = None,
        dispatch_job: Callable[[str, str], Awaitable[None]] | None = None,
        dispatch_outbox: JobDispatchOutbox | None = None,
        schedule_index: ConnectorScheduleIndex | None = None,
        webhook_route_store: ConnectorWebhookRouteStore | None = None,
        change_sink: ConnectorChangeSink | None = None,
        purge_sink: ConnectorPurgeSink | None = None,
        target_validator: TargetValidator | None = None,
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
        self._dispatch_outbox = dispatch_outbox
        self._schedule_index = schedule_index
        self._webhook_route_store = webhook_route_store
        self._change_sink = change_sink or CallbackConnectorChangeSink()
        self._purge_sink = (
            purge_sink
            if purge_sink is not None
            else (self._change_sink if isinstance(self._change_sink, ConnectorPurgeSink) else None)
        )
        self._target_validator = target_validator
        self._delete_credential = delete_credential
        self._purge_outbound = purge_outbound

    async def catalog(
        self, legacy_connected: dict[str, datetime | None] | None = None
    ) -> list[dict[str, Any]]:
        bindings = {item.connector_id: item for item in await self.repository.list_bindings()}
        legacy = legacy_connected or {}
        rows: list[dict[str, Any]] = []
        for manifest in self.registry.manifests():
            provider_status = self.registry.status(manifest.id)
            provider_enabled = provider_status.enabled
            binding = bindings.get(manifest.id)
            targets = await self.repository.list_targets(manifest.id) if binding is not None else []
            legacy_only = binding is None and manifest.id in legacy
            connected = (
                binding is not None
                and binding.status
                in {
                    ConnectorBindingStatus.connected,
                    ConnectorBindingStatus.degraded,
                }
            ) or legacy_only
            configured = (
                binding is not None and binding.status is not ConnectorBindingStatus.revoked
            ) or legacy_only
            operational_binding = (
                binding is not None
                and binding.status
                in {
                    ConnectorBindingStatus.connected,
                    ConnectorBindingStatus.degraded,
                }
            ) or legacy_only
            updated_at = binding.updated_at if binding is not None else legacy.get(manifest.id)
            rows.append(
                {
                    **manifest_to_dict(manifest),
                    "available": provider_status.available,
                    "availability_error": provider_status.error,
                    "enabled": provider_enabled,
                    "operational": (
                        operational_binding and provider_enabled and provider_status.available
                    ),
                    "configured": configured,
                    "connected": connected,
                    "next_action": _next_action(manifest, binding),
                    "updated_at": updated_at.isoformat() if updated_at else None,
                    "binding": (binding_to_dict(binding, targets) if binding is not None else None),
                    "health": (
                        ConnectorHealthStatus.error.value
                        if not provider_status.available
                        or (binding is not None and binding.status is ConnectorBindingStatus.error)
                        else (_binding_health(binding, connected).value)
                    ),
                }
            )
        for failure in self.registry.discovery_failures():
            binding = bindings.get(failure.connector_id)
            targets = (
                await self.repository.list_targets(failure.connector_id)
                if binding is not None
                else []
            )
            connected = binding is not None and binding.status in {
                ConnectorBindingStatus.connected,
                ConnectorBindingStatus.degraded,
            }
            configured = (
                binding is not None and binding.status is not ConnectorBindingStatus.revoked
            )
            rows.append(
                {
                    "id": failure.connector_id,
                    "name": failure.connector_id.replace("_", " ").title(),
                    "description": "This connector provider could not be loaded.",
                    "icon": "🔌",
                    "kind": "secret",
                    "auth_kind": "secret",
                    "capabilities": [],
                    "scopes": [],
                    "setup_fields": [],
                    "auth_action": None,
                    "setup_action_label": "Unavailable",
                    "resource_label": None,
                    "target_fields": [],
                    "actions": [],
                    "available": False,
                    "availability_error": failure.error,
                    "enabled": False,
                    "operational": False,
                    "configured": configured,
                    "connected": connected,
                    "next_action": None,
                    "updated_at": (
                        binding.updated_at.isoformat()
                        if binding is not None and binding.updated_at is not None
                        else None
                    ),
                    "binding": (binding_to_dict(binding, targets) if binding is not None else None),
                    "health": ConnectorHealthStatus.error.value,
                }
            )
        rows.sort(key=lambda item: str(item["id"]))
        return rows

    async def begin_auth(self, connector_id: str, callback_url: str) -> ConnectorAuthStart:
        provider = self.registry.create(connector_id)
        context = await self._operation_context(connector_id)
        action = provider.manifest.auth_action
        if action is None:
            raise ValueError(f"{connector_id} does not declare browser authorization")
        if action.requires_setup and (context.binding is None or context.credential is None):
            raise ValueError(f"{connector_id} requires setup before browser authorization")
        start = await provider.begin_auth(context, callback_url)
        draft = (
            ConnectorBindingDraft()
            if context.binding is None
            else ConnectorBindingDraft(
                display_name=context.binding.display_name,
                external_account_id=context.binding.external_account_id,
                external_tenant_id=context.binding.external_tenant_id,
                metadata=context.binding.metadata,
            )
        )
        await self.repository.upsert_binding(
            connector_id,
            draft,
            ConnectorBindingStatus.authorizing,
            sync_cadence_seconds=provider.manifest.default_sync_cadence_seconds,
            renewal=provider.manifest.renewal,
            renewal_expires_at=(
                None if context.binding is None else context.binding.renewal_expires_at
            ),
            expected_binding_id=(None if context.binding is None else context.binding.id),
            enforce_binding_fence=True,
        )
        return start

    async def complete_auth(
        self,
        connector_id: str,
        callback_url: str,
        parameters: dict[str, str],
    ) -> ConnectorSetupOutcome:
        provider = self.registry.create(connector_id)
        context = await self._operation_context(connector_id)
        result = await provider.complete_auth(
            context,
            callback_url,
            dict(parameters),
        )
        return ConnectorSetupOutcome(
            await self.save_setup(connector_id, result, context=context),
            result.artifacts,
        )

    async def save_setup(
        self,
        connector_id: str,
        result: ConnectorSetupResult,
        *,
        context: ConnectorOperationContext | None = None,
    ) -> ConnectorBinding:
        operation = context or await self._operation_context(connector_id)
        latest = await self.repository.get_binding(connector_id)
        expected_binding_id = None if operation.binding is None else operation.binding.id
        actual_binding_id = None if latest is None else latest.id
        if actual_binding_id != expected_binding_id:
            raise RuntimeError("connector binding changed during setup")
        manifest = self._manifest(connector_id)
        stored_version: int | None = None
        if result.credential is not None:
            if self.credentials is None:
                raise RuntimeError("encrypted connector credential storage is unavailable")
            stored_version = await self.credentials.put_if_version(
                connector_id,
                result.credential,
                operation.credential_version,
            )
            if stored_version is None:
                raise RuntimeError("connector credentials changed during setup")
        try:
            binding = await self.repository.upsert_binding(
                connector_id,
                result.binding,
                result.status,
                sync_cadence_seconds=manifest.default_sync_cadence_seconds,
                renewal=manifest.renewal,
                renewal_expires_at=result.renewal_expires_at,
                expected_binding_id=expected_binding_id,
                enforce_binding_fence=True,
            )
        except Exception as exc:
            if stored_version is not None:
                await self._rollback_credential(connector_id, operation, stored_version, exc)
            raise
        # A binding that arms a recurring sync/renewal schedule registers its scope in the global
        # schedule index so the cross-scope recurring reconciler discovers and fires it (finding
        # 1); without this a connector connected under a per-Agent scope would never sync/renew.
        if self._schedule_index is not None and (
            binding.next_sync_at is not None or binding.next_renewal_at is not None
        ):
            await self._schedule_index.record(self.repository.scope_id)
        return binding

    async def setup(
        self,
        connector_id: str,
        values: dict[str, str],
        *,
        callback_base_url: str | None = None,
    ) -> ConnectorSetupOutcome:
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
        # Webhook-capable connectors mint a high-entropy route token and register a webhook URL
        # that embeds it, so an inbound (auth-headerless) delivery resolves back to this exact
        # scope+binding rather than the app-global ``web:local`` scope. The token is threaded into
        # ``callback_base_url`` (which providers use to build their webhook URL); the route is
        # persisted after the binding is created so it points at a real binding.
        route_token: str | None = None
        if (
            self._webhook_route_store is not None
            and callback_base_url is not None
            and ConnectorCapability.webhook in provider.manifest.capabilities
        ):
            route_token = mint_route_token()
            callback_base_url = f"{callback_base_url.rstrip('/')}/r/{route_token}"
        context = await self._operation_context(
            connector_id,
            callback_base_url=callback_base_url,
        )
        result = await provider.setup(context, dict(values))
        binding = await self.save_setup(connector_id, result, context=context)
        if route_token is not None:
            assert self._webhook_route_store is not None
            await self._webhook_route_store.put(
                route_token,
                self.repository.scope_id,
                connector_id,
                binding.id,
                binding.status.value,
            )
        return ConnectorSetupOutcome(binding, result.artifacts)

    async def configure_targets(
        self, connector_id: str, values: dict[str, str | None]
    ) -> list[ConnectorBindingTarget]:
        manifest = self._manifest(connector_id)
        binding = await self._required_active_binding(connector_id)
        declared = {item.kind: item for item in manifest.target_fields}
        try:
            requested = {
                ConnectorTargetKind(key): value.strip()
                for key, value in values.items()
                if value is not None and value.strip()
            }
        except ValueError as exc:
            raise ValueError("unknown connector target kind") from exc
        unknown = set(requested) - set(declared)
        if unknown:
            labels = ", ".join(sorted(item.value for item in unknown))
            raise ValueError(f"connector does not declare targets: {labels}")
        current = {
            item.kind: item.target_id for item in await self.repository.list_targets(connector_id)
        }
        if current.get(ConnectorTargetKind.knowledge) != requested.get(
            ConnectorTargetKind.knowledge
        ):
            items = await self.repository.list_items(connector_id)
            if any(item.destination_kind is ConnectorTargetKind.knowledge for item in items):
                raise ValueError(
                    "disconnect and purge imported Knowledge before changing its target"
                )
        if self._target_validator is not None:
            for kind, target_id in requested.items():
                if not await self._target_validator(kind, target_id):
                    raise ValueError(
                        f"{kind.value} target {target_id!r} does not exist in this scope"
                    )
        return await self.repository.replace_targets(
            connector_id,
            binding.id,
            requested,
        )

    async def refresh_resources(self, connector_id: str) -> list[dict[str, Any]]:
        provider = self._enabled_provider(connector_id)
        binding = await self._required_active_binding(connector_id)
        if ConnectorCapability.resources in provider.manifest.capabilities:
            context = await self._operation_context(connector_id, binding=binding)
            result = await provider.list_resources(context)
            external_ids = [item.external_id for item in result.resources]
            if len(set(external_ids)) != len(external_ids):
                raise ValueError("connector resource refresh returned duplicate external ids")
            await self.repository.upsert_resources(connector_id, binding.id, result.resources)
            if result.mode is ConnectorResourceRefreshMode.authoritative:
                await self.repository.prune_resources(
                    connector_id,
                    binding.id,
                    set(external_ids),
                )
        return [
            resource_to_dict(item) for item in await self.repository.list_resources(connector_id)
        ]

    async def select_resources(self, connector_id: str, external_ids: set[str]) -> int:
        await self._required_binding(connector_id)
        known = {item.external_id for item in await self.repository.list_resources(connector_id)}
        unknown = external_ids - known
        if unknown:
            raise ValueError(f"unknown connector resources: {', '.join(sorted(unknown))}")
        return await self.repository.select_resources(connector_id, external_ids)

    async def _enqueue_connector_job_once(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        idempotency_key: str,
        max_attempts: int,
    ) -> tuple[JobRecord, bool]:
        """Enqueue a durable connector job, atomically recording a cross-scope dispatch intent.

        When a dispatch outbox is wired (durable Postgres substrate) the job insert and the
        global ``job_dispatch_outbox`` intent commit in ONE transaction, so a committed connector
        sync/renew job always carries a discoverable dispatch pointer that the worker's
        cross-scope reconciler can recover after a lost enqueue — an Agent-scoped connector job is
        never orphaned (finding 1). Without an outbox (in-memory/lite profile) it falls back to
        the plain enqueue.
        """
        assert self.jobs is not None
        if self._dispatch_outbox is not None:
            return await self.jobs.enqueue_once_with_dispatch_intent(
                kind=kind,
                payload=payload,
                target_session_id=None,
                idempotency_key=idempotency_key,
                max_attempts=max_attempts,
                outbox=self._dispatch_outbox,
                cancel_mode=CancelMode.cooperative,
            )
        return await self.jobs.enqueue_once(
            kind=kind,
            payload=payload,
            target_session_id=None,
            idempotency_key=idempotency_key,
            max_attempts=max_attempts,
            cancel_mode=CancelMode.cooperative,
        )

    async def enqueue_sync(
        self, connector_id: str, *, idempotency_key: str | None = None
    ) -> JobRecord:
        manifest = self._manifest(connector_id)
        if ConnectorCapability.sync not in manifest.capabilities:
            raise ValueError(f"connector {connector_id!r} does not support sync")
        binding = await self._required_active_binding(connector_id)
        await self._validate_required_targets(manifest, connector_id)
        self._enabled_provider(connector_id)
        if self.jobs is None:
            raise RuntimeError("durable connector jobs are unavailable")
        key = idempotency_key.strip() if idempotency_key else uuid.uuid4().hex
        job, created = await self._enqueue_connector_job_once(
            kind=CONNECTOR_SYNC_JOB_KIND,
            payload={"connector_id": connector_id, "binding_id": binding.id},
            idempotency_key=f"{connector_id}:{key}",
            max_attempts=CONNECTOR_SYNC_MAX_ATTEMPTS,
        )
        if created and self._dispatch_job is not None:
            await self._dispatch_job(self.repository.scope_id, job.id)
        return job

    async def sync(self, connector_id: str, binding_id: str | None = None) -> int:
        manifest = self._manifest(connector_id)
        binding = await self._required_active_binding(connector_id)
        if binding_id is not None and binding.id != binding_id:
            raise ValueError("connector binding changed before sync execution")
        await self._validate_required_targets(manifest, connector_id)
        provider = self._enabled_provider(connector_id)
        context = await self._operation_context(
            connector_id,
            binding=binding,
            selected_resources=True,
        )
        try:
            result = await provider.sync(context)
            await self._apply_state_update(
                connector_id,
                binding,
                context,
                result.state,
            )
            for change in result.changes:
                self._validate_change(connector_id, binding.id, change)
                await self._change_sink.apply(change)
            resource_ids = {item.id for item in context.resources}
            seen_cursors: set[tuple[str | None, str]] = set()
            for cursor_update in result.state.cursor_updates:
                key = (cursor_update.resource_id, cursor_update.stream)
                if key in seen_cursors:
                    raise ValueError("connector sync returned duplicate cursor updates")
                seen_cursors.add(key)
                if (
                    cursor_update.resource_id is not None
                    and cursor_update.resource_id not in resource_ids
                ):
                    raise ValueError("connector sync returned a cursor for an unselected resource")
                await self.repository.put_cursor(
                    connector_id,
                    binding.id,
                    cursor_update.stream,
                    cursor_update.value,
                    resource_id=cursor_update.resource_id,
                    etag=cursor_update.etag,
                    last_modified=cursor_update.last_modified,
                    revision=cursor_update.revision,
                )
            await self.repository.record_health(
                connector_id,
                binding.id,
                ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC)),
            )
            return len(result.changes)
        except Exception as exc:
            await self.repository.record_health(
                connector_id,
                binding.id,
                ConnectorHealth(
                    ConnectorHealthStatus.error,
                    datetime.now(UTC),
                    f"{type(exc).__name__}: connector sync failed",
                    retryable=True,
                ),
            )
            raise

    async def enqueue_recurring(self, lease: ConnectorScheduleLease) -> JobRecord:
        if self.jobs is None:
            raise RuntimeError("durable connector jobs are unavailable")
        if lease.scope_id != self.repository.scope_id:
            raise ValueError("connector schedule lease crosses its scope")
        manifest = self._manifest(lease.connector_id)
        if lease.operation is ConnectorScheduleOperation.sync:
            if (
                ConnectorCapability.sync not in manifest.capabilities
                or manifest.default_sync_cadence_seconds is None
            ):
                raise ValueError("connector no longer declares recurring sync")
            kind = CONNECTOR_SYNC_JOB_KIND
            max_attempts = CONNECTOR_SYNC_MAX_ATTEMPTS
        else:
            if manifest.renewal is None:
                raise ValueError("connector no longer declares recurring renewal")
            kind = CONNECTOR_RENEW_JOB_KIND
            max_attempts = CONNECTOR_RENEW_MAX_ATTEMPTS
        job, created = await self._enqueue_connector_job_once(
            kind=kind,
            payload={
                "connector_id": lease.connector_id,
                "binding_id": lease.binding_id,
            },
            idempotency_key=(
                f"recurring:{lease.operation.value}:{lease.binding_id}:{lease.due_at.isoformat()}"
            ),
            max_attempts=max_attempts,
        )
        # A recurring job runs in this lease's own scope: dispatch it immediately and (via the
        # dispatch outbox recorded above) let the cross-scope reconciler recover a lost enqueue.
        if created and self._dispatch_job is not None:
            await self._dispatch_job(self.repository.scope_id, job.id)
        return job

    async def reconcile_recurring(
        self,
        now: datetime,
        *,
        limit: int,
        lease_seconds: int,
        retry_base_seconds: int,
        retry_max_seconds: int,
    ) -> int:
        leases = await self.repository.claim_due_schedules(
            now,
            limit=limit,
            lease_seconds=lease_seconds,
        )
        enqueued = 0
        for lease in leases:
            try:
                provider_status = self.registry.status(lease.connector_id)
                if provider_status.available and not provider_status.enabled:
                    await self.repository.suspend_schedule(
                        lease,
                        resume_at=next_schedule_time(
                            lease.due_at,
                            now,
                            lease.cadence_seconds,
                        ),
                    )
                    continue
                if not provider_status.available:
                    raise RuntimeError("connector provider is unavailable")
                await self.enqueue_recurring(lease)
                await self.repository.complete_schedule(
                    lease,
                    next_at=next_schedule_time(
                        lease.due_at,
                        now,
                        lease.cadence_seconds,
                    ),
                )
                enqueued += 1
            except Exception as exc:
                retry_at = now + timedelta(
                    seconds=retry_delay_seconds(
                        lease.attempt,
                        retry_base_seconds,
                        retry_max_seconds,
                    )
                )
                try:
                    await self.repository.fail_schedule(
                        lease,
                        retry_at=retry_at,
                        error_code="connector_schedule_dispatch_failed",
                        error_summary="connector recurring operation dispatch failed",
                    )
                except ConnectorScheduleLeaseLostError:
                    logger.info(
                        "connector schedule lease lost while recording failure "
                        "connector=%s operation=%s",
                        lease.connector_id,
                        lease.operation.value,
                    )
                logger.warning(
                    "connector recurring dispatch failed connector=%s operation=%s "
                    "attempt=%d error_type=%s",
                    lease.connector_id,
                    lease.operation.value,
                    lease.attempt,
                    type(exc).__name__,
                )
        return enqueued

    async def renew(self, connector_id: str, binding_id: str | None = None) -> None:
        manifest = self._manifest(connector_id)
        if manifest.renewal is None:
            raise ValueError(f"connector {connector_id!r} does not support renewal")
        binding = await self._required_active_binding(connector_id)
        if binding_id is not None and binding.id != binding_id:
            raise ValueError("connector binding changed before renewal execution")
        provider = self._enabled_provider(connector_id)
        context = await self._operation_context(connector_id, binding=binding)
        try:
            result: ConnectorRenewalResult = await provider.renew(context)
            await self._apply_state_update(
                connector_id,
                binding,
                context,
                result.state,
                renewal_expires_at=result.renewal_expires_at,
                update_renewal_expiry=result.update_renewal_expiry,
            )
            await self.repository.record_health(
                connector_id,
                binding.id,
                ConnectorHealth(ConnectorHealthStatus.healthy, datetime.now(UTC)),
            )
        except Exception as exc:
            await self.repository.record_health(
                connector_id,
                binding.id,
                ConnectorHealth(
                    ConnectorHealthStatus.error,
                    datetime.now(UTC),
                    f"{type(exc).__name__}: connector renewal failed",
                    retryable=True,
                ),
            )
            raise

    async def health(self, connector_id: str) -> ConnectorHealth:
        provider = self.registry.create(connector_id)
        binding = await self._required_binding(connector_id)
        health = await provider.health(await self._operation_context(connector_id, binding=binding))
        await self.repository.record_health(connector_id, binding.id, health)
        return health

    async def ingress(
        self,
        connector_id: str,
        request: ConnectorIngressRequest,
    ) -> ConnectorIngressOutcome:
        provider = self._enabled_provider(connector_id)
        binding = await self._required_active_binding(connector_id)
        context = await self._operation_context(connector_id, binding=binding)
        result = await provider.ingress(context, request)
        if result.delivery_id is None:
            return ConnectorIngressOutcome(result.response, False, 0)
        assert result.payload_hash is not None
        claim = await self.repository.claim_delivery(
            connector_id,
            binding.id,
            result.delivery_id,
            result.payload_hash,
        )
        if claim is None:
            return ConnectorIngressOutcome(result.response, False, 0)
        if result.failure is not None:
            await self.repository.finish_delivery(claim, failure=result.failure)
            return ConnectorIngressOutcome(result.response, False, 0)
        try:
            for change in result.changes:
                self._validate_change(connector_id, binding.id, change)
                await self._change_sink.apply(change)
        except Exception:
            await self.repository.finish_delivery(
                claim,
                failure=ConnectorIngressFailure(
                    "change_sink_failed",
                    "connector delivery processing failed",
                    retryable=True,
                ),
            )
            raise
        await self.repository.finish_delivery(claim)
        return ConnectorIngressOutcome(result.response, True, len(result.changes))

    async def revoke(
        self,
        connector_id: str,
        *,
        purge: bool = False,
        local_only: bool = False,
    ) -> bool:
        binding = await self.repository.get_binding(connector_id)
        context = await self._operation_context(connector_id, binding=binding)
        if not local_only:
            provider = self.registry.create(connector_id)
            await provider.revoke(context)
        purged = 0
        outbound = 0
        if purge:
            handoff_items = tuple(
                item for item in context.items if item.destination_kind is not None
            )
            if handoff_items and self._purge_sink is None:
                raise RuntimeError("connector Knowledge purge service is unavailable")
            if binding is not None and handoff_items:
                assert self._purge_sink is not None
                purged = await self._purge_sink.handoff_purge(
                    binding,
                    handoff_items,
                )
            if self._purge_outbound is not None:
                outbound = await self._purge_outbound(connector_id)
        deleted = False
        if self.credentials is not None:
            await self.credentials.delete(connector_id)
            deleted = context.credential is not None
        elif self._delete_credential is not None:
            deleted = await self._delete_credential(connector_id)
        removed = await self.repository.delete_connector(connector_id)
        # Remove the webhook routing capability so a delivery for a revoked connector fails closed
        # (binding delete / lifecycle erasure removes the route).
        if self._webhook_route_store is not None:
            await self._webhook_route_store.delete_for_connector(
                self.repository.scope_id, connector_id
            )
        # If this scope has no more bindings with a recurring schedule, drop it from the global
        # schedule index so the cross-scope reconciler stops binding an idle scope (self-healing).
        if self._schedule_index is not None:
            remaining = await self.repository.list_bindings()
            if not any(
                b.next_sync_at is not None or b.next_renewal_at is not None for b in remaining
            ):
                await self._schedule_index.discard(self.repository.scope_id)
        return deleted or removed > 0 or outbound > 0 or purged > 0 or binding is not None

    async def _required_binding(self, connector_id: str) -> ConnectorBinding:
        binding = await self.repository.get_binding(connector_id)
        if binding is None:
            raise LookupError(f"connector {connector_id!r} is not configured")
        return binding

    async def _required_active_binding(self, connector_id: str) -> ConnectorBinding:
        binding = await self._required_binding(connector_id)
        if binding.status not in {
            ConnectorBindingStatus.connected,
            ConnectorBindingStatus.degraded,
            ConnectorBindingStatus.error,
        }:
            raise LookupError(f"connector {connector_id!r} is not connected")
        return binding

    async def _rollback_credential(
        self,
        connector_id: str,
        context: ConnectorOperationContext,
        stored_version: int,
        cause: Exception,
    ) -> None:
        assert self.credentials is not None
        if context.credential is None:
            restored = await self.credentials.delete_if_version(
                connector_id,
                stored_version,
            )
        else:
            restored = (
                await self.credentials.put_if_version(
                    connector_id,
                    context.credential,
                    stored_version,
                )
                is not None
            )
        if not restored:
            raise RuntimeError("connector credential rollback lost its version fence") from cause

    async def _apply_state_update(
        self,
        connector_id: str,
        binding: ConnectorBinding,
        context: ConnectorOperationContext,
        state: ConnectorStateUpdate,
        *,
        renewal_expires_at: datetime | None = None,
        update_renewal_expiry: bool = False,
    ) -> None:
        stored_version: int | None = None
        if state.credential is not None:
            credential_update = state.credential
            if credential_update.expected_version != context.credential_version:
                raise ValueError("connector credential update used a stale expected version")
            if self.credentials is None:
                raise RuntimeError("encrypted connector credential storage is unavailable")
            stored_version = await self.credentials.put_if_version(
                connector_id,
                credential_update.credential,
                credential_update.expected_version,
            )
            if stored_version is None:
                raise RuntimeError("connector credentials changed during operation")
        if (
            state.binding_metadata is None
            and state.binding_status is None
            and not update_renewal_expiry
        ):
            return
        try:
            await self.repository.update_binding_state(
                connector_id,
                binding.id,
                status=state.binding_status,
                metadata=state.binding_metadata,
                renewal_expires_at=renewal_expires_at,
                update_renewal_expiry=update_renewal_expiry,
            )
        except Exception as exc:
            if stored_version is not None:
                await self._rollback_credential(connector_id, context, stored_version, exc)
            raise

    def _manifest(self, connector_id: str) -> ConnectorManifest:
        registration = self.registry.get(connector_id)
        if registration is not None:
            return registration.manifest
        self.registry.create(connector_id)
        raise AssertionError("connector registry create unexpectedly returned")

    def _enabled_provider(self, connector_id: str) -> ConnectorProvider:
        provider_status = self.registry.status(connector_id)
        if not provider_status.available:
            raise RuntimeError(
                provider_status.error or f"connector {connector_id!r} is unavailable"
            )
        if not provider_status.enabled:
            raise RuntimeError(f"connector {connector_id!r} is disabled")
        provider = self.registry.create(connector_id)
        if not provider.enabled():
            raise RuntimeError(f"connector {connector_id!r} is disabled")
        return provider

    async def _credential(self, connector_id: str) -> CredentialEnvelope | None:
        if self.credentials is None:
            return None
        return await self.credentials.get(connector_id)

    async def _operation_context(
        self,
        connector_id: str,
        *,
        binding: ConnectorBinding | None = None,
        callback_base_url: str | None = None,
        selected_resources: bool = False,
    ) -> ConnectorOperationContext:
        current = binding
        if current is None:
            current = await self.repository.get_binding(connector_id)
        credential: CredentialEnvelope | None = None
        credential_version = 0
        if self.credentials is not None:
            stored = await self.credentials.get_versioned(connector_id)
            if stored is not None:
                credential = stored.envelope
                credential_version = stored.version
        if current is None:
            return ConnectorOperationContext(
                scope_id=self.repository.scope_id,
                connector_id=connector_id,
                credential=credential,
                credential_version=credential_version,
                callback_base_url=callback_base_url,
            )
        resources = tuple(
            await self.repository.list_resources(
                connector_id,
                selected_only=selected_resources,
            )
        )
        items = tuple(await self.repository.list_items(connector_id))
        cursors = tuple(await self.repository.list_cursors(connector_id, current.id))
        delivery_health = await self.repository.get_delivery_health(connector_id, current.id)
        if selected_resources:
            selected_ids = {item.id for item in resources}
            items = tuple(
                item
                for item in items
                if item.resource_id is None or item.resource_id in selected_ids
            )
            cursors = tuple(
                item
                for item in cursors
                if item.resource_id is None or item.resource_id in selected_ids
            )
        return ConnectorOperationContext(
            scope_id=self.repository.scope_id,
            connector_id=connector_id,
            binding=current,
            credential=credential,
            credential_version=credential_version,
            callback_base_url=callback_base_url,
            resources=resources,
            targets=tuple(await self.repository.list_targets(connector_id)),
            items=items,
            cursors=cursors,
            delivery_health=delivery_health,
        )

    async def _validate_required_targets(
        self, manifest: ConnectorManifest, connector_id: str
    ) -> None:
        required = {item.kind for item in manifest.target_fields if item.required}
        if not required:
            return
        configured = {
            item.kind: item.target_id for item in await self.repository.list_targets(connector_id)
        }
        missing = required - set(configured)
        if missing:
            labels = ", ".join(sorted(item.value for item in missing))
            raise RuntimeError(f"connector sync requires configured targets: {labels}")
        if self._target_validator is not None:
            for kind in sorted(required, key=lambda item: item.value):
                target_id = configured[kind]
                if not await self._target_validator(kind, target_id):
                    raise RuntimeError(
                        f"connector sync target is unavailable: {kind.value}={target_id}"
                    )

    @staticmethod
    def _validate_change(connector_id: str, binding_id: str, change: ConnectorChange) -> None:
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


def _purge_idempotency_key(binding: ConnectorBinding, item: ConnectorItem) -> str:
    raw = f"{binding.scope_id}|{binding.connector_id}|{binding.id}|{item.id}|{item.destination_id}"
    return f"connector-purge:{hashlib.sha256(raw.encode()).hexdigest()}"


def _binding_health(
    binding: ConnectorBinding | None,
    connected: bool,
) -> ConnectorHealthStatus:
    if binding is None:
        return ConnectorHealthStatus.healthy if connected else ConnectorHealthStatus.unconfigured
    if binding.status in {
        ConnectorBindingStatus.unconfigured,
        ConnectorBindingStatus.configured,
        ConnectorBindingStatus.authorizing,
    }:
        if binding.error_code == ConnectorHealthStatus.degraded.value:
            return ConnectorHealthStatus.degraded
        if binding.error_code == ConnectorHealthStatus.error.value:
            return ConnectorHealthStatus.error
    return {
        ConnectorBindingStatus.unconfigured: ConnectorHealthStatus.unconfigured,
        ConnectorBindingStatus.configured: ConnectorHealthStatus.unconfigured,
        ConnectorBindingStatus.authorizing: ConnectorHealthStatus.degraded,
        ConnectorBindingStatus.connected: ConnectorHealthStatus.healthy,
        ConnectorBindingStatus.degraded: ConnectorHealthStatus.degraded,
        ConnectorBindingStatus.error: ConnectorHealthStatus.error,
        ConnectorBindingStatus.revoked: ConnectorHealthStatus.error,
    }[binding.status]


def _next_action(
    manifest: ConnectorManifest,
    binding: ConnectorBinding | None,
) -> dict[str, str] | None:
    status = ConnectorBindingStatus.unconfigured if binding is None else binding.status
    if status in {
        ConnectorBindingStatus.connected,
        ConnectorBindingStatus.degraded,
    }:
        return None
    if status is ConnectorBindingStatus.error and manifest.auth_action is not None:
        return {
            "kind": "authorize",
            "label": f"Reconnect: {manifest.auth_action.label}",
            "instructions": (
                manifest.auth_action.help_text
                or "Restart browser authorization to restore this connector."
            ),
        }
    if manifest.setup_fields and status is ConnectorBindingStatus.unconfigured:
        return {
            "kind": "setup",
            "label": manifest.setup_action_label,
            "instructions": "Save the required connector configuration to continue.",
        }
    if manifest.auth_action is not None:
        return {
            "kind": "authorize",
            "label": manifest.auth_action.label,
            "instructions": (
                manifest.auth_action.help_text
                or (
                    "Authorization is in progress; complete or restart the browser flow."
                    if status is ConnectorBindingStatus.authorizing
                    else "Continue with browser authorization to connect this provider."
                )
            ),
        }
    if manifest.setup_fields:
        return {
            "kind": "setup",
            "label": manifest.setup_action_label,
            "instructions": "Complete the remaining provider setup steps.",
        }
    return None


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
        "auth_action": (
            None
            if manifest.auth_action is None
            else {
                "label": manifest.auth_action.label,
                "requires_setup": manifest.auth_action.requires_setup,
                "help_text": manifest.auth_action.help_text,
                "callback_parameters": [
                    {"id": item.id, "required": item.required}
                    for item in manifest.auth_action.callback_parameters
                ],
            }
        ),
        "setup_action_label": manifest.setup_action_label,
        "default_sync_cadence_seconds": manifest.default_sync_cadence_seconds,
        "renewal": (
            None
            if manifest.renewal is None
            else {
                "cadence_seconds": manifest.renewal.cadence_seconds,
                "expiry_behavior": manifest.renewal.expiry_behavior.value,
            }
        ),
        "resource_label": manifest.resource_label,
        "target_fields": [
            {
                "kind": item.kind.value,
                "label": item.label,
                "required": item.required,
                "help_text": item.help_text,
            }
            for item in manifest.target_fields
        ],
        "actions": [
            {
                "name": item.name,
                "description": item.description,
                "input_schema": dict(item.input_schema),
                "semantics": item.semantics.value,
                "idempotency": item.idempotency.value,
                "approval": item.approval.value,
            }
            for item in manifest.actions
        ],
    }


def binding_to_dict(
    binding: ConnectorBinding,
    targets: list[ConnectorBindingTarget] | tuple[ConnectorBindingTarget, ...] = (),
) -> dict[str, Any]:
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
        "renewal_expires_at": (
            binding.renewal_expires_at.isoformat() if binding.renewal_expires_at else None
        ),
        "next_sync_at": binding.next_sync_at.isoformat() if binding.next_sync_at else None,
        "next_renewal_at": (
            binding.next_renewal_at.isoformat() if binding.next_renewal_at else None
        ),
        "targets": {item.kind.value: item.target_id for item in targets},
    }


def artifact_to_dict(artifact: ConnectorSetupArtifact) -> dict[str, Any]:
    return {
        "kind": artifact.kind.value,
        "label": artifact.label,
        "value": artifact.value,
        "secret": artifact.kind.value == "secret",
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
    "CONNECTOR_RENEW_JOB_KIND",
    "CONNECTOR_RENEW_MAX_ATTEMPTS",
    "CONNECTOR_SYNC_JOB_KIND",
    "CONNECTOR_SYNC_MAX_ATTEMPTS",
    "CallbackConnectorChangeSink",
    "ConnectorChangeSink",
    "ConnectorKnowledgeService",
    "ConnectorIngressOutcome",
    "ConnectorPurgeSink",
    "ConnectorService",
    "ConnectorSetupOutcome",
    "DurableConnectorChangeSink",
    "artifact_to_dict",
    "binding_to_dict",
    "manifest_to_dict",
    "resource_to_dict",
]
