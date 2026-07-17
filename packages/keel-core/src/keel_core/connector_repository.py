"""Scope-bound connector binding, resource, cursor, and delivery persistence."""

from __future__ import annotations

import copy
import json
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.connector_contracts import (
    ConnectorBinding,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorBindingTarget,
    ConnectorCursor,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorItem,
    ConnectorItemDraft,
    ConnectorRenewalExpiryBehavior,
    ConnectorRenewalPolicy,
    ConnectorResource,
    ConnectorResourceDraft,
    ConnectorScheduleOperation,
    ConnectorTargetKind,
)

_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")
_SECRET_KEYS = frozenset(
    {
        "secret",
        "token",
        "password",
        "client_secret",
        "private_key",
        "api_key",
        "access_token",
        "refresh_token",
    }
)


def _safe_metadata(value: dict[str, Any], *, field: str) -> dict[str, Any]:
    def visit(item: Any) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if key.lower() in _SECRET_KEYS:
                    raise ValueError(f"{field} must not contain plaintext secrets")
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    json.dumps(value, ensure_ascii=False, allow_nan=False)
    return copy.deepcopy(value)


def _binding_from_row(row: Any) -> ConnectorBinding:
    return ConnectorBinding(
        id=str(row.id),
        scope_id=str(row.scope_id),
        connector_id=str(row.connector_id),
        status=ConnectorBindingStatus(str(row.status)),
        display_name=row.display_name,
        external_account_id=row.external_account_id,
        external_tenant_id=row.external_tenant_id,
        metadata=dict(row.metadata or {}),
        last_success_at=row.last_success_at,
        error_code=row.error_code,
        error_summary=row.error_summary,
        created_at=row.created_at,
        updated_at=row.updated_at,
        sync_cadence_seconds=row.sync_cadence_seconds,
        renewal_cadence_seconds=row.renewal_cadence_seconds,
        renewal_expiry_behavior=(
            None
            if row.renewal_expiry_behavior is None
            else ConnectorRenewalExpiryBehavior(str(row.renewal_expiry_behavior))
        ),
        renewal_expires_at=row.renewal_expires_at,
        next_sync_at=row.next_sync_at,
        next_renewal_at=row.next_renewal_at,
        sync_failures=int(row.sync_failures),
        renewal_failures=int(row.renewal_failures),
    )


def _resource_from_row(row: Any) -> ConnectorResource:
    return ConnectorResource(
        id=str(row.id),
        scope_id=str(row.scope_id),
        connector_id=str(row.connector_id),
        binding_id=str(row.binding_id),
        external_id=str(row.external_id),
        kind=str(row.kind),
        display_name=str(row.display_name),
        url=row.url,
        selected=bool(row.selected),
        config=dict(row.config or {}),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _item_from_row(row: Any) -> ConnectorItem:
    return ConnectorItem(
        id=str(row.id),
        scope_id=str(row.scope_id),
        connector_id=str(row.connector_id),
        binding_id=str(row.binding_id),
        resource_id=row.resource_id,
        external_id=str(row.external_id),
        kind=str(row.kind),
        display_name=str(row.display_name),
        url=row.url,
        destination_kind=(
            None if row.destination_kind is None else ConnectorTargetKind(str(row.destination_kind))
        ),
        destination_target_id=row.destination_target_id,
        destination_id=row.destination_id,
        config=dict(row.config or {}),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _target_from_row(row: Any) -> ConnectorBindingTarget:
    return ConnectorBindingTarget(
        id=str(row.id),
        scope_id=str(row.scope_id),
        connector_id=str(row.connector_id),
        binding_id=str(row.binding_id),
        kind=ConnectorTargetKind(str(row.kind)),
        target_id=str(row.target_id),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _cursor_from_row(row: Any) -> ConnectorCursor:
    return ConnectorCursor(
        id=str(row.id),
        scope_id=str(row.scope_id),
        connector_id=str(row.connector_id),
        binding_id=str(row.binding_id),
        resource_id=row.resource_id,
        stream=str(row.stream),
        value=str(row.cursor_value),
        etag=row.etag,
        last_modified=row.last_modified,
        revision=row.revision,
        updated_at=row.updated_at,
    )


@dataclass(frozen=True, slots=True)
class ConnectorScheduleLease:
    scope_id: str
    connector_id: str
    binding_id: str
    operation: ConnectorScheduleOperation
    due_at: datetime
    cadence_seconds: int
    attempt: int
    token: str
    expires_at: datetime


class ConnectorScheduleLeaseLostError(RuntimeError):
    pass


def next_schedule_time(due_at: datetime, now: datetime, cadence_seconds: int) -> datetime:
    next_at = due_at + timedelta(seconds=cadence_seconds)
    if next_at > now:
        return next_at
    elapsed = (now - due_at).total_seconds()
    intervals = int(elapsed // cadence_seconds) + 1
    return due_at + timedelta(seconds=intervals * cadence_seconds)


@runtime_checkable
class ConnectorRepository(Protocol):
    @property
    def scope_id(self) -> str: ...

    async def list_bindings(self) -> list[ConnectorBinding]: ...

    async def get_binding(self, connector_id: str) -> ConnectorBinding | None: ...

    async def upsert_binding(
        self,
        connector_id: str,
        draft: ConnectorBindingDraft,
        status: ConnectorBindingStatus,
        *,
        sync_cadence_seconds: int | None = None,
        renewal: ConnectorRenewalPolicy | None = None,
        renewal_expires_at: datetime | None = None,
        expected_binding_id: str | None = None,
        enforce_binding_fence: bool = False,
    ) -> ConnectorBinding: ...

    async def update_binding_state(
        self,
        connector_id: str,
        binding_id: str,
        *,
        status: ConnectorBindingStatus | None = None,
        metadata: dict[str, Any] | None = None,
        renewal_expires_at: datetime | None = None,
        update_renewal_expiry: bool = False,
    ) -> ConnectorBinding: ...

    async def record_health(
        self, connector_id: str, health: ConnectorHealth
    ) -> ConnectorBinding | None: ...

    async def replace_binding_metadata(
        self, connector_id: str, binding_id: str, metadata: dict[str, Any]
    ) -> ConnectorBinding: ...

    async def delete_connector(self, connector_id: str) -> int: ...

    async def list_targets(self, connector_id: str) -> list[ConnectorBindingTarget]: ...

    async def replace_targets(
        self,
        connector_id: str,
        binding_id: str,
        targets: dict[ConnectorTargetKind, str],
    ) -> list[ConnectorBindingTarget]: ...

    async def list_resources(
        self, connector_id: str, *, selected_only: bool = False
    ) -> list[ConnectorResource]: ...

    async def upsert_resources(
        self, connector_id: str, binding_id: str, resources: Iterable[ConnectorResourceDraft]
    ) -> list[ConnectorResource]: ...

    async def delete_resource(
        self, connector_id: str, binding_id: str, external_id: str
    ) -> bool: ...

    async def prune_resources(
        self, connector_id: str, binding_id: str, keep_external_ids: set[str]
    ) -> int: ...

    async def select_resources(self, connector_id: str, external_ids: set[str]) -> int: ...

    async def list_items(self, connector_id: str) -> list[ConnectorItem]: ...

    async def upsert_items(
        self, connector_id: str, binding_id: str, items: Iterable[ConnectorItemDraft]
    ) -> list[ConnectorItem]: ...

    async def delete_item(self, connector_id: str, binding_id: str, external_id: str) -> bool: ...

    async def prune_items(
        self, connector_id: str, binding_id: str, keep_external_ids: set[str]
    ) -> int: ...

    async def get_cursor(
        self, connector_id: str, binding_id: str, stream: str, resource_id: str | None = None
    ) -> ConnectorCursor | None: ...

    async def list_cursors(self, connector_id: str, binding_id: str) -> list[ConnectorCursor]: ...

    async def put_cursor(
        self,
        connector_id: str,
        binding_id: str,
        stream: str,
        value: str,
        *,
        resource_id: str | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
        revision: str | None = None,
    ) -> ConnectorCursor: ...

    async def delete_cursor(
        self,
        connector_id: str,
        binding_id: str,
        stream: str,
        resource_id: str | None = None,
    ) -> bool: ...

    async def prune_cursors(
        self,
        connector_id: str,
        binding_id: str,
        keep: set[tuple[str | None, str]],
    ) -> int: ...

    async def claim_delivery(
        self,
        connector_id: str,
        binding_id: str,
        delivery_id: str,
        payload_hash: str,
    ) -> bool: ...

    async def finish_delivery(
        self,
        connector_id: str,
        delivery_id: str,
        *,
        event_id: str | None = None,
        error_code: str | None = None,
        error_summary: str | None = None,
    ) -> None: ...

    async def claim_due_schedules(
        self, now: datetime, *, limit: int, lease_seconds: int
    ) -> list[ConnectorScheduleLease]: ...

    async def complete_schedule(
        self, lease: ConnectorScheduleLease, *, next_at: datetime
    ) -> None: ...

    async def fail_schedule(
        self,
        lease: ConnectorScheduleLease,
        *,
        retry_at: datetime,
        error_code: str,
        error_summary: str,
    ) -> None: ...

    async def suspend_schedule(
        self, lease: ConnectorScheduleLease, *, resume_at: datetime
    ) -> None: ...


class InMemoryConnectorRepository:
    def __init__(self, scope_id: str) -> None:
        self._scope_id = scope_id
        self._bindings: dict[str, ConnectorBinding] = {}
        self._targets: dict[tuple[str, ConnectorTargetKind], ConnectorBindingTarget] = {}
        self._resources: dict[tuple[str, str], ConnectorResource] = {}
        self._items: dict[tuple[str, str], ConnectorItem] = {}
        self._cursors: dict[tuple[str, str, str, str], ConnectorCursor] = {}
        self._deliveries: dict[tuple[str, str], tuple[str, str, datetime]] = {}
        self._schedule_leases: dict[str, ConnectorScheduleLease] = {}

    @property
    def scope_id(self) -> str:
        return self._scope_id

    async def list_bindings(self) -> list[ConnectorBinding]:
        return [copy.deepcopy(self._bindings[key]) for key in sorted(self._bindings)]

    async def get_binding(self, connector_id: str) -> ConnectorBinding | None:
        row = self._bindings.get(connector_id)
        return None if row is None else copy.deepcopy(row)

    async def upsert_binding(
        self,
        connector_id: str,
        draft: ConnectorBindingDraft,
        status: ConnectorBindingStatus,
        *,
        sync_cadence_seconds: int | None = None,
        renewal: ConnectorRenewalPolicy | None = None,
        renewal_expires_at: datetime | None = None,
        expected_binding_id: str | None = None,
        enforce_binding_fence: bool = False,
    ) -> ConnectorBinding:
        now = datetime.now(UTC)
        prior = self._bindings.get(connector_id)
        if enforce_binding_fence and (
            None if prior is None else prior.id
        ) != expected_binding_id:
            raise LookupError("connector binding changed before setup commit")
        renewal_cadence = None if renewal is None else renewal.cadence_seconds
        renewal_behavior = None if renewal is None else renewal.expiry_behavior
        active = status in {
            ConnectorBindingStatus.connected,
            ConnectorBindingStatus.degraded,
        }
        next_sync_at = (
            prior.next_sync_at
            if prior is not None and prior.sync_cadence_seconds == sync_cadence_seconds
            else None
        )
        next_renewal_at = (
            prior.next_renewal_at
            if prior is not None and prior.renewal_cadence_seconds == renewal_cadence
            else None
        )
        if active and sync_cadence_seconds is not None and next_sync_at is None:
            next_sync_at = now + timedelta(seconds=sync_cadence_seconds)
        if active and renewal_cadence is not None and next_renewal_at is None:
            next_renewal_at = now + timedelta(seconds=renewal_cadence)
        row = ConnectorBinding(
            id=prior.id if prior else uuid.uuid4().hex,
            scope_id=self._scope_id,
            connector_id=connector_id,
            status=status,
            display_name=draft.display_name,
            external_account_id=draft.external_account_id,
            external_tenant_id=draft.external_tenant_id,
            metadata=_safe_metadata(draft.metadata, field="binding metadata"),
            last_success_at=prior.last_success_at if prior else None,
            created_at=prior.created_at if prior else now,
            updated_at=now,
            sync_cadence_seconds=sync_cadence_seconds,
            renewal_cadence_seconds=renewal_cadence,
            renewal_expiry_behavior=renewal_behavior,
            renewal_expires_at=renewal_expires_at,
            next_sync_at=next_sync_at,
            next_renewal_at=next_renewal_at,
            sync_failures=prior.sync_failures if prior else 0,
            renewal_failures=prior.renewal_failures if prior else 0,
        )
        self._bindings[connector_id] = row
        return copy.deepcopy(row)

    async def update_binding_state(
        self,
        connector_id: str,
        binding_id: str,
        *,
        status: ConnectorBindingStatus | None = None,
        metadata: dict[str, Any] | None = None,
        renewal_expires_at: datetime | None = None,
        update_renewal_expiry: bool = False,
    ) -> ConnectorBinding:
        prior = self._bindings.get(connector_id)
        if prior is None or prior.id != binding_id:
            raise LookupError("connector binding changed before state update")
        now = datetime.now(UTC)
        next_sync_at = prior.next_sync_at
        next_renewal_at = prior.next_renewal_at
        next_status = status or prior.status
        if next_status in {
            ConnectorBindingStatus.connected,
            ConnectorBindingStatus.degraded,
        }:
            if prior.sync_cadence_seconds is not None and next_sync_at is None:
                next_sync_at = now + timedelta(seconds=prior.sync_cadence_seconds)
            if prior.renewal_cadence_seconds is not None and next_renewal_at is None:
                next_renewal_at = now + timedelta(seconds=prior.renewal_cadence_seconds)
        row = replace(
            prior,
            status=next_status,
            metadata=(
                prior.metadata
                if metadata is None
                else _safe_metadata(metadata, field="binding metadata")
            ),
            renewal_expires_at=(
                renewal_expires_at if update_renewal_expiry else prior.renewal_expires_at
            ),
            next_sync_at=next_sync_at,
            next_renewal_at=next_renewal_at,
            error_code=(
                None
                if next_status in {
                    ConnectorBindingStatus.connected,
                    ConnectorBindingStatus.configured,
                    ConnectorBindingStatus.authorizing,
                }
                else prior.error_code
            ),
            error_summary=(
                None
                if next_status in {
                    ConnectorBindingStatus.connected,
                    ConnectorBindingStatus.configured,
                    ConnectorBindingStatus.authorizing,
                }
                else prior.error_summary
            ),
            updated_at=now,
        )
        self._bindings[connector_id] = row
        return copy.deepcopy(row)

    async def record_health(
        self, connector_id: str, health: ConnectorHealth
    ) -> ConnectorBinding | None:
        prior = self._bindings.get(connector_id)
        if prior is None:
            return None
        healthy = health.status is ConnectorHealthStatus.healthy
        mapped_status = {
            ConnectorHealthStatus.healthy: ConnectorBindingStatus.connected,
            ConnectorHealthStatus.degraded: ConnectorBindingStatus.degraded,
            ConnectorHealthStatus.error: ConnectorBindingStatus.error,
            ConnectorHealthStatus.unconfigured: ConnectorBindingStatus.configured,
        }[health.status]
        staged = {
            ConnectorBindingStatus.unconfigured,
            ConnectorBindingStatus.configured,
            ConnectorBindingStatus.authorizing,
        }
        next_status: ConnectorBindingStatus
        if prior.status is ConnectorBindingStatus.revoked:
            next_status = prior.status
        elif prior.status in staged and health.status in {
            ConnectorHealthStatus.healthy,
            ConnectorHealthStatus.degraded,
            ConnectorHealthStatus.unconfigured,
        }:
            next_status = prior.status
        else:
            next_status = mapped_status
        row = replace(
            prior,
            status=next_status,
            last_success_at=(
                health.checked_at
                if healthy and prior.status is not ConnectorBindingStatus.revoked
                else prior.last_success_at
            ),
            error_code=(
                prior.error_code
                if prior.status is ConnectorBindingStatus.revoked
                else (None if healthy else health.status.value)
            ),
            error_summary=(
                prior.error_summary
                if prior.status is ConnectorBindingStatus.revoked
                else (None if healthy else health.message)
            ),
            updated_at=health.checked_at,
        )
        self._bindings[connector_id] = row
        return copy.deepcopy(row)

    async def replace_binding_metadata(
        self, connector_id: str, binding_id: str, metadata: dict[str, Any]
    ) -> ConnectorBinding:
        prior = self._bindings.get(connector_id)
        if prior is None or prior.id != binding_id:
            raise LookupError("connector binding changed before state update")
        row = replace(
            prior,
            metadata=_safe_metadata(metadata, field="binding metadata"),
            updated_at=datetime.now(UTC),
        )
        self._bindings[connector_id] = row
        return copy.deepcopy(row)

    async def delete_connector(self, connector_id: str) -> int:
        removed = int(self._bindings.pop(connector_id, None) is not None)
        for resource_key in [key for key in self._resources if key[0] == connector_id]:
            del self._resources[resource_key]
            removed += 1
        for target_key in [key for key in self._targets if key[0] == connector_id]:
            del self._targets[target_key]
            removed += 1
        for item_key in [key for key in self._items if key[0] == connector_id]:
            del self._items[item_key]
            removed += 1
        for cursor_key in [key for key in self._cursors if key[0] == connector_id]:
            del self._cursors[cursor_key]
            removed += 1
        for delivery_key in [key for key in self._deliveries if key[0] == connector_id]:
            del self._deliveries[delivery_key]
            removed += 1
        self._schedule_leases.pop(connector_id, None)
        return removed

    async def list_targets(self, connector_id: str) -> list[ConnectorBindingTarget]:
        rows = [row for (cid, _), row in self._targets.items() if cid == connector_id]
        return [copy.deepcopy(row) for row in sorted(rows, key=lambda item: item.kind.value)]

    async def replace_targets(
        self,
        connector_id: str,
        binding_id: str,
        targets: dict[ConnectorTargetKind, str],
    ) -> list[ConnectorBindingTarget]:
        now = datetime.now(UTC)
        for key in [key for key in self._targets if key[0] == connector_id]:
            if key[1] not in targets:
                del self._targets[key]
        for kind, target_id in targets.items():
            key = (connector_id, kind)
            prior = self._targets.get(key)
            self._targets[key] = ConnectorBindingTarget(
                id=prior.id if prior else uuid.uuid4().hex,
                scope_id=self._scope_id,
                connector_id=connector_id,
                binding_id=binding_id,
                kind=kind,
                target_id=target_id,
                created_at=prior.created_at if prior else now,
                updated_at=now,
            )
        return await self.list_targets(connector_id)

    async def list_resources(
        self, connector_id: str, *, selected_only: bool = False
    ) -> list[ConnectorResource]:
        rows = [
            row
            for (cid, _), row in self._resources.items()
            if cid == connector_id and (row.selected or not selected_only)
        ]
        return [copy.deepcopy(row) for row in sorted(rows, key=lambda item: item.external_id)]

    async def upsert_resources(
        self, connector_id: str, binding_id: str, resources: Iterable[ConnectorResourceDraft]
    ) -> list[ConnectorResource]:
        now = datetime.now(UTC)
        for draft in resources:
            key = (connector_id, draft.external_id)
            prior = self._resources.get(key)
            self._resources[key] = ConnectorResource(
                id=prior.id if prior else uuid.uuid4().hex,
                scope_id=self._scope_id,
                connector_id=connector_id,
                binding_id=binding_id,
                external_id=draft.external_id,
                kind=draft.kind,
                display_name=draft.display_name,
                url=draft.url,
                selected=draft.selected if prior is None else prior.selected,
                config=_safe_metadata(draft.config, field="resource config"),
                created_at=prior.created_at if prior else now,
                updated_at=now,
            )
        return await self.list_resources(connector_id)

    async def delete_resource(self, connector_id: str, binding_id: str, external_id: str) -> bool:
        key = (connector_id, external_id)
        resource = self._resources.get(key)
        if resource is None or resource.binding_id != binding_id:
            return False
        del self._resources[key]
        for item_key, item in list(self._items.items()):
            if item.resource_id == resource.id:
                del self._items[item_key]
        for cursor_key, cursor in list(self._cursors.items()):
            if cursor.resource_id == resource.id:
                del self._cursors[cursor_key]
        return True

    async def prune_resources(
        self, connector_id: str, binding_id: str, keep_external_ids: set[str]
    ) -> int:
        removed = 0
        for external_id in [
            external_id
            for (cid, external_id), row in self._resources.items()
            if cid == connector_id
            and row.binding_id == binding_id
            and external_id not in keep_external_ids
        ]:
            removed += int(await self.delete_resource(connector_id, binding_id, external_id))
        return removed

    async def select_resources(self, connector_id: str, external_ids: set[str]) -> int:
        changed = 0
        for key, row in list(self._resources.items()):
            if key[0] != connector_id:
                continue
            selected = row.external_id in external_ids
            if row.selected != selected:
                self._resources[key] = replace(
                    row,
                    selected=selected,
                    updated_at=datetime.now(UTC),
                )
                changed += 1
        return changed

    async def list_items(self, connector_id: str) -> list[ConnectorItem]:
        rows = [row for (cid, _), row in self._items.items() if cid == connector_id]
        return [copy.deepcopy(row) for row in sorted(rows, key=lambda item: item.external_id)]

    async def upsert_items(
        self, connector_id: str, binding_id: str, items: Iterable[ConnectorItemDraft]
    ) -> list[ConnectorItem]:
        now = datetime.now(UTC)
        for draft in items:
            key = (connector_id, draft.external_id)
            prior = self._items.get(key)
            self._items[key] = ConnectorItem(
                id=prior.id if prior else uuid.uuid4().hex,
                scope_id=self._scope_id,
                connector_id=connector_id,
                binding_id=binding_id,
                external_id=draft.external_id,
                kind=draft.kind,
                display_name=draft.display_name,
                url=draft.url,
                resource_id=draft.resource_id,
                destination_kind=draft.destination_kind,
                destination_target_id=draft.destination_target_id,
                destination_id=draft.destination_id,
                config=_safe_metadata(draft.config, field="item config"),
                created_at=prior.created_at if prior else now,
                updated_at=now,
            )
        return await self.list_items(connector_id)

    async def delete_item(self, connector_id: str, binding_id: str, external_id: str) -> bool:
        key = (connector_id, external_id)
        item = self._items.get(key)
        if item is None or item.binding_id != binding_id:
            return False
        del self._items[key]
        return True

    async def prune_items(
        self, connector_id: str, binding_id: str, keep_external_ids: set[str]
    ) -> int:
        removed = 0
        for key, item in list(self._items.items()):
            if (
                key[0] == connector_id
                and item.binding_id == binding_id
                and item.external_id not in keep_external_ids
            ):
                del self._items[key]
                removed += 1
        return removed

    async def get_cursor(
        self, connector_id: str, binding_id: str, stream: str, resource_id: str | None = None
    ) -> ConnectorCursor | None:
        row = self._cursors.get((connector_id, binding_id, resource_id or "", stream))
        return None if row is None else copy.deepcopy(row)

    async def list_cursors(self, connector_id: str, binding_id: str) -> list[ConnectorCursor]:
        rows = [
            row
            for (cid, bid, _, _), row in self._cursors.items()
            if cid == connector_id and bid == binding_id
        ]
        return [
            copy.deepcopy(row)
            for row in sorted(rows, key=lambda item: (item.resource_id or "", item.stream))
        ]

    async def put_cursor(
        self,
        connector_id: str,
        binding_id: str,
        stream: str,
        value: str,
        *,
        resource_id: str | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
        revision: str | None = None,
    ) -> ConnectorCursor:
        key = (connector_id, binding_id, resource_id or "", stream)
        prior = self._cursors.get(key)
        row = ConnectorCursor(
            id=prior.id if prior else uuid.uuid4().hex,
            scope_id=self._scope_id,
            connector_id=connector_id,
            binding_id=binding_id,
            resource_id=resource_id,
            stream=stream,
            value=value,
            etag=etag,
            last_modified=last_modified,
            revision=revision,
            updated_at=datetime.now(UTC),
        )
        self._cursors[key] = row
        return copy.deepcopy(row)

    async def delete_cursor(
        self,
        connector_id: str,
        binding_id: str,
        stream: str,
        resource_id: str | None = None,
    ) -> bool:
        key = (connector_id, binding_id, resource_id or "", stream)
        return self._cursors.pop(key, None) is not None

    async def prune_cursors(
        self,
        connector_id: str,
        binding_id: str,
        keep: set[tuple[str | None, str]],
    ) -> int:
        removed = 0
        for key, cursor in list(self._cursors.items()):
            if (
                key[0] == connector_id
                and key[1] == binding_id
                and (cursor.resource_id, cursor.stream) not in keep
            ):
                del self._cursors[key]
                removed += 1
        return removed

    async def claim_delivery(
        self,
        connector_id: str,
        binding_id: str,
        delivery_id: str,
        payload_hash: str,
    ) -> bool:
        key = (connector_id, delivery_id)
        if key in self._deliveries:
            existing_hash, delivery_status, updated_at = self._deliveries[key]
            if existing_hash != payload_hash:
                raise ValueError("connector delivery id was reused with a different payload")
            stale = (datetime.now(UTC) - updated_at).total_seconds() >= 300
            if delivery_status == "failed" or (
                delivery_status in {"received", "processing"} and stale
            ):
                self._deliveries[key] = (payload_hash, "processing", datetime.now(UTC))
                return True
            return False
        self._deliveries[key] = (payload_hash, "processing", datetime.now(UTC))
        return True

    async def finish_delivery(
        self,
        connector_id: str,
        delivery_id: str,
        *,
        event_id: str | None = None,
        error_code: str | None = None,
        error_summary: str | None = None,
    ) -> None:
        if (connector_id, delivery_id) not in self._deliveries:
            raise LookupError("connector delivery was not claimed")
        payload_hash, _, _ = self._deliveries[(connector_id, delivery_id)]
        self._deliveries[(connector_id, delivery_id)] = (
            payload_hash,
            "failed" if error_code is not None else "processed",
            datetime.now(UTC),
        )

    async def claim_due_schedules(
        self, now: datetime, *, limit: int, lease_seconds: int
    ) -> list[ConnectorScheduleLease]:
        if limit <= 0 or lease_seconds <= 0:
            raise ValueError("connector schedule bounds must be positive")
        leases: list[ConnectorScheduleLease] = []
        for connector_id in sorted(self._bindings):
            if len(leases) >= limit:
                break
            row = self._bindings[connector_id]
            existing = self._schedule_leases.get(connector_id)
            if existing is not None:
                if existing.expires_at > now:
                    continue
                self._schedule_leases.pop(connector_id, None)
            if (
                row.renewal_expires_at is not None
                and row.renewal_expires_at <= now
                and row.renewal_expiry_behavior is not None
            ):
                expired_status = {
                    ConnectorRenewalExpiryBehavior.degraded: ConnectorBindingStatus.degraded,
                    ConnectorRenewalExpiryBehavior.error: ConnectorBindingStatus.error,
                    ConnectorRenewalExpiryBehavior.revoked: ConnectorBindingStatus.revoked,
                }[row.renewal_expiry_behavior]
                row = replace(
                    row,
                    status=expired_status,
                    error_code="connector_renewal_expired",
                    error_summary="connector subscription renewal expired",
                    updated_at=now,
                )
                self._bindings[connector_id] = row
            if row.status not in {
                ConnectorBindingStatus.connected,
                ConnectorBindingStatus.degraded,
            }:
                continue
            due: list[tuple[datetime, ConnectorScheduleOperation, int, int]] = []
            if row.next_sync_at is not None and row.next_sync_at <= now:
                if row.sync_cadence_seconds is not None:
                    due.append(
                        (
                            row.next_sync_at,
                            ConnectorScheduleOperation.sync,
                            row.sync_cadence_seconds,
                            row.sync_failures + 1,
                        )
                    )
            if row.next_renewal_at is not None and row.next_renewal_at <= now:
                if row.renewal_cadence_seconds is not None:
                    due.append(
                        (
                            row.next_renewal_at,
                            ConnectorScheduleOperation.renewal,
                            row.renewal_cadence_seconds,
                            row.renewal_failures + 1,
                        )
                    )
            if not due:
                continue
            due_at, operation, cadence, attempt = min(
                due,
                key=lambda item: (
                    item[0],
                    0 if item[1] is ConnectorScheduleOperation.renewal else 1,
                ),
            )
            lease = ConnectorScheduleLease(
                self._scope_id,
                connector_id,
                row.id,
                operation,
                due_at,
                cadence,
                attempt,
                uuid.uuid4().hex,
                now + timedelta(seconds=lease_seconds),
            )
            self._schedule_leases[connector_id] = lease
            leases.append(lease)
        return leases

    def _require_schedule_lease(self, lease: ConnectorScheduleLease) -> ConnectorBinding:
        current = self._schedule_leases.get(lease.connector_id)
        row = self._bindings.get(lease.connector_id)
        if (
            current != lease
            or row is None
            or row.id != lease.binding_id
            or lease.scope_id != self._scope_id
            or lease.expires_at <= datetime.now(UTC)
            or row.status
            not in {
                ConnectorBindingStatus.connected,
                ConnectorBindingStatus.degraded,
            }
        ):
            raise ConnectorScheduleLeaseLostError("connector schedule lease was lost")
        return row

    async def complete_schedule(
        self, lease: ConnectorScheduleLease, *, next_at: datetime
    ) -> None:
        row = self._require_schedule_lease(lease)
        updates: dict[str, Any] = {"updated_at": datetime.now(UTC)}
        if lease.operation is ConnectorScheduleOperation.sync:
            updates.update(next_sync_at=next_at, sync_failures=0)
        else:
            updates.update(next_renewal_at=next_at, renewal_failures=0)
        self._bindings[lease.connector_id] = replace(row, **updates)
        del self._schedule_leases[lease.connector_id]

    async def fail_schedule(
        self,
        lease: ConnectorScheduleLease,
        *,
        retry_at: datetime,
        error_code: str,
        error_summary: str,
    ) -> None:
        row = self._require_schedule_lease(lease)
        updates: dict[str, Any] = {
            "status": ConnectorBindingStatus.degraded,
            "error_code": error_code,
            "error_summary": error_summary,
            "updated_at": datetime.now(UTC),
        }
        if lease.operation is ConnectorScheduleOperation.sync:
            updates.update(next_sync_at=retry_at, sync_failures=row.sync_failures + 1)
        else:
            updates.update(
                next_renewal_at=retry_at,
                renewal_failures=row.renewal_failures + 1,
            )
        self._bindings[lease.connector_id] = replace(row, **updates)
        del self._schedule_leases[lease.connector_id]

    async def suspend_schedule(
        self, lease: ConnectorScheduleLease, *, resume_at: datetime
    ) -> None:
        row = self._require_schedule_lease(lease)
        updates: dict[str, Any] = {"updated_at": datetime.now(UTC)}
        if lease.operation is ConnectorScheduleOperation.sync:
            updates["next_sync_at"] = resume_at
        else:
            updates["next_renewal_at"] = resume_at
        self._bindings[lease.connector_id] = replace(row, **updates)
        del self._schedule_leases[lease.connector_id]


class PostgresConnectorRepository:
    def __init__(self, engine: AsyncEngine, scope_id: str) -> None:
        self._engine = engine
        self._scope_id = scope_id

    @property
    def scope_id(self) -> str:
        return self._scope_id

    async def list_bindings(self) -> list[ConnectorBinding]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text(
                        "SELECT * FROM connector_bindings WHERE scope_id = :scope "
                        "ORDER BY connector_id"
                    ),
                    {"scope": self._scope_id},
                )
            ).all()
        return [_binding_from_row(row) for row in rows]

    async def get_binding(self, connector_id: str) -> ConnectorBinding | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "SELECT * FROM connector_bindings "
                        "WHERE scope_id = :scope AND connector_id = :cid"
                    ),
                    {"scope": self._scope_id, "cid": connector_id},
                )
            ).one_or_none()
        return None if row is None else _binding_from_row(row)

    async def replace_binding_metadata(
        self, connector_id: str, binding_id: str, metadata: dict[str, Any]
    ) -> ConnectorBinding:
        safe = _safe_metadata(metadata, field="binding metadata")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "UPDATE connector_bindings SET metadata = CAST(:metadata AS jsonb), "
                        "updated_at = now() WHERE scope_id = :scope AND connector_id = :cid "
                        "AND id = :binding RETURNING *"
                    ),
                    {
                        "metadata": json.dumps(safe, ensure_ascii=False),
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "binding": binding_id,
                    },
                )
            ).one_or_none()
        if row is None:
            raise LookupError("connector binding changed before state update")
        return _binding_from_row(row)

    async def update_binding_state(
        self,
        connector_id: str,
        binding_id: str,
        *,
        status: ConnectorBindingStatus | None = None,
        metadata: dict[str, Any] | None = None,
        renewal_expires_at: datetime | None = None,
        update_renewal_expiry: bool = False,
    ) -> ConnectorBinding:
        safe_metadata = (
            None if metadata is None else _safe_metadata(metadata, field="binding metadata")
        )
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "UPDATE connector_bindings SET "
                        "status = COALESCE(:status, status), "
                        "metadata = CASE WHEN :update_metadata "
                        "THEN CAST(:metadata AS jsonb) ELSE metadata END, "
                        "renewal_expires_at = CASE WHEN :update_expiry "
                        "THEN :expires ELSE renewal_expires_at END, "
                        "next_sync_at = CASE WHEN COALESCE(:status, status) "
                        "IN ('connected', 'degraded') AND next_sync_at IS NULL "
                        "AND sync_cadence_seconds IS NOT NULL "
                        "THEN now() + make_interval(secs => sync_cadence_seconds) "
                        "ELSE next_sync_at END, "
                        "next_renewal_at = CASE WHEN COALESCE(:status, status) "
                        "IN ('connected', 'degraded') AND next_renewal_at IS NULL "
                        "AND renewal_cadence_seconds IS NOT NULL "
                        "THEN now() + make_interval(secs => renewal_cadence_seconds) "
                        "ELSE next_renewal_at END, "
                        "error_code = CASE WHEN COALESCE(:status, status) "
                        "IN ('connected', 'configured', 'authorizing') THEN NULL "
                        "ELSE error_code END, "
                        "error_summary = CASE WHEN COALESCE(:status, status) "
                        "IN ('connected', 'configured', 'authorizing') THEN NULL "
                        "ELSE error_summary END, updated_at = now() "
                        "WHERE scope_id = :scope AND connector_id = :cid AND id = :binding "
                        "RETURNING *"
                    ),
                    {
                        "status": None if status is None else status.value,
                        "update_metadata": metadata is not None,
                        "metadata": json.dumps(safe_metadata or {}, ensure_ascii=False),
                        "update_expiry": update_renewal_expiry,
                        "expires": renewal_expires_at,
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "binding": binding_id,
                    },
                )
            ).one_or_none()
        if row is None:
            raise LookupError("connector binding changed before state update")
        return _binding_from_row(row)

    async def upsert_binding(
        self,
        connector_id: str,
        draft: ConnectorBindingDraft,
        status: ConnectorBindingStatus,
        *,
        sync_cadence_seconds: int | None = None,
        renewal: ConnectorRenewalPolicy | None = None,
        renewal_expires_at: datetime | None = None,
        expected_binding_id: str | None = None,
        enforce_binding_fence: bool = False,
    ) -> ConnectorBinding:
        metadata = _safe_metadata(draft.metadata, field="binding metadata")
        now = datetime.now(UTC)
        renewal_cadence = None if renewal is None else renewal.cadence_seconds
        renewal_behavior = None if renewal is None else renewal.expiry_behavior.value
        active = status in {
            ConnectorBindingStatus.connected,
            ConnectorBindingStatus.degraded,
        }
        next_sync_at = (
            now + timedelta(seconds=sync_cadence_seconds)
            if active and sync_cadence_seconds is not None
            else None
        )
        next_renewal_at = (
            now + timedelta(seconds=renewal_cadence)
            if active and renewal_cadence is not None
            else None
        )
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            if enforce_binding_fence:
                await conn.execute(
                    text(
                        "SELECT pg_advisory_xact_lock("
                        "hashtextextended(:binding_key, 0))"
                    ),
                    {"binding_key": f"{self._scope_id}:{connector_id}"},
                )
                current_id = await conn.scalar(
                    text(
                        "SELECT id FROM connector_bindings "
                        "WHERE scope_id = :scope AND connector_id = :cid FOR UPDATE"
                    ),
                    {"scope": self._scope_id, "cid": connector_id},
                )
                if (
                    None if current_id is None else str(current_id)
                ) != expected_binding_id:
                    raise LookupError("connector binding changed before setup commit")
            row = (
                await conn.execute(
                    text(
                        "INSERT INTO connector_bindings "
                        "(id, scope_id, connector_id, status, display_name, "
                        "external_account_id, external_tenant_id, metadata, "
                        "sync_cadence_seconds, renewal_cadence_seconds, "
                        "renewal_expiry_behavior, renewal_expires_at, "
                        "next_sync_at, next_renewal_at) "
                        "VALUES (:id, :scope, :cid, :status, :name, :account, :tenant, "
                        "CAST(:metadata AS jsonb), :sync_cadence, :renewal_cadence, "
                        ":renewal_behavior, :renewal_expires, :next_sync, :next_renewal) "
                        "ON CONFLICT (scope_id, connector_id) DO UPDATE SET "
                        "status = EXCLUDED.status, display_name = EXCLUDED.display_name, "
                        "external_account_id = EXCLUDED.external_account_id, "
                        "external_tenant_id = EXCLUDED.external_tenant_id, "
                        "metadata = EXCLUDED.metadata, "
                        "sync_cadence_seconds = EXCLUDED.sync_cadence_seconds, "
                        "renewal_cadence_seconds = EXCLUDED.renewal_cadence_seconds, "
                        "renewal_expiry_behavior = EXCLUDED.renewal_expiry_behavior, "
                        "renewal_expires_at = EXCLUDED.renewal_expires_at, "
                        "next_sync_at = CASE "
                        "WHEN EXCLUDED.sync_cadence_seconds IS NULL THEN NULL "
                        "WHEN connector_bindings.sync_cadence_seconds "
                        "IS DISTINCT FROM EXCLUDED.sync_cadence_seconds "
                        "THEN EXCLUDED.next_sync_at "
                        "WHEN connector_bindings.next_sync_at IS NULL "
                        "AND EXCLUDED.status IN ('connected', 'degraded') "
                        "THEN EXCLUDED.next_sync_at ELSE connector_bindings.next_sync_at END, "
                        "next_renewal_at = CASE "
                        "WHEN EXCLUDED.renewal_cadence_seconds IS NULL THEN NULL "
                        "WHEN connector_bindings.renewal_cadence_seconds "
                        "IS DISTINCT FROM EXCLUDED.renewal_cadence_seconds "
                        "THEN EXCLUDED.next_renewal_at "
                        "WHEN connector_bindings.next_renewal_at IS NULL "
                        "AND EXCLUDED.status IN ('connected', 'degraded') "
                        "THEN EXCLUDED.next_renewal_at "
                        "ELSE connector_bindings.next_renewal_at END, "
                        "error_code = NULL, error_summary = NULL, "
                        "updated_at = now() RETURNING *"
                    ),
                    {
                        "id": uuid.uuid4().hex,
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "status": status.value,
                        "name": draft.display_name,
                        "account": draft.external_account_id,
                        "tenant": draft.external_tenant_id,
                        "metadata": json.dumps(metadata, ensure_ascii=False),
                        "sync_cadence": sync_cadence_seconds,
                        "renewal_cadence": renewal_cadence,
                        "renewal_behavior": renewal_behavior,
                        "renewal_expires": renewal_expires_at,
                        "next_sync": next_sync_at,
                        "next_renewal": next_renewal_at,
                    },
                )
            ).one()
        return _binding_from_row(row)

    async def record_health(
        self, connector_id: str, health: ConnectorHealth
    ) -> ConnectorBinding | None:
        healthy = health.status is ConnectorHealthStatus.healthy
        binding_status = {
            ConnectorHealthStatus.healthy: ConnectorBindingStatus.connected,
            ConnectorHealthStatus.degraded: ConnectorBindingStatus.degraded,
            ConnectorHealthStatus.error: ConnectorBindingStatus.error,
            ConnectorHealthStatus.unconfigured: ConnectorBindingStatus.configured,
        }[health.status]
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "UPDATE connector_bindings SET status = CASE "
                        "WHEN status = 'revoked' THEN status "
                        "WHEN status IN ('unconfigured', 'configured', 'authorizing') "
                        "AND (:healthy OR :degraded OR :unconfigured) THEN status "
                        "ELSE :status END, "
                        "last_success_at = CASE WHEN :healthy "
                        "AND status <> 'revoked' THEN :checked "
                        "ELSE last_success_at END, "
                        "error_code = CASE WHEN status = 'revoked' THEN error_code "
                        "ELSE :code END, error_summary = CASE WHEN status = 'revoked' "
                        "THEN error_summary ELSE :summary END, updated_at = :checked "
                        "WHERE scope_id = :scope AND connector_id = :cid RETURNING *"
                    ),
                    {
                        "status": binding_status.value,
                        "healthy": healthy,
                        "degraded": health.status is ConnectorHealthStatus.degraded,
                        "unconfigured": health.status is ConnectorHealthStatus.unconfigured,
                        "checked": health.checked_at,
                        "code": None if healthy else health.status.value,
                        "summary": None if healthy else health.message,
                        "scope": self._scope_id,
                        "cid": connector_id,
                    },
                )
            ).one_or_none()
        return None if row is None else _binding_from_row(row)

    async def delete_connector(self, connector_id: str) -> int:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "SELECT pg_advisory_xact_lock("
                    "hashtextextended(:binding_key, 0))"
                ),
                {"binding_key": f"{self._scope_id}:{connector_id}"},
            )
            result = await conn.execute(
                text(
                    "DELETE FROM connector_bindings WHERE scope_id = :scope AND connector_id = :cid"
                ),
                {"scope": self._scope_id, "cid": connector_id},
            )
        return int(result.rowcount or 0)

    async def list_targets(self, connector_id: str) -> list[ConnectorBindingTarget]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text(
                        "SELECT * FROM connector_binding_targets "
                        "WHERE scope_id = :scope AND connector_id = :cid ORDER BY kind"
                    ),
                    {"scope": self._scope_id, "cid": connector_id},
                )
            ).all()
        return [_target_from_row(row) for row in rows]

    async def replace_targets(
        self,
        connector_id: str,
        binding_id: str,
        targets: dict[ConnectorTargetKind, str],
    ) -> list[ConnectorBindingTarget]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "DELETE FROM connector_binding_targets "
                    "WHERE scope_id = :scope AND connector_id = :cid"
                ),
                {"scope": self._scope_id, "cid": connector_id},
            )
            for kind, target_id in targets.items():
                await conn.execute(
                    text(
                        "INSERT INTO connector_binding_targets "
                        "(id, scope_id, connector_id, binding_id, kind, target_id) "
                        "VALUES (:id, :scope, :cid, :binding, :kind, :target) "
                        "ON CONFLICT (scope_id, binding_id, kind) DO UPDATE SET "
                        "target_id = EXCLUDED.target_id, updated_at = now()"
                    ),
                    {
                        "id": uuid.uuid4().hex,
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "binding": binding_id,
                        "kind": kind.value,
                        "target": target_id,
                    },
                )
        return await self.list_targets(connector_id)

    async def list_resources(
        self, connector_id: str, *, selected_only: bool = False
    ) -> list[ConnectorResource]:
        selected = " AND selected" if selected_only else ""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text(
                        "SELECT * FROM connector_resources "
                        f"WHERE scope_id = :scope AND connector_id = :cid{selected} "
                        "ORDER BY external_id"
                    ),
                    {"scope": self._scope_id, "cid": connector_id},
                )
            ).all()
        return [_resource_from_row(row) for row in rows]

    async def upsert_resources(
        self, connector_id: str, binding_id: str, resources: Iterable[ConnectorResourceDraft]
    ) -> list[ConnectorResource]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            for draft in resources:
                config = _safe_metadata(draft.config, field="resource config")
                await conn.execute(
                    text(
                        "INSERT INTO connector_resources "
                        "(id, scope_id, connector_id, binding_id, external_id, kind, "
                        "display_name, url, selected, config) "
                        "VALUES (:id, :scope, :cid, :binding, :external, :kind, :name, :url, "
                        ":selected, CAST(:config AS jsonb)) "
                        "ON CONFLICT (scope_id, binding_id, external_id) DO UPDATE SET "
                        "kind = EXCLUDED.kind, display_name = EXCLUDED.display_name, "
                        "url = EXCLUDED.url, config = EXCLUDED.config, updated_at = now()"
                    ),
                    {
                        "id": uuid.uuid4().hex,
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "binding": binding_id,
                        "external": draft.external_id,
                        "kind": draft.kind,
                        "name": draft.display_name,
                        "url": draft.url,
                        "selected": draft.selected,
                        "config": json.dumps(config, ensure_ascii=False),
                    },
                )
        return await self.list_resources(connector_id)

    async def delete_resource(self, connector_id: str, binding_id: str, external_id: str) -> bool:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "DELETE FROM connector_resources WHERE scope_id = :scope "
                    "AND connector_id = :cid AND binding_id = :binding "
                    "AND external_id = :external"
                ),
                {
                    "scope": self._scope_id,
                    "cid": connector_id,
                    "binding": binding_id,
                    "external": external_id,
                },
            )
        return bool(result.rowcount)

    async def prune_resources(
        self, connector_id: str, binding_id: str, keep_external_ids: set[str]
    ) -> int:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "DELETE FROM connector_resources WHERE scope_id = :scope "
                    "AND connector_id = :cid AND binding_id = :binding "
                    "AND NOT (external_id = ANY(:keep))"
                ),
                {
                    "scope": self._scope_id,
                    "cid": connector_id,
                    "binding": binding_id,
                    "keep": sorted(keep_external_ids),
                },
            )
        return int(result.rowcount or 0)

    async def select_resources(self, connector_id: str, external_ids: set[str]) -> int:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE connector_resources SET selected = (external_id = ANY(:ids)), "
                    "updated_at = now() WHERE scope_id = :scope AND connector_id = :cid "
                    "AND selected IS DISTINCT FROM (external_id = ANY(:ids))"
                ),
                {"ids": sorted(external_ids), "scope": self._scope_id, "cid": connector_id},
            )
        return int(result.rowcount or 0)

    async def list_items(self, connector_id: str) -> list[ConnectorItem]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text(
                        "SELECT * FROM connector_items "
                        "WHERE scope_id = :scope AND connector_id = :cid ORDER BY external_id"
                    ),
                    {"scope": self._scope_id, "cid": connector_id},
                )
            ).all()
        return [_item_from_row(row) for row in rows]

    async def upsert_items(
        self, connector_id: str, binding_id: str, items: Iterable[ConnectorItemDraft]
    ) -> list[ConnectorItem]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            for draft in items:
                config = _safe_metadata(draft.config, field="item config")
                await conn.execute(
                    text(
                        "INSERT INTO connector_items "
                        "(id, scope_id, connector_id, binding_id, resource_id, external_id, "
                        "kind, display_name, url, destination_kind, destination_target_id, "
                        "destination_id, config) "
                        "VALUES (:id, :scope, :cid, :binding, :resource, :external, :kind, "
                        ":name, :url, :destination_kind, :destination_target_id, "
                        ":destination_id, CAST(:config AS jsonb)) "
                        "ON CONFLICT (scope_id, binding_id, external_id) DO UPDATE SET "
                        "resource_id = EXCLUDED.resource_id, kind = EXCLUDED.kind, "
                        "display_name = EXCLUDED.display_name, url = EXCLUDED.url, "
                        "destination_kind = EXCLUDED.destination_kind, "
                        "destination_target_id = EXCLUDED.destination_target_id, "
                        "destination_id = EXCLUDED.destination_id, config = EXCLUDED.config, "
                        "updated_at = now()"
                    ),
                    {
                        "id": uuid.uuid4().hex,
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "binding": binding_id,
                        "resource": draft.resource_id,
                        "external": draft.external_id,
                        "kind": draft.kind,
                        "name": draft.display_name,
                        "url": draft.url,
                        "destination_kind": (
                            None if draft.destination_kind is None else draft.destination_kind.value
                        ),
                        "destination_target_id": draft.destination_target_id,
                        "destination_id": draft.destination_id,
                        "config": json.dumps(config, ensure_ascii=False),
                    },
                )
        return await self.list_items(connector_id)

    async def delete_item(self, connector_id: str, binding_id: str, external_id: str) -> bool:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "DELETE FROM connector_items WHERE scope_id = :scope "
                    "AND connector_id = :cid AND binding_id = :binding "
                    "AND external_id = :external"
                ),
                {
                    "scope": self._scope_id,
                    "cid": connector_id,
                    "binding": binding_id,
                    "external": external_id,
                },
            )
        return bool(result.rowcount)

    async def prune_items(
        self, connector_id: str, binding_id: str, keep_external_ids: set[str]
    ) -> int:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "DELETE FROM connector_items WHERE scope_id = :scope "
                    "AND connector_id = :cid AND binding_id = :binding "
                    "AND NOT (external_id = ANY(:keep))"
                ),
                {
                    "scope": self._scope_id,
                    "cid": connector_id,
                    "binding": binding_id,
                    "keep": sorted(keep_external_ids),
                },
            )
        return int(result.rowcount or 0)

    async def get_cursor(
        self, connector_id: str, binding_id: str, stream: str, resource_id: str | None = None
    ) -> ConnectorCursor | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "SELECT * FROM connector_cursors WHERE scope_id = :scope "
                        "AND connector_id = :cid AND binding_id = :binding "
                        "AND resource_key = :resource AND stream = :stream"
                    ),
                    {
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "binding": binding_id,
                        "resource": resource_id or "",
                        "stream": stream,
                    },
                )
            ).one_or_none()
        return None if row is None else _cursor_from_row(row)

    async def list_cursors(self, connector_id: str, binding_id: str) -> list[ConnectorCursor]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            rows = (
                await conn.execute(
                    text(
                        "SELECT * FROM connector_cursors WHERE scope_id = :scope "
                        "AND connector_id = :cid AND binding_id = :binding "
                        "ORDER BY resource_key, stream"
                    ),
                    {
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "binding": binding_id,
                    },
                )
            ).all()
        return [_cursor_from_row(row) for row in rows]

    async def put_cursor(
        self,
        connector_id: str,
        binding_id: str,
        stream: str,
        value: str,
        *,
        resource_id: str | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
        revision: str | None = None,
    ) -> ConnectorCursor:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "INSERT INTO connector_cursors "
                        "(id, scope_id, connector_id, binding_id, resource_id, resource_key, "
                        "stream, cursor_value, etag, last_modified, revision) "
                        "VALUES (:id, :scope, :cid, :binding, :resource_id, :resource_key, "
                        ":stream, :value, :etag, :modified, :revision) "
                        "ON CONFLICT (scope_id, binding_id, resource_key, stream) DO UPDATE SET "
                        "cursor_value = EXCLUDED.cursor_value, etag = EXCLUDED.etag, "
                        "last_modified = EXCLUDED.last_modified, revision = EXCLUDED.revision, "
                        "updated_at = now() RETURNING *"
                    ),
                    {
                        "id": uuid.uuid4().hex,
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "binding": binding_id,
                        "resource_id": resource_id,
                        "resource_key": resource_id or "",
                        "stream": stream,
                        "value": value,
                        "etag": etag,
                        "modified": last_modified,
                        "revision": revision,
                    },
                )
            ).one()
        return _cursor_from_row(row)

    async def delete_cursor(
        self,
        connector_id: str,
        binding_id: str,
        stream: str,
        resource_id: str | None = None,
    ) -> bool:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "DELETE FROM connector_cursors WHERE scope_id = :scope "
                    "AND connector_id = :cid AND binding_id = :binding "
                    "AND resource_key = :resource AND stream = :stream"
                ),
                {
                    "scope": self._scope_id,
                    "cid": connector_id,
                    "binding": binding_id,
                    "resource": resource_id or "",
                    "stream": stream,
                },
            )
        return bool(result.rowcount)

    async def prune_cursors(
        self,
        connector_id: str,
        binding_id: str,
        keep: set[tuple[str | None, str]],
    ) -> int:
        removed = 0
        for cursor in await self.list_cursors(connector_id, binding_id):
            if (cursor.resource_id, cursor.stream) not in keep:
                removed += int(
                    await self.delete_cursor(
                        connector_id,
                        binding_id,
                        cursor.stream,
                        cursor.resource_id,
                    )
                )
        return removed

    async def claim_delivery(
        self,
        connector_id: str,
        binding_id: str,
        delivery_id: str,
        payload_hash: str,
    ) -> bool:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "INSERT INTO connector_deliveries "
                        "(id, scope_id, connector_id, binding_id, delivery_id, payload_hash, "
                        "status) VALUES (:id, :scope, :cid, :binding, :delivery, :hash, "
                        "'processing') "
                        "ON CONFLICT (scope_id, connector_id, delivery_id) DO NOTHING RETURNING id"
                    ),
                    {
                        "id": uuid.uuid4().hex,
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "binding": binding_id,
                        "delivery": delivery_id,
                        "hash": payload_hash,
                    },
                )
            ).one_or_none()
            if row is not None:
                return True
            existing = (
                await conn.execute(
                    text(
                        "SELECT payload_hash, status FROM connector_deliveries "
                        "WHERE scope_id = :scope AND connector_id = :cid "
                        "AND delivery_id = :delivery"
                    ),
                    {
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "delivery": delivery_id,
                    },
                )
            ).one()
            if existing.payload_hash != payload_hash:
                raise ValueError("connector delivery id was reused with a different payload")
            reclaimed = (
                await conn.execute(
                    text(
                        "UPDATE connector_deliveries SET status = 'processing', "
                        "error_code = NULL, error_summary = NULL, processed_at = NULL, "
                        "updated_at = now() WHERE scope_id = :scope AND connector_id = :cid "
                        "AND delivery_id = :delivery AND (status = 'failed' OR "
                        "(status IN ('received', 'processing') "
                        "AND updated_at < now() - interval '5 minutes')) RETURNING id"
                    ),
                    {
                        "scope": self._scope_id,
                        "cid": connector_id,
                        "delivery": delivery_id,
                    },
                )
            ).one_or_none()
            return reclaimed is not None

    async def finish_delivery(
        self,
        connector_id: str,
        delivery_id: str,
        *,
        event_id: str | None = None,
        error_code: str | None = None,
        error_summary: str | None = None,
    ) -> None:
        failed = error_code is not None
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE connector_deliveries SET status = :status, event_id = :event, "
                    "error_code = :code, error_summary = :summary, processed_at = now(), "
                    "updated_at = now() WHERE scope_id = :scope AND connector_id = :cid "
                    "AND delivery_id = :delivery"
                ),
                {
                    "status": "failed" if failed else "processed",
                    "event": event_id,
                    "code": error_code,
                    "summary": error_summary,
                    "scope": self._scope_id,
                    "cid": connector_id,
                    "delivery": delivery_id,
                },
            )
        if not result.rowcount:
            raise LookupError("connector delivery was not claimed")

    async def claim_due_schedules(
        self, now: datetime, *, limit: int, lease_seconds: int
    ) -> list[ConnectorScheduleLease]:
        if limit <= 0 or lease_seconds <= 0:
            raise ValueError("connector schedule bounds must be positive")
        leases: list[ConnectorScheduleLease] = []
        expires_at = now + timedelta(seconds=lease_seconds)
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            await conn.execute(
                text(
                    "UPDATE connector_bindings SET "
                    "status = CASE renewal_expiry_behavior "
                    "WHEN 'error' THEN 'error' WHEN 'revoked' THEN 'revoked' "
                    "ELSE 'degraded' END, "
                    "error_code = 'connector_renewal_expired', "
                    "error_summary = 'connector subscription renewal expired', "
                    "updated_at = :now "
                    "WHERE scope_id = :scope AND status IN ('connected', 'degraded') "
                    "AND renewal_expires_at IS NOT NULL AND renewal_expires_at <= :now "
                    "AND renewal_expiry_behavior IS NOT NULL"
                ),
                {"scope": self._scope_id, "now": now},
            )
            rows = (
                await conn.execute(
                    text(
                        "SELECT *, CASE "
                        "WHEN next_renewal_at IS NOT NULL AND next_renewal_at <= :now "
                        "AND (next_sync_at IS NULL OR next_sync_at > :now "
                        "OR next_renewal_at <= next_sync_at) THEN 'renewal' "
                        "ELSE 'sync' END AS schedule_operation "
                        "FROM connector_bindings WHERE scope_id = :scope "
                        "AND status IN ('connected', 'degraded') "
                        "AND (schedule_lease_expires_at IS NULL "
                        "OR schedule_lease_expires_at <= :now) "
                        "AND ((next_sync_at IS NOT NULL AND next_sync_at <= :now "
                        "AND sync_cadence_seconds IS NOT NULL) "
                        "OR (next_renewal_at IS NOT NULL AND next_renewal_at <= :now "
                        "AND renewal_cadence_seconds IS NOT NULL)) "
                        "ORDER BY LEAST(COALESCE(next_sync_at, 'infinity'::timestamptz), "
                        "COALESCE(next_renewal_at, 'infinity'::timestamptz)), connector_id "
                        "FOR UPDATE SKIP LOCKED LIMIT :limit"
                    ),
                    {"scope": self._scope_id, "now": now, "limit": limit},
                )
            ).all()
            for row in rows:
                operation = ConnectorScheduleOperation(str(row.schedule_operation))
                token = uuid.uuid4().hex
                await conn.execute(
                    text(
                        "UPDATE connector_bindings SET schedule_lease_token = :token, "
                        "schedule_lease_operation = :operation, "
                        "schedule_lease_expires_at = :expires, updated_at = :now "
                        "WHERE scope_id = :scope AND id = :binding"
                    ),
                    {
                        "token": token,
                        "operation": operation.value,
                        "expires": expires_at,
                        "now": now,
                        "scope": self._scope_id,
                        "binding": row.id,
                    },
                )
                if operation is ConnectorScheduleOperation.sync:
                    due_at = row.next_sync_at
                    cadence = row.sync_cadence_seconds
                    attempt = int(row.sync_failures) + 1
                else:
                    due_at = row.next_renewal_at
                    cadence = row.renewal_cadence_seconds
                    attempt = int(row.renewal_failures) + 1
                assert due_at is not None and cadence is not None
                leases.append(
                    ConnectorScheduleLease(
                        self._scope_id,
                        str(row.connector_id),
                        str(row.id),
                        operation,
                        due_at,
                        int(cadence),
                        attempt,
                        token,
                        expires_at,
                    )
                )
        return leases

    async def complete_schedule(
        self, lease: ConnectorScheduleLease, *, next_at: datetime
    ) -> None:
        next_column = (
            "next_sync_at"
            if lease.operation is ConnectorScheduleOperation.sync
            else "next_renewal_at"
        )
        failures_column = (
            "sync_failures"
            if lease.operation is ConnectorScheduleOperation.sync
            else "renewal_failures"
        )
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    f"UPDATE connector_bindings SET {next_column} = :next_at, "
                    f"{failures_column} = 0, schedule_lease_token = NULL, "
                    "schedule_lease_operation = NULL, schedule_lease_expires_at = NULL, "
                    "updated_at = now() WHERE scope_id = :scope AND id = :binding "
                    "AND connector_id = :cid AND schedule_lease_token = :token "
                    "AND schedule_lease_operation = :operation "
                    "AND schedule_lease_expires_at > now() "
                    "AND status IN ('connected', 'degraded')"
                ),
                {
                    "next_at": next_at,
                    "scope": self._scope_id,
                    "binding": lease.binding_id,
                    "cid": lease.connector_id,
                    "token": lease.token,
                    "operation": lease.operation.value,
                },
            )
        if not result.rowcount:
            raise ConnectorScheduleLeaseLostError("connector schedule lease was lost")

    async def fail_schedule(
        self,
        lease: ConnectorScheduleLease,
        *,
        retry_at: datetime,
        error_code: str,
        error_summary: str,
    ) -> None:
        next_column = (
            "next_sync_at"
            if lease.operation is ConnectorScheduleOperation.sync
            else "next_renewal_at"
        )
        failures_column = (
            "sync_failures"
            if lease.operation is ConnectorScheduleOperation.sync
            else "renewal_failures"
        )
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    f"UPDATE connector_bindings SET {next_column} = :retry_at, "
                    f"{failures_column} = {failures_column} + 1, status = 'degraded', "
                    "error_code = :code, error_summary = :summary, "
                    "schedule_lease_token = NULL, schedule_lease_operation = NULL, "
                    "schedule_lease_expires_at = NULL, updated_at = now() "
                    "WHERE scope_id = :scope AND id = :binding AND connector_id = :cid "
                    "AND schedule_lease_token = :token "
                    "AND schedule_lease_operation = :operation "
                    "AND schedule_lease_expires_at > now() "
                    "AND status IN ('connected', 'degraded')"
                ),
                {
                    "retry_at": retry_at,
                    "code": error_code[:128],
                    "summary": error_summary[:500],
                    "scope": self._scope_id,
                    "binding": lease.binding_id,
                    "cid": lease.connector_id,
                    "token": lease.token,
                    "operation": lease.operation.value,
                },
            )
        if not result.rowcount:
            raise ConnectorScheduleLeaseLostError("connector schedule lease was lost")

    async def suspend_schedule(
        self, lease: ConnectorScheduleLease, *, resume_at: datetime
    ) -> None:
        next_column = (
            "next_sync_at"
            if lease.operation is ConnectorScheduleOperation.sync
            else "next_renewal_at"
        )
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    f"UPDATE connector_bindings SET {next_column} = :resume_at, "
                    "schedule_lease_token = NULL, schedule_lease_operation = NULL, "
                    "schedule_lease_expires_at = NULL, updated_at = now() "
                    "WHERE scope_id = :scope AND id = :binding AND connector_id = :cid "
                    "AND schedule_lease_token = :token "
                    "AND schedule_lease_operation = :operation "
                    "AND schedule_lease_expires_at > now() "
                    "AND status IN ('connected', 'degraded')"
                ),
                {
                    "resume_at": resume_at,
                    "scope": self._scope_id,
                    "binding": lease.binding_id,
                    "cid": lease.connector_id,
                    "token": lease.token,
                    "operation": lease.operation.value,
                },
            )
        if not result.rowcount:
            raise ConnectorScheduleLeaseLostError("connector schedule lease was lost")


async def purge_scope(engine: AsyncEngine, scope_id: str) -> int:
    total = 0
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        for table in (
            "connector_deliveries",
            "connector_cursors",
            "connector_items",
            "connector_resources",
            "connector_binding_targets",
            "connector_bindings",
        ):
            result = await conn.execute(
                text(f"DELETE FROM {table} WHERE scope_id = :scope"),
                {"scope": scope_id},
            )
            total += int(result.rowcount or 0)
    return total


__all__ = [
    "ConnectorRepository",
    "ConnectorScheduleLease",
    "ConnectorScheduleLeaseLostError",
    "InMemoryConnectorRepository",
    "PostgresConnectorRepository",
    "next_schedule_time",
    "purge_scope",
]
