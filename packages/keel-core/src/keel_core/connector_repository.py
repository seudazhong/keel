"""Scope-bound connector binding, resource, cursor, and delivery persistence."""

from __future__ import annotations

import copy
import json
import uuid
from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, datetime
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
    ConnectorResource,
    ConnectorResourceDraft,
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


class InMemoryConnectorRepository:
    def __init__(self, scope_id: str) -> None:
        self._scope_id = scope_id
        self._bindings: dict[str, ConnectorBinding] = {}
        self._targets: dict[tuple[str, ConnectorTargetKind], ConnectorBindingTarget] = {}
        self._resources: dict[tuple[str, str], ConnectorResource] = {}
        self._items: dict[tuple[str, str], ConnectorItem] = {}
        self._cursors: dict[tuple[str, str, str, str], ConnectorCursor] = {}
        self._deliveries: dict[tuple[str, str], tuple[str, str, datetime]] = {}

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
    ) -> ConnectorBinding:
        now = datetime.now(UTC)
        prior = self._bindings.get(connector_id)
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
        row = replace(
            prior,
            status=(ConnectorBindingStatus.connected if healthy else ConnectorBindingStatus.error),
            last_success_at=health.checked_at if healthy else prior.last_success_at,
            error_code=None if healthy else health.status.value,
            error_summary=None if healthy else health.message,
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

    async def upsert_binding(
        self,
        connector_id: str,
        draft: ConnectorBindingDraft,
        status: ConnectorBindingStatus,
    ) -> ConnectorBinding:
        metadata = _safe_metadata(draft.metadata, field="binding metadata")
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "INSERT INTO connector_bindings "
                        "(id, scope_id, connector_id, status, display_name, "
                        "external_account_id, external_tenant_id, metadata) "
                        "VALUES (:id, :scope, :cid, :status, :name, :account, :tenant, "
                        "CAST(:metadata AS jsonb)) "
                        "ON CONFLICT (scope_id, connector_id) DO UPDATE SET "
                        "status = EXCLUDED.status, display_name = EXCLUDED.display_name, "
                        "external_account_id = EXCLUDED.external_account_id, "
                        "external_tenant_id = EXCLUDED.external_tenant_id, "
                        "metadata = EXCLUDED.metadata, error_code = NULL, error_summary = NULL, "
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
                    },
                )
            ).one()
        return _binding_from_row(row)

    async def record_health(
        self, connector_id: str, health: ConnectorHealth
    ) -> ConnectorBinding | None:
        healthy = health.status is ConnectorHealthStatus.healthy
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "UPDATE connector_bindings SET status = :status, "
                        "last_success_at = CASE WHEN :healthy THEN :checked "
                        "ELSE last_success_at END, "
                        "error_code = :code, error_summary = :summary, updated_at = :checked "
                        "WHERE scope_id = :scope AND connector_id = :cid RETURNING *"
                    ),
                    {
                        "status": (
                            ConnectorBindingStatus.connected.value
                            if healthy
                            else ConnectorBindingStatus.error.value
                        ),
                        "healthy": healthy,
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
    "InMemoryConnectorRepository",
    "PostgresConnectorRepository",
    "purge_scope",
]
