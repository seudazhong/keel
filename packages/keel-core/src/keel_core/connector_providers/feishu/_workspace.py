"""Workspace resource discovery and Knowledge change normalization."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from keel_core.connector_contracts import (
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorCursorUpdate,
    ConnectorItem,
    ConnectorOperationContext,
    ConnectorProvenance,
    ConnectorResource,
    ConnectorResourceDraft,
    ConnectorResourceRefreshMode,
    ConnectorResourceResult,
    ConnectorSyncResult,
)

from ._client import FeishuClient, paginate

_SUPPORTED_DOC_TYPES = frozenset({"doc", "docx"})
_EXCLUDED_TYPES = frozenset(
    {"bitable", "base", "calendar", "task", "sheet", "slides", "mindnote", "shortcut"}
)


def _string(item: dict[str, Any], *names: str) -> str:
    for name in names:
        value = item.get(name)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, int):
            return str(value)
    return ""


def _resource_external_id(kind: str, token: str) -> str:
    return f"{kind}:{token}"


def _file_url(item: dict[str, Any], file_type: str, token: str) -> str:
    url = _string(item, "url")
    if url:
        return url
    if file_type == "docx":
        return f"https://feishu.cn/docx/{quote(token)}"
    if file_type == "doc":
        return f"https://feishu.cn/docs/{quote(token)}"
    return f"https://feishu.cn/drive/folder/{quote(token)}"


async def discover_resources(client: FeishuClient, token: str) -> ConnectorResourceResult:
    resources: list[ConnectorResourceDraft] = []
    spaces = await paginate(client, "/open-apis/wiki/v2/spaces", token=token)
    for item in spaces:
        space_id = _string(item, "space_id")
        if not space_id:
            continue
        resources.append(
            ConnectorResourceDraft(
                external_id=_resource_external_id("wiki", space_id),
                kind="wiki_space",
                display_name=_string(item, "name") or space_id,
                url=_string(item, "url") or f"https://feishu.cn/wiki/{quote(space_id)}",
                config={"space_id": space_id},
            )
        )

    files = await paginate(client, "/open-apis/drive/v1/files", token=token)
    for item in files:
        file_type = _string(item, "type").lower()
        token_value = _string(item, "token", "file_token")
        if not token_value or file_type in _EXCLUDED_TYPES:
            continue
        if file_type in _SUPPORTED_DOC_TYPES:
            resources.append(
                ConnectorResourceDraft(
                    external_id=_resource_external_id(file_type, token_value),
                    kind="docs_document",
                    display_name=_string(item, "name", "title") or token_value,
                    url=_file_url(item, file_type, token_value),
                    config={
                        "document_type": file_type,
                        "token": token_value,
                        "revision": _revision(item),
                    },
                )
            )
        elif file_type == "folder":
            resources.append(
                ConnectorResourceDraft(
                    external_id=_resource_external_id("drive", token_value),
                    kind="drive_folder",
                    display_name=_string(item, "name", "title") or token_value,
                    url=_file_url(item, file_type, token_value),
                    config={"folder_token": token_value},
                )
            )

    chats = await paginate(client, "/open-apis/im/v1/chats", token=token)
    for item in chats:
        chat_id = _string(item, "chat_id")
        if not chat_id:
            continue
        resources.append(
            ConnectorResourceDraft(
                external_id=_resource_external_id("chat", chat_id),
                kind="chat",
                display_name=_string(item, "name") or chat_id,
                url=_string(item, "avatar"),
                config={"chat_id": chat_id, "chat_mode": _string(item, "chat_mode")},
            )
        )
    return ConnectorResourceResult(tuple(resources), ConnectorResourceRefreshMode.authoritative)


@dataclass(frozen=True, slots=True)
class WorkspaceDocument:
    external_id: str
    title: str
    content: str
    url: str
    revision: str


def _revision(item: dict[str, Any]) -> str:
    explicit = _string(item, "revision_id", "revision", "obj_edit_time", "modified_time")
    if explicit:
        return explicit
    stable = "|".join(
        (_string(item, "token", "obj_token"), _string(item, "name", "title"), _string(item, "url"))
    )
    return hashlib.sha256(stable.encode()).hexdigest()


async def _raw_document(
    client: FeishuClient,
    token: str,
    document_type: str,
    document_token: str,
    *,
    title: str,
    url: str,
    revision: str,
) -> WorkspaceDocument:
    if document_type == "docx":
        path = f"/open-apis/docx/v1/documents/{quote(document_token)}/raw_content"
    else:
        path = f"/open-apis/doc/v2/{quote(document_token)}/raw_content"
    payload = await client.request("GET", path, token=token)
    data = payload.get("data")
    content = data.get("content") if isinstance(data, dict) else None
    if not isinstance(content, str):
        raise ValueError(f"Feishu document {document_token!r} did not return text content")
    normalized_content = content if content else "\n"
    returned_revision = _string(data, "revision_id", "revision") if isinstance(data, dict) else ""
    return WorkspaceDocument(
        external_id=_resource_external_id(document_type, document_token),
        title=title or document_token,
        content=normalized_content,
        url=url,
        revision=returned_revision or revision or hashlib.sha256(content.encode()).hexdigest(),
    )


async def _folder_documents(
    client: FeishuClient,
    token: str,
    folder_token: str,
) -> list[WorkspaceDocument]:
    pending = [folder_token]
    documents: list[WorkspaceDocument] = []
    visited: set[str] = set()
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        rows = await paginate(
            client,
            "/open-apis/drive/v1/files",
            token=token,
            params={"folder_token": current},
        )
        for item in rows:
            file_type = _string(item, "type").lower()
            item_token = _string(item, "token", "file_token")
            if not item_token or file_type in _EXCLUDED_TYPES:
                continue
            if file_type == "folder":
                pending.append(item_token)
            elif file_type in _SUPPORTED_DOC_TYPES:
                documents.append(
                    await _raw_document(
                        client,
                        token,
                        file_type,
                        item_token,
                        title=_string(item, "name", "title"),
                        url=_file_url(item, file_type, item_token),
                        revision=_revision(item),
                    )
                )
    return documents


async def _wiki_documents(
    client: FeishuClient,
    token: str,
    space_id: str,
) -> list[WorkspaceDocument]:
    rows = await paginate(
        client,
        f"/open-apis/wiki/v2/spaces/{quote(space_id)}/nodes",
        token=token,
    )
    documents: list[WorkspaceDocument] = []
    for item in rows:
        document_type = _string(item, "obj_type").lower()
        document_token = _string(item, "obj_token")
        if document_type not in _SUPPORTED_DOC_TYPES or not document_token:
            continue
        node_token = _string(item, "node_token")
        url = _string(item, "url") or (
            f"https://feishu.cn/wiki/{quote(node_token or document_token)}"
        )
        documents.append(
            await _raw_document(
                client,
                token,
                document_type,
                document_token,
                title=_string(item, "title", "name"),
                url=url,
                revision=_revision(item),
            )
        )
    return documents


async def _resource_documents(
    client: FeishuClient,
    token: str,
    resource: ConnectorResource,
) -> list[WorkspaceDocument]:
    if resource.kind == "docs_document":
        document_type = str(resource.config.get("document_type", ""))
        document_token = str(resource.config.get("token", ""))
        if document_type not in _SUPPORTED_DOC_TYPES or not document_token:
            return []
        return [
            await _raw_document(
                client,
                token,
                document_type,
                document_token,
                title=resource.display_name,
                url=resource.url or _file_url({}, document_type, document_token),
                revision=str(resource.config.get("revision", "")),
            )
        ]
    if resource.kind == "drive_folder":
        folder_token = str(resource.config.get("folder_token", ""))
        return await _folder_documents(client, token, folder_token) if folder_token else []
    if resource.kind == "wiki_space":
        space_id = str(resource.config.get("space_id", ""))
        return await _wiki_documents(client, token, space_id) if space_id else []
    return []


def _existing_workspace_items(
    items: tuple[ConnectorItem, ...],
    roots: tuple[ConnectorResource, ...],
) -> dict[str, ConnectorItem]:
    root_ids = tuple(resource.external_id for resource in roots)
    return {
        item.external_id: item
        for item in items
        if item.kind == "knowledge_document"
        and any(
            item.external_id == root_id or item.external_id.startswith(f"{root_id}/")
            for root_id in root_ids
        )
    }


async def sync_workspace(
    client: FeishuClient,
    token: str,
    context: ConnectorOperationContext,
) -> ConnectorSyncResult:
    if context.binding is None:
        raise ValueError("Feishu sync requires a binding")
    discovered: dict[str, WorkspaceDocument] = {}
    cursor_updates: list[ConnectorCursorUpdate] = []
    roots = tuple(
        resource
        for resource in context.resources
        if resource.kind in {"docs_document", "drive_folder", "wiki_space"}
    )
    for resource in roots:
        documents = await _resource_documents(client, token, resource)
        for document in documents:
            external_id = (
                resource.external_id
                if resource.kind == "docs_document"
                else f"{resource.external_id}/{document.external_id}"
            )
            discovered[external_id] = replace(document, external_id=external_id)
        watermark = max((item.revision for item in documents), default="")
        cursor_updates.append(
            ConnectorCursorUpdate(
                stream="workspace",
                value=datetime.now(UTC).isoformat(),
                resource_id=resource.id,
                revision=watermark or None,
            )
        )

    existing = _existing_workspace_items(context.items, roots)
    changes: list[ConnectorChange] = []
    for external_id, document in sorted(discovered.items()):
        current = existing.get(external_id)
        current_revision = str(current.config.get("revision", "")) if current else ""
        if current is not None and current_revision == document.revision:
            continue
        provenance = ConnectorProvenance(
            connector_id=context.connector_id,
            binding_id=context.binding.id,
            external_resource_id=external_id,
            source_url=document.url,
            revision=document.revision,
        )
        changes.append(
            ConnectorChange(
                kind=ConnectorChangeKind.upsert,
                provenance=provenance,
                title=document.title,
                content=document.content,
                mime_type="text/plain",
            )
        )
    for external_id, current in sorted(existing.items()):
        if external_id in discovered:
            continue
        changes.append(
            ConnectorChange(
                kind=ConnectorChangeKind.delete,
                provenance=ConnectorProvenance(
                    connector_id=context.connector_id,
                    binding_id=context.binding.id,
                    external_resource_id=external_id,
                    source_url=current.url,
                    revision=str(current.config.get("revision", "")) or None,
                ),
            )
        )
    from keel_core.connector_contracts import ConnectorStateUpdate

    return ConnectorSyncResult(
        tuple(changes),
        ConnectorStateUpdate(cursor_updates=tuple(cursor_updates)),
    )


__all__ = ["WorkspaceDocument", "discover_resources", "sync_workspace"]
