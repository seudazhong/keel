"""Read-only Notion connector using an Internal Integration Token."""

from __future__ import annotations

import hashlib
import json
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import urlencode

import httpx

from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAuthenticationError,
    ConnectorAuthKind,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCapability,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorCursorUpdate,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorProvenance,
    ConnectorResourceDraft,
    ConnectorResourceRefreshMode,
    ConnectorResourceResult,
    ConnectorSetupField,
    ConnectorSetupResult,
    ConnectorStateUpdate,
    ConnectorSyncResult,
    ConnectorTargetField,
    ConnectorTargetKind,
    ConnectorUnavailableError,
)
from keel_core.connector_credentials import CredentialEnvelope

NOTION_CONNECTOR_ID = "notion"
NOTION_API_BASE_URL = "https://api.notion.com/v1"
NOTION_API_VERSION = "2025-09-03"
NOTION_CURSOR_STREAM = "notion_inventory_v1"
NOTION_CREDENTIAL_KIND = "notion_internal_integration"
NOTION_SYNC_CADENCE_SECONDS = 900
_PAGE_SIZE = 100
_MAX_PAGES = 100


class NotionError(Exception):
    """Sanitized provider-local Notion failure."""


class NotionPermissionError(NotionError):
    pass


class NotionNotFoundError(NotionError):
    pass


class NotionRateLimitError(NotionError):
    def __init__(self, retry_after: str | None = None) -> None:
        self.retry_after = retry_after
        super().__init__("Notion rate limit exceeded.")


class NotionTimeoutError(NotionError):
    pass


class NotionResponseError(NotionError):
    pass


class NotionTransport(Protocol):
    async def request(
        self,
        method: str,
        path: str,
        token: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]: ...


class HttpxNotionTransport:
    """Small fixed-origin HTTP transport; Notion content never enters errors."""

    def __init__(self, *, timeout_seconds: float = 15.0) -> None:
        self._timeout = timeout_seconds

    async def request(
        self,
        method: str,
        path: str,
        token: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        authorization = "Bearer " + token
        headers = {
            "Authorization": authorization,
            "Notion-Version": NOTION_API_VERSION,
            "Content-Type": "application/json",
        }
        try:
            async with httpx.AsyncClient(
                base_url=NOTION_API_BASE_URL,
                timeout=self._timeout,
                follow_redirects=False,
            ) as client:
                response = await client.request(
                    method,
                    path,
                    headers=headers,
                    json=dict(payload) if payload is not None else None,
                )
        except httpx.TimeoutException as exc:
            raise NotionTimeoutError("Notion request timed out.") from exc
        except httpx.RequestError as exc:
            raise ConnectorUnavailableError("Notion is unavailable.") from exc
        if response.status_code == 401:
            raise ConnectorAuthenticationError("Notion rejected the integration token.")
        if response.status_code == 403:
            raise NotionPermissionError("Notion denied access to this resource.")
        if response.status_code == 404:
            raise NotionNotFoundError("Notion resource was not found or is no longer shared.")
        if response.status_code == 429:
            raise NotionRateLimitError(response.headers.get("retry-after"))
        if response.status_code >= 500:
            raise ConnectorUnavailableError("Notion is temporarily unavailable.")
        if not 200 <= response.status_code < 300:
            raise NotionResponseError(f"Notion returned HTTP {response.status_code}.")
        try:
            document = response.json()
        except ValueError as exc:
            raise NotionResponseError("Notion returned an invalid JSON response.") from exc
        if not isinstance(document, dict):
            raise NotionResponseError("Notion returned an invalid response object.")
        return document


@dataclass(frozen=True, slots=True)
class _RenderedObject:
    external_id: str
    kind: str
    title: str
    url: str | None
    last_edited_time: str
    revision: str
    content: str
    children: tuple[str, ...] = ()

    def state(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "title": self.title,
            "url": self.url,
            "last_edited_time": self.last_edited_time,
            "revision": self.revision,
            "children": list(self.children),
        }


@dataclass(slots=True)
class _SyncAccumulator:
    objects: dict[str, _RenderedObject]
    states: dict[str, dict[str, Any]]
    changes: list[ConnectorChange]
    processed_pages: set[str]


manifest = ConnectorManifest(
    id=NOTION_CONNECTOR_ID,
    name="Notion",
    description="Import explicitly shared Notion pages and data sources into Knowledge.",
    icon="N",
    auth_kind=ConnectorAuthKind.secret,
    capabilities=(
        ConnectorCapability.read,
        ConnectorCapability.resources,
        ConnectorCapability.sync,
    ),
    setup_fields=(
        ConnectorSetupField(
            id="token",
            label="Internal Integration Token",
            secret=True,
            help_text=(
                "Create a Notion internal integration, then share pages or data sources with it."
            ),
        ),
    ),
    setup_action_label="Validate and save",
    resource_label="Shared Notion roots",
    target_fields=(
        ConnectorTargetField(
            ConnectorTargetKind.knowledge,
            "Knowledge Base",
            help_text="Imported Notion content is written only to this Knowledge Base.",
        ),
    ),
    default_sync_cadence_seconds=NOTION_SYNC_CADENCE_SECONDS,
)


class NotionProvider(BaseConnectorProvider):
    manifest = manifest

    def __init__(self, transport: NotionTransport | None = None) -> None:
        self._transport = transport or HttpxNotionTransport()

    async def setup(
        self, context: ConnectorOperationContext, values: dict[str, str]
    ) -> ConnectorSetupResult:
        token = values.get("token", "").strip()
        if not token:
            raise ValueError("Notion integration token is required.")
        me = await self._transport.request("GET", "/users/me", token)
        user_id = _required_id(me)
        workspace_name = _workspace_name(me)
        return ConnectorSetupResult(
            binding=ConnectorBindingDraft(
                display_name=workspace_name or "Notion",
                external_account_id=user_id,
                external_tenant_id=_optional_string(me.get("workspace_id")),
                metadata={
                    "api_version": NOTION_API_VERSION,
                    "workspace_name": workspace_name,
                    "read_only": True,
                },
            ),
            credential=CredentialEnvelope(
                kind=NOTION_CREDENTIAL_KIND,
                values={"token": token},
            ),
            status=ConnectorBindingStatus.connected,
        )

    async def list_resources(
        self, context: ConnectorOperationContext
    ) -> ConnectorResourceResult:
        token = _token(context)
        rows = await self._paginate("POST", "/search", token, {})
        resources: list[ConnectorResourceDraft] = []
        for row in rows:
            object_type = _optional_string(row.get("object"))
            if object_type not in {"page", "data_source", "database"}:
                continue
            external_id = _required_id(row)
            kind = "page" if object_type == "page" else "data_source"
            resources.append(
                ConnectorResourceDraft(
                    external_id=external_id,
                    kind=kind,
                    display_name=_object_title(row, kind),
                    url=_optional_string(row.get("url")),
                    config={
                        "api_object": object_type,
                        "last_edited_time": _optional_string(row.get("last_edited_time")),
                    },
                )
            )
        resources.sort(key=lambda item: (item.display_name.casefold(), item.external_id))
        return ConnectorResourceResult(
            tuple(resources),
            ConnectorResourceRefreshMode.authoritative,
        )

    async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
        if context.binding is None:
            raise RuntimeError("Notion binding is unavailable.")
        token = _token(context)
        previous = _load_inventory(context)
        old_objects = _state_objects(previous)
        old_roots = _state_roots(previous)
        visible = {
            _required_id(row): row
            for row in await self._paginate("POST", "/search", token, {})
            if _optional_string(row.get("object")) in {"page", "data_source", "database"}
        }
        accumulator = _SyncAccumulator({}, {}, [], set())
        roots: dict[str, list[str]] = {}

        for resource in sorted(context.resources, key=lambda item: item.external_id):
            root_id = resource.external_id
            result = visible.get(root_id)
            if result is None:
                roots[root_id] = []
                continue
            object_type = _optional_string(result.get("object"))
            if resource.kind == "page" and object_type == "page":
                roots[root_id] = await self._collect_page_tree(
                    context,
                    token,
                    root_id,
                    tuple(old_roots.get(root_id, ())),
                    old_objects,
                    accumulator,
                )
            elif resource.kind == "data_source" and object_type in {"data_source", "database"}:
                roots[root_id] = await self._collect_data_source(
                    context,
                    token,
                    root_id,
                    object_type,
                    tuple(old_roots.get(root_id, ())),
                    old_objects,
                    accumulator,
                )

        current_ids = set(accumulator.states)
        for external_id in sorted(set(old_objects) - current_ids):
            prior = old_objects[external_id]
            accumulator.changes.append(
                ConnectorChange(
                    ConnectorChangeKind.delete,
                    ConnectorProvenance(
                        NOTION_CONNECTOR_ID,
                        context.binding.id,
                        external_id,
                        source_url=_optional_string(prior.get("url")),
                        revision=_optional_string(prior.get("revision")),
                    ),
                )
            )

        inventory = json.dumps(
            {
                "version": 1,
                "roots": {key: sorted(set(value)) for key, value in sorted(roots.items())},
                "objects": {key: accumulator.states[key] for key in sorted(accumulator.states)},
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return ConnectorSyncResult(
            changes=tuple(accumulator.changes),
            state=ConnectorStateUpdate(
                cursor_updates=(ConnectorCursorUpdate(NOTION_CURSOR_STREAM, inventory),)
            ),
        )

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        checked_at = datetime.now(UTC)
        try:
            token = _token(context)
            await self._transport.request("GET", "/users/me", token)
        except ConnectorAuthenticationError:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                checked_at,
                "Notion rejected the integration token (401).",
                retryable=False,
            )
        except NotionPermissionError:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                checked_at,
                "Notion denied access (403).",
                retryable=False,
            )
        except NotionNotFoundError:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                checked_at,
                "Notion endpoint or resource was not found (404).",
                retryable=False,
            )
        except NotionRateLimitError:
            return ConnectorHealth(
                ConnectorHealthStatus.degraded,
                checked_at,
                "Notion rate limited the connector (429).",
                retryable=True,
            )
        except NotionTimeoutError:
            return ConnectorHealth(
                ConnectorHealthStatus.degraded,
                checked_at,
                "Notion health check timed out.",
                retryable=True,
            )
        except ConnectorUnavailableError:
            return ConnectorHealth(
                ConnectorHealthStatus.degraded,
                checked_at,
                "Notion is temporarily unavailable.",
                retryable=True,
            )
        except (NotionError, ValueError):
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                checked_at,
                "Notion returned an invalid health response.",
                retryable=False,
            )
        return ConnectorHealth(ConnectorHealthStatus.healthy, checked_at)

    async def revoke(self, context: ConnectorOperationContext) -> None:
        # Internal integration tokens have no remote revoke endpoint. Keel deletes its
        # encrypted local copy; users may additionally rotate/revoke it in Notion.
        return None

    async def _collect_page_tree(
        self,
        context: ConnectorOperationContext,
        token: str,
        root_id: str,
        prior_members: tuple[str, ...],
        old_objects: Mapping[str, Mapping[str, Any]],
        accumulator: _SyncAccumulator,
    ) -> list[str]:
        queue = deque([root_id])
        prior = set(prior_members)
        members: list[str] = []
        while queue:
            page_id = queue.popleft()
            if page_id in accumulator.processed_pages:
                members.append(page_id)
                state = accumulator.states.get(page_id, {})
                queue.extend(_string_tuple(state.get("children")))
                continue
            try:
                page = await self._transport.request("GET", f"/pages/{page_id}", token)
            except NotionNotFoundError:
                continue
            if _is_archived(page):
                continue
            accumulator.processed_pages.add(page_id)
            old = old_objects.get(page_id)
            if _can_reuse(page, old):
                assert old is not None
                children = _string_tuple(old.get("children"))
                accumulator.states[page_id] = dict(old)
            else:
                rendered = await self._render_page(token, page)
                accumulator.objects[page_id] = rendered
                accumulator.states[page_id] = rendered.state()
                self._append_upsert(context, rendered, old, accumulator)
                children = rendered.children
            members.append(page_id)
            queue.extend(child for child in children if child in prior or child not in members)
        return sorted(set(members))

    async def _collect_data_source(
        self,
        context: ConnectorOperationContext,
        token: str,
        data_source_id: str,
        api_object: str,
        prior_members: tuple[str, ...],
        old_objects: Mapping[str, Mapping[str, Any]],
        accumulator: _SyncAccumulator,
    ) -> list[str]:
        base = "data_sources" if api_object == "data_source" else "databases"
        try:
            source = await self._transport.request("GET", f"/{base}/{data_source_id}", token)
        except NotionNotFoundError:
            return []
        if _is_archived(source):
            return []
        old = old_objects.get(data_source_id)
        if _can_reuse(source, old):
            assert old is not None
            accumulator.states[data_source_id] = dict(old)
        else:
            rendered = _render_data_source(source)
            accumulator.objects[data_source_id] = rendered
            accumulator.states[data_source_id] = rendered.state()
            self._append_upsert(context, rendered, old, accumulator)
        rows = await self._paginate("POST", f"/{base}/{data_source_id}/query", token, {})
        page_ids = sorted(
            {
                _required_id(row)
                for row in rows
                if _optional_string(row.get("object")) == "page" and not _is_archived(row)
            }
        )
        members = [data_source_id]
        old_page_members = tuple(item for item in prior_members if item != data_source_id)
        for page_id in page_ids:
            members.extend(
                await self._collect_page_tree(
                    context,
                    token,
                    page_id,
                    old_page_members,
                    old_objects,
                    accumulator,
                )
            )
        return sorted(set(members))

    async def _render_page(self, token: str, page: Mapping[str, Any]) -> _RenderedObject:
        page_id = _required_id(page)
        blocks = await self._block_children(token, page_id)
        body, children = await self._render_blocks(token, blocks, 0)
        title = _object_title(page, "page")
        url = _optional_string(page.get("url"))
        last_edited = _optional_string(page.get("last_edited_time")) or ""
        properties = _render_page_properties(page.get("properties"))
        parent = _render_parent(page.get("parent"))
        core = _provenance_header(
            kind="page",
            external_id=page_id,
            url=url,
            last_edited=last_edited,
            parent=parent,
        )
        sections = [f"# {title}", core]
        if properties:
            sections.extend(("## Properties", properties))
        if body:
            sections.extend(("## Content", body))
        content_without_revision = "\n\n".join(sections).strip()
        revision = _revision(last_edited, content_without_revision)
        content = content_without_revision.replace(
            f"> Last edited: {last_edited or 'unknown'}",
            f"> Last edited: {last_edited or 'unknown'}\n> Revision: {revision}",
            1,
        )
        return _RenderedObject(
            page_id,
            "page",
            title,
            url,
            last_edited,
            revision,
            content,
            tuple(sorted(set(children))),
        )

    async def _block_children(self, token: str, block_id: str) -> list[dict[str, Any]]:
        return await self._paginate("GET", f"/blocks/{block_id}/children", token, None)

    async def _render_blocks(
        self,
        token: str,
        blocks: Sequence[Mapping[str, Any]],
        depth: int,
    ) -> tuple[str, list[str]]:
        lines: list[str] = []
        child_pages: list[str] = []
        for block in blocks:
            block_type = _optional_string(block.get("type")) or "unknown"
            data = block.get(block_type)
            payload = data if isinstance(data, Mapping) else {}
            rendered = _render_block(block_type, payload, depth)
            if rendered:
                lines.append(rendered)
            if block_type == "child_page":
                child_id = _optional_string(block.get("id"))
                if child_id:
                    child_pages.append(child_id)
            if bool(block.get("has_children")) and block_type != "child_page":
                block_id = _optional_string(block.get("id"))
                if block_id:
                    nested = await self._block_children(token, block_id)
                    nested_text, nested_pages = await self._render_blocks(token, nested, depth + 1)
                    if nested_text:
                        lines.append(nested_text)
                    child_pages.extend(nested_pages)
        return "\n\n".join(item for item in lines if item.strip()).strip(), child_pages

    async def _paginate(
        self,
        method: str,
        path: str,
        token: str,
        payload: Mapping[str, Any] | None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        for _ in range(_MAX_PAGES):
            page_payload = dict(payload or {})
            page_payload["page_size"] = _PAGE_SIZE
            request_path = path
            if method == "GET":
                query: dict[str, str | int] = {"page_size": _PAGE_SIZE}
                if cursor is not None:
                    query["start_cursor"] = cursor
                request_path = f"{path}?{urlencode(query)}"
                page_payload_arg: Mapping[str, Any] | None = None
            else:
                if cursor is not None:
                    page_payload["start_cursor"] = cursor
                page_payload_arg = page_payload
            response = await self._transport.request(
                method,
                request_path,
                token,
                page_payload_arg,
            )
            results = response.get("results")
            if not isinstance(results, list):
                raise NotionResponseError("Notion paginated response omitted results.")
            for row in results:
                if not isinstance(row, dict):
                    raise NotionResponseError(
                        "Notion paginated response contained an invalid item."
                    )
                rows.append(row)
            if not bool(response.get("has_more")):
                return rows
            cursor = _optional_string(response.get("next_cursor"))
            if cursor is None:
                raise NotionResponseError("Notion pagination omitted the next cursor.")
        raise NotionResponseError("Notion pagination exceeded the safety limit.")

    @staticmethod
    def _append_upsert(
        context: ConnectorOperationContext,
        rendered: _RenderedObject,
        previous: Mapping[str, Any] | None,
        accumulator: _SyncAccumulator,
    ) -> None:
        if previous is not None and _optional_string(previous.get("revision")) == rendered.revision:
            return
        assert context.binding is not None
        accumulator.changes.append(
            ConnectorChange(
                ConnectorChangeKind.upsert,
                ConnectorProvenance(
                    NOTION_CONNECTOR_ID,
                    context.binding.id,
                    rendered.external_id,
                    source_url=rendered.url,
                    revision=rendered.revision,
                ),
                title=rendered.title,
                content=rendered.content,
                mime_type="text/markdown",
            )
        )


def _token(context: ConnectorOperationContext) -> str:
    credential = context.credential
    if credential is None or credential.kind != NOTION_CREDENTIAL_KIND:
        raise ConnectorAuthenticationError("Notion integration credentials are missing.")
    token = credential.values.get("token")
    if not isinstance(token, str) or not token.strip():
        raise ConnectorAuthenticationError("Notion integration credentials are invalid.")
    return token.strip()


def _load_inventory(context: ConnectorOperationContext) -> dict[str, Any]:
    cursor = next(
        (
            item
            for item in context.cursors
            if item.resource_id is None and item.stream == NOTION_CURSOR_STREAM
        ),
        None,
    )
    if cursor is None:
        return {}
    try:
        document = json.loads(cursor.value)
    except (TypeError, ValueError):
        return {}
    if not isinstance(document, dict) or document.get("version") != 1:
        return {}
    return document


def _state_objects(document: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw = document.get("objects")
    if not isinstance(raw, dict):
        return {}
    return {
        key: dict(value)
        for key, value in raw.items()
        if isinstance(key, str) and isinstance(value, dict)
    }


def _state_roots(document: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    raw = document.get("roots")
    if not isinstance(raw, dict):
        return {}
    return {
        key: _string_tuple(value)
        for key, value in raw.items()
        if isinstance(key, str)
    }


def _string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str) and item)


def _required_id(value: Mapping[str, Any]) -> str:
    result = _optional_string(value.get("id"))
    if result is None:
        raise NotionResponseError("Notion response omitted a resource id.")
    return result


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _workspace_name(me: Mapping[str, Any]) -> str | None:
    bot = me.get("bot")
    if isinstance(bot, Mapping):
        owner = bot.get("owner")
        if isinstance(owner, Mapping):
            workspace = owner.get("workspace")
            if isinstance(workspace, bool) and workspace:
                return _optional_string(me.get("name"))
    return _optional_string(me.get("name"))


def _is_archived(value: Mapping[str, Any]) -> bool:
    return bool(value.get("archived")) or bool(value.get("in_trash"))


def _can_reuse(
    current: Mapping[str, Any],
    previous: Mapping[str, Any] | None,
) -> bool:
    if previous is None:
        return False
    return (
        _optional_string(current.get("last_edited_time"))
        == _optional_string(previous.get("last_edited_time"))
        and _object_title(current, _optional_string(previous.get("kind")) or "page")
        == _optional_string(previous.get("title"))
        and _optional_string(current.get("url")) == _optional_string(previous.get("url"))
        and _optional_string(previous.get("revision")) is not None
    )


def _object_title(value: Mapping[str, Any], kind: str) -> str:
    if kind == "data_source":
        title = _rich_text(value.get("title"))
        return title or "Untitled data source"
    properties = value.get("properties")
    if isinstance(properties, Mapping):
        for property_value in properties.values():
            if isinstance(property_value, Mapping) and property_value.get("type") == "title":
                title = _rich_text(property_value.get("title"))
                if title:
                    return title
    title = _rich_text(value.get("title"))
    return title or ("Untitled page" if kind == "page" else "Untitled data source")


def _rich_text(value: object) -> str:
    if not isinstance(value, list):
        return ""
    chunks: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        plain = _optional_string(item.get("plain_text")) or ""
        href = _optional_string(item.get("href"))
        annotations = item.get("annotations")
        annotated = plain
        if isinstance(annotations, Mapping):
            if bool(annotations.get("code")):
                annotated = f"`{annotated.replace('`', '\\`')}`"
            if bool(annotations.get("bold")):
                annotated = f"**{annotated}**"
            if bool(annotations.get("italic")):
                annotated = f"*{annotated}*"
            if bool(annotations.get("strikethrough")):
                annotated = f"~~{annotated}~~"
            if bool(annotations.get("underline")):
                annotated = f"<u>{annotated}</u>"
        if href:
            annotated = f"[{annotated or href}]({href})"
        chunks.append(annotated)
    return "".join(chunks).strip()


def _render_block(block_type: str, payload: Mapping[str, Any], depth: int) -> str:
    text = _rich_text(payload.get("rich_text"))
    indent = "  " * depth
    if block_type == "paragraph":
        return f"{indent}{text}" if text else ""
    if block_type.startswith("heading_"):
        level_text = block_type.removeprefix("heading_")
        level = int(level_text) if level_text.isdigit() else 2
        return f"{'#' * min(6, level + depth)} {text or 'Untitled'}"
    if block_type == "bulleted_list_item":
        return f"{indent}- {text}"
    if block_type == "numbered_list_item":
        return f"{indent}1. {text}"
    if block_type == "to_do":
        checked = "x" if bool(payload.get("checked")) else " "
        return f"{indent}- [{checked}] {text}"
    if block_type == "toggle":
        return f"{indent}<details><summary>{text or 'Details'}</summary></details>"
    if block_type == "quote":
        return "\n".join(f"> {line}" for line in (text or "").splitlines())
    if block_type == "callout":
        icon = payload.get("icon")
        emoji = ""
        if isinstance(icon, Mapping):
            emoji = _optional_string(icon.get("emoji")) or ""
        return f"> {emoji} {text}".rstrip()
    if block_type == "code":
        language = _optional_string(payload.get("language")) or ""
        return f"```{language}\n{text}\n```"
    if block_type == "divider":
        return "---"
    if block_type == "equation":
        expression = _optional_string(payload.get("expression")) or ""
        return f"$${expression}$$"
    if block_type == "child_page":
        return f"{indent}- Child page: {(_optional_string(payload.get('title')) or 'Untitled')}"
    if block_type in {"child_database", "child_data_source"}:
        title = _optional_string(payload.get("title")) or "Untitled"
        return f"{indent}- Child data source: {title}"
    if block_type in {"bookmark", "link_preview", "embed"}:
        url = _optional_string(payload.get("url")) or "unknown"
        caption = _rich_text(payload.get("caption"))
        return f"[{caption or url}]({url})"
    if block_type in {"image", "video", "audio", "file", "pdf"}:
        media_url = _file_url(payload)
        caption = _rich_text(payload.get("caption")) or block_type.title()
        return (
            f"[{caption}]({media_url})"
            if media_url
            else f"> [{block_type.title()} without accessible URL]"
        )
    if block_type == "table_row":
        cells = payload.get("cells")
        if isinstance(cells, list):
            return "| " + " | ".join(_rich_text(cell).replace("|", "\\|") for cell in cells) + " |"
    if block_type in {"table", "column_list", "column", "synced_block", "template", "breadcrumb"}:
        return ""
    return f"> [Unsupported Notion block: {block_type}]"


def _file_url(payload: Mapping[str, Any]) -> str | None:
    file_type = _optional_string(payload.get("type"))
    if file_type:
        details = payload.get(file_type)
        if isinstance(details, Mapping):
            return _optional_string(details.get("url"))
    return None


def _render_page_properties(value: object) -> str:
    if not isinstance(value, Mapping):
        return ""
    lines: list[str] = []
    for name in sorted(value, key=str.casefold):
        raw = value[name]
        if not isinstance(name, str) or not isinstance(raw, Mapping):
            continue
        rendered = _property_value(raw)
        if rendered:
            lines.append(f"- **{name}:** {rendered}")
    return "\n".join(lines)


def _property_value(value: Mapping[str, Any]) -> str:
    property_type = _optional_string(value.get("type"))
    raw = value.get(property_type) if property_type else None
    if property_type in {"title", "rich_text"}:
        return _rich_text(raw)
    if property_type in {"number", "checkbox", "url", "email", "phone_number"}:
        return "" if raw is None else str(raw)
    if property_type in {"select", "status"} and isinstance(raw, Mapping):
        return _optional_string(raw.get("name")) or ""
    if property_type == "multi_select" and isinstance(raw, list):
        return ", ".join(
            name
            for item in raw
            if isinstance(item, Mapping) and (name := _optional_string(item.get("name")))
        )
    if property_type == "date" and isinstance(raw, Mapping):
        start = _optional_string(raw.get("start")) or ""
        end = _optional_string(raw.get("end"))
        return f"{start} – {end}" if end else start
    if property_type in {"created_time", "last_edited_time"}:
        return _optional_string(raw) or ""
    if property_type in {"created_by", "last_edited_by"} and isinstance(raw, Mapping):
        return _optional_string(raw.get("name")) or _optional_string(raw.get("id")) or ""
    if property_type == "people" and isinstance(raw, list):
        return ", ".join(
            _optional_string(item.get("name")) or _optional_string(item.get("id")) or ""
            for item in raw
            if isinstance(item, Mapping)
        )
    if property_type == "relation" and isinstance(raw, list):
        return ", ".join(
            item_id
            for item in raw
            if isinstance(item, Mapping) and (item_id := _optional_string(item.get("id")))
        )
    if property_type in {"formula", "rollup"} and isinstance(raw, Mapping):
        nested_type = _optional_string(raw.get("type"))
        nested = raw.get(nested_type) if nested_type else None
        return json.dumps(nested, ensure_ascii=False, sort_keys=True) if nested is not None else ""
    if property_type == "files" and isinstance(raw, list):
        return ", ".join(
            _optional_string(item.get("name")) or _file_url(item) or ""
            for item in raw
            if isinstance(item, Mapping)
        )
    if property_type is None:
        return ""
    return f"[Unsupported Notion property: {property_type}]"


def _render_parent(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    parent_type = _optional_string(value.get("type"))
    if parent_type is None:
        return None
    parent_id = _optional_string(value.get(parent_type))
    return f"{parent_type}:{parent_id}" if parent_id else parent_type


def _provenance_header(
    *,
    kind: str,
    external_id: str,
    url: str | None,
    last_edited: str,
    parent: str | None,
) -> str:
    rows = [
        f"> Notion kind: {kind}",
        f"> Notion ID: {external_id}",
        f"> URL: {url or 'unavailable'}",
        f"> Last edited: {last_edited or 'unknown'}",
    ]
    if parent:
        rows.append(f"> Parent: {parent}")
    return "\n".join(rows)


def _render_data_source(source: Mapping[str, Any]) -> _RenderedObject:
    external_id = _required_id(source)
    title = _object_title(source, "data_source")
    url = _optional_string(source.get("url"))
    last_edited = _optional_string(source.get("last_edited_time")) or ""
    properties = source.get("properties")
    schema: list[str] = []
    if isinstance(properties, Mapping):
        for name in sorted(properties, key=str.casefold):
            raw = properties[name]
            if isinstance(name, str) and isinstance(raw, Mapping):
                property_type = _optional_string(raw.get("type")) or "unknown"
                schema.append(f"- **{name}:** `{property_type}`")
    parent = _render_parent(source.get("parent"))
    core = _provenance_header(
        kind="data_source",
        external_id=external_id,
        url=url,
        last_edited=last_edited,
        parent=parent,
    )
    content_without_revision = "\n\n".join(
        [f"# {title}", core, "## Schema", "\n".join(schema) or "_No properties exposed._"]
    )
    revision = _revision(last_edited, content_without_revision)
    content = content_without_revision.replace(
        f"> Last edited: {last_edited or 'unknown'}",
        f"> Last edited: {last_edited or 'unknown'}\n> Revision: {revision}",
        1,
    )
    return _RenderedObject(
        external_id,
        "data_source",
        title,
        url,
        last_edited,
        revision,
        content,
    )


def _revision(last_edited: str, content: str) -> str:
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]
    return f"{last_edited or 'unknown'}:{digest}"


def factory() -> NotionProvider:
    return NotionProvider()


__all__ = [
    "HttpxNotionTransport",
    "NOTION_API_VERSION",
    "NOTION_CONNECTOR_ID",
    "NOTION_CREDENTIAL_KIND",
    "NOTION_CURSOR_STREAM",
    "NotionError",
    "NotionNotFoundError",
    "NotionPermissionError",
    "NotionProvider",
    "NotionRateLimitError",
    "NotionResponseError",
    "NotionTimeoutError",
    "NotionTransport",
    "factory",
    "manifest",
]
