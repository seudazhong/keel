"""Sanitized provider-local coverage for the read-only Notion connector."""

from __future__ import annotations

import hmac
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from keel_core.connector_contracts import (
    ConnectorAuthenticationError,
    ConnectorBinding,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCursor,
    ConnectorHealthStatus,
    ConnectorOperationContext,
    ConnectorResource,
)
from keel_core.connector_credentials import ConnectorCredentialStore, CredentialEnvelope
from keel_core.connector_providers.notion import (
    NOTION_API_VERSION,
    NOTION_CONNECTOR_ID,
    NOTION_CREDENTIAL_KIND,
    NOTION_CURSOR_STREAM,
    HttpxNotionTransport,
    NotionNotFoundError,
    NotionPermissionError,
    NotionProvider,
    NotionRateLimitError,
    NotionTimeoutError,
    NotionTransport,
    manifest,
)
from keel_core.connector_registry import (
    ConnectorRegistration,
    ConnectorRegistry,
    discover_connector_registry,
)
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.connector_service import ConnectorService, DurableConnectorChangeSink
from keel_core.knowledge.models import (
    CreateKnowledgeDocumentCommand,
    DeleteKnowledgeCommand,
    UpdateKnowledgeDocumentCommand,
)
from keel_core.secrets import EnvelopeCipher
from keel_core.tokens import InMemoryTokenStore
from keel_core.types import ContentTaint

_TOKEN = "secret_internal_fixture_token"


@dataclass(frozen=True)
class _Request:
    method: str
    path: str
    payload: Mapping[str, Any] | None


class _Transport(NotionTransport):
    def __init__(
        self,
        handler: Callable[[str, str, Mapping[str, Any] | None], dict[str, Any]],
    ) -> None:
        self.handler = handler
        self.requests: list[_Request] = []

    async def request(
        self,
        method: str,
        path: str,
        token: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        assert token == _TOKEN
        self.requests.append(_Request(method, path, payload))
        return self.handler(method, path, payload)


def _credential() -> CredentialEnvelope:
    return CredentialEnvelope(NOTION_CREDENTIAL_KIND, {"token": _TOKEN})


def _binding(binding_id: str = "binding-notion") -> ConnectorBinding:
    return ConnectorBinding(
        binding_id,
        "scope:a",
        NOTION_CONNECTOR_ID,
        ConnectorBindingStatus.connected,
    )


def _resource(
    external_id: str,
    kind: str = "page",
    *,
    resource_id: str | None = None,
) -> ConnectorResource:
    return ConnectorResource(
        resource_id or f"resource-{external_id}",
        "scope:a",
        NOTION_CONNECTOR_ID,
        "binding-notion",
        external_id,
        kind,
        external_id,
        selected=True,
    )


def _context(
    *resources: ConnectorResource,
    cursor: str | None = None,
    binding: ConnectorBinding | None = None,
) -> ConnectorOperationContext:
    current = binding or _binding()
    cursors = (
        ()
        if cursor is None
        else (
            ConnectorCursor(
                "cursor-1",
                "scope:a",
                NOTION_CONNECTOR_ID,
                current.id,
                NOTION_CURSOR_STREAM,
                cursor,
            ),
        )
    )
    return ConnectorOperationContext(
        "scope:a",
        NOTION_CONNECTOR_ID,
        binding=current,
        credential=_credential(),
        credential_version=1,
        resources=resources,
        cursors=cursors,
    )


def _page(
    page_id: str,
    title: str,
    edited: str,
    *,
    archived: bool = False,
    parent: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "object": "page",
        "id": page_id,
        "url": f"https://notion.so/{page_id}",
        "last_edited_time": edited,
        "archived": archived,
        "in_trash": archived,
        "parent": dict(parent or {"type": "workspace", "workspace": True}),
        "properties": {
            "Name": {
                "type": "title",
                "title": [{"plain_text": title, "annotations": {}}],
            },
            "Status": {
                "type": "status",
                "status": {"name": "Ready"},
            },
        },
    }


def _data_source(data_source_id: str, edited: str = "2026-07-18T00:00:00.000Z") -> dict[str, Any]:
    return {
        "object": "data_source",
        "id": data_source_id,
        "url": f"https://notion.so/{data_source_id}",
        "last_edited_time": edited,
        "parent": {"type": "database_id", "database_id": "database-parent"},
        "title": [{"plain_text": "Project data", "annotations": {}}],
        "properties": {
            "Name": {"type": "title"},
            "Owner": {"type": "people"},
        },
    }


def _page_result(results: list[dict[str, Any]], *, cursor: str | None = None) -> dict[str, Any]:
    return {
        "results": results,
        "has_more": cursor is not None,
        "next_cursor": cursor,
    }


def test_builtin_registry_discovers_notion_independently() -> None:
    registry = discover_connector_registry()
    assert NOTION_CONNECTOR_ID in {item.id for item in registry.manifests()}
    assert registry.create(NOTION_CONNECTOR_ID).manifest is manifest


async def test_httpx_transport_sends_live_auth_and_version_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = False

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal observed
        observed = True
        authorization = request.headers.get("Authorization")
        assert authorization is not None
        scheme, separator, credential = authorization.partition(" ")
        assert scheme == "Bearer"
        assert separator == " "
        assert hmac.compare_digest(credential, _TOKEN)
        assert request.headers.get("Notion-Version") == NOTION_API_VERSION
        return httpx.Response(200, json={"object": "user", "id": "bot-user"})

    mock_transport = httpx.MockTransport(handle)
    async_client = httpx.AsyncClient

    def client_factory(**kwargs: Any) -> httpx.AsyncClient:
        kwargs["transport"] = mock_transport
        return async_client(**kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    result = await HttpxNotionTransport().request("GET", "/users/me", _TOKEN)
    assert observed is True
    assert result == {"object": "user", "id": "bot-user"}


async def test_setup_validates_token_and_never_echoes_it() -> None:
    transport = _Transport(
        lambda method, path, payload: {
            "object": "user",
            "id": "bot-user",
            "name": "Fixture workspace",
            "workspace_id": "workspace-1",
            "bot": {"owner": {"type": "workspace", "workspace": True}},
        }
    )
    result = await NotionProvider(transport).setup(
        ConnectorOperationContext("scope:a", NOTION_CONNECTOR_ID),
        {"token": _TOKEN},
    )
    assert result.binding.display_name == "Fixture workspace"
    assert result.binding.metadata["read_only"] is True
    assert result.credential is not None
    assert result.credential.values == {"token": _TOKEN}
    assert _TOKEN not in repr(result)
    assert _TOKEN not in repr(result.credential)
    assert _TOKEN not in json.dumps(result.binding.metadata)
    assert transport.requests == [_Request("GET", "/users/me", None)]

    class RejectingTransport(NotionTransport):
        async def request(
            self,
            method: str,
            path: str,
            token: str,
            payload: Mapping[str, Any] | None = None,
        ) -> dict[str, Any]:
            raise ConnectorAuthenticationError("Notion rejected the integration token.")

    with pytest.raises(ConnectorAuthenticationError) as error:
        await NotionProvider(RejectingTransport()).setup(
            ConnectorOperationContext("scope:a", NOTION_CONNECTOR_ID),
            {"token": _TOKEN},
        )
    assert _TOKEN not in str(error.value)


async def test_discovery_returns_only_shared_pages_and_data_sources_with_pagination() -> None:
    shared_page = _page("page-shared", "Shared page", "2026-07-18T00:00:00.000Z")
    shared_source = _data_source("source-shared")

    def handler(
        method: str,
        path: str,
        payload: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        assert (method, path) == ("POST", "/search")
        if payload is not None and payload.get("start_cursor") == "next":
            return _page_result([shared_source])
        return _page_result(
            [shared_page, {"object": "user", "id": "not-a-resource"}],
            cursor="next",
        )

    transport = _Transport(handler)
    result = await NotionProvider(transport).list_resources(_context())
    assert [(item.external_id, item.kind) for item in result.resources] == [
        ("source-shared", "data_source"),
        ("page-shared", "page"),
    ]
    assert "page-unshared" not in {item.external_id for item in result.resources}
    assert transport.requests[1].payload is not None
    assert transport.requests[1].payload["start_cursor"] == "next"


async def test_sync_renders_paginated_nested_blocks_and_marks_unsupported_content() -> None:
    root = _page("page-root", "Root", "2026-07-18T01:00:00.000Z")
    child = _page(
        "page-child",
        "Child",
        "2026-07-18T01:01:00.000Z",
        parent={"type": "page_id", "page_id": "page-root"},
    )

    def handler(
        method: str,
        path: str,
        payload: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if (method, path) == ("POST", "/search"):
            return _page_result([root, child])
        if (method, path) == ("GET", "/pages/page-root"):
            return root
        if (method, path) == ("GET", "/pages/page-child"):
            return child
        if path == "/blocks/page-root/children?page_size=100":
            return _page_result(
                [
                    {
                        "id": "heading",
                        "type": "heading_1",
                        "heading_1": {"rich_text": [{"plain_text": "Overview", "annotations": {}}]},
                        "has_children": False,
                    },
                    {
                        "id": "toggle",
                        "type": "toggle",
                        "toggle": {"rich_text": [{"plain_text": "Details", "annotations": {}}]},
                        "has_children": True,
                    },
                ],
                cursor="block-next",
            )
        if path == ("/blocks/page-root/children?page_size=100&start_cursor=block-next"):
            return _page_result(
                [
                    {
                        "id": "page-child",
                        "type": "child_page",
                        "child_page": {"title": "Child"},
                        "has_children": False,
                    }
                ]
            )
        if path == "/blocks/toggle/children?page_size=100":
            return _page_result(
                [
                    {
                        "id": "unsupported",
                        "type": "unsupported_future_block",
                        "unsupported_future_block": {},
                        "has_children": False,
                    }
                ]
            )
        if path == "/blocks/page-child/children?page_size=100":
            return _page_result(
                [
                    {
                        "id": "paragraph",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [
                                {
                                    "plain_text": "tainted child content",
                                    "annotations": {"bold": True},
                                }
                            ]
                        },
                        "has_children": False,
                    }
                ]
            )
        raise AssertionError((method, path, payload))

    result = await NotionProvider(_Transport(handler)).sync(_context(_resource("page-root")))
    assert len(result.changes) == 2
    by_id = {item.provenance.external_resource_id: item for item in result.changes}
    root_change = by_id["page-root"]
    assert root_change.taint is ContentTaint.tainted
    assert root_change.provenance.source_url == "https://notion.so/page-root"
    assert root_change.provenance.revision is not None
    assert "> Notion ID: page-root" in (root_change.content or "")
    assert "> Last edited: 2026-07-18T01:00:00.000Z" in (root_change.content or "")
    assert "[Unsupported Notion block: unsupported_future_block]" in (root_change.content or "")
    assert "**tainted child content**" in (by_id["page-child"].content or "")
    cursor = result.cursor_updates[0]
    inventory = json.loads(cursor.value)
    assert inventory["roots"]["page-root"] == ["page-child", "page-root"]


async def test_incremental_update_archive_delete_and_malformed_cursor_recovery() -> None:
    pages: dict[str, dict[str, Any]] = {
        "root": _page("page-root", "Root", "2026-07-18T01:00:00.000Z"),
        "child": _page("page-child", "Child", "2026-07-18T01:00:00.000Z"),
    }
    visible = [True]

    def handler(
        method: str,
        path: str,
        payload: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if (method, path) == ("POST", "/search"):
            results = [pages["root"], pages["child"]] if visible[0] else []
            return _page_result(results)
        if path == "/pages/page-root":
            return pages["root"]
        if path == "/pages/page-child":
            return pages["child"]
        if path == "/blocks/page-root/children?page_size=100":
            return _page_result(
                [
                    {
                        "id": "page-child",
                        "type": "child_page",
                        "child_page": {"title": "Child"},
                        "has_children": False,
                    },
                    {
                        "id": "root-text",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [
                                {
                                    "plain_text": f"body {pages['root']['last_edited_time']}",
                                    "annotations": {},
                                }
                            ]
                        },
                        "has_children": False,
                    },
                ]
            )
        if path == "/blocks/page-child/children?page_size=100":
            return _page_result([])
        raise AssertionError((method, path, payload))

    provider = NotionProvider(_Transport(handler))
    initial = await provider.sync(_context(_resource("page-root"), cursor="{broken"))
    assert {item.provenance.external_resource_id for item in initial.changes} == {
        "page-root",
        "page-child",
    }
    cursor = initial.cursor_updates[0].value

    pages["root"] = _page("page-root", "Renamed", "2026-07-18T02:00:00.000Z")
    pages["child"] = _page(
        "page-child",
        "Child",
        "2026-07-18T01:00:00.000Z",
        archived=True,
    )
    updated = await provider.sync(_context(_resource("page-root"), cursor=cursor))
    update_operations = [
        (item.kind.value, item.provenance.external_resource_id) for item in updated.changes
    ]
    assert update_operations == [
        ("upsert", "page-root"),
        ("delete", "page-child"),
    ]
    assert updated.changes[0].title == "Renamed"

    visible[0] = False
    deleted = await provider.sync(
        _context(_resource("page-root"), cursor=updated.cursor_updates[0].value)
    )
    delete_operations = [
        (item.kind.value, item.provenance.external_resource_id) for item in deleted.changes
    ]
    assert delete_operations == [("delete", "page-root")]


async def test_duplicate_page_across_selected_roots_is_upserted_once() -> None:
    page = _page(
        "page-row",
        "Row",
        "2026-07-18T03:00:00.000Z",
        parent={"type": "data_source_id", "data_source_id": "source-root"},
    )
    source = _data_source("source-root")

    def handler(
        method: str,
        path: str,
        payload: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if (method, path) == ("POST", "/search"):
            return _page_result([page, source])
        if path == "/pages/page-row":
            return page
        if path == "/blocks/page-row/children?page_size=100":
            return _page_result([])
        if path == "/data_sources/source-root":
            return source
        if (method, path) == ("POST", "/data_sources/source-root/query"):
            return _page_result([page])
        raise AssertionError((method, path, payload))

    transport = _Transport(handler)
    result = await NotionProvider(transport).sync(
        _context(_resource("page-row"), _resource("source-root", "data_source"))
    )
    ids = [item.provenance.external_resource_id for item in result.changes]
    assert ids.count("page-row") == 1
    assert ids.count("source-root") == 1
    source_change = next(
        item for item in result.changes if item.provenance.external_resource_id == "source-root"
    )
    assert "> Parent: database_id:database-parent" in (source_change.content or "")
    assert "**Owner:** `people`" in (source_change.content or "")
    assert sum(item.path == "/pages/page-row" for item in transport.requests) == 1


@pytest.mark.parametrize(
    ("error", "status", "retryable", "message_fragment"),
    [
        (
            ConnectorAuthenticationError("sanitized"),
            ConnectorHealthStatus.error,
            False,
            "401",
        ),
        (NotionPermissionError("sanitized"), ConnectorHealthStatus.error, False, "403"),
        (NotionNotFoundError("sanitized"), ConnectorHealthStatus.error, False, "404"),
        (NotionRateLimitError(), ConnectorHealthStatus.degraded, True, "429"),
        (NotionTimeoutError("sanitized"), ConnectorHealthStatus.degraded, True, "timed out"),
    ],
)
async def test_health_classifies_provider_failures_without_secret_echo(
    error: Exception,
    status: ConnectorHealthStatus,
    retryable: bool,
    message_fragment: str,
) -> None:
    class FailingTransport(NotionTransport):
        async def request(
            self,
            method: str,
            path: str,
            token: str,
            payload: Mapping[str, Any] | None = None,
        ) -> dict[str, Any]:
            raise error

    health = await NotionProvider(FailingTransport()).health(_context())
    assert health.status is status
    assert health.retryable is retryable
    assert message_fragment in (health.message or "")
    assert _TOKEN not in (health.message or "")


async def test_target_is_required_before_provider_work() -> None:
    called = False

    def handler(
        method: str,
        path: str,
        payload: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        nonlocal called
        called = True
        raise AssertionError

    transport = _Transport(handler)
    registry = ConnectorRegistry(
        (
            ConnectorRegistration(
                manifest,
                lambda: NotionProvider(transport),
                "tests.notion",
            ),
        )
    )
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        NOTION_CONNECTOR_ID,
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
    )
    credentials = ConnectorCredentialStore(
        InMemoryTokenStore("scope:a", EnvelopeCipher("test-key"))
    )
    await credentials.put(NOTION_CONNECTOR_ID, _credential())
    service = ConnectorService(registry, repository, credentials=credentials)
    with pytest.raises(RuntimeError, match="configured targets"):
        await service.sync(NOTION_CONNECTOR_ID, binding.id)
    assert called is False


async def test_retain_vs_purge_disconnect_and_old_job_cannot_resurrect() -> None:
    page = _page("page-root", "Root", "2026-07-18T04:00:00.000Z")

    def handler(
        method: str,
        path: str,
        payload: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if path == "/users/me":
            return {"object": "user", "id": "bot", "name": "Notion"}
        if (method, path) == ("POST", "/search"):
            return _page_result([page])
        if path == "/pages/page-root":
            return page
        if path == "/blocks/page-root/children?page_size=100":
            return _page_result([])
        raise AssertionError((method, path, payload))

    transport = _Transport(handler)
    registry = ConnectorRegistry(
        (
            ConnectorRegistration(
                manifest,
                lambda: NotionProvider(transport),
                "tests.notion",
            ),
        )
    )
    repository = InMemoryConnectorRepository("scope:a")
    credentials = ConnectorCredentialStore(
        InMemoryTokenStore("scope:a", EnvelopeCipher("test-key"))
    )
    knowledge_calls: list[tuple[str, str]] = []

    class Knowledge:
        async def get_base(self, kb_id: str) -> Any:
            return object()

        async def create_document(
            self,
            kb_id: str,
            command: CreateKnowledgeDocumentCommand,
            idempotency_key: str,
        ) -> Any:
            knowledge_calls.append(("create", command.source_uri or ""))
            return SimpleNamespace(document=SimpleNamespace(id=f"document-{len(knowledge_calls)}"))

        async def update_document(
            self,
            kb_id: str,
            document_id: str,
            command: UpdateKnowledgeDocumentCommand,
            idempotency_key: str,
        ) -> Any:
            knowledge_calls.append(("update", document_id))
            return object()

        async def delete_document(
            self,
            kb_id: str,
            document_id: str,
            command: DeleteKnowledgeCommand,
            idempotency_key: str,
        ) -> Any:
            knowledge_calls.append(("delete", document_id))
            return object()

    sink = DurableConnectorChangeSink(repository, knowledge=Knowledge())
    service = ConnectorService(
        registry,
        repository,
        credentials=credentials,
        change_sink=sink,
    )

    setup = await service.setup(NOTION_CONNECTOR_ID, {"token": _TOKEN})
    old_binding_id = setup.binding.id
    await service.configure_targets(NOTION_CONNECTOR_ID, {"knowledge": "kb-1"})
    await service.refresh_resources(NOTION_CONNECTOR_ID)
    await service.select_resources(NOTION_CONNECTOR_ID, {"page-root"})
    await service.sync(NOTION_CONNECTOR_ID, old_binding_id)
    assert knowledge_calls == [("create", "https://notion.so/page-root")]

    assert await service.revoke(NOTION_CONNECTOR_ID, purge=False)
    assert knowledge_calls == [("create", "https://notion.so/page-root")]

    setup = await service.setup(NOTION_CONNECTOR_ID, {"token": _TOKEN})
    await service.configure_targets(NOTION_CONNECTOR_ID, {"knowledge": "kb-1"})
    await service.refresh_resources(NOTION_CONNECTOR_ID)
    await service.select_resources(NOTION_CONNECTOR_ID, {"page-root"})
    await service.sync(NOTION_CONNECTOR_ID, setup.binding.id)
    assert knowledge_calls[-1][0] == "create"
    assert await service.revoke(NOTION_CONNECTOR_ID, purge=True)
    assert knowledge_calls[-1][0] == "delete"

    reconnected = await service.setup(NOTION_CONNECTOR_ID, {"token": _TOKEN})
    with pytest.raises(ValueError, match="binding changed"):
        await service.sync(NOTION_CONNECTOR_ID, setup.binding.id)
    assert reconnected.binding.id != setup.binding.id
