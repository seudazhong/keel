from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from keel_core.connector_contracts import (
    ConnectorActionContext,
    ConnectorBinding,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorHealthStatus,
    ConnectorIngressRequest,
    ConnectorItem,
    ConnectorItemDraft,
    ConnectorOperationContext,
    ConnectorResource,
    ConnectorResourceDraft,
    ConnectorTargetKind,
)
from keel_core.connector_credentials import ConnectorCredentialStore
from keel_core.connector_providers.feishu._auth import FeishuCredential
from keel_core.connector_providers.feishu._client import FeishuApiError
from keel_core.connector_providers.feishu._crypto import (
    encrypt_event_for_test,
    event_signature,
)
from keel_core.connector_providers.feishu._provider import (
    FEISHU_REQUIRED_SCOPES,
    REPLY_ACTION,
    FeishuProvider,
    manifest,
)
from keel_core.connector_providers.feishu._workspace import discover_resources, sync_workspace
from keel_core.connector_registry import ConnectorRegistration, ConnectorRegistry
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.connector_service import CallbackConnectorChangeSink, ConnectorService
from keel_core.connectors import ConnectorTool
from keel_core.outbox import InMemoryOutboundStore
from keel_core.protocols import ToolContext
from keel_core.secrets import EnvelopeCipher
from keel_core.tokens import InMemoryTokenStore
from keel_core.types import ContentTaint

Handler = Callable[
    [str, str, str | None, Mapping[str, str | int], Mapping[str, Any] | None],
    dict[str, Any],
]


class FakeFeishuClient:
    def __init__(self, handler: Handler) -> None:
        self.handler = handler
        self.calls: list[tuple[str, str, dict[str, str | int], dict[str, Any] | None]] = []

    async def request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: Mapping[str, str | int] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        query = dict(params or {})
        payload = dict(body) if body is not None else None
        self.calls.append((method, path, query, payload))
        return self.handler(method, path, token, query, payload)


def _credential(*, expired: bool = False) -> FeishuCredential:
    return FeishuCredential(
        app_id="cli_app",
        app_secret="app-secret",
        tenant_key="tenant-a",
        verification_token="verify-token",
        encrypt_key="encrypt-key",
        tenant_access_token="tenant-token",
        token_expires_at=datetime.now(UTC)
        + (timedelta(minutes=-1) if expired else timedelta(hours=1)),
        bot_open_id="ou_bot",
        bot_name="Keel",
    )


def _binding(*, metadata: dict[str, Any] | None = None) -> ConnectorBinding:
    return ConnectorBinding(
        id="binding-1",
        scope_id="scope-a",
        connector_id="feishu",
        status=ConnectorBindingStatus.connected,
        external_account_id="cli_app",
        external_tenant_id="tenant-a",
        metadata=metadata or {},
    )


def _chat_resource(*, selected: bool = True, scope_id: str = "scope-a") -> ConnectorResource:
    return ConnectorResource(
        id="resource-chat",
        scope_id=scope_id,
        connector_id="feishu",
        binding_id="binding-1",
        external_id="chat:oc_chat",
        kind="chat",
        display_name="Project chat",
        selected=selected,
        config={"chat_id": "oc_chat"},
    )


def _message_event(*, chat_type: str = "group", tenant_key: str = "tenant-a") -> dict[str, Any]:
    return {
        "schema": "2.0",
        "header": {
            "event_id": "event-1",
            "event_type": "im.message.receive_v1",
            "create_time": "1700000000000",
            "token": "verify-token",
            "tenant_key": tenant_key,
        },
        "event": {
            "sender": {
                "sender_id": {"open_id": "ou_user"},
                "sender_type": "user",
                "tenant_key": tenant_key,
            },
            "message": {
                "message_id": "om_message",
                "root_id": "om_root",
                "parent_id": "om_parent",
                "create_time": "1700000000000",
                "chat_id": "oc_chat",
                "chat_type": chat_type,
                "message_type": "text",
                "content": json.dumps({"text": "@_user_1 hello"}),
                "mentions": [{"key": "@_user_1", "id": {"open_id": "ou_bot"}}],
            },
        },
    }


def _encrypted_request(payload: dict[str, Any], *, now: datetime) -> ConnectorIngressRequest:
    encrypted = encrypt_event_for_test("encrypt-key", json.dumps(payload).encode())
    body = json.dumps({"encrypt": encrypted}, separators=(",", ":")).encode()
    timestamp = str(int(now.timestamp()))
    nonce = "nonce-1"
    return ConnectorIngressRequest(
        method="POST",
        query={},
        headers={
            "X-Lark-Request-Timestamp": timestamp,
            "X-Lark-Request-Nonce": nonce,
            "X-Lark-Signature": event_signature(timestamp, nonce, "encrypt-key", body),
        },
        body=body,
        public_url="https://keel.example/v1/connectors/feishu/webhook",
    )


@pytest.mark.asyncio
async def test_setup_encrypts_app_and_webhook_secrets_and_verifies_tenant() -> None:
    def handler(
        method: str,
        path: str,
        token: str | None,
        params: Mapping[str, str | int],
        body: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if path.endswith("tenant_access_token/internal"):
            assert body == {"app_id": "cli_app", "app_secret": "app-secret"}
            return {"code": 0, "tenant_access_token": "tenant-token", "expire": 7200}
        if path.endswith("tenant/query"):
            return {"code": 0, "data": {"tenant": {"tenant_key": "tenant-a", "name": "Acme"}}}
        if path.endswith("/scopes"):
            return {
                "code": 0,
                "data": {
                    "scopes": [
                        {"scope_name": scope, "grant_status": 1}
                        for scope in FEISHU_REQUIRED_SCOPES
                    ]
                },
            }
        if path.endswith("/bot/v3/info/"):
            return {"code": 0, "bot": {"open_id": "ou_bot", "app_name": "Keel"}}
        raise AssertionError(path)

    provider = FeishuProvider(lambda: FakeFeishuClient(handler))
    result = await provider.setup(
        context=ConnectorOperationContext(
            scope_id="scope-a",
            connector_id="feishu",
            callback_base_url="https://keel.example/v1/connectors/feishu",
        ),
        values={
            "app_id": "cli_app",
            "app_secret": "app-secret",
            "tenant_key": "tenant-a",
            "verification_token": "verify-token",
            "encrypt_key": "encrypt-key",
        },
    )
    assert result.binding.external_tenant_id == "tenant-a"
    assert result.binding.metadata["webhook_url"].endswith("/webhook")
    assert "app-secret" not in json.dumps(result.binding.metadata)
    assert "verify-token" not in json.dumps(result.binding.metadata)
    assert "encrypt-key" not in json.dumps(result.binding.metadata)
    assert result.credential is not None
    assert result.credential.values["app_secret"] == "app-secret"
    assert result.credential.values["verification_token"] == "verify-token"
    assert result.credential.values["encrypt_key"] == "encrypt-key"


@pytest.mark.asyncio
async def test_setup_stages_credentials_until_tenant_authorization() -> None:
    def handler(
        method: str,
        path: str,
        token: str | None,
        params: Mapping[str, str | int],
        body: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        raise FeishuApiError(99991663, "app is not installed in this tenant", status_code=403)

    result = await FeishuProvider(lambda: FakeFeishuClient(handler)).setup(
        ConnectorOperationContext(
            scope_id="scope-a",
            connector_id="feishu",
            callback_base_url="https://keel.example/v1/connectors/feishu",
        ),
        {
            "app_id": "cli_app",
            "app_secret": "app-secret",
            "tenant_key": "tenant-a",
            "verification_token": "verify-token",
            "encrypt_key": "encrypt-key",
        },
    )
    assert result.status is ConnectorBindingStatus.authorizing
    assert result.binding.metadata["authorization_state"] == "pending_tenant_install"
    assert result.credential is not None
    assert result.credential.values["tenant_access_token"] == ""


@pytest.mark.asyncio
async def test_sync_rotates_expired_tenant_token_with_credential_cas_update() -> None:
    def handler(
        method: str,
        path: str,
        token: str | None,
        params: Mapping[str, str | int],
        body: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        assert path.endswith("tenant_access_token/internal")
        return {"code": 0, "tenant_access_token": "rotated-token", "expire": 7200}

    provider = FeishuProvider(lambda: FakeFeishuClient(handler))
    result = await provider.sync(
        ConnectorOperationContext(
            scope_id="scope-a",
            connector_id="feishu",
            binding=_binding(),
            credential=_credential(expired=True).envelope(),
            credential_version=7,
        )
    )
    assert result.state.credential is not None
    assert result.state.credential.expected_version == 7
    assert result.state.credential.credential.values["tenant_access_token"] == "rotated-token"


@pytest.mark.asyncio
async def test_challenge_decryption_signature_thread_mapping_and_replay() -> None:
    provider = FeishuProvider(lambda: FakeFeishuClient(lambda *_: {"code": 0}))
    context = ConnectorOperationContext(
        scope_id="scope-a",
        connector_id="feishu",
        binding=_binding(),
        credential=_credential().envelope(),
        credential_version=1,
        resources=(_chat_resource(),),
    )
    challenge = await provider.ingress(
        context,
        ConnectorIngressRequest(
            method="POST",
            query={},
            headers={},
            body=json.dumps(
                {
                    "type": "url_verification",
                    "token": "verify-token",
                    "challenge": "challenge-1",
                }
            ).encode(),
            public_url="https://keel.example/v1/connectors/feishu/webhook",
        ),
    )
    assert challenge.delivery_id is None
    assert json.loads(challenge.response.body) == {"challenge": "challenge-1"}

    repository = InMemoryConnectorRepository("scope-a")
    binding = await repository.upsert_binding(
        "feishu",
        ConnectorBindingDraft(external_tenant_id="tenant-a"),
        ConnectorBindingStatus.connected,
    )
    await repository.upsert_resources(
        "feishu",
        binding.id,
        (
            ConnectorResourceDraft(
                external_id="chat:oc_chat",
                kind="chat",
                display_name="Project chat",
                selected=True,
            ),
        ),
    )
    token_store = InMemoryTokenStore("scope-a", EnvelopeCipher("test-key"))
    credentials = ConnectorCredentialStore(token_store)
    await credentials.put("feishu", _credential().envelope())
    changes: list[ConnectorChange] = []

    async def capture(change: ConnectorChange) -> None:
        changes.append(change)

    service = ConnectorService(
        ConnectorRegistry((ConnectorRegistration(manifest, lambda: provider, "test.feishu"),)),
        repository,
        credentials=credentials,
        change_sink=CallbackConnectorChangeSink(event=capture),
    )
    now = datetime.now(UTC)
    request = _encrypted_request(_message_event(), now=now)
    first = await service.ingress("feishu", request)
    second = await service.ingress("feishu", request)
    assert first.accepted is True
    assert second.accepted is False
    assert len(changes) == 1
    event = changes[0].event
    assert event is not None
    assert event.payload["thread_id"] == "om_root"
    assert event.payload["sender_open_id"] == "ou_user"
    assert event.payload["text"] == "hello"
    assert changes[0].taint is ContentTaint.tainted

    bad = _encrypted_request(_message_event(), now=now)
    bad = ConnectorIngressRequest(
        method=bad.method,
        query=bad.query,
        headers={**bad.headers, "X-Lark-Signature": "0" * 64},
        body=bad.body,
        public_url=bad.public_url,
    )
    with pytest.raises(Exception, match="signature"):
        await provider.ingress(context, bad)
    stale = _encrypted_request(_message_event(), now=now - timedelta(minutes=10))
    with pytest.raises(Exception, match="timestamp"):
        await provider.ingress(context, stale)


@pytest.mark.asyncio
async def test_chat_and_tenant_grants_isolate_inbound_events() -> None:
    provider = FeishuProvider(lambda: FakeFeishuClient(lambda *_: {"code": 0}))
    now = datetime.now(UTC)
    unselected = ConnectorOperationContext(
        scope_id="scope-a",
        connector_id="feishu",
        binding=_binding(),
        credential=_credential().envelope(),
        credential_version=1,
        resources=(_chat_resource(selected=False),),
    )
    result = await provider.ingress(
        unselected,
        _encrypted_request(_message_event(), now=now),
    )
    assert result.changes == ()

    selected = ConnectorOperationContext(
        scope_id="scope-a",
        connector_id="feishu",
        binding=_binding(),
        credential=_credential().envelope(),
        credential_version=1,
        resources=(_chat_resource(),),
    )
    with pytest.raises(Exception, match="tenant"):
        await provider.ingress(
            selected,
            _encrypted_request(_message_event(tenant_key="tenant-b"), now=now),
        )


@pytest.mark.asyncio
async def test_duplicate_reply_uses_outbox_and_requires_selected_chat() -> None:
    replies = 0

    def handler(
        method: str,
        path: str,
        token: str | None,
        params: Mapping[str, str | int],
        body: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        nonlocal replies
        assert path.endswith("/om_message/reply")
        replies += 1
        return {"code": 0, "data": {"message_id": "om_reply"}}

    client = FakeFeishuClient(handler)
    token_store = InMemoryTokenStore("scope-a", EnvelopeCipher("test-key"))
    await token_store.put("feishu", _credential().envelope().serialize())
    async def load_state(connector_id: str) -> ConnectorOperationContext:
        assert connector_id == "feishu"
        return ConnectorOperationContext(
            scope_id="scope-a",
            connector_id="feishu",
            binding=_binding(),
            resources=(_chat_resource(),),
        )

    action_context = ConnectorActionContext(
        "scope-a",
        credential_store=token_store,
        _state_loader=load_state,
    )
    action = FeishuProvider(lambda: client).build_actions(action_context)[0]
    tool = ConnectorTool(
        name=REPLY_ACTION.name,
        description=REPLY_ACTION.description,
        action=action.action,
        outbound=True,
        idempotency_required=True,
        idempotency_store=InMemoryOutboundStore(),
        input_schema=dict(REPLY_ACTION.input_schema),
    )
    arguments = {
        "chat_id": "oc_chat",
        "message_id": "om_message",
        "thread_id": "om_root",
        "text": "reply",
        "idempotency_key": "reply-1",
    }
    tool_context = ToolContext(scope_id="scope-a", session_id="session-a")
    first = await tool.run(arguments, tool_context)
    second = await tool.run(arguments, tool_context)
    assert first.output == second.output == "replied (id=om_reply)"
    assert replies == 1

    async def load_unselected(connector_id: str) -> ConnectorOperationContext:
        return ConnectorOperationContext(
            scope_id="scope-a",
            connector_id=connector_id,
            binding=_binding(),
            resources=(),
        )

    denied = ConnectorActionContext(
        "scope-a",
        credential_store=token_store,
        _state_loader=load_unselected,
    )
    denied_action = FeishuProvider(lambda: client).build_actions(denied)[0]
    with pytest.raises(PermissionError, match="not selected"):
        await denied_action.action(arguments, tool_context)


@pytest.mark.asyncio
async def test_resource_discovery_lists_docs_wiki_drive_and_chats_but_excludes_base() -> None:
    def handler(
        method: str,
        path: str,
        token: str | None,
        params: Mapping[str, str | int],
        body: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if path.endswith("/wiki/v2/spaces"):
            return {
                "code": 0,
                "data": {
                    "items": [{"space_id": "space-a", "name": "Engineering"}],
                    "has_more": False,
                },
            }
        if path.endswith("/drive/v1/files"):
            if params.get("page_token") == "drive-next":
                return {
                    "code": 0,
                    "data": {
                        "items": [
                            {"type": "folder", "token": "folder-a", "name": "Shared"}
                        ],
                        "has_more": False,
                    },
                }
            return {
                "code": 0,
                "data": {
                    "items": [
                        {"type": "docx", "token": "doc-a", "name": "Runbook"},
                        {"type": "bitable", "token": "base-a", "name": "Excluded"},
                    ],
                    "has_more": True,
                    "page_token": "drive-next",
                },
            }
        if path.endswith("/im/v1/chats"):
            return {
                "code": 0,
                "data": {
                    "items": [{"chat_id": "oc_chat", "name": "Project chat"}],
                    "has_more": False,
                },
            }
        raise AssertionError((path, params))

    result = await discover_resources(FakeFeishuClient(handler), "tenant-token")
    by_id = {resource.external_id: resource for resource in result.resources}
    assert by_id["wiki:space-a"].kind == "wiki_space"
    assert by_id["docx:doc-a"].kind == "docs_document"
    assert by_id["drive:folder-a"].kind == "drive_folder"
    assert by_id["chat:oc_chat"].kind == "chat"
    assert "bitable:base-a" not in by_id


@pytest.mark.asyncio
async def test_docs_wiki_pagination_update_delete_and_taint() -> None:
    def handler(
        method: str,
        path: str,
        token: str | None,
        params: Mapping[str, str | int],
        body: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if path.endswith("/nodes"):
            if params.get("page_token") == "next":
                return {
                    "code": 0,
                    "data": {
                        "items": [
                            {
                                "obj_type": "doc",
                                "obj_token": "legacy",
                                "title": "Legacy",
                                "node_token": "wiki-legacy",
                                "obj_edit_time": "3",
                            }
                        ],
                        "has_more": False,
                    },
                }
            return {
                "code": 0,
                "data": {
                    "items": [
                        {
                            "obj_type": "docx",
                            "obj_token": "current",
                            "title": "Current",
                            "node_token": "wiki-current",
                            "obj_edit_time": "2",
                        },
                        {"obj_type": "bitable", "obj_token": "excluded-base"},
                    ],
                    "has_more": True,
                    "page_token": "next",
                },
            }
        if path.endswith("/current/raw_content"):
            return {"code": 0, "data": {"content": "updated content"}}
        if path.endswith("/legacy/raw_content"):
            return {"code": 0, "data": {"content": "legacy content"}}
        raise AssertionError((path, params))

    client = FakeFeishuClient(handler)
    context = ConnectorOperationContext(
        scope_id="scope-a",
        connector_id="feishu",
        binding=_binding(),
        resources=(
            ConnectorResource(
                id="wiki-resource",
                scope_id="scope-a",
                connector_id="feishu",
                binding_id="binding-1",
                external_id="wiki:space-a",
                kind="wiki_space",
                display_name="Engineering",
                selected=True,
                config={"space_id": "space-a"},
            ),
        ),
        items=(
            ConnectorItem(
                id="item-current",
                scope_id="scope-a",
                connector_id="feishu",
                binding_id="binding-1",
                external_id="wiki:space-a/docx:current",
                kind="knowledge_document",
                display_name="Current",
                config={"revision": "1"},
            ),
            ConnectorItem(
                id="item-deleted",
                scope_id="scope-a",
                connector_id="feishu",
                binding_id="binding-1",
                external_id="wiki:space-a/docx:deleted",
                kind="knowledge_document",
                display_name="Deleted",
                config={"revision": "1"},
            ),
            ConnectorItem(
                id="item-other-root",
                scope_id="scope-a",
                connector_id="feishu",
                binding_id="binding-1",
                external_id="drive:folder-b/docx:outside",
                kind="knowledge_document",
                display_name="Outside selected root",
                config={"revision": "1"},
            ),
        ),
    )
    result = await sync_workspace(client, "tenant-token", context)
    by_id = {change.provenance.external_resource_id: change for change in result.changes}
    assert by_id["wiki:space-a/docx:current"].kind is ConnectorChangeKind.upsert
    assert by_id["wiki:space-a/doc:legacy"].kind is ConnectorChangeKind.upsert
    assert by_id["wiki:space-a/docx:deleted"].kind is ConnectorChangeKind.delete
    assert "drive:folder-b/docx:outside" not in by_id
    assert all(change.taint is ContentTaint.tainted for change in result.changes)
    assert "bitable:excluded-base" not in by_id
    node_calls = [call for call in client.calls if call[1].endswith("/nodes")]
    assert len(node_calls) == 2


@pytest.mark.asyncio
async def test_health_reports_permission_shrink_tenant_mismatch_and_uninstall() -> None:
    def handler(
        method: str,
        path: str,
        token: str | None,
        params: Mapping[str, str | int],
        body: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if path.endswith("tenant/query"):
            return {"code": 0, "data": {"tenant": {"tenant_key": "tenant-a", "name": "Acme"}}}
        if path.endswith("/scopes"):
            return {"code": 0, "data": {"scopes": list(FEISHU_REQUIRED_SCOPES[:-1])}}
        raise AssertionError(path)

    provider = FeishuProvider(lambda: FakeFeishuClient(handler))
    health = await provider.health(
        ConnectorOperationContext(
            scope_id="scope-a",
            connector_id="feishu",
            binding=_binding(),
            credential=_credential().envelope(),
            credential_version=1,
        )
    )
    assert health.status is ConnectorHealthStatus.degraded
    assert health.message is not None and "permissions shrank" in health.message

    def healthy_handler(
        method: str,
        path: str,
        token: str | None,
        params: Mapping[str, str | int],
        body: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if path.endswith("tenant/query"):
            return {"code": 0, "data": {"tenant": {"tenant_key": "tenant-a"}}}
        if path.endswith("/scopes"):
            return {"code": 0, "data": {"scopes": list(FEISHU_REQUIRED_SCOPES)}}
        raise AssertionError(path)

    provider = FeishuProvider(lambda: FakeFeishuClient(healthy_handler))
    mismatch = await provider.health(
        ConnectorOperationContext(
            scope_id="scope-a",
            connector_id="feishu",
            binding=replace(_binding(), external_tenant_id="tenant-b"),
            credential=_credential().envelope(),
            credential_version=1,
        )
    )
    assert mismatch.status is ConnectorHealthStatus.error
    assert mismatch.message is not None and "does not match" in mismatch.message

    def uninstalled_handler(
        method: str,
        path: str,
        token: str | None,
        params: Mapping[str, str | int],
        body: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        raise FeishuApiError(99991663, "app was uninstalled", status_code=403)

    uninstalled = await FeishuProvider(lambda: FakeFeishuClient(uninstalled_handler)).health(
        ConnectorOperationContext(
            scope_id="scope-a",
            connector_id="feishu",
            binding=_binding(),
            credential=_credential().envelope(),
            credential_version=1,
        )
    )
    assert uninstalled.status is ConnectorHealthStatus.error
    assert uninstalled.message is not None and "uninstalled" in uninstalled.message


@dataclass
class PurgeSink:
    purged: int = 0

    async def handoff_purge(
        self,
        binding: ConnectorBinding,
        items: tuple[ConnectorItem, ...],
    ) -> int:
        self.purged += len(items)
        return len(items)


@pytest.mark.asyncio
async def test_knowledge_target_purge_disconnect_prevents_resurrection() -> None:
    assert any(
        field.kind is ConnectorTargetKind.knowledge and field.required
        for field in manifest.target_fields
    )
    repository = InMemoryConnectorRepository("scope-a")
    binding = await repository.upsert_binding(
        "feishu",
        ConnectorBindingDraft(external_tenant_id="tenant-a"),
        ConnectorBindingStatus.connected,
        sync_cadence_seconds=manifest.default_sync_cadence_seconds,
    )
    await repository.upsert_items(
        "feishu",
        binding.id,
        (
            ConnectorItemDraft(
                external_id="docx:current",
                kind="knowledge_document",
                display_name="Current",
                destination_kind=ConnectorTargetKind.knowledge,
                destination_target_id="kb-1",
                destination_id="document-1",
            ),
        ),
    )
    token_store = InMemoryTokenStore("scope-a", EnvelopeCipher("test-key"))
    credentials = ConnectorCredentialStore(token_store)
    await credentials.put("feishu", _credential().envelope())
    provider = FeishuProvider(lambda: FakeFeishuClient(lambda *_: {"code": 0}))
    purge = PurgeSink()
    service = ConnectorService(
        ConnectorRegistry((ConnectorRegistration(manifest, lambda: provider, "test.feishu"),)),
        repository,
        credentials=credentials,
        purge_sink=purge,
    )
    assert await service.revoke("feishu", purge=True, local_only=True) is True
    assert purge.purged == 1
    assert await repository.get_binding("feishu") is None
    assert await credentials.get("feishu") is None
    with pytest.raises(LookupError, match="not configured"):
        await service.sync("feishu")
