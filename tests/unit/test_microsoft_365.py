"""Sanitized Microsoft 365 provider tests; no request reaches Microsoft."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

from keel_core.connector_contracts import (
    ConnectorActionContext,
    ConnectorBinding,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCursor,
    ConnectorItem,
    ConnectorOperationContext,
    ConnectorResource,
    ConnectorResourceDraft,
    ConnectorUnsupportedError,
)
from keel_core.connector_credentials import CredentialEnvelope
from keel_core.connector_providers.microsoft_365 import (
    APP_CREDENTIAL_KIND,
    CALENDAR_GET_ACTION,
    CALENDAR_LIST_ACTION,
    CALENDAR_UPCOMING_ACTION,
    GRAPH_ROOT,
    MAIL_GET_ACTION,
    MAIL_SEARCH_ACTION,
    MICROSOFT_365_CONNECTOR_ID,
    OAUTH_CREDENTIAL_KIND,
    SCOPES,
    Microsoft365HttpResponse,
    Microsoft365Provider,
    Microsoft365TenantMismatchError,
    Microsoft365ThrottledError,
    manifest,
)
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.protocols import ToolContext
from keel_core.secrets import EnvelopeCipher
from keel_core.tokens import InMemoryTokenStore
from keel_core.types import ContentTaint

TENANT_ID = "11111111-1111-1111-1111-111111111111"
OTHER_TENANT_ID = "22222222-2222-2222-2222-222222222222"
CLIENT_ID = "33333333-3333-3333-3333-333333333333"
NOW = datetime(2026, 7, 18, 0, 0, tzinfo=UTC)


class FakeTransport:
    def __init__(
        self,
        responses: list[tuple[str, str, Microsoft365HttpResponse]],
    ) -> None:
        self.responses = list(responses)
        self.requests: list[
            tuple[str, str, Mapping[str, str] | None, Mapping[str, str] | None]
        ] = []

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        data: Mapping[str, str] | None = None,
    ) -> Microsoft365HttpResponse:
        self.requests.append((method, url, headers, data))
        if not self.responses:
            raise AssertionError(f"unexpected request: {method} {url}")
        expected_method, expected_url_part, response = self.responses.pop(0)
        assert method == expected_method
        assert expected_url_part in url
        return response


def _jwt(tenant_id: str) -> str:
    def part(value: dict[str, Any]) -> str:
        encoded = base64.urlsafe_b64encode(
            json.dumps(value, separators=(",", ":")).encode()
        ).decode()
        return encoded.rstrip("=")

    return f"{part({'alg': 'none'})}.{part({'tid': tenant_id})}."


def _token_response(
    *,
    tenant_id: str = TENANT_ID,
    refresh_token: str = "refresh-1",
    scopes: str = "User.Read Mail.Read Calendars.Read",
) -> Microsoft365HttpResponse:
    return Microsoft365HttpResponse(
        200,
        payload={
            "token_type": "Bearer",
            "access_token": _jwt(tenant_id),
            "refresh_token": refresh_token,
            "expires_in": 3600,
            "scope": scopes,
            "id_token": _jwt(tenant_id),
        },
    )


def _oauth_values(
    *,
    expires_at: int | None = None,
    scopes: list[str] | None = None,
    refresh_token: str = "refresh-1",
) -> dict[str, Any]:
    return {
        "tenant_id": TENANT_ID,
        "client_id": CLIENT_ID,
        "client_secret": "sanitized-secret",
        "access_token": _jwt(TENANT_ID),
        "refresh_token": refresh_token,
        "expires_at": expires_at or int(NOW.timestamp()) + 3600,
        "granted_scopes": scopes or ["User.Read", "Mail.Read", "Calendars.Read"],
        "id_token": _jwt(TENANT_ID),
        "account_id": "account-1",
        "account_name": "Sanitized User",
    }


def _binding(
    *, status: ConnectorBindingStatus = ConnectorBindingStatus.connected
) -> ConnectorBinding:
    return ConnectorBinding(
        id="binding-1",
        scope_id="scope:test",
        connector_id=MICROSOFT_365_CONNECTOR_ID,
        status=status,
        display_name="Sanitized User",
        external_account_id="account-1",
        external_tenant_id=TENANT_ID,
    )


def _mail_resource() -> ConnectorResource:
    return ConnectorResource(
        id="resource-mail",
        scope_id="scope:test",
        connector_id=MICROSOFT_365_CONNECTOR_ID,
        binding_id="binding-1",
        external_id="mail:folder-1",
        kind="mail_folder",
        display_name="Inbox",
        selected=True,
        config={"graph_id": "folder-1"},
    )


def _calendar_resource() -> ConnectorResource:
    return ConnectorResource(
        id="resource-calendar",
        scope_id="scope:test",
        connector_id=MICROSOFT_365_CONNECTOR_ID,
        binding_id="binding-1",
        external_id="calendar:calendar-1",
        kind="calendar",
        display_name="Calendar",
        selected=True,
        config={"graph_id": "calendar-1"},
    )


def _context(
    *,
    resources: tuple[ConnectorResource, ...] = (),
    cursors: tuple[ConnectorCursor, ...] = (),
    items: tuple[ConnectorItem, ...] = (),
    values: dict[str, Any] | None = None,
) -> ConnectorOperationContext:
    return ConnectorOperationContext(
        scope_id="scope:test",
        connector_id=MICROSOFT_365_CONNECTOR_ID,
        binding=_binding(),
        credential=CredentialEnvelope(OAUTH_CREDENTIAL_KIND, values or _oauth_values()),
        credential_version=1,
        resources=resources,
        cursors=cursors,
        items=items,
    )


def _profile_response() -> Microsoft365HttpResponse:
    return Microsoft365HttpResponse(
        200,
        payload={
            "id": "account-1",
            "displayName": "Sanitized User",
            "userPrincipalName": "user@example.invalid",
        },
    )


def _action(actions: tuple[Any, ...], name: str) -> Any:
    return next(item.action for item in actions if item.manifest.name == name)


async def _action_fixture(
    transport: FakeTransport,
    *,
    values: dict[str, Any] | None = None,
) -> tuple[tuple[Any, ...], InMemoryTokenStore]:
    repository = InMemoryConnectorRepository("scope:test")
    binding = await repository.upsert_binding(
        MICROSOFT_365_CONNECTOR_ID,
        ConnectorBindingDraft(
            display_name="Sanitized User",
            external_account_id="account-1",
            external_tenant_id=TENANT_ID,
        ),
        ConnectorBindingStatus.connected,
    )
    await repository.upsert_resources(
        MICROSOFT_365_CONNECTOR_ID,
        binding.id,
        (
            ConnectorResourceDraft(
                external_id="mail:folder-1",
                kind="mail_folder",
                display_name="Inbox",
                selected=True,
                config={"graph_id": "folder-1"},
            ),
            ConnectorResourceDraft(
                external_id="calendar:calendar-1",
                kind="calendar",
                display_name="Calendar",
                selected=True,
                config={"graph_id": "calendar-1"},
            ),
        ),
    )
    await repository.select_resources(
        MICROSOFT_365_CONNECTOR_ID,
        {"mail:folder-1", "calendar:calendar-1"},
    )
    store = InMemoryTokenStore("scope:test", EnvelopeCipher("test-key"))
    await store.put(
        MICROSOFT_365_CONNECTOR_ID,
        CredentialEnvelope(OAUTH_CREDENTIAL_KIND, values or _oauth_values()).serialize(),
    )
    context = ConnectorActionContext.with_repository(
        "scope:test",
        repository,
        credential_store=store,
    )
    provider = Microsoft365Provider(transport, now=lambda: NOW)
    return provider.build_actions(context), store


def test_manifest_is_read_only_and_declares_least_mvp_scopes() -> None:
    assert manifest.id == "microsoft_365"
    assert set(SCOPES) == {
        "openid",
        "profile",
        "offline_access",
        "User.Read",
        "Mail.Read",
        "Calendars.Read",
    }
    assert {action.name for action in manifest.actions} == {
        "m365_mail_search",
        "m365_mail_get",
        "m365_calendar_list",
        "m365_calendar_get",
        "m365_calendar_upcoming",
    }
    assert all(action.semantics.value == "read" for action in manifest.actions)
    forbidden = {"Mail.Send", "Calendars.ReadWrite", "Files.Read", "Contacts.Read", "Chat.Read"}
    assert forbidden.isdisjoint(manifest.scopes)


async def test_staged_setup_authorization_and_callback() -> None:
    transport = FakeTransport(
        [
            ("POST", f"/{TENANT_ID}/oauth2/v2.0/token", _token_response()),
            ("GET", "/v1.0/me?", _profile_response()),
        ]
    )
    provider = Microsoft365Provider(transport, now=lambda: NOW)
    configured = await provider.setup(
        ConnectorOperationContext("scope:test", MICROSOFT_365_CONNECTOR_ID),
        {
            "tenant_id": TENANT_ID,
            "client_id": CLIENT_ID,
            "client_secret": "sanitized-secret",
        },
    )
    assert configured.status is ConnectorBindingStatus.configured
    assert configured.credential is not None
    assert configured.credential.kind == APP_CREDENTIAL_KIND
    assert "sanitized-secret" not in repr(configured.credential)

    staged = ConnectorOperationContext(
        "scope:test",
        MICROSOFT_365_CONNECTOR_ID,
        binding=ConnectorBinding(
            id="binding-1",
            scope_id="scope:test",
            connector_id=MICROSOFT_365_CONNECTOR_ID,
            status=ConnectorBindingStatus.configured,
            external_tenant_id=TENANT_ID,
        ),
        credential=configured.credential,
        credential_version=1,
    )
    start = await provider.begin_auth(staged, "https://keel.example/callback")
    parsed = urlsplit(start.url)
    query = parse_qs(parsed.query)
    assert parsed.path.startswith(f"/{TENANT_ID}/oauth2/v2.0/authorize")
    assert query["scope"] == [" ".join(SCOPES)]
    assert query["prompt"] == ["consent"]
    assert query["state"] == [start.state]

    authorizing = ConnectorOperationContext(
        "scope:test",
        MICROSOFT_365_CONNECTOR_ID,
        binding=ConnectorBinding(
            id="binding-1",
            scope_id="scope:test",
            connector_id=MICROSOFT_365_CONNECTOR_ID,
            status=ConnectorBindingStatus.authorizing,
            external_tenant_id=TENANT_ID,
        ),
        credential=configured.credential,
        credential_version=1,
    )
    completed = await provider.complete_auth(
        authorizing,
        "https://keel.example/callback",
        {"state": start.state, "code": "sanitized-code"},
    )
    assert completed.credential is not None
    assert completed.credential.kind == OAUTH_CREDENTIAL_KIND
    assert completed.credential.values["refresh_token"] == "refresh-1"
    assert completed.binding.external_account_id == "account-1"
    assert completed.binding.external_tenant_id == TENANT_ID
    token_request = transport.requests[0]
    assert token_request[3] is not None
    assert token_request[3]["grant_type"] == "authorization_code"
    assert "offline_access" in token_request[3]["scope"]


async def test_callback_rejects_tenant_mismatch() -> None:
    transport = FakeTransport(
        [("POST", "/oauth2/v2.0/token", _token_response(tenant_id=OTHER_TENANT_ID))]
    )
    provider = Microsoft365Provider(transport, now=lambda: NOW)
    context = ConnectorOperationContext(
        "scope:test",
        MICROSOFT_365_CONNECTOR_ID,
        binding=ConnectorBinding(
            id="binding-1",
            scope_id="scope:test",
            connector_id=MICROSOFT_365_CONNECTOR_ID,
            status=ConnectorBindingStatus.authorizing,
        ),
        credential=CredentialEnvelope(
            APP_CREDENTIAL_KIND,
            {
                "tenant_id": TENANT_ID,
                "client_id": CLIENT_ID,
                "client_secret": "sanitized-secret",
            },
        ),
        credential_version=1,
    )
    with pytest.raises(Microsoft365TenantMismatchError, match="tenant"):
        await provider.complete_auth(
            context,
            "https://keel.example/callback",
            {"code": "sanitized-code"},
        )


async def test_action_refresh_uses_versioned_cas_rotation() -> None:
    expired = _oauth_values(expires_at=int(NOW.timestamp()) - 1)
    transport = FakeTransport(
        [
            (
                "POST",
                "/oauth2/v2.0/token",
                _token_response(refresh_token="refresh-rotated"),
            ),
            ("GET", "/v1.0/me?", _profile_response()),
            (
                "GET",
                "/mailFolders/folder-1/messages?",
                Microsoft365HttpResponse(
                    200,
                    payload={
                        "value": [
                            {
                                "id": "message-1",
                                "subject": "Sanitized",
                                "bodyPreview": "External content",
                                "from": {
                                    "emailAddress": {
                                        "name": "Sender",
                                        "address": "sender@example.invalid",
                                    }
                                },
                                "webLink": "https://outlook.office.com/mail/message-1",
                                "lastModifiedDateTime": "2026-07-18T00:00:00Z",
                            }
                        ]
                    },
                ),
            ),
        ]
    )
    actions, store = await _action_fixture(transport, values=expired)
    rendered = await _action(actions, MAIL_SEARCH_ACTION.name)(
        {"resource_id": "mail:folder-1", "query": "sanitized"},
        ToolContext(scope_id="scope:test", session_id="session-1"),
    )
    payload = json.loads(rendered)
    assert payload["items"][0]["provenance"]["taint"] == "tainted"
    stored = await store.get_versioned(MICROSOFT_365_CONNECTOR_ID)
    assert stored is not None and stored[1] == 2
    envelope = CredentialEnvelope.parse(stored[0])
    assert envelope is not None
    assert envelope.values["refresh_token"] == "refresh-rotated"


async def test_delta_paging_invalid_cursor_full_resync_and_mail_mapping() -> None:
    cursor_url = f"{GRAPH_ROOT}/me/mailFolders/folder-1/messages/delta?$deltatoken=old"
    next_url = f"{GRAPH_ROOT}/me/mailFolders/folder-1/messages/delta?$skiptoken=page-2"
    delta_url = f"{GRAPH_ROOT}/me/mailFolders/folder-1/messages/delta?$deltatoken=new"
    transport = FakeTransport(
        [
            (
                "GET",
                "$deltatoken=old",
                Microsoft365HttpResponse(
                    410,
                    payload={"error": {"code": "syncStateNotFound"}},
                ),
            ),
            (
                "GET",
                "/messages/delta?",
                Microsoft365HttpResponse(
                    200,
                    payload={
                        "value": [
                            {
                                "id": "message-current",
                                "subject": "Current",
                                "bodyPreview": "Mapped external mail",
                                "from": {
                                    "emailAddress": {
                                        "address": "sender@example.invalid",
                                    }
                                },
                                "webLink": "https://outlook.office.com/current",
                                "lastModifiedDateTime": "2026-07-18T00:00:00Z",
                            }
                        ],
                        "@odata.nextLink": next_url,
                    },
                ),
            ),
            (
                "GET",
                "$skiptoken=page-2",
                Microsoft365HttpResponse(
                    200,
                    payload={
                        "value": [{"id": "message-removed", "@removed": {"reason": "deleted"}}],
                        "@odata.deltaLink": delta_url,
                    },
                ),
            ),
        ]
    )
    resource = _mail_resource()
    context = _context(
        resources=(resource,),
        cursors=(
            ConnectorCursor(
                id="cursor-1",
                scope_id="scope:test",
                connector_id=MICROSOFT_365_CONNECTOR_ID,
                binding_id="binding-1",
                stream="delta",
                value=cursor_url,
                resource_id=resource.id,
            ),
        ),
        items=(
            ConnectorItem(
                id="item-1",
                scope_id="scope:test",
                connector_id=MICROSOFT_365_CONNECTOR_ID,
                binding_id="binding-1",
                external_id="mail_folder:folder-1:message-stale",
                kind="knowledge_document",
                display_name="Stale",
            ),
        ),
    )
    result = await Microsoft365Provider(transport, now=lambda: NOW).sync(context)
    assert result.cursor_updates[0].value == delta_url
    assert result.cursor_updates[0].revision == "full_resync"
    assert [change.kind.value for change in result.changes] == [
        "upsert",
        "delete",
        "delete",
    ]
    assert result.changes[0].taint is ContentTaint.tainted
    assert result.changes[0].provenance.connector_id == MICROSOFT_365_CONNECTOR_ID
    assert result.changes[0].provenance.source_url == "https://outlook.office.com/current"


async def test_graph_honors_retry_after_then_succeeds() -> None:
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    transport = FakeTransport(
        [
            (
                "GET",
                "/mailFolders?",
                Microsoft365HttpResponse(429, headers={"Retry-After": "3"}),
            ),
            (
                "GET",
                "/mailFolders?",
                Microsoft365HttpResponse(200, payload={"value": []}),
            ),
            (
                "GET",
                "/calendars?",
                Microsoft365HttpResponse(200, payload={"value": []}),
            ),
        ]
    )
    result = await Microsoft365Provider(
        transport,
        sleep=sleep,
        now=lambda: NOW,
    ).list_resources(_context())
    assert result.resources == ()
    assert sleeps == [3.0]


async def test_graph_throttling_fails_after_bounded_retries() -> None:
    async def sleep(delay: float) -> None:
        return None

    transport = FakeTransport(
        [
            (
                "GET",
                "/mailFolders?",
                Microsoft365HttpResponse(429, headers={"Retry-After": "2"}),
            ),
            (
                "GET",
                "/mailFolders?",
                Microsoft365HttpResponse(429, headers={"Retry-After": "2"}),
            ),
            (
                "GET",
                "/mailFolders?",
                Microsoft365HttpResponse(429, headers={"Retry-After": "2"}),
            ),
        ]
    )
    with pytest.raises(Microsoft365ThrottledError) as caught:
        await Microsoft365Provider(transport, sleep=sleep, now=lambda: NOW).list_resources(
            _context()
        )
    assert caught.value.retry_after == 2.0


async def test_mail_and_calendar_tools_preserve_pagination_timezone_and_provenance() -> None:
    next_mail = f"{GRAPH_ROOT}/me/mailFolders/folder-1/messages?$skiptoken=mail-next"
    next_calendar = (
        f"{GRAPH_ROOT}/me/calendars/calendar-1/calendarView?$skiptoken=calendar-next"
    )
    message = {
        "id": "message-1",
        "subject": "Sanitized mail",
        "bodyPreview": "External mail",
        "body": {"contentType": "text", "content": "External body"},
        "from": {"emailAddress": {"address": "sender@example.invalid"}},
        "webLink": "https://outlook.office.com/message-1",
        "lastModifiedDateTime": "2026-07-18T00:00:00Z",
    }
    event = {
        "id": "event-1",
        "subject": "Sanitized event",
        "bodyPreview": "External event",
        "start": {"dateTime": "2026-07-18T09:00:00", "timeZone": "Pacific Standard Time"},
        "end": {"dateTime": "2026-07-18T10:00:00", "timeZone": "Pacific Standard Time"},
        "webLink": "https://outlook.office.com/event-1",
        "changeKey": "revision-1",
    }
    transport = FakeTransport(
        [
            (
                "GET",
                "/mailFolders/folder-1/messages?",
                Microsoft365HttpResponse(
                    200,
                    payload={"value": [message], "@odata.nextLink": next_mail},
                ),
            ),
            (
                "GET",
                "/mailFolders/folder-1/messages/message-1?",
                Microsoft365HttpResponse(200, payload=message),
            ),
            (
                "GET",
                "/calendars/calendar-1/calendarView?",
                Microsoft365HttpResponse(
                    200,
                    payload={"value": [event], "@odata.nextLink": next_calendar},
                ),
            ),
            (
                "GET",
                "/calendars/calendar-1/events/event-1",
                Microsoft365HttpResponse(200, payload=event),
            ),
            (
                "GET",
                "/calendars/calendar-1/calendarView?",
                Microsoft365HttpResponse(200, payload={"value": [event]}),
            ),
        ]
    )
    actions, _ = await _action_fixture(transport)
    tool_context = ToolContext(scope_id="scope:test", session_id="session-1")

    mail_page = json.loads(
        await _action(actions, MAIL_SEARCH_ACTION.name)(
            {"resource_id": "mail:folder-1", "query": "sanitized"},
            tool_context,
        )
    )
    assert mail_page["next_page"] == next_mail
    assert mail_page["items"][0]["provenance"]["binding_id"]
    mail_item = json.loads(
        await _action(actions, MAIL_GET_ACTION.name)(
            {"resource_id": "mail:folder-1", "message_id": "message-1"},
            tool_context,
        )
    )
    assert mail_item["item"]["body"]["content"] == "External body"

    calendar_page = json.loads(
        await _action(actions, CALENDAR_LIST_ACTION.name)(
            {
                "resource_id": "calendar:calendar-1",
                "start": "2026-07-18T00:00:00Z",
                "end": "2026-07-19T00:00:00Z",
                "timezone": "Pacific Standard Time",
            },
            tool_context,
        )
    )
    assert calendar_page["timezone"] == "Pacific Standard Time"
    assert calendar_page["next_page"] == next_calendar
    calendar_item = json.loads(
        await _action(actions, CALENDAR_GET_ACTION.name)(
            {
                "resource_id": "calendar:calendar-1",
                "event_id": "event-1",
                "timezone": "Pacific Standard Time",
            },
            tool_context,
        )
    )
    assert calendar_item["item"]["provenance"]["revision"] == "revision-1"
    upcoming = json.loads(
        await _action(actions, CALENDAR_UPCOMING_ACTION.name)(
            {
                "resource_id": "calendar:calendar-1",
                "days": 14,
                "timezone": "UTC",
            },
            tool_context,
        )
    )
    assert upcoming["timezone"] == "UTC"
    calendar_headers = [
        headers
        for method, url, headers, data in transport.requests
        if method == "GET" and "/calendars/" in url
    ]
    assert calendar_headers[0] is not None
    assert calendar_headers[0]["Prefer"] == 'outlook.timezone="Pacific Standard Time"'


async def test_calendar_delta_mapping() -> None:
    delta_url = (
        f"{GRAPH_ROOT}/me/calendars/calendar-1/calendarView/delta?$deltatoken=calendar-new"
    )
    event = {
        "id": "event-1",
        "subject": "Sanitized event",
        "bodyPreview": "External event body",
        "start": {"dateTime": "2026-07-18T09:00:00", "timeZone": "UTC"},
        "end": {"dateTime": "2026-07-18T10:00:00", "timeZone": "UTC"},
        "location": {"displayName": "Room"},
        "webLink": "https://outlook.office.com/event-1",
        "changeKey": "revision-1",
    }
    transport = FakeTransport(
        [
            (
                "GET",
                "/calendarView/delta?",
                Microsoft365HttpResponse(
                    200,
                    payload={"value": [event], "@odata.deltaLink": delta_url},
                ),
            )
        ]
    )
    result = await Microsoft365Provider(transport, now=lambda: NOW).sync(
        _context(resources=(_calendar_resource(),))
    )
    assert result.changes[0].title == "Sanitized event"
    assert "Location:" in (result.changes[0].content or "")
    assert result.changes[0].provenance.revision == "revision-1"


async def test_insufficient_scope_health_and_revoke_fail_closed() -> None:
    provider = Microsoft365Provider(FakeTransport([]), now=lambda: NOW)
    context = _context(
        values=_oauth_values(scopes=["User.Read", "Mail.Read"]),
    )
    health = await provider.health(context)
    assert health.status.value == "error"
    assert "calendars.read" in (health.message or "")
    assert health.retryable is False

    with pytest.raises(ConnectorUnsupportedError, match="no per-refresh-token"):
        await provider.revoke(_context())


async def test_health_enforces_account_and_succeeds_only_on_match() -> None:
    healthy_provider = Microsoft365Provider(
        FakeTransport([("GET", "/v1.0/me?", _profile_response())]),
        now=lambda: NOW,
    )
    healthy = await healthy_provider.health(_context())
    assert healthy.status.value == "healthy"

    mismatch_provider = Microsoft365Provider(
        FakeTransport(
            [
                (
                    "GET",
                    "/v1.0/me?",
                    Microsoft365HttpResponse(200, payload={"id": "other-account"}),
                )
            ]
        ),
        now=lambda: NOW,
    )
    mismatch = await mismatch_provider.health(_context())
    assert mismatch.status.value == "error"
    assert mismatch.retryable is False
