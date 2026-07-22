"""Hermetic Google Calendar provider, sync, OAuth, and action contracts."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs

import google_auth_httplib2
import googleapiclient.discovery
import httplib2
import httpx
import pytest
from google.auth.exceptions import RefreshError
from google.auth.transport import requests as google_requests
from google.oauth2 import credentials as google_credentials

from keel_core.connector_contracts import (
    ConnectorActionApproval,
    ConnectorActionContext,
    ConnectorActionIdempotency,
    ConnectorActionSemantics,
    ConnectorBinding,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCursor,
    ConnectorHealthStatus,
    ConnectorOperationContext,
    ConnectorResource,
    ConnectorResourceDraft,
)
from keel_core.connector_credentials import CredentialEnvelope
from keel_core.connector_providers import google_calendar
from keel_core.connector_providers.google_calendar import (
    GOOGLE_CALENDAR_CONNECTOR_ID,
    GOOGLE_CALENDAR_READ_SCOPES,
    GOOGLE_CALENDAR_WRITE_SCOPE,
    GOOGLE_CALENDAR_WRITE_SCOPES,
    GoogleCalendarAuthenticationError,
    GoogleCalendarError,
    GoogleCalendarProvider,
    GoogleCalendarRateLimitError,
    GoogleCalendarRevokeError,
    GoogleCalendarSyncTokenExpired,
    GoogleCalendarWriteAuthorizationRequired,
)
from keel_core.connector_providers.google_calendar._client import (
    CalendarClient,
    GoogleCalendarNotFoundError,
    _map_http_error,
    build_client,
    revoke_google_token,
)
from keel_core.connector_registry import discover_connector_registry
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.connectors import ConnectorTool
from keel_core.digest import digest_permissions
from keel_core.effect_store import InMemoryEffectStore
from keel_core.protocols import ToolContext
from keel_core.types import ContentTaint, PermissionDecision

FIXTURE = Path(__file__).parents[1] / "fixtures" / "connectors" / "google-calendar-events.json"


def _credential(scopes: tuple[str, ...] = GOOGLE_CALENDAR_READ_SCOPES) -> CredentialEnvelope:
    return CredentialEnvelope(
        "oauth",
        {
            "client_id": "sanitized-client.apps.googleusercontent.com",
            "client_secret": "sanitized-client-secret",
            "refresh_token": "sanitized-refresh-token",
            "scopes": list(scopes),
            "token": "sanitized-access-token",
            "token_uri": "https://oauth2.googleapis.com/token",
        },
    )


def _binding() -> ConnectorBinding:
    return ConnectorBinding(
        id="binding-calendar",
        scope_id="scope:a",
        connector_id=GOOGLE_CALENDAR_CONNECTOR_ID,
        status=ConnectorBindingStatus.connected,
    )


def _resource(*, selected: bool = True) -> ConnectorResource:
    return ConnectorResource(
        id="resource-calendar",
        scope_id="scope:a",
        connector_id=GOOGLE_CALENDAR_CONNECTOR_ID,
        binding_id="binding-calendar",
        external_id="team-calendar@example.test",
        kind="calendar",
        display_name="Team Calendar",
        selected=selected,
        config={"time_zone": "Asia/Shanghai"},
    )


class FakeCredentialStore:
    def __init__(self, credential: CredentialEnvelope | None) -> None:
        self.credential = credential
        self.puts: list[CredentialEnvelope] = []

    async def get(self, connector_id: str) -> CredentialEnvelope | None:
        assert connector_id == GOOGLE_CALENDAR_CONNECTOR_ID
        return self.credential

    async def put(self, connector_id: str, credential: CredentialEnvelope) -> None:
        assert connector_id == GOOGLE_CALENDAR_CONNECTOR_ID
        self.credential = credential
        self.puts.append(credential)


class FakeCalendarClient(CalendarClient):
    def __init__(
        self,
        credential: CredentialEnvelope,
        *,
        calendar_pages: Mapping[str | None, dict[str, Any]] | None = None,
        event_responses: list[dict[str, Any] | Exception] | None = None,
    ) -> None:
        self._credential = credential
        self.calendar_pages = dict(calendar_pages or {})
        self.event_responses = list(event_responses or [])
        self.list_event_parameters: list[dict[str, Any]] = []
        self.created: dict[str, dict[str, Any]] = {}
        self.updated: dict[str, dict[str, Any]] = {}
        self.create_calls = 0
        self.update_calls = 0
        self.closed = 0

    @property
    def credential(self) -> CredentialEnvelope:
        return self._credential

    def list_calendars(self, page_token: str | None = None) -> dict[str, Any]:
        return self.calendar_pages.get(page_token, {"items": []})

    def list_events(
        self,
        calendar_id: str,
        parameters: Mapping[str, Any],
    ) -> dict[str, Any]:
        assert calendar_id == "team-calendar@example.test"
        self.list_event_parameters.append(dict(parameters))
        if not self.event_responses:
            return {"items": [], "nextSyncToken": "sync-default"}
        response = self.event_responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def get_event(
        self,
        calendar_id: str,
        event_id: str,
        time_zone: str | None = None,
    ) -> dict[str, Any]:
        assert calendar_id == "team-calendar@example.test"
        if time_zone is not None:
            assert time_zone == "Asia/Shanghai"
        if event_id in self.updated:
            return self.updated[event_id]
        if event_id in self.created:
            return self.created[event_id]
        return {
            "end": {"dateTime": "2026-07-20T11:00:00+08:00"},
            "etag": '"get-revision"',
            "id": event_id,
            "start": {"dateTime": "2026-07-20T10:00:00+08:00"},
            "status": "confirmed",
            "summary": "Existing event",
        }

    def create_event_reconciled(
        self,
        calendar_id: str,
        event_id: str,
        request_id: str,
        body: Mapping[str, Any],
    ) -> dict[str, Any]:
        assert calendar_id == "team-calendar@example.test"
        existing = self.created.get(event_id)
        if existing is not None:
            marker = existing["extendedProperties"]["private"]["keel_create_request_id"]
            assert marker == request_id
            return existing
        self.create_calls += 1
        event = {
            **dict(body),
            "etag": '"created-revision"',
            "extendedProperties": {"private": {"keel_create_request_id": request_id}},
            "id": event_id,
            "status": "confirmed",
        }
        self.created[event_id] = event
        return event

    def update_event_reconciled(
        self,
        calendar_id: str,
        event_id: str,
        request_id: str,
        body: Mapping[str, Any],
    ) -> dict[str, Any]:
        assert calendar_id == "team-calendar@example.test"
        existing = self.updated.get(event_id)
        if (
            existing is not None
            and existing["extendedProperties"]["private"]["keel_update_request_id"] == request_id
        ):
            return existing
        self.update_calls += 1
        event = {
            **self.get_event(calendar_id, event_id),
            **dict(body),
            "etag": '"updated-revision"',
            "extendedProperties": {"private": {"keel_update_request_id": request_id}},
        }
        self.updated[event_id] = event
        return event

    def close(self) -> None:
        self.closed += 1


def _factory(
    client: CalendarClient,
) -> Callable[[CredentialEnvelope, tuple[str, ...]], CalendarClient]:
    def create(
        credential: CredentialEnvelope,
        required_scopes: tuple[str, ...],
    ) -> CalendarClient:
        assert set(required_scopes).issubset(set(credential.values.get("scopes", [])))
        return client

    return create


def _tool_context(taint: ContentTaint = ContentTaint.clean) -> ToolContext:
    return ToolContext(
        scope_id="scope:a",
        session_id="session-calendar",
        content_taint=taint,
    )


def _fixture() -> dict[str, Any]:
    loaded = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def test_manifest_declares_least_scope_and_safe_outbound_contracts() -> None:
    assert google_calendar.manifest.id == GOOGLE_CALENDAR_CONNECTOR_ID
    assert google_calendar.manifest.scopes == GOOGLE_CALENDAR_READ_SCOPES
    assert GOOGLE_CALENDAR_WRITE_SCOPE not in google_calendar.manifest.scopes
    actions = {item.name: item for item in google_calendar.manifest.actions}
    for name in ("google_calendar_event_create", "google_calendar_event_update"):
        assert actions[name].semantics is ConnectorActionSemantics.outbound
        assert actions[name].approval is ConnectorActionApproval.tainted
        assert actions[name].idempotency is ConnectorActionIdempotency.required
    assert actions["google_calendar_events_list"].semantics is ConnectorActionSemantics.read


def test_registry_discovers_and_creates_google_calendar_provider() -> None:
    registry = discover_connector_registry()
    assert GOOGLE_CALENDAR_CONNECTOR_ID in {item.id for item in registry.manifests()}
    assert registry.create(GOOGLE_CALENDAR_CONNECTOR_ID).manifest == google_calendar.manifest


def test_build_client_uses_google_credentials_refresh_and_timeout_wiring(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    observed: dict[str, object] = {}
    request_marker = object()
    http_marker = object()
    authorized_http_marker = object()

    class Credentials:
        valid = False
        refresh_token = "present"

        def refresh(self, request: object) -> None:
            observed["refresh_request_matches"] = request is request_marker
            self.valid = True

        def to_json(self) -> str:
            return json.dumps(
                {
                    "client_id": "sanitized-client.apps.googleusercontent.com",
                    "refresh_token": "sanitized-refresh-token",
                    "scopes": list(GOOGLE_CALENDAR_READ_SCOPES),
                    "token": "rotated-sanitized-token",
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
            )

    credentials = Credentials()

    def from_authorized_user_info(
        info: dict[str, Any],
        scopes: list[str],
    ) -> Credentials:
        observed["credential_keys"] = tuple(sorted(info))
        observed["requested_scopes"] = tuple(scopes)
        observed["token_present"] = isinstance(info.get("token"), str)
        return credentials

    def make_http(*, timeout: int) -> object:
        observed["timeout"] = timeout
        return http_marker

    def make_authorized_http(
        supplied_credentials: object,
        *,
        http: object,
    ) -> object:
        observed["authorized_credentials_match"] = supplied_credentials is credentials
        observed["authorized_http_match"] = http is http_marker
        return authorized_http_marker

    service = SimpleNamespace(close=lambda: observed.update(service_closed=True))

    def build(
        api: str,
        version: str,
        *,
        http: object,
        cache_discovery: bool,
    ) -> object:
        observed["build"] = (api, version, http is authorized_http_marker, cache_discovery)
        return service

    monkeypatch.setattr(
        google_credentials.Credentials,
        "from_authorized_user_info",
        staticmethod(from_authorized_user_info),
    )
    monkeypatch.setattr(google_requests, "Request", lambda: request_marker)
    monkeypatch.setattr(httplib2, "Http", make_http)
    monkeypatch.setattr(google_auth_httplib2, "AuthorizedHttp", make_authorized_http)
    monkeypatch.setattr(googleapiclient.discovery, "build", build)

    client = build_client(_credential(), GOOGLE_CALENDAR_READ_SCOPES)
    client.close()

    assert observed == {
        "authorized_credentials_match": True,
        "authorized_http_match": True,
        "build": ("calendar", "v3", True, False),
        "credential_keys": (
            "client_id",
            "client_secret",
            "refresh_token",
            "scopes",
            "token",
            "token_uri",
        ),
        "refresh_request_matches": True,
        "requested_scopes": GOOGLE_CALENDAR_READ_SCOPES,
        "service_closed": True,
        "timeout": 10,
        "token_present": True,
    }
    for secret in (
        "sanitized-access-token",
        "sanitized-client-secret",
        "sanitized-refresh-token",
    ):
        assert secret not in caplog.text


def test_build_client_sanitizes_google_refresh_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Credentials:
        valid = False
        refresh_token = "present"

        def refresh(self, request: object) -> None:
            del request
            raise RefreshError(  # type: ignore[no-untyped-call]
                "synthetic refresh failure containing sanitized-access-token"
            )

    monkeypatch.setattr(
        google_credentials.Credentials,
        "from_authorized_user_info",
        staticmethod(lambda info, scopes: Credentials()),
    )
    with pytest.raises(GoogleCalendarAuthenticationError) as captured:
        build_client(_credential(), GOOGLE_CALENDAR_READ_SCOPES)
    assert "refresh failed" in str(captured.value)
    assert "sanitized-access-token" not in str(captured.value)
    assert "sanitized-access-token" not in repr(captured.value)


@pytest.mark.parametrize(
    ("status", "content", "expected_type", "message"),
    [
        (401, b'{"error":"sanitized-access-token"}', GoogleCalendarAuthenticationError, "rejected"),
        (403, b'{"error":"forbidden"}', GoogleCalendarAuthenticationError, "rejected"),
        (404, b'{"error":"missing"}', GoogleCalendarNotFoundError, "no longer exists"),
        (409, b'{"error":"conflict"}', GoogleCalendarError, "(409)"),
        (410, b'{"error":"gone"}', GoogleCalendarSyncTokenExpired, "sync token"),
        (429, b'{"error":"quota"}', GoogleCalendarRateLimitError, "rate limited"),
        (418, b'{"error":"default"}', GoogleCalendarError, "HTTP 418"),
    ],
)
def test_map_http_error_is_typed_and_sanitized(
    status: int,
    content: bytes,
    expected_type: type[GoogleCalendarError],
    message: str,
) -> None:
    mapped = _map_http_error(SimpleNamespace(resp=SimpleNamespace(status=status), content=content))
    assert type(mapped) is expected_type
    assert message in str(mapped)
    assert "sanitized-access-token" not in str(mapped)
    assert "sanitized-access-token" not in repr(mapped)


def test_map_http_error_detects_403_rate_limit_reason() -> None:
    mapped = _map_http_error(
        SimpleNamespace(
            resp=SimpleNamespace(status=403),
            content=b'{"reason":"userRateLimitExceeded","token":"sanitized-access-token"}',
        )
    )
    assert type(mapped) is GoogleCalendarRateLimitError
    assert "sanitized-access-token" not in str(mapped)


@pytest.mark.parametrize("status", [200, 204])
async def test_revoke_google_token_uses_real_mock_transport_without_query_secret(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    status: int,
) -> None:
    token = "sanitized-revoke-token"
    observed: dict[str, object] = {}
    real_client = httpx.AsyncClient

    def handler(request: httpx.Request) -> httpx.Response:
        form = parse_qs(request.content.decode("ascii"))
        observed.update(
            method=request.method,
            query_empty=not request.url.query,
            token_matches=form.get("token") == [token],
        )
        return httpx.Response(status, request=request)

    def client(*, timeout: float) -> httpx.AsyncClient:
        observed["timeout"] = timeout
        return real_client(timeout=timeout, transport=httpx.MockTransport(handler))

    monkeypatch.setattr(httpx, "AsyncClient", client)
    await revoke_google_token(token)

    assert observed == {
        "method": "POST",
        "query_empty": True,
        "timeout": 10.0,
        "token_matches": True,
    }
    assert token not in caplog.text


async def test_revoke_google_token_sanitizes_status_and_transport_failures(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = "sanitized-revoke-token"
    real_client = httpx.AsyncClient

    def install(handler: Callable[[httpx.Request], httpx.Response]) -> None:
        def client(*, timeout: float) -> httpx.AsyncClient:
            return real_client(timeout=timeout, transport=httpx.MockTransport(handler))

        monkeypatch.setattr(httpx, "AsyncClient", client)

    def rejected(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, request=request)

    install(rejected)
    with pytest.raises(GoogleCalendarRevokeError) as rejected_error:
        await revoke_google_token(token)
    assert "remote revoke failed" in str(rejected_error.value)
    assert token not in str(rejected_error.value)
    assert token not in repr(rejected_error.value)

    def unavailable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("transport unavailable", request=request)

    install(unavailable)
    with pytest.raises(GoogleCalendarRevokeError) as unavailable_error:
        await revoke_google_token(token)
    assert "could not be reached" in str(unavailable_error.value)
    assert token not in str(unavailable_error.value)
    assert token not in repr(unavailable_error.value)
    assert token not in caplog.text


async def test_oauth_stages_read_then_incremental_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested: list[tuple[str, ...]] = []

    class Flow:
        credentials = SimpleNamespace(
            to_json=lambda: json.dumps(
                {
                    "client_id": "sanitized",
                    "scopes": list(GOOGLE_CALENDAR_WRITE_SCOPES),
                    "token": "sanitized",
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
            )
        )

        def authorization_url(self, **kwargs: str) -> tuple[str, str]:
            assert kwargs["include_granted_scopes"] == "true"
            return "https://accounts.google.test/auth", "state"

        def fetch_token(self, *, code: str) -> None:
            assert code == "code"

    def fake_flow(redirect_uri: str, scopes: tuple[str, ...]) -> Flow:
        assert redirect_uri == "https://keel.test/callback"
        requested.append(scopes)
        return Flow()

    monkeypatch.setattr(google_calendar, "_flow", fake_flow)
    provider = GoogleCalendarProvider()
    initial = ConnectorOperationContext("scope:a", GOOGLE_CALENDAR_CONNECTOR_ID)
    await provider.begin_auth(initial, "https://keel.test/callback")
    assert requested[-1] == GOOGLE_CALENDAR_READ_SCOPES

    existing = ConnectorOperationContext(
        "scope:a",
        GOOGLE_CALENDAR_CONNECTOR_ID,
        binding=_binding(),
        credential=_credential(),
        credential_version=1,
    )
    await provider.begin_auth(existing, "https://keel.test/callback")
    assert requested[-1] == GOOGLE_CALENDAR_WRITE_SCOPES
    result = await provider.complete_auth(
        existing,
        "https://keel.test/callback",
        {"code": "code"},
    )
    assert result.credential is not None
    assert result.credential.values["refresh_token"] == "sanitized-refresh-token"
    assert result.binding.metadata["authorization_stage"] == "read_write"


def test_write_scope_is_fail_closed_before_api_construction() -> None:
    with pytest.raises(GoogleCalendarWriteAuthorizationRequired, match="incremental"):
        build_client(_credential(), GOOGLE_CALENDAR_WRITE_SCOPES)


async def test_resource_discovery_is_paginated_and_authoritative() -> None:
    fixture = _fixture()
    calendar = fixture["calendar"]
    client = FakeCalendarClient(
        _credential(),
        calendar_pages={
            None: {"items": [calendar], "nextPageToken": "page-2"},
            "page-2": {"items": []},
        },
    )
    provider = GoogleCalendarProvider(client_factory=_factory(client))
    context = ConnectorOperationContext(
        "scope:a",
        GOOGLE_CALENDAR_CONNECTOR_ID,
        binding=_binding(),
        credential=_credential(),
        credential_version=1,
    )
    result = await provider.list_resources(context)
    assert result.mode.value == "authoritative"
    assert [(item.external_id, item.display_name) for item in result.resources] == [
        ("team-calendar@example.test", "Team Calendar")
    ]
    assert result.resources[0].config["time_zone"] == "Asia/Shanghai"


async def test_read_action_preserves_pagination_timezone_all_day_recurrence_and_taint() -> None:
    fixture = _fixture()
    client = FakeCalendarClient(
        _credential(),
        event_responses=[fixture["events"]],
    )
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        GOOGLE_CALENDAR_CONNECTOR_ID,
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
    )
    assert binding.id
    await repository.upsert_resources(
        GOOGLE_CALENDAR_CONNECTOR_ID,
        binding.id,
        (
            ConnectorResourceDraft(
                "team-calendar@example.test",
                "calendar",
                "Team Calendar",
                selected=True,
                config={"access_role": "owner"},
            ),
        ),
    )
    store = FakeCredentialStore(_credential())
    provider = GoogleCalendarProvider(
        client_factory=_factory(client),
        clock=lambda: datetime(2026, 7, 18, 3, 19, 39, 559000, tzinfo=UTC),
    )
    action_context = ConnectorActionContext.with_repository(
        "scope:a",
        repository,
        credential_store=store,
    )
    action = next(
        item
        for item in provider.build_actions(action_context)
        if item.manifest.name == "google_calendar_events_list"
    )
    tool = ConnectorTool(
        name=action.manifest.name,
        description=action.manifest.description,
        action=action.action,
        input_schema=dict(action.manifest.input_schema),
    )
    result = await tool.run(
        {
            "calendar_id": "team-calendar@example.test",
            "max_results": 2,
            "page_token": "page-1",
            "time_zone": "Asia/Shanghai",
        },
        _tool_context(),
    )
    payload = json.loads(result.output)
    assert result.taint is ContentTaint.tainted
    assert payload["next_page_token"] == "sanitized-next-page"
    assert payload["events"][0]["all_day"] is True
    assert payload["events"][0]["recurrence"] == ["RRULE:FREQ=WEEKLY;COUNT=4"]
    assert payload["events"][1]["recurring_event_id"] == "event-series"
    assert payload["events"][1]["start"]["time_zone"] == "Asia/Shanghai"
    assert payload["events"][0]["provenance"]["connector_id"] == GOOGLE_CALENDAR_CONNECTOR_ID
    client.event_responses.append(fixture["events"])
    search = next(
        item
        for item in provider.build_actions(action_context)
        if item.manifest.name == "google_calendar_events_search"
    )
    searched = json.loads(
        await search.action(
            {
                "calendar_id": "team-calendar@example.test",
                "query": "standup",
                "time_zone": "Asia/Shanghai",
            },
            _tool_context(),
        )
    )
    assert searched["events"][1]["summary"] == "Sanitized recurring standup"
    get = next(
        item
        for item in provider.build_actions(action_context)
        if item.manifest.name == "google_calendar_event_get"
    )
    fetched = json.loads(
        await get.action(
            {
                "calendar_id": "team-calendar@example.test",
                "event_id": "event-all-day",
                "time_zone": "Asia/Shanghai",
            },
            _tool_context(),
        )
    )
    assert fetched["event_id"] == "event-all-day"
    assert client.list_event_parameters == [
        {
            "maxResults": 2,
            "orderBy": "startTime",
            "pageToken": "page-1",
            "singleEvents": True,
            "timeMin": "2026-07-18T03:19:39.559000Z",
            "timeZone": "Asia/Shanghai",
        },
        {
            "maxResults": 50,
            "orderBy": "startTime",
            "q": "standup",
            "singleEvents": True,
            "timeMin": "2026-07-18T03:19:39.559000Z",
            "timeZone": "Asia/Shanghai",
        },
    ]


async def test_sync_resumes_per_calendar_and_recovers_expired_token() -> None:
    fixture = _fixture()
    client = FakeCalendarClient(
        _credential(),
        event_responses=[
            GoogleCalendarSyncTokenExpired("expired"),
            {**fixture["events"], "nextPageToken": None, "nextSyncToken": "sync-fresh"},
        ],
    )
    provider = GoogleCalendarProvider(
        client_factory=_factory(client),
        clock=lambda: datetime(2026, 7, 18, 0, 0, tzinfo=UTC),
    )
    context = ConnectorOperationContext(
        "scope:a",
        GOOGLE_CALENDAR_CONNECTOR_ID,
        binding=_binding(),
        credential=_credential(),
        credential_version=1,
        resources=(_resource(),),
        cursors=(
            ConnectorCursor(
                id="cursor",
                scope_id="scope:a",
                connector_id=GOOGLE_CALENDAR_CONNECTOR_ID,
                binding_id="binding-calendar",
                stream="events",
                value="expired",
                resource_id="resource-calendar",
            ),
        ),
    )
    result = await provider.sync(context)
    assert client.list_event_parameters[0]["syncToken"] == "expired"
    assert "syncToken" not in client.list_event_parameters[1]
    assert client.list_event_parameters[1]["timeMin"] == "2026-06-18T00:00:00Z"
    assert result.cursor_updates[0].resource_id == "resource-calendar"
    assert result.cursor_updates[0].value == "sync-fresh"
    assert all(change.taint is ContentTaint.tainted for change in result.changes)
    assert result.changes[0].event is not None
    assert result.changes[0].event.payload["full_resync"] is True


async def test_outbound_actions_require_selection_approval_and_reconcile_retries() -> None:
    client = FakeCalendarClient(_credential(GOOGLE_CALENDAR_WRITE_SCOPES))
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        GOOGLE_CALENDAR_CONNECTOR_ID,
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
    )
    await repository.upsert_resources(
        GOOGLE_CALENDAR_CONNECTOR_ID,
        binding.id,
        (
            ConnectorResourceDraft(
                "team-calendar@example.test",
                "calendar",
                "Team Calendar",
                selected=True,
                config={"access_role": "owner"},
            ),
            ConnectorResourceDraft(
                "blocked@example.test",
                "calendar",
                "Blocked",
                selected=False,
                config={"access_role": "owner"},
            ),
            ConnectorResourceDraft(
                "read-only@example.test",
                "calendar",
                "Read only",
                selected=True,
                config={"access_role": "reader"},
            ),
        ),
    )
    store = FakeCredentialStore(_credential(GOOGLE_CALENDAR_WRITE_SCOPES))
    provider = GoogleCalendarProvider(client_factory=_factory(client))
    actions = provider.build_actions(
        ConnectorActionContext.with_repository(
            "scope:a",
            repository,
            credential_store=store,
        )
    )
    create = next(item for item in actions if item.manifest.name == "google_calendar_event_create")
    permission = digest_permissions(actions)
    assert (
        permission.evaluate(create.manifest.name, {}, _tool_context(ContentTaint.tainted))
        is PermissionDecision.ask
    )
    arguments = {
        "calendar_id": "team-calendar@example.test",
        "summary": "Approved event",
        "start": {"date_time": "2026-07-20T10:00:00+08:00"},
        "end": {"date_time": "2026-07-20T11:00:00+08:00"},
        "idempotency_key": "request-1",
    }
    first = await create.action(arguments, _tool_context())
    second = await create.action(arguments, _tool_context())
    assert first == second
    assert client.create_calls == 1
    update = next(item for item in actions if item.manifest.name == "google_calendar_event_update")
    update_arguments = {
        "calendar_id": "team-calendar@example.test",
        "event_id": "event-existing",
        "summary": "Approved update",
        "idempotency_key": "request-2",
    }
    updated_first = await update.action(update_arguments, _tool_context())
    updated_second = await update.action(update_arguments, _tool_context())
    assert updated_first == updated_second
    assert client.update_calls == 1

    tool = ConnectorTool(
        name=create.manifest.name,
        description=create.manifest.description,
        action=create.action,
        outbound=True,
        idempotency_required=True,
        effect_store=InMemoryEffectStore(),
    )
    with pytest.raises(ValueError, match="idempotency_key"):
        await tool.run(
            {
                "calendar_id": "team-calendar@example.test",
                "summary": "Missing key",
                "start": {"date": "2026-07-20"},
                "end": {"date": "2026-07-21"},
            },
            _tool_context(),
        )
    with pytest.raises(PermissionError, match="not selected"):
        await create.action(
            {**arguments, "calendar_id": "blocked@example.test"},
            _tool_context(),
        )
    with pytest.raises(PermissionError, match="does not grant event write"):
        await create.action(
            {**arguments, "calendar_id": "read-only@example.test"},
            _tool_context(),
        )


async def _writable_context(store: FakeCredentialStore) -> ConnectorActionContext:
    """A context with ``team-calendar@example.test`` selected + write-granted (matches
    ``test_outbound_actions_require_selection_approval_and_reconcile_retries``'s setup)."""
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        GOOGLE_CALENDAR_CONNECTOR_ID,
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
    )
    await repository.upsert_resources(
        GOOGLE_CALENDAR_CONNECTOR_ID,
        binding.id,
        (
            ConnectorResourceDraft(
                "team-calendar@example.test",
                "calendar",
                "Team Calendar",
                selected=True,
                config={"access_role": "owner"},
            ),
        ),
    )
    return ConnectorActionContext.with_repository("scope:a", repository, credential_store=store)


async def test_calendar_reconciler_confirms_existing_created_event() -> None:
    """R1B (C4): the reconciler recomputes the same deterministic identity the create
    action used and proves the event exists via a direct lookup."""
    from keel_core.connector_contracts import (
        ConnectorReconciliationOutcome,
        ConnectorReconciliationRequest,
    )

    client = FakeCalendarClient(_credential(GOOGLE_CALENDAR_WRITE_SCOPES))
    store = FakeCredentialStore(_credential(GOOGLE_CALENDAR_WRITE_SCOPES))
    provider = GoogleCalendarProvider(client_factory=_factory(client))
    context = await _writable_context(store)
    actions = provider.build_actions(context)
    create = next(item for item in actions if item.manifest.name == "google_calendar_event_create")
    arguments = {
        "calendar_id": "team-calendar@example.test",
        "summary": "Approved event",
        "start": {"date_time": "2026-07-20T10:00:00+08:00"},
        "end": {"date_time": "2026-07-20T11:00:00+08:00"},
        "idempotency_key": "request-1",
    }
    await create.action(arguments, _tool_context())

    reconciler = provider.build_reconciler(context)
    assert reconciler is not None
    outcome = await reconciler.reconcile(
        ConnectorReconciliationRequest(
            scope_id="scope:a",
            connector_id=GOOGLE_CALENDAR_CONNECTOR_ID,
            action_name="google_calendar_event_create",
            idempotency_key="request-1",
            resource_id="team-calendar@example.test",
            provider_ref="",
            canonical_args=json.dumps(arguments, sort_keys=True),
        )
    )
    assert outcome.outcome is ConnectorReconciliationOutcome.confirmed


async def test_calendar_reconciler_reports_absent_when_event_never_landed() -> None:
    from keel_core.connector_contracts import (
        ConnectorReconciliationOutcome,
        ConnectorReconciliationRequest,
    )

    client = FakeCalendarClient(_credential(GOOGLE_CALENDAR_WRITE_SCOPES))
    store = FakeCredentialStore(_credential(GOOGLE_CALENDAR_WRITE_SCOPES))
    provider = GoogleCalendarProvider(client_factory=_factory(client))
    context = ConnectorActionContext("scope:a", credential_store=store)

    reconciler = provider.build_reconciler(context)
    assert reconciler is not None
    # No create ever happened for this idempotency key: get_event's fake fallback
    # returns an event with no matching marker, so the reconciler proves absence.
    outcome = await reconciler.reconcile(
        ConnectorReconciliationRequest(
            scope_id="scope:a",
            connector_id=GOOGLE_CALENDAR_CONNECTOR_ID,
            action_name="google_calendar_event_create",
            idempotency_key="never-sent",
            resource_id="team-calendar@example.test",
            provider_ref="",
            canonical_args="{}",
        )
    )
    assert outcome.outcome is ConnectorReconciliationOutcome.absent


async def test_calendar_reconciler_confirms_existing_updated_event() -> None:
    from keel_core.connector_contracts import (
        ConnectorReconciliationOutcome,
        ConnectorReconciliationRequest,
    )

    client = FakeCalendarClient(_credential(GOOGLE_CALENDAR_WRITE_SCOPES))
    store = FakeCredentialStore(_credential(GOOGLE_CALENDAR_WRITE_SCOPES))
    provider = GoogleCalendarProvider(client_factory=_factory(client))
    context = await _writable_context(store)
    actions = provider.build_actions(context)
    update = next(item for item in actions if item.manifest.name == "google_calendar_event_update")
    update_arguments = {
        "calendar_id": "team-calendar@example.test",
        "event_id": "event-existing",
        "summary": "Approved update",
        "idempotency_key": "request-2",
    }
    await update.action(update_arguments, _tool_context())

    reconciler = provider.build_reconciler(context)
    assert reconciler is not None
    outcome = await reconciler.reconcile(
        ConnectorReconciliationRequest(
            scope_id="scope:a",
            connector_id=GOOGLE_CALENDAR_CONNECTOR_ID,
            action_name="google_calendar_event_update",
            idempotency_key="request-2",
            resource_id="team-calendar@example.test",
            provider_ref="",
            canonical_args=json.dumps(update_arguments, sort_keys=True),
        )
    )
    assert outcome.outcome is ConnectorReconciliationOutcome.confirmed


async def test_calendar_reconciler_is_incapable_without_credentials() -> None:
    from keel_core.connector_contracts import (
        ConnectorReconciliationOutcome,
        ConnectorReconciliationRequest,
    )

    store = FakeCredentialStore(None)
    provider = GoogleCalendarProvider()
    context = ConnectorActionContext("scope:a", credential_store=store)
    reconciler = provider.build_reconciler(context)
    assert reconciler is not None
    outcome = await reconciler.reconcile(
        ConnectorReconciliationRequest(
            scope_id="scope:a",
            connector_id=GOOGLE_CALENDAR_CONNECTOR_ID,
            action_name="google_calendar_event_create",
            idempotency_key="k",
            resource_id="team-calendar@example.test",
            provider_ref="",
            canonical_args="{}",
        )
    )
    assert outcome.outcome is ConnectorReconciliationOutcome.incapable


async def test_refresh_health_and_remote_revoke_errors_are_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(google_calendar, "enabled", lambda: True)

    def fail_client(
        credential: CredentialEnvelope,
        scopes: tuple[str, ...],
    ) -> CalendarClient:
        del credential, scopes
        raise GoogleCalendarAuthenticationError(
            "Google Calendar credential refresh failed; reconnect the connector."
        )

    provider = GoogleCalendarProvider(
        client_factory=fail_client,
        clock=lambda: datetime(2026, 7, 18, tzinfo=UTC),
    )
    context = ConnectorOperationContext(
        "scope:a",
        GOOGLE_CALENDAR_CONNECTOR_ID,
        binding=_binding(),
        credential=_credential(),
        credential_version=1,
    )
    health = await provider.health(context)
    assert health.status is ConnectorHealthStatus.error
    assert health.retryable is False
    assert health.message is not None and "refresh failed" in health.message

    def rate_limited(
        credential: CredentialEnvelope,
        scopes: tuple[str, ...],
    ) -> CalendarClient:
        del credential, scopes
        raise GoogleCalendarRateLimitError("Google Calendar is rate limited.")

    degraded = await GoogleCalendarProvider(client_factory=rate_limited).health(context)
    assert degraded.status is ConnectorHealthStatus.degraded
    assert degraded.retryable is True

    async def fail_revoke(token: str) -> None:
        assert token == "sanitized-refresh-token"
        raise GoogleCalendarRevokeError(
            "Google Calendar remote revoke failed; local credentials were retained."
        )

    provider = GoogleCalendarProvider(revoker=fail_revoke)
    with pytest.raises(GoogleCalendarRevokeError, match="retained"):
        await provider.revoke(context)
