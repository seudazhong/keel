"""Hermetic Google Calendar provider, sync, OAuth, and action contracts."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

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
    GoogleCalendarProvider,
    GoogleCalendarRateLimitError,
    GoogleCalendarRevokeError,
    GoogleCalendarSyncTokenExpired,
    GoogleCalendarWriteAuthorizationRequired,
)
from keel_core.connector_providers.google_calendar._client import (
    CalendarClient,
    build_client,
)
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.connectors import ConnectorTool
from keel_core.digest import digest_permissions
from keel_core.outbox import InMemoryOutboundStore
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
        idempotency_store=InMemoryOutboundStore(),
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
