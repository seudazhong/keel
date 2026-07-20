"""Google Calendar connector manifest, provider operations, and tool actions."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import quote

from keel_core.config import get_settings
from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAction,
    ConnectorActionApproval,
    ConnectorActionContext,
    ConnectorActionIdempotency,
    ConnectorActionManifest,
    ConnectorActionSemantics,
    ConnectorAuthAction,
    ConnectorAuthKind,
    ConnectorAuthStart,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCallbackParameter,
    ConnectorCapability,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorCredentialUpdate,
    ConnectorCursorUpdate,
    ConnectorEvent,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorProvenance,
    ConnectorResource,
    ConnectorResourceDraft,
    ConnectorResourceRefreshMode,
    ConnectorResourceResult,
    ConnectorSetupResult,
    ConnectorStateUpdate,
    ConnectorSyncResult,
    ConnectorTargetField,
    ConnectorTargetKind,
)
from keel_core.connector_credentials import CredentialEnvelope
from keel_core.protocols import ToolContext

from ._client import (
    GOOGLE_CALENDAR_CONNECTOR_ID,
    GOOGLE_CALENDAR_READ_SCOPES,
    GOOGLE_CALENDAR_WRITE_SCOPE,
    GOOGLE_CALENDAR_WRITE_SCOPES,
    CalendarClient,
    GoogleCalendarAuthenticationError,
    GoogleCalendarError,
    GoogleCalendarRateLimitError,
    GoogleCalendarRevokeError,
    GoogleCalendarSyncTokenExpired,
    GoogleCalendarWriteAuthorizationRequired,
    build_client,
    merge_authorized_user_values,
    revoke_google_token,
)

_MAX_API_PAGES = 20
_SYNC_PAGE_SIZE = 2500
_FULL_RESYNC_LOOKBACK_DAYS = 30
_EVENTS_STREAM = "events"


class CredentialStore(Protocol):
    async def get(self, connector_id: str) -> CredentialEnvelope | None: ...

    async def put(self, connector_id: str, credential: CredentialEnvelope) -> None: ...


ClientFactory = Callable[[CredentialEnvelope, tuple[str, ...]], CalendarClient]
RevokeFn = Callable[[str], Awaitable[None]]
Clock = Callable[[], datetime]


def _object_schema(
    properties: Mapping[str, Any],
    *,
    required: tuple[str, ...] = (),
) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": dict(properties),
        "additionalProperties": False,
    }
    if required:
        schema["required"] = list(required)
    return schema


_READ_PROPERTIES: dict[str, Any] = {
    "calendar_id": {"type": "string"},
    "max_results": {"type": "integer", "minimum": 1, "maximum": 250},
    "page_token": {"type": "string"},
    "time_min": {"type": "string", "format": "date-time"},
    "time_max": {"type": "string", "format": "date-time"},
    "time_zone": {"type": "string"},
}
_TIME_PROPERTIES: dict[str, Any] = {
    "date": {"type": "string", "format": "date"},
    "date_time": {"type": "string", "format": "date-time"},
    "time_zone": {"type": "string"},
}
_WRITE_PROPERTIES: dict[str, Any] = {
    "calendar_id": {"type": "string"},
    "summary": {"type": "string"},
    "description": {"type": "string"},
    "location": {"type": "string"},
    "start": _object_schema(_TIME_PROPERTIES),
    "end": _object_schema(_TIME_PROPERTIES),
    "idempotency_key": {"type": "string"},
}

EVENTS_LIST_ACTION = ConnectorActionManifest(
    name="google_calendar_events_list",
    description="List upcoming events from a selected Google calendar.",
    input_schema=_object_schema(_READ_PROPERTIES, required=("calendar_id",)),
    semantics=ConnectorActionSemantics.read,
)
EVENTS_SEARCH_ACTION = ConnectorActionManifest(
    name="google_calendar_events_search",
    description="Search upcoming events in a selected Google calendar.",
    input_schema=_object_schema(
        {**_READ_PROPERTIES, "query": {"type": "string"}},
        required=("calendar_id", "query"),
    ),
    semantics=ConnectorActionSemantics.read,
)
EVENT_GET_ACTION = ConnectorActionManifest(
    name="google_calendar_event_get",
    description="Get one event from a selected Google calendar.",
    input_schema=_object_schema(
        {
            "calendar_id": {"type": "string"},
            "event_id": {"type": "string"},
            "time_zone": {"type": "string"},
        },
        required=("calendar_id", "event_id"),
    ),
    semantics=ConnectorActionSemantics.read,
)
EVENT_CREATE_ACTION = ConnectorActionManifest(
    name="google_calendar_event_create",
    description="Create an approved event in a selected Google calendar.",
    input_schema=_object_schema(
        _WRITE_PROPERTIES,
        required=("calendar_id", "summary", "start", "end", "idempotency_key"),
    ),
    semantics=ConnectorActionSemantics.outbound,
    idempotency=ConnectorActionIdempotency.required,
    approval=ConnectorActionApproval.tainted,
)
EVENT_UPDATE_ACTION = ConnectorActionManifest(
    name="google_calendar_event_update",
    description="Update an approved event in a selected Google calendar.",
    input_schema=_object_schema(
        {**_WRITE_PROPERTIES, "event_id": {"type": "string"}},
        required=("calendar_id", "event_id", "idempotency_key"),
    ),
    semantics=ConnectorActionSemantics.outbound,
    idempotency=ConnectorActionIdempotency.required,
    approval=ConnectorActionApproval.tainted,
)

manifest = ConnectorManifest(
    id=GOOGLE_CALENDAR_CONNECTOR_ID,
    name="Google Calendar",
    description="Read and sync selected calendars; create or update approved events.",
    icon="📅",
    auth_kind=ConnectorAuthKind.oauth,
    capabilities=(
        ConnectorCapability.read,
        ConnectorCapability.write,
        ConnectorCapability.sync,
        ConnectorCapability.resources,
    ),
    scopes=GOOGLE_CALENDAR_READ_SCOPES,
    auth_action=ConnectorAuthAction(
        callback_parameters=(ConnectorCallbackParameter("code"),),
        help_text=(
            "Initial consent is read-only. Reconnect after requesting create/update "
            "to grant the incremental event-write scope."
        ),
    ),
    resource_label="Calendars",
    target_fields=(
        ConnectorTargetField(
            ConnectorTargetKind.trigger_session,
            "Session receiving calendar changes",
            help_text="Synced Google Calendar changes are admitted as tainted external events.",
        ),
    ),
    actions=(
        EVENTS_LIST_ACTION,
        EVENTS_SEARCH_ACTION,
        EVENT_GET_ACTION,
        EVENT_CREATE_ACTION,
        EVENT_UPDATE_ACTION,
    ),
    default_sync_cadence_seconds=300,
)


def enabled() -> bool:
    return Path(get_settings().gmail_client_secrets_path).is_file()


def availability() -> None:
    import google_auth_httplib2  # noqa: F401
    import google_auth_oauthlib.flow  # noqa: F401
    import googleapiclient.discovery  # noqa: F401
    import httplib2  # noqa: F401


def _flow(redirect_uri: str, scopes: tuple[str, ...]) -> object:
    from google_auth_oauthlib.flow import Flow

    return Flow.from_client_secrets_file(
        get_settings().gmail_client_secrets_path,
        scopes=list(scopes),
        redirect_uri=redirect_uri,
    )


class GoogleCalendarProvider(BaseConnectorProvider):
    manifest = manifest

    def __init__(
        self,
        *,
        client_factory: ClientFactory = build_client,
        revoker: RevokeFn = revoke_google_token,
        clock: Clock | None = None,
    ) -> None:
        self._client_factory = client_factory
        self._revoker = revoker
        self._clock = clock or (lambda: datetime.now(UTC))

    def enabled(self) -> bool:
        return enabled()

    async def begin_auth(
        self,
        context: ConnectorOperationContext,
        callback_url: str,
    ) -> ConnectorAuthStart:
        scopes = _authorization_scopes(context.credential)
        flow = _flow(callback_url, scopes)
        auth_url, state = flow.authorization_url(  # type: ignore[attr-defined]
            access_type="offline",
            prompt="consent",
            include_granted_scopes="true",
        )
        return ConnectorAuthStart(str(auth_url), str(state))

    async def complete_auth(
        self,
        context: ConnectorOperationContext,
        callback_url: str,
        parameters: dict[str, str],
    ) -> ConnectorSetupResult:
        code = parameters.get("code", "").strip()
        if not code:
            raise ValueError("missing authorization code")
        scopes = _authorization_scopes(context.credential)
        flow = _flow(callback_url, scopes)
        try:
            flow.fetch_token(code=code)  # type: ignore[attr-defined]
            credentials = flow.credentials  # type: ignore[attr-defined]
            raw = json.loads(cast(str, credentials.to_json()))
        except Exception as exc:
            raise GoogleCalendarAuthenticationError(
                "Google Calendar authorization exchange failed."
            ) from exc
        if not isinstance(raw, dict):
            raise GoogleCalendarAuthenticationError(
                "Google Calendar credentials did not serialize to an object."
            )
        values = merge_authorized_user_values(context.credential, raw, scopes)
        granted = frozenset(str(item) for item in values["scopes"])
        if not set(scopes).issubset(granted):
            raise GoogleCalendarAuthenticationError(
                "Google Calendar did not grant all requested scopes."
            )
        write_granted = GOOGLE_CALENDAR_WRITE_SCOPE in granted
        return ConnectorSetupResult(
            binding=ConnectorBindingDraft(
                display_name="Google Calendar",
                metadata={
                    "authorization_stage": "read_write" if write_granted else "read",
                    "granted_scopes": sorted(granted),
                },
            ),
            credential=CredentialEnvelope("oauth", values),
            status=ConnectorBindingStatus.connected,
        )

    async def list_resources(
        self,
        context: ConnectorOperationContext,
    ) -> ConnectorResourceResult:
        client = self._context_client(context, GOOGLE_CALENDAR_READ_SCOPES)
        try:
            resources: list[ConnectorResourceDraft] = []
            page_token: str | None = None
            for _ in range(_MAX_API_PAGES):
                response = await asyncio.to_thread(client.list_calendars, page_token)
                for raw in _items(response):
                    calendar_id = _required_text(raw, "id", "calendar")
                    summary = str(raw.get("summaryOverride") or raw.get("summary") or calendar_id)
                    resources.append(
                        ConnectorResourceDraft(
                            external_id=calendar_id,
                            kind="calendar",
                            display_name=summary,
                            url=_calendar_url(calendar_id),
                            config={
                                "access_role": str(raw.get("accessRole", "")),
                                "primary": bool(raw.get("primary", False)),
                                "time_zone": str(raw.get("timeZone", "")),
                            },
                        )
                    )
                page_token = _optional_text(response.get("nextPageToken"))
                if page_token is None:
                    return ConnectorResourceResult(
                        tuple(resources),
                        ConnectorResourceRefreshMode.authoritative,
                    )
            raise GoogleCalendarRateLimitError(
                "Google Calendar calendar discovery exceeded the pagination limit."
            )
        finally:
            client.close()

    async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
        if context.binding is None:
            raise GoogleCalendarError("Google Calendar binding is unavailable.")
        client = self._context_client(context, GOOGLE_CALENDAR_READ_SCOPES)
        changes: list[ConnectorChange] = []
        cursor_updates: list[ConnectorCursorUpdate] = []
        try:
            cursors = {
                cursor.resource_id: cursor.value
                for cursor in context.cursors
                if cursor.stream == _EVENTS_STREAM and cursor.resource_id is not None
            }
            for resource in context.resources:
                sync_token = cursors.get(resource.id)
                full_resync = False
                try:
                    events, next_sync_token = await asyncio.to_thread(
                        self._sync_calendar,
                        client,
                        resource,
                        sync_token,
                    )
                except GoogleCalendarSyncTokenExpired:
                    full_resync = True
                    events, next_sync_token = await asyncio.to_thread(
                        self._sync_calendar,
                        client,
                        resource,
                        None,
                    )
                for raw in events:
                    changes.append(
                        _sync_change(
                            context.binding.id,
                            resource,
                            raw,
                            full_resync=full_resync,
                        )
                    )
                cursor_updates.append(
                    ConnectorCursorUpdate(
                        _EVENTS_STREAM,
                        next_sync_token,
                        resource_id=resource.id,
                    )
                )
            state = ConnectorStateUpdate(
                credential=_credential_update(context, client.credential),
                cursor_updates=tuple(cursor_updates),
            )
            return ConnectorSyncResult(tuple(changes), state)
        finally:
            client.close()

    def _sync_calendar(
        self,
        client: CalendarClient,
        resource: ConnectorResource,
        sync_token: str | None,
    ) -> tuple[list[dict[str, Any]], str]:
        events: list[dict[str, Any]] = []
        page_token: str | None = None
        full_time_min = _rfc3339(self._clock() - timedelta(days=_FULL_RESYNC_LOOKBACK_DAYS))
        for _ in range(_MAX_API_PAGES):
            parameters: dict[str, Any] = {
                "maxResults": _SYNC_PAGE_SIZE,
                "pageToken": page_token,
                "showDeleted": True,
                "singleEvents": True,
            }
            if sync_token is None:
                parameters.update(
                    orderBy="startTime",
                    timeMin=full_time_min,
                )
            else:
                parameters["syncToken"] = sync_token
            response = client.list_events(resource.external_id, parameters)
            events.extend(_items(response))
            page_token = _optional_text(response.get("nextPageToken"))
            if page_token is not None:
                continue
            next_sync_token = _optional_text(response.get("nextSyncToken"))
            if next_sync_token is None:
                raise GoogleCalendarError(
                    "Google Calendar sync completed without a next sync token."
                )
            return events, next_sync_token
        raise GoogleCalendarRateLimitError(
            "Google Calendar sync exceeded the controlled pagination limit."
        )

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        checked_at = self._clock()
        if not self.enabled():
            return ConnectorHealth(
                ConnectorHealthStatus.degraded,
                checked_at,
                "Google Calendar OAuth client configuration is unavailable.",
                retryable=False,
            )
        if context.credential is None:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                checked_at,
                "Google Calendar credentials are missing.",
            )
        try:
            client = self._context_client(context, GOOGLE_CALENDAR_READ_SCOPES)
            try:
                await asyncio.to_thread(client.list_calendars, None)
            finally:
                client.close()
        except GoogleCalendarAuthenticationError as exc:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                checked_at,
                str(exc),
                retryable=False,
            )
        except GoogleCalendarRateLimitError as exc:
            return ConnectorHealth(
                ConnectorHealthStatus.degraded,
                checked_at,
                str(exc),
                retryable=True,
            )
        except GoogleCalendarError as exc:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                checked_at,
                str(exc),
                retryable=True,
            )
        return ConnectorHealth(
            ConnectorHealthStatus.healthy,
            checked_at,
            "Google Calendar read authorization is healthy.",
        )

    async def revoke(self, context: ConnectorOperationContext) -> None:
        if context.credential is None or context.credential.kind != "oauth":
            raise GoogleCalendarRevokeError(
                "Google Calendar credentials are missing; remote revoke was not attempted."
            )
        token = context.credential.values.get("refresh_token") or context.credential.values.get(
            "token"
        )
        if not isinstance(token, str) or not token:
            raise GoogleCalendarRevokeError(
                "Google Calendar credential has no revocable token; local state was retained."
            )
        await self._revoker(token)

    def build_actions(self, context: ConnectorActionContext) -> tuple[ConnectorAction, ...]:
        if context.credential_store is None:
            raise RuntimeError("encrypted connector credential storage is unavailable")
        store = cast(
            CredentialStore,
            context.envelope_credential_store or context.credential_store,
        )

        async def list_events(arguments: dict[str, Any], tool_context: ToolContext) -> str:
            del tool_context
            return await self._read_events_action(context, store, arguments)

        async def search_events(arguments: dict[str, Any], tool_context: ToolContext) -> str:
            del tool_context
            query = _required_argument(arguments, "query")
            return await self._read_events_action(
                context,
                store,
                arguments,
                query=query,
            )

        async def get_event(arguments: dict[str, Any], tool_context: ToolContext) -> str:
            del tool_context
            calendar_id = _required_argument(arguments, "calendar_id")
            event_id = _required_argument(arguments, "event_id")
            resource = await context.require_selected_resource(
                GOOGLE_CALENDAR_CONNECTOR_ID,
                calendar_id,
            )
            time_zone = _optional_argument(arguments, "time_zone")
            client, credential = await _action_client(
                store,
                GOOGLE_CALENDAR_READ_SCOPES,
                self._client_factory,
            )
            try:
                raw = await asyncio.to_thread(
                    client.get_event,
                    calendar_id,
                    event_id,
                    time_zone,
                )
                await _persist_action_credential(store, credential, client.credential)
                return _json(
                    _normalize_event(
                        raw,
                        binding_id=(await _binding_id(context)),
                        calendar_id=resource.external_id,
                    )
                )
            finally:
                client.close()

        async def create_event(arguments: dict[str, Any], tool_context: ToolContext) -> str:
            calendar_id = _required_argument(arguments, "calendar_id")
            await _require_writable_resource(context, calendar_id)
            summary = _required_argument(arguments, "summary")
            request_key = _required_argument(arguments, "idempotency_key")
            body = _write_body(arguments, require_times=True)
            body["summary"] = summary
            request_id = _request_id(tool_context.scope_id, calendar_id, request_key)
            event_id = _event_id(request_id)
            client, credential = await _action_client(
                store,
                GOOGLE_CALENDAR_WRITE_SCOPES,
                self._client_factory,
            )
            try:
                raw = await asyncio.to_thread(
                    client.create_event_reconciled,
                    calendar_id,
                    event_id,
                    request_id,
                    body,
                )
                await _persist_action_credential(store, credential, client.credential)
                return _json(
                    _normalize_event(
                        raw,
                        binding_id=(await _binding_id(context)),
                        calendar_id=calendar_id,
                    )
                )
            finally:
                client.close()

        async def update_event(arguments: dict[str, Any], tool_context: ToolContext) -> str:
            calendar_id = _required_argument(arguments, "calendar_id")
            await _require_writable_resource(context, calendar_id)
            event_id = _required_argument(arguments, "event_id")
            request_key = _required_argument(arguments, "idempotency_key")
            body = _write_body(arguments, require_times=False)
            if not body:
                raise GoogleCalendarError(
                    "google_calendar_event_update requires at least one event field."
                )
            request_id = _request_id(
                tool_context.scope_id,
                calendar_id,
                f"{event_id}:{request_key}",
            )
            client, credential = await _action_client(
                store,
                GOOGLE_CALENDAR_WRITE_SCOPES,
                self._client_factory,
            )
            try:
                raw = await asyncio.to_thread(
                    client.update_event_reconciled,
                    calendar_id,
                    event_id,
                    request_id,
                    body,
                )
                await _persist_action_credential(store, credential, client.credential)
                return _json(
                    _normalize_event(
                        raw,
                        binding_id=(await _binding_id(context)),
                        calendar_id=calendar_id,
                    )
                )
            finally:
                client.close()

        return (
            ConnectorAction(EVENTS_LIST_ACTION, list_events),
            ConnectorAction(EVENTS_SEARCH_ACTION, search_events),
            ConnectorAction(EVENT_GET_ACTION, get_event),
            ConnectorAction(EVENT_CREATE_ACTION, create_event),
            ConnectorAction(EVENT_UPDATE_ACTION, update_event),
        )

    async def _read_events_action(
        self,
        context: ConnectorActionContext,
        store: CredentialStore,
        arguments: Mapping[str, Any],
        *,
        query: str | None = None,
    ) -> str:
        calendar_id = _required_argument(arguments, "calendar_id")
        resource = await context.require_selected_resource(
            GOOGLE_CALENDAR_CONNECTOR_ID,
            calendar_id,
        )
        parameters = _read_parameters(arguments, self._clock(), query=query)
        client, credential = await _action_client(
            store,
            GOOGLE_CALENDAR_READ_SCOPES,
            self._client_factory,
        )
        try:
            response = await asyncio.to_thread(
                client.list_events,
                calendar_id,
                parameters,
            )
            await _persist_action_credential(store, credential, client.credential)
            binding_id = await _binding_id(context)
            return _json(
                {
                    "calendar_id": resource.external_id,
                    "time_zone": str(response.get("timeZone") or arguments.get("time_zone") or ""),
                    "events": [
                        _normalize_event(
                            event,
                            binding_id=binding_id,
                            calendar_id=resource.external_id,
                        )
                        for event in _items(response)
                    ],
                    "next_page_token": _optional_text(response.get("nextPageToken")),
                }
            )
        finally:
            client.close()

    def _context_client(
        self,
        context: ConnectorOperationContext,
        required_scopes: tuple[str, ...],
    ) -> CalendarClient:
        if context.credential is None:
            raise GoogleCalendarAuthenticationError(
                "Google Calendar credentials are missing; reconnect the connector."
            )
        return self._client_factory(context.credential, required_scopes)


def _authorization_scopes(
    credential: CredentialEnvelope | None,
) -> tuple[str, ...]:
    if credential is None:
        return GOOGLE_CALENDAR_READ_SCOPES
    return GOOGLE_CALENDAR_WRITE_SCOPES


def _credential_update(
    context: ConnectorOperationContext,
    latest: CredentialEnvelope,
) -> ConnectorCredentialUpdate | None:
    if context.credential == latest:
        return None
    return ConnectorCredentialUpdate(latest, context.credential_version)


async def _action_client(
    store: CredentialStore,
    required_scopes: tuple[str, ...],
    client_factory: ClientFactory,
) -> tuple[CalendarClient, CredentialEnvelope]:
    credential = await store.get(GOOGLE_CALENDAR_CONNECTOR_ID)
    if credential is None:
        raise GoogleCalendarAuthenticationError("Google Calendar is not authorized for this scope.")
    return client_factory(credential, required_scopes), credential


async def _persist_action_credential(
    store: CredentialStore,
    previous: CredentialEnvelope,
    latest: CredentialEnvelope,
) -> None:
    if latest != previous:
        await store.put(GOOGLE_CALENDAR_CONNECTOR_ID, latest)


async def _binding_id(context: ConnectorActionContext) -> str:
    state = await context.load_state(GOOGLE_CALENDAR_CONNECTOR_ID)
    if state.binding is None:
        raise GoogleCalendarError("Google Calendar binding is unavailable.")
    return state.binding.id


async def _require_writable_resource(
    context: ConnectorActionContext,
    calendar_id: str,
) -> ConnectorResource:
    resource = await context.require_selected_resource(
        GOOGLE_CALENDAR_CONNECTOR_ID,
        calendar_id,
    )
    role = resource.config.get("access_role")
    if role not in {"writer", "owner"}:
        raise PermissionError(
            f"selected Google calendar {calendar_id!r} does not grant event write access"
        )
    return resource


def _read_parameters(
    arguments: Mapping[str, Any],
    now: datetime,
    *,
    query: str | None,
) -> dict[str, Any]:
    raw_max = arguments.get("max_results", 50)
    if isinstance(raw_max, bool):
        raise GoogleCalendarError("max_results must be an integer.")
    try:
        max_results = int(raw_max)
    except (TypeError, ValueError) as exc:
        raise GoogleCalendarError("max_results must be an integer.") from exc
    if not 1 <= max_results <= 250:
        raise GoogleCalendarError("max_results must be between 1 and 250.")
    parameters: dict[str, Any] = {
        "maxResults": max_results,
        "singleEvents": True,
        "orderBy": "startTime",
        "timeMin": str(arguments.get("time_min") or _rfc3339(now)),
    }
    optional = {
        "pageToken": arguments.get("page_token"),
        "timeMax": arguments.get("time_max"),
        "timeZone": arguments.get("time_zone"),
        "q": query,
    }
    parameters.update(
        {key: value for key, value in optional.items() if isinstance(value, str) and value}
    )
    return parameters


def _write_body(
    arguments: Mapping[str, Any],
    *,
    require_times: bool,
) -> dict[str, Any]:
    body: dict[str, Any] = {}
    for field in ("summary", "description", "location"):
        value = arguments.get(field)
        if value is not None:
            if not isinstance(value, str):
                raise GoogleCalendarError(f"{field} must be text.")
            body[field] = value
    for field in ("start", "end"):
        raw = arguments.get(field)
        if raw is None:
            if require_times:
                raise GoogleCalendarError(f"Google Calendar event {field} is required.")
            continue
        body[field] = _event_time(raw, field)
    if ("start" in body) != ("end" in body):
        raise GoogleCalendarError("Google Calendar event start and end must be updated together.")
    return body


def _event_time(raw: object, field: str) -> dict[str, str]:
    if not isinstance(raw, dict):
        raise GoogleCalendarError(f"Google Calendar event {field} must be an object.")
    date = raw.get("date")
    date_time = raw.get("date_time")
    if bool(date) == bool(date_time):
        raise GoogleCalendarError(
            f"Google Calendar event {field} requires exactly one of date or date_time."
        )
    result: dict[str, str]
    if isinstance(date, str) and date:
        result = {"date": date}
    elif isinstance(date_time, str) and date_time:
        result = {"dateTime": date_time}
    else:
        raise GoogleCalendarError(f"Google Calendar event {field} time is invalid.")
    time_zone = raw.get("time_zone")
    if time_zone is not None:
        if not isinstance(time_zone, str) or not time_zone:
            raise GoogleCalendarError(f"Google Calendar event {field} time_zone is invalid.")
        result["timeZone"] = time_zone
    return result


def _normalize_event(
    raw: Mapping[str, Any],
    *,
    binding_id: str,
    calendar_id: str,
) -> dict[str, Any]:
    event_id = _required_text(raw, "id", "event")
    start = _normalize_event_time(raw.get("start"))
    end = _normalize_event_time(raw.get("end"))
    source_url = _optional_text(raw.get("htmlLink")) or _event_url(calendar_id, event_id)
    revision = _optional_text(raw.get("etag")) or _optional_text(raw.get("updated"))
    return {
        "calendar_id": calendar_id,
        "event_id": event_id,
        "status": str(raw.get("status", "")),
        "summary": str(raw.get("summary", "")),
        "description": str(raw.get("description", "")),
        "location": str(raw.get("location", "")),
        "start": start,
        "end": end,
        "all_day": "date" in start,
        "recurrence": [str(item) for item in raw.get("recurrence", []) if isinstance(item, str)],
        "recurring_event_id": _optional_text(raw.get("recurringEventId")),
        "original_start_time": _normalize_event_time(raw.get("originalStartTime")),
        "updated": _optional_text(raw.get("updated")),
        "provenance": {
            "connector_id": GOOGLE_CALENDAR_CONNECTOR_ID,
            "binding_id": binding_id,
            "external_resource_id": f"{calendar_id}:{event_id}",
            "source_url": source_url,
            "revision": revision,
            "event_id": event_id,
        },
    }


def _sync_change(
    binding_id: str,
    resource: ConnectorResource,
    raw: Mapping[str, Any],
    *,
    full_resync: bool,
) -> ConnectorChange:
    normalized = _normalize_event(
        raw,
        binding_id=binding_id,
        calendar_id=resource.external_id,
    )
    provenance_raw = cast(dict[str, Any], normalized["provenance"])
    provenance = ConnectorProvenance(
        GOOGLE_CALENDAR_CONNECTOR_ID,
        binding_id,
        str(provenance_raw["external_resource_id"]),
        source_url=cast(str, provenance_raw["source_url"]),
        revision=cast(str | None, provenance_raw["revision"]),
        event_id=cast(str, provenance_raw["event_id"]),
    )
    payload = dict(normalized)
    payload["full_resync"] = full_resync
    event = ConnectorEvent("google_calendar.event_changed", provenance, payload)
    return ConnectorChange(
        ConnectorChangeKind.event,
        provenance,
        event=event,
    )


def _normalize_event_time(raw: object) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, str] = {}
    for source, target in (("date", "date"), ("dateTime", "date_time"), ("timeZone", "time_zone")):
        value = raw.get(source)
        if isinstance(value, str) and value:
            result[target] = value
    return result


def _items(response: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = response.get("items", [])
    if not isinstance(raw, list):
        raise GoogleCalendarError("Google Calendar returned an invalid items collection.")
    return [cast(dict[str, Any], item) for item in raw if isinstance(item, dict)]


def _required_text(raw: Mapping[str, Any], key: str, kind: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value:
        raise GoogleCalendarError(f"Google Calendar returned a {kind} without {key}.")
    return value


def _required_argument(arguments: Mapping[str, Any], key: str) -> str:
    value = arguments.get(key)
    if not isinstance(value, str) or not value.strip():
        raise GoogleCalendarError(f"{key} is required.")
    return value.strip()


def _optional_argument(arguments: Mapping[str, Any], key: str) -> str | None:
    value = arguments.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise GoogleCalendarError(f"{key} must be non-empty text.")
    return value.strip()


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _request_id(scope_id: str, calendar_id: str, key: str) -> str:
    return hashlib.sha256(
        f"{GOOGLE_CALENDAR_CONNECTOR_ID}\0{scope_id}\0{calendar_id}\0{key}".encode()
    ).hexdigest()


def _event_id(request_id: str) -> str:
    return f"keel{request_id[:48]}"


def _calendar_url(calendar_id: str) -> str:
    return f"https://calendar.google.com/calendar/u/0/r?cid={quote(calendar_id, safe='')}"


def _event_url(calendar_id: str, event_id: str) -> str:
    return (
        "https://calendar.google.com/calendar/u/0/r/eventedit/"
        f"{quote(event_id, safe='')}?cid={quote(calendar_id, safe='')}"
    )


def _rfc3339(value: datetime) -> str:
    normalized = value.astimezone(UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def factory() -> GoogleCalendarProvider:
    return GoogleCalendarProvider()


__all__ = [
    "EVENTS_LIST_ACTION",
    "EVENTS_SEARCH_ACTION",
    "EVENT_CREATE_ACTION",
    "EVENT_GET_ACTION",
    "EVENT_UPDATE_ACTION",
    "GOOGLE_CALENDAR_CONNECTOR_ID",
    "GOOGLE_CALENDAR_READ_SCOPES",
    "GOOGLE_CALENDAR_WRITE_SCOPE",
    "GOOGLE_CALENDAR_WRITE_SCOPES",
    "GoogleCalendarAuthenticationError",
    "GoogleCalendarError",
    "GoogleCalendarProvider",
    "GoogleCalendarRateLimitError",
    "GoogleCalendarRevokeError",
    "GoogleCalendarSyncTokenExpired",
    "GoogleCalendarWriteAuthorizationRequired",
    "availability",
    "enabled",
    "factory",
    "manifest",
]
