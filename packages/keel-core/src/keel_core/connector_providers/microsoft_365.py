"""Microsoft 365 delegated Outlook Mail and Calendar connector."""

from __future__ import annotations

import asyncio
import base64
import json
import secrets
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, Protocol, cast
from urllib.parse import quote, urlencode, urlsplit

from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAction,
    ConnectorActionContext,
    ConnectorActionManifest,
    ConnectorActionSemantics,
    ConnectorAuthAction,
    ConnectorAuthenticationError,
    ConnectorAuthKind,
    ConnectorAuthStart,
    ConnectorBinding,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCallbackParameter,
    ConnectorCapability,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorCredentialUpdate,
    ConnectorCursorUpdate,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorProvenance,
    ConnectorResource,
    ConnectorResourceDraft,
    ConnectorResourceResult,
    ConnectorSetupField,
    ConnectorSetupResult,
    ConnectorStateUpdate,
    ConnectorSyncResult,
    ConnectorTargetField,
    ConnectorTargetKind,
    ConnectorUnsupportedError,
)
from keel_core.connector_credentials import CredentialEnvelope
from keel_core.connectors import ActionFn
from keel_core.errors import KeelError
from keel_core.protocols import ToolContext

MICROSOFT_365_CONNECTOR_ID = "microsoft_365"
GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
LOGIN_ROOT = "https://login.microsoftonline.com"
APP_CREDENTIAL_KIND = "microsoft_365_app"
OAUTH_CREDENTIAL_KIND = "microsoft_365_oauth"
SCOPES: tuple[str, ...] = (
    "openid",
    "profile",
    "offline_access",
    "User.Read",
    "Mail.Read",
    "Calendars.Read",
)
REQUIRED_GRAPH_SCOPES = frozenset({"user.read", "mail.read", "calendars.read"})
TOKEN_REFRESH_SKEW_SECONDS = 120
MAX_GRAPH_PAGES = 100
DEFAULT_SYNC_SECONDS = 300
CALENDAR_SYNC_PAST_DAYS = 30
CALENDAR_SYNC_FUTURE_DAYS = 365


class Microsoft365Error(KeelError):
    """The Microsoft 365 connector cannot safely complete an operation."""


class Microsoft365PermissionError(Microsoft365Error):
    """The delegated grant lacks a required read-only permission."""


class Microsoft365TenantMismatchError(Microsoft365Error):
    """The token or account does not belong to the configured tenant."""


class Microsoft365AccountMismatchError(Microsoft365Error):
    """The refreshed token belongs to a different account."""


class Microsoft365CursorInvalidError(Microsoft365Error):
    """A Microsoft Graph delta cursor must be replaced by a full synchronization."""


class Microsoft365ThrottledError(Microsoft365Error):
    """Microsoft Graph kept throttling after bounded Retry-After retries."""

    def __init__(self, retry_after: float) -> None:
        self.retry_after = retry_after
        super().__init__(f"Microsoft Graph throttled the request; retry after {retry_after:g}s")


@dataclass(frozen=True, slots=True)
class Microsoft365HttpResponse:
    status_code: int
    headers: Mapping[str, str] = field(default_factory=dict)
    payload: Any = field(default=None, repr=False)


class Microsoft365HttpTransport(Protocol):
    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        data: Mapping[str, str] | None = None,
    ) -> Microsoft365HttpResponse: ...


class VersionedActionTokenStore(Protocol):
    async def get_versioned(self, connector_id: str) -> tuple[str, int] | None: ...

    async def put_if_version(
        self, connector_id: str, secret: str, expected_version: int
    ) -> int | None: ...


class HttpxMicrosoft365Transport:
    """Small httpx adapter; Graph SDKs are intentionally not required."""

    def __init__(self, timeout_seconds: float = 30.0) -> None:
        self._timeout = timeout_seconds

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        data: Mapping[str, str] | None = None,
    ) -> Microsoft365HttpResponse:
        import httpx

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.request(
                    method,
                    url,
                    headers=dict(headers or {}),
                    data=dict(data) if data is not None else None,
                )
        except httpx.HTTPError as exc:
            raise Microsoft365Error("Microsoft 365 network request failed") from exc
        try:
            payload: Any = response.json()
        except ValueError:
            payload = None
        return Microsoft365HttpResponse(response.status_code, dict(response.headers), payload)


Sleep = Callable[[float], Awaitable[None]]


class MicrosoftGraphClient:
    def __init__(
        self,
        access_token: str,
        transport: Microsoft365HttpTransport,
        *,
        sleep: Sleep = asyncio.sleep,
        max_throttle_retries: int = 2,
    ) -> None:
        if not access_token:
            raise ConnectorAuthenticationError("Microsoft 365 access token is missing")
        self._access_token = access_token
        self._transport = transport
        self._sleep = sleep
        self._max_throttle_retries = max_throttle_retries

    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        cursor_request: bool = False,
    ) -> dict[str, Any]:
        _validate_graph_url(url)
        request_headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
        }
        request_headers.update(headers or {})
        for attempt in range(self._max_throttle_retries + 1):
            response = await self._transport.request("GET", url, headers=request_headers)
            if response.status_code == 429:
                delay = _retry_after_seconds(response.headers.get("Retry-After"))
                if attempt < self._max_throttle_retries:
                    await self._sleep(delay)
                    continue
                raise Microsoft365ThrottledError(delay)
            if response.status_code == 401:
                raise ConnectorAuthenticationError(
                    "Microsoft 365 authorization expired or was revoked"
                )
            if response.status_code == 403:
                raise Microsoft365PermissionError(
                    "Microsoft Graph rejected the required read-only delegated scopes"
                )
            code = _graph_error_code(response.payload)
            if cursor_request and (
                response.status_code == 410
                or code.lower()
                in {
                    "invaliddeltatoken",
                    "resyncrequired",
                    "syncstatenotfound",
                }
            ):
                raise Microsoft365CursorInvalidError("Microsoft Graph delta cursor is invalid")
            if not 200 <= response.status_code < 300:
                label = code or f"HTTP {response.status_code}"
                raise Microsoft365Error(f"Microsoft Graph request failed ({label})")
            return _object(response.payload, "Microsoft Graph response")
        raise AssertionError("Microsoft Graph retry loop did not terminate")


MAIL_SEARCH_ACTION = ConnectorActionManifest(
    name="m365_mail_search",
    description="Search messages in one selected Microsoft 365 mail folder.",
    input_schema={
        "type": "object",
        "required": ["resource_id", "query"],
        "properties": {
            "resource_id": {"type": "string"},
            "query": {"type": "string"},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 50},
            "page_token": {"type": "string"},
        },
    },
    semantics=ConnectorActionSemantics.read,
)
MAIL_GET_ACTION = ConnectorActionManifest(
    name="m365_mail_get",
    description="Get one message from a selected Microsoft 365 mail folder.",
    input_schema={
        "type": "object",
        "required": ["resource_id", "message_id"],
        "properties": {
            "resource_id": {"type": "string"},
            "message_id": {"type": "string"},
        },
    },
    semantics=ConnectorActionSemantics.read,
)
CALENDAR_LIST_ACTION = ConnectorActionManifest(
    name="m365_calendar_list",
    description="List events in a selected Microsoft 365 calendar and time range.",
    input_schema={
        "type": "object",
        "required": ["resource_id", "start", "end"],
        "properties": {
            "resource_id": {"type": "string"},
            "start": {"type": "string"},
            "end": {"type": "string"},
            "timezone": {"type": "string"},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 50},
            "page_token": {"type": "string"},
        },
    },
    semantics=ConnectorActionSemantics.read,
)
CALENDAR_GET_ACTION = ConnectorActionManifest(
    name="m365_calendar_get",
    description="Get one event from a selected Microsoft 365 calendar.",
    input_schema={
        "type": "object",
        "required": ["resource_id", "event_id"],
        "properties": {
            "resource_id": {"type": "string"},
            "event_id": {"type": "string"},
            "timezone": {"type": "string"},
        },
    },
    semantics=ConnectorActionSemantics.read,
)
CALENDAR_UPCOMING_ACTION = ConnectorActionManifest(
    name="m365_calendar_upcoming",
    description="List upcoming events from a selected Microsoft 365 calendar.",
    input_schema={
        "type": "object",
        "required": ["resource_id"],
        "properties": {
            "resource_id": {"type": "string"},
            "days": {"type": "integer", "minimum": 1, "maximum": 90},
            "timezone": {"type": "string"},
            "page_size": {"type": "integer", "minimum": 1, "maximum": 50},
            "page_token": {"type": "string"},
        },
    },
    semantics=ConnectorActionSemantics.read,
)

manifest = ConnectorManifest(
    id=MICROSOFT_365_CONNECTOR_ID,
    name="Microsoft 365",
    description="Read Outlook Mail and Calendar with a single-tenant Entra application.",
    icon="📨",
    auth_kind=ConnectorAuthKind.oauth,
    capabilities=(
        ConnectorCapability.read,
        ConnectorCapability.resources,
        ConnectorCapability.sync,
    ),
    scopes=SCOPES,
    setup_fields=(
        ConnectorSetupField(
            "tenant_id",
            "Microsoft Entra tenant ID",
            help_text="Directory (tenant) GUID for the single-tenant application.",
        ),
        ConnectorSetupField("client_id", "Application (client) ID"),
        ConnectorSetupField(
            "client_secret",
            "Client secret",
            secret=True,
            help_text="Stored only in Keel's encrypted connector credential envelope.",
        ),
    ),
    auth_action=ConnectorAuthAction(
        label="Authorize Microsoft 365",
        callback_parameters=(ConnectorCallbackParameter("code"),),
        requires_setup=True,
        help_text="Save the Entra application settings, then authorize the mailbox account.",
    ),
    setup_action_label="Save Entra application",
    resource_label="Mail folders and calendars",
    target_fields=(
        ConnectorTargetField(
            ConnectorTargetKind.knowledge,
            "Knowledge Base",
            help_text="Delta-synchronized mail and calendar items are stored as tainted content.",
        ),
    ),
    actions=(
        MAIL_SEARCH_ACTION,
        MAIL_GET_ACTION,
        CALENDAR_LIST_ACTION,
        CALENDAR_GET_ACTION,
        CALENDAR_UPCOMING_ACTION,
    ),
    default_sync_cadence_seconds=DEFAULT_SYNC_SECONDS,
)


class Microsoft365Provider(BaseConnectorProvider):
    manifest = manifest

    def __init__(
        self,
        transport: Microsoft365HttpTransport | None = None,
        *,
        sleep: Sleep = asyncio.sleep,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._transport = transport or HttpxMicrosoft365Transport()
        self._sleep = sleep
        self._now = now or (lambda: datetime.now(UTC))

    async def setup(
        self, context: ConnectorOperationContext, values: dict[str, str]
    ) -> ConnectorSetupResult:
        tenant_id = _guid(values.get("tenant_id"), "tenant_id")
        client_id = _guid(values.get("client_id"), "client_id")
        client_secret = values.get("client_secret", "").strip()
        if not client_secret:
            raise ValueError("client_secret must not be blank")
        return ConnectorSetupResult(
            ConnectorBindingDraft(
                display_name="Microsoft 365",
                external_tenant_id=tenant_id,
                metadata={"setup": "single_tenant_delegated"},
            ),
            CredentialEnvelope(
                APP_CREDENTIAL_KIND,
                {
                    "tenant_id": tenant_id,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
            ),
            status=ConnectorBindingStatus.configured,
        )

    async def begin_auth(
        self, context: ConnectorOperationContext, callback_url: str
    ) -> ConnectorAuthStart:
        app = _app_credential(context.credential)
        _enforce_staged_tenant(context.binding, app["tenant_id"])
        state = secrets.token_urlsafe(32)
        query = urlencode(
            {
                "client_id": app["client_id"],
                "response_type": "code",
                "redirect_uri": callback_url,
                "response_mode": "query",
                "scope": " ".join(SCOPES),
                "state": state,
                "prompt": "consent",
            }
        )
        return ConnectorAuthStart(
            f"{LOGIN_ROOT}/{quote(app['tenant_id'], safe='')}/oauth2/v2.0/authorize?{query}",
            state,
        )

    async def complete_auth(
        self,
        context: ConnectorOperationContext,
        callback_url: str,
        parameters: dict[str, str],
    ) -> ConnectorSetupResult:
        code = parameters.get("code", "").strip()
        if not code:
            raise ConnectorAuthenticationError("missing Microsoft authorization code")
        app = _app_credential(context.credential)
        _enforce_staged_tenant(context.binding, app["tenant_id"])
        token = await _token_request(
            self._transport,
            app,
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": callback_url,
                "scope": " ".join(SCOPES),
            },
        )
        values = _oauth_values_from_token(app, token, now=self._now())
        graph = self._graph(values)
        profile = await graph.get(f"{GRAPH_ROOT}/me?$select=id,displayName,userPrincipalName,mail")
        account_id = _required_string(profile, "id", "Microsoft Graph profile")
        values["account_id"] = account_id
        values["account_name"] = _account_name(profile)
        _enforce_tenant(values, app["tenant_id"])
        return ConnectorSetupResult(
            ConnectorBindingDraft(
                display_name=values["account_name"],
                external_account_id=account_id,
                external_tenant_id=app["tenant_id"],
                metadata={
                    "grant": "delegated",
                    "granted_scopes": sorted(_scope_set(values)),
                },
            ),
            CredentialEnvelope(OAUTH_CREDENTIAL_KIND, values),
        )

    async def list_resources(
        self, context: ConnectorOperationContext
    ) -> ConnectorResourceResult:
        binding, values = _bound_oauth(context)
        _require_unexpired(values, now=self._now())
        _enforce_binding(binding, values)
        graph = self._graph(values)
        folders = await _all_values(
            graph,
            (
                f"{GRAPH_ROOT}/me/mailFolders?"
                + urlencode(
                    {
                        "$select": (
                            "id,displayName,parentFolderId,wellKnownName,isHidden,totalItemCount"
                        ),
                        "$top": "100",
                        "includeHiddenFolders": "false",
                    }
                )
            ),
        )
        calendars = await _all_values(
            graph,
            (
                f"{GRAPH_ROOT}/me/calendars?"
                + urlencode(
                    {
                        "$select": "id,name,canEdit,canShare,owner",
                        "$top": "100",
                    }
                )
            ),
        )
        resources = [
            ConnectorResourceDraft(
                external_id=f"mail:{_required_string(item, 'id', 'mail folder')}",
                kind="mail_folder",
                display_name=_string(item.get("displayName")) or "Mail folder",
                config={
                    "graph_id": _required_string(item, "id", "mail folder"),
                    "well_known_name": _string(item.get("wellKnownName")),
                    "hidden": bool(item.get("isHidden", False)),
                },
            )
            for item in folders
            if not bool(item.get("isHidden", False))
        ]
        resources.extend(
            ConnectorResourceDraft(
                external_id=f"calendar:{_required_string(item, 'id', 'calendar')}",
                kind="calendar",
                display_name=_string(item.get("name")) or "Calendar",
                config={
                    "graph_id": _required_string(item, "id", "calendar"),
                    "can_edit": bool(item.get("canEdit", False)),
                },
            )
            for item in calendars
        )
        return ConnectorResourceResult(tuple(resources))

    async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
        binding, values = _bound_oauth(context)
        _enforce_binding(binding, values)
        values, rotated = await self._refresh(values, binding)
        graph = self._graph(values)
        changes: list[ConnectorChange] = []
        cursor_updates: list[ConnectorCursorUpdate] = []
        cursors = {(item.resource_id, item.stream): item for item in context.cursors}
        for resource in context.resources:
            cursor = cursors.get((resource.id, "delta"))
            initial_url = self._initial_delta_url(resource)
            full_resync = cursor is None
            try:
                items, delta_link = await _delta_values(
                    graph,
                    cursor.value if cursor is not None else initial_url,
                    cursor_request=cursor is not None,
                )
            except Microsoft365CursorInvalidError:
                items, delta_link = await _delta_values(graph, initial_url, cursor_request=False)
                full_resync = True
            resource_changes, current_ids = _map_delta(binding, resource, items)
            changes.extend(resource_changes)
            if full_resync:
                prefix = f"{resource.kind}:{_resource_graph_id(resource)}:"
                for item in context.items:
                    if item.external_id.startswith(prefix) and item.external_id not in current_ids:
                        changes.append(
                            ConnectorChange(
                                ConnectorChangeKind.delete,
                                ConnectorProvenance(
                                    MICROSOFT_365_CONNECTOR_ID,
                                    binding.id,
                                    item.external_id,
                                    source_url=item.url,
                                    revision="full_resync",
                                ),
                            )
                        )
            cursor_updates.append(
                ConnectorCursorUpdate(
                    "delta",
                    delta_link,
                    resource_id=resource.id,
                    revision="full_resync" if full_resync else "delta",
                )
            )
        state = ConnectorStateUpdate(
            credential=(
                ConnectorCredentialUpdate(
                    CredentialEnvelope(OAUTH_CREDENTIAL_KIND, values),
                    context.credential_version,
                )
                if rotated
                else None
            ),
            cursor_updates=tuple(cursor_updates),
        )
        return ConnectorSyncResult(tuple(changes), state)

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        checked_at = self._now()
        try:
            binding, values = _bound_oauth(context)
            _require_unexpired(values, now=checked_at)
            _enforce_binding(binding, values)
            profile = await self._graph(values).get(f"{GRAPH_ROOT}/me?$select=id")
            account_id = _required_string(profile, "id", "Microsoft Graph profile")
            if account_id != binding.external_account_id:
                raise Microsoft365AccountMismatchError(
                    "Microsoft 365 token belongs to a different account"
                )
        except (ConnectorAuthenticationError, Microsoft365Error, ValueError) as exc:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                checked_at,
                str(exc),
                retryable=not isinstance(
                    exc,
                    (
                        Microsoft365PermissionError,
                        Microsoft365TenantMismatchError,
                        Microsoft365AccountMismatchError,
                    ),
                ),
            )
        return ConnectorHealth(ConnectorHealthStatus.healthy, checked_at)

    async def revoke(self, context: ConnectorOperationContext) -> None:
        if context.credential is None:
            return
        raise ConnectorUnsupportedError(
            "Microsoft Entra has no per-refresh-token revocation endpoint for delegated apps; "
            "revoke the app grant in Entra, then use explicit local forget"
        )

    def build_actions(self, context: ConnectorActionContext) -> tuple[ConnectorAction, ...]:
        if context.credential_store is None:
            raise RuntimeError("encrypted connector credential storage is unavailable")
        store = cast(VersionedActionTokenStore, context.credential_store)
        return (
            ConnectorAction(MAIL_SEARCH_ACTION, self._mail_search_action(context, store)),
            ConnectorAction(MAIL_GET_ACTION, self._mail_get_action(context, store)),
            ConnectorAction(CALENDAR_LIST_ACTION, self._calendar_list_action(context, store)),
            ConnectorAction(CALENDAR_GET_ACTION, self._calendar_get_action(context, store)),
            ConnectorAction(
                CALENDAR_UPCOMING_ACTION,
                self._calendar_upcoming_action(context, store),
            ),
        )

    def _graph(self, values: Mapping[str, Any]) -> MicrosoftGraphClient:
        return MicrosoftGraphClient(
            _required_string(values, "access_token", "Microsoft 365 credential"),
            self._transport,
            sleep=self._sleep,
        )

    async def _refresh(
        self,
        values: dict[str, Any],
        binding: ConnectorBinding,
    ) -> tuple[dict[str, Any], bool]:
        if not _needs_refresh(values, now=self._now()):
            return values, False
        refreshed = await _refresh_values(self._transport, values, now=self._now())
        profile = await self._graph(refreshed).get(f"{GRAPH_ROOT}/me?$select=id")
        account_id = _required_string(profile, "id", "Microsoft Graph profile")
        if account_id != binding.external_account_id:
            raise Microsoft365AccountMismatchError(
                "Microsoft 365 refresh returned a different account"
            )
        _enforce_binding(binding, refreshed)
        return refreshed, True

    def _initial_delta_url(self, resource: ConnectorResource) -> str:
        graph_id = quote(_resource_graph_id(resource), safe="")
        if resource.kind == "mail_folder":
            return (
                f"{GRAPH_ROOT}/me/mailFolders/{graph_id}/messages/delta?"
                + urlencode(
                    {
                        "$select": (
                            "id,subject,from,sender,receivedDateTime,sentDateTime,"
                            "lastModifiedDateTime,bodyPreview,isRead,webLink"
                        ),
                        "$top": "50",
                    }
                )
            )
        if resource.kind == "calendar":
            now = self._now()
            start = _graph_datetime(now - timedelta(days=CALENDAR_SYNC_PAST_DAYS))
            end = _graph_datetime(now + timedelta(days=CALENDAR_SYNC_FUTURE_DAYS))
            return (
                f"{GRAPH_ROOT}/me/calendars/{graph_id}/calendarView/delta?"
                + urlencode(
                    {
                        "startDateTime": start,
                        "endDateTime": end,
                    }
                )
            )
        raise ValueError(f"unsupported Microsoft 365 resource kind: {resource.kind}")

    def _mail_search_action(
        self, action_context: ConnectorActionContext, store: VersionedActionTokenStore
    ) -> ActionFn:
        async def invoke(args: dict[str, Any], tool_context: ToolContext) -> str:
            graph, state = await self._action_graph(action_context, store)
            resource = _selected_resource(state.resources, args, "mail_folder")
            folder_id = quote(_resource_graph_id(resource), safe="")
            page_token = _optional_string(args.get("page_token"))
            if page_token:
                url = _validated_page_token(page_token, f"/mailFolders/{folder_id}/messages")
            else:
                query = _required_argument(args, "query", max_length=512)
                search = '"' + query.replace("\\", "\\\\").replace('"', '\\"') + '"'
                url = (
                    f"{GRAPH_ROOT}/me/mailFolders/{folder_id}/messages?"
                    + urlencode(
                        {
                            "$search": search,
                            "$select": (
                                "id,subject,from,sender,receivedDateTime,sentDateTime,"
                                "bodyPreview,isRead,webLink,lastModifiedDateTime"
                            ),
                            "$top": str(_page_size(args)),
                        }
                    )
                )
            payload = await graph.get(url, headers={"ConsistencyLevel": "eventual"})
            binding = _active_binding(state.binding)
            return _render_page(
                payload,
                [
                    _mail_record(binding, resource, item, include_body=False)
                    for item in _values(payload)
                ],
            )

        return invoke

    def _mail_get_action(
        self, action_context: ConnectorActionContext, store: VersionedActionTokenStore
    ) -> ActionFn:
        async def invoke(args: dict[str, Any], tool_context: ToolContext) -> str:
            graph, state = await self._action_graph(action_context, store)
            resource = _selected_resource(state.resources, args, "mail_folder")
            folder_id = quote(_resource_graph_id(resource), safe="")
            message_id = quote(_required_argument(args, "message_id"), safe="")
            url = (
                f"{GRAPH_ROOT}/me/mailFolders/{folder_id}/messages/{message_id}?"
                + urlencode(
                    {
                        "$select": (
                            "id,subject,from,sender,toRecipients,ccRecipients,"
                            "receivedDateTime,sentDateTime,body,bodyPreview,isRead,"
                            "webLink,lastModifiedDateTime,internetMessageId"
                        )
                    }
                )
            )
            item = await graph.get(url)
            return _render_object(
                _mail_record(
                    _active_binding(state.binding),
                    resource,
                    item,
                    include_body=True,
                )
            )

        return invoke

    def _calendar_list_action(
        self, action_context: ConnectorActionContext, store: VersionedActionTokenStore
    ) -> ActionFn:
        async def invoke(args: dict[str, Any], tool_context: ToolContext) -> str:
            start = _required_argument(args, "start")
            end = _required_argument(args, "end")
            return await self._calendar_page(
                action_context,
                store,
                args,
                start=start,
                end=end,
            )

        return invoke

    def _calendar_upcoming_action(
        self, action_context: ConnectorActionContext, store: VersionedActionTokenStore
    ) -> ActionFn:
        async def invoke(args: dict[str, Any], tool_context: ToolContext) -> str:
            days = _integer_argument(args, "days", default=7, minimum=1, maximum=90)
            now = self._now()
            return await self._calendar_page(
                action_context,
                store,
                args,
                start=_graph_datetime(now),
                end=_graph_datetime(now + timedelta(days=days)),
            )

        return invoke

    async def _calendar_page(
        self,
        action_context: ConnectorActionContext,
        store: VersionedActionTokenStore,
        args: dict[str, Any],
        *,
        start: str,
        end: str,
    ) -> str:
        graph, state = await self._action_graph(action_context, store)
        resource = _selected_resource(state.resources, args, "calendar")
        calendar_id = quote(_resource_graph_id(resource), safe="")
        page_token = _optional_string(args.get("page_token"))
        if page_token:
            url = _validated_page_token(page_token, f"/calendars/{calendar_id}/calendarView")
        else:
            url = (
                f"{GRAPH_ROOT}/me/calendars/{calendar_id}/calendarView?"
                + urlencode(
                    {
                        "startDateTime": start,
                        "endDateTime": end,
                        "$select": (
                            "id,subject,bodyPreview,start,end,location,organizer,"
                            "attendees,isCancelled,lastModifiedDateTime,changeKey,webLink"
                        ),
                        "$orderby": "start/dateTime",
                        "$top": str(_page_size(args)),
                    }
                )
            )
        timezone = _timezone(args)
        payload = await graph.get(url, headers={"Prefer": f'outlook.timezone="{timezone}"'})
        binding = _active_binding(state.binding)
        return _render_page(
            payload,
            [
                _calendar_record(binding, resource, item, timezone)
                for item in _values(payload)
            ],
            timezone=timezone,
        )

    def _calendar_get_action(
        self, action_context: ConnectorActionContext, store: VersionedActionTokenStore
    ) -> ActionFn:
        async def invoke(args: dict[str, Any], tool_context: ToolContext) -> str:
            graph, state = await self._action_graph(action_context, store)
            resource = _selected_resource(state.resources, args, "calendar")
            calendar_id = quote(_resource_graph_id(resource), safe="")
            event_id = quote(_required_argument(args, "event_id"), safe="")
            timezone = _timezone(args)
            item = await graph.get(
                f"{GRAPH_ROOT}/me/calendars/{calendar_id}/events/{event_id}",
                headers={"Prefer": f'outlook.timezone="{timezone}"'},
            )
            return _render_object(
                _calendar_record(
                    _active_binding(state.binding),
                    resource,
                    item,
                    timezone,
                ),
                timezone=timezone,
            )

        return invoke

    async def _action_graph(
        self,
        action_context: ConnectorActionContext,
        store: VersionedActionTokenStore,
    ) -> tuple[MicrosoftGraphClient, ConnectorOperationContext]:
        state = await action_context.load_state(MICROSOFT_365_CONNECTOR_ID)
        binding = _active_binding(state.binding)
        stored = await store.get_versioned(MICROSOFT_365_CONNECTOR_ID)
        if stored is None:
            raise ConnectorAuthenticationError("Microsoft 365 credentials are missing")
        encoded, version = stored
        envelope = CredentialEnvelope.parse(encoded)
        values = _oauth_credential(envelope)
        _enforce_binding(binding, values)
        if _needs_refresh(values, now=self._now()):
            refreshed = await _refresh_values(self._transport, values, now=self._now())
            profile = await self._graph(refreshed).get(f"{GRAPH_ROOT}/me?$select=id")
            account_id = _required_string(profile, "id", "Microsoft Graph profile")
            if account_id != binding.external_account_id:
                raise Microsoft365AccountMismatchError(
                    "Microsoft 365 refresh returned a different account"
                )
            _enforce_binding(binding, refreshed)
            updated = await store.put_if_version(
                MICROSOFT_365_CONNECTOR_ID,
                CredentialEnvelope(OAUTH_CREDENTIAL_KIND, refreshed).serialize(),
                version,
            )
            if updated is None:
                latest = await store.get_versioned(MICROSOFT_365_CONNECTOR_ID)
                if latest is None:
                    raise ConnectorAuthenticationError(
                        "Microsoft 365 credentials changed during refresh"
                    )
                values = _oauth_credential(CredentialEnvelope.parse(latest[0]))
                _require_unexpired(values, now=self._now())
                _enforce_binding(binding, values)
            else:
                values = refreshed
        return self._graph(values), state


def _guid(value: str | None, label: str) -> str:
    try:
        return str(uuid.UUID((value or "").strip()))
    except ValueError as exc:
        raise ValueError(f"{label} must be a GUID") from exc


def _string(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _optional_string(value: Any) -> str | None:
    rendered = _string(value).strip()
    return rendered or None


def _required_string(values: Mapping[str, Any], key: str, label: str) -> str:
    value = _string(values.get(key)).strip()
    if not value:
        raise ValueError(f"{label} is missing {key}")
    return value


def _required_argument(
    args: Mapping[str, Any], key: str, *, max_length: int = 4096
) -> str:
    value = _string(args.get(key)).strip()
    if not value:
        raise ValueError(f"{key} is required")
    if len(value) > max_length:
        raise ValueError(f"{key} is too long")
    return value


def _integer_argument(
    args: Mapping[str, Any],
    key: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = args.get(key, default)
    if isinstance(raw, bool):
        raise ValueError(f"{key} must be an integer")
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be an integer") from exc
    if value < minimum or value > maximum:
        raise ValueError(f"{key} must be between {minimum} and {maximum}")
    return value


def _page_size(args: Mapping[str, Any]) -> int:
    return _integer_argument(args, "page_size", default=25, minimum=1, maximum=50)


def _timezone(args: Mapping[str, Any]) -> str:
    value = _string(args.get("timezone", "UTC")).strip() or "UTC"
    if len(value) > 100 or any(ch in value for ch in {'"', "\r", "\n", "\x00"}):
        raise ValueError("timezone is invalid")
    return value


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise Microsoft365Error(f"{label} was not a JSON object")
    return cast(dict[str, Any], value)


def _values(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("value")
    if not isinstance(raw, list):
        raise Microsoft365Error("Microsoft Graph collection response is missing value")
    return [_object(item, "Microsoft Graph collection item") for item in raw]


def _graph_error_code(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    error = payload.get("error")
    if not isinstance(error, dict):
        return ""
    return _string(error.get("code")).strip()


def _validate_graph_url(url: str) -> None:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "graph.microsoft.com"
        or parsed.port not in {None, 443}
        or not parsed.path.startswith("/v1.0/")
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise Microsoft365Error("Microsoft Graph returned an unsafe continuation URL")


def _validated_page_token(value: str, expected_path_fragment: str) -> str:
    _validate_graph_url(value)
    if expected_path_fragment not in urlsplit(value).path:
        raise PermissionError("Microsoft Graph page token crosses the selected resource")
    return value


def _retry_after_seconds(value: str | None) -> float:
    if value:
        try:
            return max(0.0, min(float(value), 300.0))
        except ValueError:
            try:
                parsed = parsedate_to_datetime(value)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                return max(
                    0.0,
                    min((parsed - datetime.now(UTC)).total_seconds(), 300.0),
                )
            except (TypeError, ValueError, OverflowError):
                pass
    return 1.0


async def _token_request(
    transport: Microsoft365HttpTransport,
    app: Mapping[str, str],
    values: Mapping[str, str],
) -> dict[str, Any]:
    tenant_id = app["tenant_id"]
    url = f"{LOGIN_ROOT}/{quote(tenant_id, safe='')}/oauth2/v2.0/token"
    response = await transport.request(
        "POST",
        url,
        headers={"Accept": "application/json"},
        data={
            "client_id": app["client_id"],
            "client_secret": app["client_secret"],
            **values,
        },
    )
    if response.status_code in {400, 401}:
        raise ConnectorAuthenticationError("Microsoft 365 token exchange was rejected")
    if not 200 <= response.status_code < 300:
        raise Microsoft365Error(
            f"Microsoft 365 token endpoint failed (HTTP {response.status_code})"
        )
    return _object(response.payload, "Microsoft 365 token response")


def _app_credential(envelope: CredentialEnvelope | None) -> dict[str, str]:
    if envelope is None or envelope.kind not in {APP_CREDENTIAL_KIND, OAUTH_CREDENTIAL_KIND}:
        raise ConnectorAuthenticationError("Microsoft 365 application setup is missing")
    return {
        "tenant_id": _required_string(envelope.values, "tenant_id", "Microsoft 365 credential"),
        "client_id": _required_string(envelope.values, "client_id", "Microsoft 365 credential"),
        "client_secret": _required_string(
            envelope.values, "client_secret", "Microsoft 365 credential"
        ),
    }


def _oauth_credential(envelope: CredentialEnvelope | None) -> dict[str, Any]:
    if envelope is None or envelope.kind != OAUTH_CREDENTIAL_KIND:
        raise ConnectorAuthenticationError("Microsoft 365 delegated authorization is missing")
    values = dict(envelope.values)
    _required_string(values, "access_token", "Microsoft 365 credential")
    _required_string(values, "refresh_token", "Microsoft 365 credential")
    _app_credential(envelope)
    _require_scopes(values)
    return values


def _bound_oauth(
    context: ConnectorOperationContext,
) -> tuple[ConnectorBinding, dict[str, Any]]:
    binding = _active_binding(context.binding)
    values = _oauth_credential(context.credential)
    return binding, values


def _active_binding(binding: ConnectorBinding | None) -> ConnectorBinding:
    if binding is None or binding.status not in {
        ConnectorBindingStatus.connected,
        ConnectorBindingStatus.degraded,
    }:
        raise ConnectorAuthenticationError("Microsoft 365 connector is not connected")
    return binding


def _scope_set(values: Mapping[str, Any]) -> frozenset[str]:
    raw = values.get("granted_scopes")
    if isinstance(raw, list):
        return frozenset(_string(item).lower() for item in raw if _string(item))
    if isinstance(raw, str):
        return frozenset(item.lower() for item in raw.split() if item)
    return frozenset()


def _require_scopes(values: Mapping[str, Any]) -> None:
    missing = REQUIRED_GRAPH_SCOPES - _scope_set(values)
    if missing:
        raise Microsoft365PermissionError(
            "Microsoft 365 grant is missing required scopes: " + ", ".join(sorted(missing))
        )


def _oauth_values_from_token(
    app: Mapping[str, str],
    token: Mapping[str, Any],
    *,
    now: datetime,
    prior: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    access_token = _required_string(token, "access_token", "Microsoft 365 token response")
    refresh_token = _string(token.get("refresh_token")).strip()
    if not refresh_token and prior is not None:
        refresh_token = _required_string(prior, "refresh_token", "Microsoft 365 credential")
    if not refresh_token:
        raise ConnectorAuthenticationError(
            "Microsoft 365 did not issue an offline refresh credential"
        )
    token_type = _string(token.get("token_type")).strip().lower()
    if token_type != "bearer":
        raise ConnectorAuthenticationError("Microsoft 365 returned an unsupported token type")
    expires_in = token.get("expires_in", 0)
    if isinstance(expires_in, bool):
        raise ConnectorAuthenticationError("Microsoft 365 returned an invalid token lifetime")
    try:
        lifetime = int(expires_in)
    except (TypeError, ValueError) as exc:
        raise ConnectorAuthenticationError(
            "Microsoft 365 returned an invalid token lifetime"
        ) from exc
    if lifetime <= 0:
        raise ConnectorAuthenticationError("Microsoft 365 returned an expired token")
    scopes = _string(token.get("scope")).split()
    if not scopes and prior is not None:
        scopes = list(_scope_set(prior))
    values: dict[str, Any] = {
        **app,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": int(now.timestamp()) + lifetime,
        "granted_scopes": scopes,
    }
    if prior is not None:
        for key in ("account_id", "account_name"):
            if key in prior:
                values[key] = prior[key]
    id_token = _string(token.get("id_token")).strip()
    if id_token:
        values["id_token"] = id_token
    _require_scopes(values)
    _enforce_tenant(values, app["tenant_id"])
    return values


async def _refresh_values(
    transport: Microsoft365HttpTransport,
    values: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    app = {
        "tenant_id": _required_string(values, "tenant_id", "Microsoft 365 credential"),
        "client_id": _required_string(values, "client_id", "Microsoft 365 credential"),
        "client_secret": _required_string(values, "client_secret", "Microsoft 365 credential"),
    }
    token = await _token_request(
        transport,
        app,
        {
            "grant_type": "refresh_token",
            "refresh_token": _required_string(
                values, "refresh_token", "Microsoft 365 credential"
            ),
            "scope": " ".join(SCOPES),
        },
    )
    return _oauth_values_from_token(app, token, now=now, prior=values)


def _needs_refresh(values: Mapping[str, Any], *, now: datetime) -> bool:
    expires_at = values.get("expires_at")
    if expires_at is None or isinstance(expires_at, bool):
        return True
    try:
        expires = int(expires_at)
    except (TypeError, ValueError):
        return True
    return expires <= int(now.timestamp()) + TOKEN_REFRESH_SKEW_SECONDS


def _require_unexpired(values: Mapping[str, Any], *, now: datetime) -> None:
    if _needs_refresh(values, now=now):
        raise ConnectorAuthenticationError(
            "Microsoft 365 access token needs CAS-safe refresh; run sync or reconnect"
        )


def _jwt_claims(token: str) -> Mapping[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        return {}
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload.encode("ascii"))
        value = json.loads(decoded)
    except (UnicodeEncodeError, ValueError, json.JSONDecodeError):
        return {}
    return cast(Mapping[str, Any], value) if isinstance(value, dict) else {}


def _enforce_tenant(values: Mapping[str, Any], expected_tenant_id: str) -> None:
    token = _string(values.get("id_token")) or _string(values.get("access_token"))
    tenant = _string(_jwt_claims(token).get("tid")).strip()
    if tenant and tenant.lower() != expected_tenant_id.lower():
        raise Microsoft365TenantMismatchError(
            "Microsoft 365 token tenant does not match the configured tenant"
        )


def _enforce_binding(binding: ConnectorBinding, values: Mapping[str, Any]) -> None:
    tenant_id = _required_string(values, "tenant_id", "Microsoft 365 credential")
    if (
        not binding.external_tenant_id
        or binding.external_tenant_id.lower() != tenant_id.lower()
    ):
        raise Microsoft365TenantMismatchError(
            "Microsoft 365 binding tenant does not match the configured tenant"
        )
    _enforce_tenant(values, tenant_id)
    account_id = _required_string(values, "account_id", "Microsoft 365 credential")
    if not binding.external_account_id or binding.external_account_id != account_id:
        raise Microsoft365AccountMismatchError(
            "Microsoft 365 binding account does not match the authorized account"
        )
    _require_scopes(values)


def _enforce_staged_tenant(
    binding: ConnectorBinding | None,
    tenant_id: str,
) -> None:
    if (
        binding is not None
        and binding.external_tenant_id is not None
        and binding.external_tenant_id.lower() != tenant_id.lower()
    ):
        raise Microsoft365TenantMismatchError(
            "Microsoft 365 staged binding tenant does not match the configured tenant"
        )


def _account_name(profile: Mapping[str, Any]) -> str:
    return (
        _string(profile.get("displayName")).strip()
        or _string(profile.get("mail")).strip()
        or _string(profile.get("userPrincipalName")).strip()
        or "Microsoft 365"
    )


async def _all_values(
    graph: MicrosoftGraphClient,
    url: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    current: str | None = url
    for _ in range(MAX_GRAPH_PAGES):
        if current is None:
            return rows
        payload = await graph.get(current)
        rows.extend(_values(payload))
        current = _optional_string(payload.get("@odata.nextLink"))
    raise Microsoft365Error("Microsoft Graph collection exceeded the page limit")


async def _delta_values(
    graph: MicrosoftGraphClient,
    url: str,
    *,
    cursor_request: bool,
) -> tuple[list[dict[str, Any]], str]:
    rows: list[dict[str, Any]] = []
    current = url
    first = True
    for _ in range(MAX_GRAPH_PAGES):
        payload = await graph.get(current, cursor_request=cursor_request and first)
        first = False
        rows.extend(_values(payload))
        next_link = _optional_string(payload.get("@odata.nextLink"))
        if next_link is not None:
            current = next_link
            continue
        delta_link = _optional_string(payload.get("@odata.deltaLink"))
        if delta_link is None:
            raise Microsoft365Error("Microsoft Graph delta response is missing deltaLink")
        _validate_graph_url(delta_link)
        return rows, delta_link
    raise Microsoft365Error("Microsoft Graph delta exceeded the page limit")


def _resource_graph_id(resource: ConnectorResource) -> str:
    graph_id = _string(resource.config.get("graph_id")).strip()
    if not graph_id:
        raise ValueError("Microsoft 365 resource is missing its Graph id")
    return graph_id


def _selected_resource(
    resources: tuple[ConnectorResource, ...],
    args: Mapping[str, Any],
    kind: str,
) -> ConnectorResource:
    external_id = _required_argument(args, "resource_id")
    resource = next(
        (
            item
            for item in resources
            if item.external_id == external_id and item.kind == kind and item.selected
        ),
        None,
    )
    if resource is None:
        raise PermissionError(
            f"Microsoft 365 resource {external_id!r} is not selected for this scope"
        )
    return resource


def _map_delta(
    binding: ConnectorBinding,
    resource: ConnectorResource,
    items: list[dict[str, Any]],
) -> tuple[list[ConnectorChange], set[str]]:
    changes: list[ConnectorChange] = []
    current_ids: set[str] = set()
    graph_resource_id = _resource_graph_id(resource)
    for item in items:
        item_id = _required_string(item, "id", "Microsoft Graph delta item")
        external_id = f"{resource.kind}:{graph_resource_id}:{item_id}"
        if "@removed" in item:
            changes.append(
                ConnectorChange(
                    ConnectorChangeKind.delete,
                    ConnectorProvenance(
                        MICROSOFT_365_CONNECTOR_ID,
                        binding.id,
                        external_id,
                        revision="removed",
                    ),
                )
            )
            continue
        current_ids.add(external_id)
        if resource.kind == "mail_folder":
            record = _mail_record(binding, resource, item, include_body=False)
            changes.append(
                ConnectorChange(
                    ConnectorChangeKind.upsert,
                    ConnectorProvenance(
                        MICROSOFT_365_CONNECTOR_ID,
                        binding.id,
                        external_id,
                        source_url=_optional_string(item.get("webLink")),
                        revision=_optional_string(item.get("lastModifiedDateTime")),
                    ),
                    title=_string(item.get("subject")).strip() or "(no subject)",
                    content=_mail_content(record),
                    mime_type="text/plain",
                )
            )
        elif resource.kind == "calendar":
            record = _calendar_record(binding, resource, item, "UTC")
            changes.append(
                ConnectorChange(
                    ConnectorChangeKind.upsert,
                    ConnectorProvenance(
                        MICROSOFT_365_CONNECTOR_ID,
                        binding.id,
                        external_id,
                        source_url=_optional_string(item.get("webLink")),
                        revision=(
                            _optional_string(item.get("changeKey"))
                            or _optional_string(item.get("lastModifiedDateTime"))
                        ),
                    ),
                    title=_string(item.get("subject")).strip() or "(untitled event)",
                    content=_calendar_content(record),
                    mime_type="text/plain",
                )
            )
        else:
            raise ValueError(f"unsupported Microsoft 365 resource kind: {resource.kind}")
    return changes, current_ids


def _email_address(value: Any) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    address = value.get("emailAddress")
    if not isinstance(address, dict):
        return None
    return {
        "name": _string(address.get("name")),
        "address": _string(address.get("address")),
    }


def _email_addresses(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    return [item for raw in value if (item := _email_address(raw)) is not None]


def _provenance(
    binding: ConnectorBinding,
    resource: ConnectorResource,
    item: Mapping[str, Any],
) -> dict[str, Any]:
    item_id = _required_string(item, "id", "Microsoft Graph item")
    return {
        "connector_id": MICROSOFT_365_CONNECTOR_ID,
        "binding_id": binding.id,
        "external_resource_id": f"{resource.kind}:{_resource_graph_id(resource)}:{item_id}",
        "source_url": _optional_string(item.get("webLink")),
        "revision": (
            _optional_string(item.get("changeKey"))
            or _optional_string(item.get("lastModifiedDateTime"))
        ),
        "taint": "tainted",
    }


def _mail_record(
    binding: ConnectorBinding,
    resource: ConnectorResource,
    item: Mapping[str, Any],
    *,
    include_body: bool,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "id": _required_string(item, "id", "Microsoft Graph message"),
        "subject": _string(item.get("subject")),
        "from": _email_address(item.get("from")),
        "sender": _email_address(item.get("sender")),
        "received_at": _optional_string(item.get("receivedDateTime")),
        "sent_at": _optional_string(item.get("sentDateTime")),
        "body_preview": _string(item.get("bodyPreview")),
        "is_read": bool(item.get("isRead", False)),
        "provenance": _provenance(binding, resource, item),
    }
    if include_body:
        body = item.get("body")
        record.update(
            {
                "to": _email_addresses(item.get("toRecipients")),
                "cc": _email_addresses(item.get("ccRecipients")),
                "internet_message_id": _optional_string(item.get("internetMessageId")),
                "body": (
                    {
                        "content_type": _string(body.get("contentType")),
                        "content": _string(body.get("content")),
                    }
                    if isinstance(body, dict)
                    else None
                ),
            }
        )
    return record


def _calendar_record(
    binding: ConnectorBinding,
    resource: ConnectorResource,
    item: Mapping[str, Any],
    timezone: str,
) -> dict[str, Any]:
    return {
        "id": _required_string(item, "id", "Microsoft Graph event"),
        "subject": _string(item.get("subject")),
        "body_preview": _string(item.get("bodyPreview")),
        "start": item.get("start"),
        "end": item.get("end"),
        "location": item.get("location"),
        "organizer": _email_address(item.get("organizer")),
        "attendees": item.get("attendees") if isinstance(item.get("attendees"), list) else [],
        "is_cancelled": bool(item.get("isCancelled", False)),
        "timezone": timezone,
        "provenance": _provenance(binding, resource, item),
    }


def _mail_content(record: Mapping[str, Any]) -> str:
    sender = record.get("from")
    sender_text = ""
    if isinstance(sender, dict):
        sender_text = _string(sender.get("address")) or _string(sender.get("name"))
    return "\n".join(
        (
            f"From: {sender_text}",
            f"Received: {_string(record.get('received_at'))}",
            "",
            _string(record.get("body_preview")),
        )
    ).strip()


def _calendar_content(record: Mapping[str, Any]) -> str:
    start = record.get("start")
    end = record.get("end")
    location = record.get("location")
    return "\n".join(
        (
            f"Start: {json.dumps(start, ensure_ascii=False, sort_keys=True)}",
            f"End: {json.dumps(end, ensure_ascii=False, sort_keys=True)}",
            f"Location: {json.dumps(location, ensure_ascii=False, sort_keys=True)}",
            "",
            _string(record.get("body_preview")),
        )
    ).strip()


def _render_page(
    payload: Mapping[str, Any],
    records: list[dict[str, Any]],
    *,
    timezone: str | None = None,
) -> str:
    next_page = _optional_string(payload.get("@odata.nextLink"))
    if next_page is not None:
        _validate_graph_url(next_page)
    result: dict[str, Any] = {"items": records, "next_page": next_page}
    if timezone is not None:
        result["timezone"] = timezone
    return json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _render_object(record: dict[str, Any], *, timezone: str | None = None) -> str:
    result: dict[str, Any] = {"item": record}
    if timezone is not None:
        result["timezone"] = timezone
    return json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _graph_datetime(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def factory() -> Microsoft365Provider:
    return Microsoft365Provider()


__all__ = [
    "APP_CREDENTIAL_KIND",
    "CALENDAR_GET_ACTION",
    "CALENDAR_LIST_ACTION",
    "CALENDAR_UPCOMING_ACTION",
    "GRAPH_ROOT",
    "MAIL_GET_ACTION",
    "MAIL_SEARCH_ACTION",
    "MICROSOFT_365_CONNECTOR_ID",
    "Microsoft365AccountMismatchError",
    "Microsoft365CursorInvalidError",
    "Microsoft365Error",
    "Microsoft365HttpResponse",
    "Microsoft365HttpTransport",
    "Microsoft365PermissionError",
    "Microsoft365Provider",
    "Microsoft365TenantMismatchError",
    "Microsoft365ThrottledError",
    "OAUTH_CREDENTIAL_KIND",
    "SCOPES",
    "factory",
    "manifest",
]
