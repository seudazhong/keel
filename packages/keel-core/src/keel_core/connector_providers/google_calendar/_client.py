"""Google Calendar API adapter with explicit auth, retry, and reconciliation errors."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Protocol, cast

from keel_core.connector_credentials import CredentialEnvelope

GOOGLE_CALENDAR_CONNECTOR_ID = "google_calendar"
GOOGLE_CALENDAR_READ_SCOPES: tuple[str, ...] = (
    "https://www.googleapis.com/auth/calendar.readonly",
)
GOOGLE_CALENDAR_WRITE_SCOPE = "https://www.googleapis.com/auth/calendar.events"
GOOGLE_CALENDAR_WRITE_SCOPES: tuple[str, ...] = (
    *GOOGLE_CALENDAR_READ_SCOPES,
    GOOGLE_CALENDAR_WRITE_SCOPE,
)


class GoogleCalendarError(RuntimeError):
    """A sanitized Google Calendar operation failure."""


class GoogleCalendarAuthenticationError(GoogleCalendarError):
    """The credential is missing, invalid, revoked, or insufficiently scoped."""


class GoogleCalendarWriteAuthorizationRequired(GoogleCalendarAuthenticationError):
    """The independent Calendar credential needs incremental write consent."""


class GoogleCalendarRateLimitError(GoogleCalendarError):
    """Google asked the caller to retry later."""


class GoogleCalendarSyncTokenExpired(GoogleCalendarError):
    """Google invalidated a Calendar incremental sync token."""


class GoogleCalendarNotFoundError(GoogleCalendarError):
    """The selected calendar or event no longer exists."""


class GoogleCalendarRevokeError(GoogleCalendarError):
    """Remote token revocation failed and local state must be retained."""


class CalendarClient(Protocol):
    @property
    def credential(self) -> CredentialEnvelope: ...

    def list_calendars(self, page_token: str | None = None) -> dict[str, Any]: ...

    def list_events(self, calendar_id: str, parameters: Mapping[str, Any]) -> dict[str, Any]: ...

    def get_event(
        self,
        calendar_id: str,
        event_id: str,
        time_zone: str | None = None,
    ) -> dict[str, Any]: ...

    def create_event_reconciled(
        self,
        calendar_id: str,
        event_id: str,
        request_id: str,
        body: Mapping[str, Any],
    ) -> dict[str, Any]: ...

    def update_event_reconciled(
        self,
        calendar_id: str,
        event_id: str,
        request_id: str,
        body: Mapping[str, Any],
    ) -> dict[str, Any]: ...

    def close(self) -> None: ...


def credential_scopes(credential: CredentialEnvelope | None) -> frozenset[str]:
    if credential is None or credential.kind != "oauth":
        return frozenset()
    raw = credential.values.get("scopes", credential.values.get("scope", ()))
    if isinstance(raw, str):
        return frozenset(item for item in raw.split() if item)
    if isinstance(raw, list):
        return frozenset(str(item) for item in raw if isinstance(item, str) and item)
    return frozenset()


def merge_authorized_user_values(
    previous: CredentialEnvelope | None,
    current: Mapping[str, Any],
    requested_scopes: tuple[str, ...],
) -> dict[str, Any]:
    merged = dict(current)
    if previous is not None and previous.kind == "oauth":
        for key in ("refresh_token", "client_id", "client_secret", "token_uri"):
            if not merged.get(key) and previous.values.get(key):
                merged[key] = previous.values[key]
        granted = credential_scopes(previous)
    else:
        granted = frozenset()
    raw_scopes = merged.get("scopes")
    if isinstance(raw_scopes, list):
        granted = granted | frozenset(str(item) for item in raw_scopes if isinstance(item, str))
    elif isinstance(raw_scopes, str):
        granted = granted | frozenset(raw_scopes.split())
    if not set(requested_scopes).issubset(granted):
        merged["scopes"] = sorted(granted)
        return merged
    merged["scopes"] = sorted(granted)
    return merged


def build_client(
    credential: CredentialEnvelope,
    required_scopes: tuple[str, ...],
) -> CalendarClient:
    if credential.kind != "oauth":
        raise GoogleCalendarAuthenticationError(
            "Google Calendar credentials have an invalid envelope."
        )
    granted = credential_scopes(credential)
    missing = set(required_scopes) - granted
    if missing:
        if GOOGLE_CALENDAR_WRITE_SCOPE in missing:
            raise GoogleCalendarWriteAuthorizationRequired(
                "Google Calendar write access is not authorized. Reconnect Google Calendar "
                "to grant the incremental calendar.events scope."
            )
        raise GoogleCalendarAuthenticationError(
            "Google Calendar read access is not authorized. Reconnect Google Calendar."
        )

    from google.auth.exceptions import RefreshError
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials

    try:
        credentials = Credentials.from_authorized_user_info(  # type: ignore[no-untyped-call]
            dict(credential.values),
            list(required_scopes),
        )
        if not credentials.valid:
            if not credentials.refresh_token:
                raise GoogleCalendarAuthenticationError(
                    "Google Calendar credentials are invalid and have no refresh token."
                )
            credentials.refresh(Request())
    except RefreshError as exc:
        raise GoogleCalendarAuthenticationError(
            "Google Calendar credential refresh failed; reconnect the connector."
        ) from exc
    except ValueError as exc:
        raise GoogleCalendarAuthenticationError(
            "Google Calendar credentials are malformed; reconnect the connector."
        ) from exc
    return _GoogleCalendarClient(credentials, credential, required_scopes)


class _GoogleCalendarClient:
    def __init__(
        self,
        credentials: Any,
        prior: CredentialEnvelope,
        required_scopes: tuple[str, ...],
    ) -> None:
        import httplib2
        from google_auth_httplib2 import AuthorizedHttp
        from googleapiclient.discovery import build

        self._credentials = credentials
        self._prior = prior
        self._required_scopes = required_scopes
        self._service = build(
            "calendar",
            "v3",
            http=AuthorizedHttp(credentials, http=httplib2.Http(timeout=10)),
            cache_discovery=False,
        )

    @property
    def credential(self) -> CredentialEnvelope:
        raw = json.loads(cast(str, self._credentials.to_json()))
        if not isinstance(raw, dict):
            raise GoogleCalendarAuthenticationError(
                "Google Calendar credentials did not serialize to an object."
            )
        return CredentialEnvelope(
            "oauth",
            merge_authorized_user_values(self._prior, raw, self._required_scopes),
        )

    def list_calendars(self, page_token: str | None = None) -> dict[str, Any]:
        request = self._service.calendarList().list(
            maxResults=250,
            pageToken=page_token,
            showDeleted=False,
            showHidden=False,
        )
        return self._execute(request)

    def list_events(self, calendar_id: str, parameters: Mapping[str, Any]) -> dict[str, Any]:
        request = self._service.events().list(calendarId=calendar_id, **dict(parameters))
        return self._execute(request)

    def get_event(
        self,
        calendar_id: str,
        event_id: str,
        time_zone: str | None = None,
    ) -> dict[str, Any]:
        parameters = {"calendarId": calendar_id, "eventId": event_id}
        if time_zone:
            parameters["timeZone"] = time_zone
        request = self._service.events().get(**parameters)
        return self._execute(request)

    def create_event_reconciled(
        self,
        calendar_id: str,
        event_id: str,
        request_id: str,
        body: Mapping[str, Any],
    ) -> dict[str, Any]:
        existing = self._maybe_get_event(calendar_id, event_id)
        if existing is not None:
            self._validate_request_marker(existing, "keel_create_request_id", request_id)
            return existing
        payload = dict(body)
        payload["id"] = event_id
        payload["extendedProperties"] = _merge_private_properties(
            payload.get("extendedProperties"),
            {"keel_create_request_id": request_id},
        )
        request = self._service.events().insert(calendarId=calendar_id, body=payload)
        try:
            return self._execute(request)
        except GoogleCalendarError as exc:
            if not _is_conflict(exc):
                raise
            reconciled = self._maybe_get_event(calendar_id, event_id)
            if reconciled is None:
                raise
            self._validate_request_marker(
                reconciled,
                "keel_create_request_id",
                request_id,
            )
            return reconciled

    def update_event_reconciled(
        self,
        calendar_id: str,
        event_id: str,
        request_id: str,
        body: Mapping[str, Any],
    ) -> dict[str, Any]:
        existing = self.get_event(calendar_id, event_id)
        private = _private_properties(existing)
        if private.get("keel_update_request_id") == request_id:
            return existing
        payload = dict(body)
        payload["extendedProperties"] = _merge_private_properties(
            existing.get("extendedProperties"),
            {"keel_update_request_id": request_id},
        )
        request = self._service.events().patch(
            calendarId=calendar_id,
            eventId=event_id,
            body=payload,
        )
        try:
            return self._execute(request)
        except GoogleCalendarError:
            reconciled = self._maybe_get_event(calendar_id, event_id)
            if (
                reconciled is not None
                and _private_properties(reconciled).get("keel_update_request_id") == request_id
            ):
                return reconciled
            raise

    def close(self) -> None:
        self._service.close()

    def _maybe_get_event(self, calendar_id: str, event_id: str) -> dict[str, Any] | None:
        try:
            return self.get_event(calendar_id, event_id)
        except GoogleCalendarNotFoundError:
            return None

    @staticmethod
    def _validate_request_marker(
        event: Mapping[str, Any],
        marker: str,
        request_id: str,
    ) -> None:
        if _private_properties(event).get(marker) != request_id:
            raise GoogleCalendarError("Google Calendar event id collided with a different request.")

    @staticmethod
    def _execute(request: Any) -> dict[str, Any]:
        from googleapiclient.errors import HttpError

        try:
            result = request.execute(num_retries=3)
        except HttpError as exc:
            raise _map_http_error(exc) from exc
        if not isinstance(result, dict):
            raise GoogleCalendarError("Google Calendar returned an invalid response.")
        return cast(dict[str, Any], result)


def _private_properties(event: Mapping[str, Any]) -> dict[str, str]:
    extended = event.get("extendedProperties")
    if not isinstance(extended, dict):
        return {}
    private = extended.get("private")
    if not isinstance(private, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in private.items()
        if isinstance(key, str) and isinstance(value, str)
    }


def _merge_private_properties(
    raw: object,
    updates: Mapping[str, str],
) -> dict[str, Any]:
    extended = dict(raw) if isinstance(raw, dict) else {}
    private = extended.get("private")
    merged_private = dict(private) if isinstance(private, dict) else {}
    merged_private.update(updates)
    extended["private"] = merged_private
    return extended


def _map_http_error(exc: Any) -> GoogleCalendarError:
    status = int(getattr(getattr(exc, "resp", None), "status", 0) or 0)
    content = getattr(exc, "content", b"")
    if isinstance(content, bytes):
        detail = content.decode("utf-8", errors="ignore")
    else:
        detail = str(content)
    if status == 410:
        return GoogleCalendarSyncTokenExpired("Google Calendar invalidated the sync token.")
    if status == 404:
        return GoogleCalendarNotFoundError("The Google Calendar resource no longer exists.")
    if status == 403 and any(
        reason in detail
        for reason in ("rateLimitExceeded", "userRateLimitExceeded", "quotaExceeded")
    ):
        return GoogleCalendarRateLimitError(
            "Google Calendar is temporarily unavailable or rate limited."
        )
    if status in {401, 403}:
        return GoogleCalendarAuthenticationError(
            "Google Calendar authorization was rejected; reconnect or review the selected grant."
        )
    if status == 409:
        return GoogleCalendarError("Google Calendar request conflict (409).")
    if status == 429 or status >= 500:
        return GoogleCalendarRateLimitError(
            "Google Calendar is temporarily unavailable or rate limited."
        )
    return GoogleCalendarError(f"Google Calendar API request failed (HTTP {status or 'unknown'}).")


def _is_conflict(exc: GoogleCalendarError) -> bool:
    return "(409)" in str(exc)


async def revoke_google_token(token: str) -> None:
    import httpx

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                "https://oauth2.googleapis.com/revoke",
                data={"token": token},
                headers={"content-type": "application/x-www-form-urlencoded"},
            )
    except httpx.HTTPError as exc:
        raise GoogleCalendarRevokeError(
            "Google Calendar remote revoke could not be reached; local credentials were retained."
        ) from exc
    if response.status_code not in {200, 204}:
        raise GoogleCalendarRevokeError(
            "Google Calendar remote revoke failed; local credentials were retained."
        )


__all__ = [
    "CalendarClient",
    "GOOGLE_CALENDAR_CONNECTOR_ID",
    "GOOGLE_CALENDAR_READ_SCOPES",
    "GOOGLE_CALENDAR_WRITE_SCOPE",
    "GOOGLE_CALENDAR_WRITE_SCOPES",
    "GoogleCalendarAuthenticationError",
    "GoogleCalendarError",
    "GoogleCalendarNotFoundError",
    "GoogleCalendarRateLimitError",
    "GoogleCalendarRevokeError",
    "GoogleCalendarSyncTokenExpired",
    "GoogleCalendarWriteAuthorizationRequired",
    "build_client",
    "credential_scopes",
    "merge_authorized_user_values",
    "revoke_google_token",
]
