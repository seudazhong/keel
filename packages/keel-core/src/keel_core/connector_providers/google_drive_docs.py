"""Read-only Google Drive/Docs ingestion into an explicit Knowledge target."""

from __future__ import annotations

import asyncio
import io
import json
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from collections.abc import Buffer, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn, Protocol

import httpx

from keel_core.config import get_settings
from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAuthAction,
    ConnectorAuthenticationError,
    ConnectorAuthKind,
    ConnectorAuthStart,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCallbackParameter,
    ConnectorCapability,
    ConnectorChange,
    ConnectorChangeKind,
    ConnectorCredentialUpdate,
    ConnectorCursor,
    ConnectorCursorUpdate,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorItem,
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
from keel_core.types import ContentTaint

GOOGLE_DRIVE_DOCS_CONNECTOR_ID = "google_drive_docs"
GOOGLE_DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
GOOGLE_DRIVE_DOCS_SCOPES = (GOOGLE_DRIVE_READONLY_SCOPE,)

GOOGLE_FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
GOOGLE_DOC_MIME_TYPE = "application/vnd.google-apps.document"
TEXT_MIME_TYPE = "text/plain"
MARKDOWN_MIME_TYPE = "text/markdown"
SUPPORTED_FILE_MIME_TYPES = frozenset(
    {
        GOOGLE_DOC_MIME_TYPE,
        TEXT_MIME_TYPE,
        MARKDOWN_MIME_TYPE,
    }
)

_CURSOR_STREAM = "drive_changes"
_PAGE_SIZE = 1000
_MAX_CONTENT_BYTES = 1_048_576
_DOWNLOAD_CHUNK_BYTES = 262_144
_EXPORT_CHUNK_BYTES = 65_536
_ERROR_RESPONSE_BYTES = 65_536
_DRIVE_API_ORIGIN = "https://www.googleapis.com"
_FILE_FIELDS = "id,name,mimeType,parents,webViewLink,modifiedTime,version,trashed,size,md5Checksum"

manifest = ConnectorManifest(
    id=GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
    name="Google Drive / Docs",
    description="Import selected Drive folders, Google Docs, text, and Markdown into Knowledge.",
    icon="📄",
    auth_kind=ConnectorAuthKind.oauth,
    capabilities=(
        ConnectorCapability.read,
        ConnectorCapability.resources,
        ConnectorCapability.sync,
    ),
    scopes=GOOGLE_DRIVE_DOCS_SCOPES,
    auth_action=ConnectorAuthAction(
        callback_parameters=(ConnectorCallbackParameter("code"),),
        help_text="Uses an independent read-only Drive grant. Keel never edits Drive content.",
    ),
    resource_label="Drive folders and supported files",
    target_fields=(
        ConnectorTargetField(
            ConnectorTargetKind.knowledge,
            "Knowledge Base",
            help_text="Imported content is tainted and follows the Knowledge version lifecycle.",
        ),
    ),
    default_sync_cadence_seconds=300,
)


class GoogleDriveError(RuntimeError):
    """Bounded provider failure."""


class GoogleDriveRateLimitError(GoogleDriveError):
    """Retryable Google Drive quota or rate-limit response."""


class GoogleDriveCursorInvalidError(GoogleDriveError):
    """Drive changes cursor expired and requires a controlled full resync."""


class GoogleDriveContentTooLargeError(GoogleDriveError):
    """Remote content exceeds the existing Knowledge input limit."""


@dataclass(frozen=True, slots=True)
class DriveFile:
    id: str
    name: str
    mime_type: str
    parents: tuple[str, ...] = ()
    source_url: str | None = None
    modified_time: str | None = None
    version: str | None = None
    size: int | None = None
    trashed: bool = False

    @classmethod
    def from_api(cls, value: Mapping[str, Any]) -> DriveFile:
        file_id = _required_string(value.get("id"), "Drive file id")
        name = _required_string(value.get("name"), "Drive file name")
        mime_type = _required_string(value.get("mimeType"), "Drive MIME type")
        parents_value = value.get("parents")
        parents = (
            tuple(item for item in parents_value if isinstance(item, str) and item)
            if isinstance(parents_value, list)
            else ()
        )
        return cls(
            id=file_id,
            name=name,
            mime_type=mime_type,
            parents=parents,
            source_url=_optional_string(value.get("webViewLink")),
            modified_time=_optional_string(value.get("modifiedTime")),
            version=_optional_string(value.get("version")),
            size=_optional_size(value.get("size")),
            trashed=value.get("trashed") is True,
        )

    @classmethod
    def from_change_api(cls, value: Mapping[str, Any], file_id: str) -> DriveFile:
        parents_value = value.get("parents")
        return cls(
            id=file_id,
            name=_optional_string(value.get("name")) or file_id,
            mime_type=_optional_string(value.get("mimeType")) or "",
            parents=tuple(item for item in parents_value if isinstance(item, str) and item)
            if isinstance(parents_value, list)
            else (),
            source_url=_optional_string(value.get("webViewLink")),
            modified_time=_optional_string(value.get("modifiedTime")),
            version=_optional_string(value.get("version")),
            size=_optional_size(value.get("size")),
            trashed=value.get("trashed") is True,
        )


@dataclass(frozen=True, slots=True)
class DriveFilePage:
    files: tuple[DriveFile, ...]
    next_page_token: str | None = None


@dataclass(frozen=True, slots=True)
class DriveChange:
    file_id: str
    removed: bool
    file: DriveFile | None = None


@dataclass(frozen=True, slots=True)
class DriveChangePage:
    changes: tuple[DriveChange, ...]
    next_page_token: str | None = None
    new_start_page_token: str | None = None


class DriveClient(Protocol):
    @property
    def credential(self) -> CredentialEnvelope: ...

    def list_files(
        self,
        *,
        page_token: str | None = None,
        parent_id: str | None = None,
    ) -> DriveFilePage: ...

    def get_file(self, file_id: str) -> DriveFile | None: ...

    def get_start_page_token(self) -> str: ...

    def list_changes(self, page_token: str) -> DriveChangePage: ...

    def read_content(self, file: DriveFile) -> tuple[str, str]: ...

    def check_health(self) -> None: ...


DriveClientFactory = Callable[[CredentialEnvelope], DriveClient]
TokenRevoker = Callable[[CredentialEnvelope], None]
ExportClientFactory = Callable[[], httpx.Client]


def enabled() -> bool:
    """Enable independently when the shared Google OAuth client exists."""

    return Path(get_settings().gmail_client_secrets_path).is_file()


def availability() -> None:
    import google_auth_oauthlib.flow  # noqa: F401
    import googleapiclient.discovery  # noqa: F401


def _flow(redirect_uri: str) -> object:
    from google_auth_oauthlib.flow import Flow

    return Flow.from_client_secrets_file(
        get_settings().gmail_client_secrets_path,
        scopes=list(GOOGLE_DRIVE_DOCS_SCOPES),
        redirect_uri=redirect_uri,
    )


def _export_http_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=False,
        timeout=httpx.Timeout(30.0, connect=10.0),
    )


class _GoogleDriveExportTransport:
    def __init__(
        self,
        access_token: str,
        client_factory: ExportClientFactory = _export_http_client,
    ) -> None:
        if not access_token:
            raise ConnectorAuthenticationError(
                "Google Drive authorization has no access token; reconnect the connector."
            )
        self._access_token = access_token
        self._client_factory = client_factory

    def export_text(self, file_id: str) -> bytes:
        file_path = urllib.parse.quote(file_id, safe="")
        url = f"{_DRIVE_API_ORIGIN}/drive/v3/files/{file_path}/export"
        try:
            with self._client_factory() as client:
                with client.stream(
                    "GET",
                    url,
                    params={"mimeType": TEXT_MIME_TYPE},
                    headers={
                        "Accept-Encoding": "identity",
                        "Authorization": f"Bearer {self._access_token}",
                    },
                ) as response:
                    if response.status_code != 200:
                        _raise_google_status(
                            response.status_code,
                            _bounded_response_body(response, _ERROR_RESPONSE_BYTES),
                        )
                    content_encoding = response.headers.get(
                        "content-encoding",
                        "identity",
                    ).lower()
                    if content_encoding not in {"", "identity"}:
                        raise GoogleDriveError(
                            "Google Drive export returned an unsupported content encoding."
                        )
                    content_length = _optional_size(response.headers.get("content-length"))
                    if content_length is not None and content_length > _MAX_CONTENT_BYTES:
                        raise GoogleDriveContentTooLargeError(
                            "Google Drive text content exceeds the 1 MiB Knowledge limit."
                        )
                    buffer = _BoundedBytesIO(_MAX_CONTENT_BYTES)
                    for chunk in response.iter_bytes(chunk_size=_EXPORT_CHUNK_BYTES):
                        buffer.write(chunk)
                    return buffer.getvalue()
        except GoogleDriveError:
            raise
        except httpx.HTTPError:
            raise GoogleDriveError("Google Drive export request failed.") from None


class _GoogleApiDriveClient:
    def __init__(self, credential: CredentialEnvelope) -> None:
        if credential.kind != "oauth":
            raise ConnectorAuthenticationError("Google Drive credentials have the wrong kind.")
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        try:
            self._credentials = Credentials.from_authorized_user_info(  # type: ignore[no-untyped-call]
                dict(credential.values),
                scopes=list(GOOGLE_DRIVE_DOCS_SCOPES),
            )
            if not self._credentials.valid:
                if not self._credentials.refresh_token:
                    raise ConnectorAuthenticationError(
                        "Google Drive authorization has expired; reconnect the connector."
                    )
                self._credentials.refresh(Request())
            self._service = build(
                "drive",
                "v3",
                credentials=self._credentials,
                cache_discovery=False,
            )
            access_token = self._credentials.token
            if not isinstance(access_token, str):
                raise ConnectorAuthenticationError(
                    "Google Drive authorization has no access token; reconnect the connector."
                )
            self._export_transport = _GoogleDriveExportTransport(access_token)
        except ConnectorAuthenticationError:
            raise
        except Exception as exc:
            _raise_google_error(exc)
            raise AssertionError("unreachable") from exc

    @property
    def credential(self) -> CredentialEnvelope:
        values = json.loads(self._credentials.to_json())
        if not isinstance(values, dict):
            raise GoogleDriveError("Google credentials did not serialize to an object.")
        return CredentialEnvelope("oauth", values)

    def list_files(
        self,
        *,
        page_token: str | None = None,
        parent_id: str | None = None,
    ) -> DriveFilePage:
        query = "trashed = false"
        if parent_id is not None:
            escaped = parent_id.replace("\\", "\\\\").replace("'", "\\'")
            query = f"'{escaped}' in parents and trashed = false"
        response = self._execute(
            self._service.files().list(
                q=query,
                pageSize=_PAGE_SIZE,
                pageToken=page_token,
                spaces="drive",
                corpora="user",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields=f"nextPageToken,files({_FILE_FIELDS})",
            )
        )
        return DriveFilePage(
            tuple(
                DriveFile.from_api(item)
                for item in _mapping_list(response.get("files"), "Drive files")
            ),
            _optional_string(response.get("nextPageToken")),
        )

    def get_file(self, file_id: str) -> DriveFile | None:
        try:
            response = self._execute(
                self._service.files().get(
                    fileId=file_id,
                    supportsAllDrives=True,
                    fields=_FILE_FIELDS,
                )
            )
        except _GoogleNotFoundError:
            return None
        return DriveFile.from_api(response)

    def get_start_page_token(self) -> str:
        response = self._execute(
            self._service.changes().getStartPageToken(
                supportsAllDrives=True,
                fields="startPageToken",
            )
        )
        return _required_string(response.get("startPageToken"), "Drive start page token")

    def list_changes(self, page_token: str) -> DriveChangePage:
        response = self._execute(
            self._service.changes().list(
                pageToken=page_token,
                pageSize=_PAGE_SIZE,
                spaces="drive",
                includeRemoved=True,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields=(
                    f"nextPageToken,newStartPageToken,changes(fileId,removed,file({_FILE_FIELDS}))"
                ),
            )
        )
        changes: list[DriveChange] = []
        for item in _mapping_list(response.get("changes"), "Drive changes"):
            file_id = _required_string(item.get("fileId"), "Drive changed file id")
            raw_file = item.get("file")
            changes.append(
                DriveChange(
                    file_id=file_id,
                    removed=item.get("removed") is True,
                    file=(
                        DriveFile.from_change_api(raw_file, file_id)
                        if isinstance(raw_file, Mapping)
                        else None
                    ),
                )
            )
        return DriveChangePage(
            tuple(changes),
            _optional_string(response.get("nextPageToken")),
            _optional_string(response.get("newStartPageToken")),
        )

    def read_content(self, file: DriveFile) -> tuple[str, str]:
        if file.mime_type == GOOGLE_DOC_MIME_TYPE:
            raw = self._export_transport.export_text(file.id)
            mime_type = TEXT_MIME_TYPE
        elif file.mime_type in {TEXT_MIME_TYPE, MARKDOWN_MIME_TYPE}:
            if file.size is not None and file.size > _MAX_CONTENT_BYTES:
                raise GoogleDriveContentTooLargeError(
                    "Google Drive text content exceeds the 1 MiB Knowledge limit."
                )
            request = self._service.files().get_media(
                fileId=file.id,
                supportsAllDrives=True,
            )
            mime_type = file.mime_type
            raw = self._download_media(request)
        else:
            raise GoogleDriveError(f"unsupported Drive MIME type: {file.mime_type}")
        return _normalize_text(raw), mime_type

    def check_health(self) -> None:
        self._execute(
            self._service.about().get(
                fields="user(permissionId),storageQuota(limit,usage)",
            )
        )

    @staticmethod
    def _execute(request: Any) -> dict[str, Any]:
        try:
            response = request.execute(num_retries=0)
        except Exception as exc:
            _raise_google_error(exc)
            raise AssertionError("unreachable") from exc
        if not isinstance(response, dict):
            raise GoogleDriveError("Google Drive returned an invalid response.")
        return response

    @staticmethod
    def _download_media(request: Any) -> bytes:
        from googleapiclient.http import MediaIoBaseDownload

        buffer = _BoundedBytesIO(_MAX_CONTENT_BYTES)
        downloader = MediaIoBaseDownload(
            buffer,
            request,
            chunksize=_DOWNLOAD_CHUNK_BYTES,
        )
        try:
            done = False
            while not done:
                progress, done = downloader.next_chunk(num_retries=0)
                total_size = getattr(progress, "total_size", None)
                if isinstance(total_size, int) and total_size > _MAX_CONTENT_BYTES:
                    raise GoogleDriveContentTooLargeError(
                        "Google Drive text content exceeds the 1 MiB Knowledge limit."
                    )
        except GoogleDriveContentTooLargeError:
            raise
        except Exception as exc:
            _raise_google_error(exc)
            raise AssertionError("unreachable") from exc
        return buffer.getvalue()


class _BoundedBytesIO(io.BytesIO):
    def __init__(self, limit: int) -> None:
        super().__init__()
        self._limit = limit
        self.peak_size = 0

    def write(self, data: Buffer, /) -> int:
        size = memoryview(data).nbytes
        end = self.tell() + size
        if end > self._limit:
            raise GoogleDriveContentTooLargeError(
                "Google Drive text content exceeds the 1 MiB Knowledge limit."
            )
        written = super().write(data)
        self.peak_size = max(self.peak_size, end)
        return written


class _GoogleNotFoundError(GoogleDriveError):
    pass


def _raise_google_error(exc: Exception) -> None:
    status = getattr(getattr(exc, "resp", None), "status", None)
    content = getattr(exc, "content", b"")
    _raise_google_status(status, content, cause=exc)


def _raise_google_status(
    status: object,
    content: object,
    *,
    cause: Exception | None = None,
) -> NoReturn:
    reason = ""
    if isinstance(content, str):
        content = content.encode()
    if isinstance(content, bytes):
        try:
            payload = json.loads(content.decode("utf-8", errors="replace"))
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            error = payload.get("error")
            if isinstance(error, dict):
                errors = error.get("errors")
                if isinstance(errors, list) and errors and isinstance(errors[0], dict):
                    reason = str(errors[0].get("reason", ""))
    if status == 404:
        raise _GoogleNotFoundError("Google Drive file was not found.") from cause
    if status == 410:
        raise GoogleDriveCursorInvalidError("Google Drive changes cursor expired.") from cause
    if status in {401}:
        raise ConnectorAuthenticationError(
            "Google Drive authorization is invalid; reconnect the connector."
        ) from cause
    if status == 429 or reason in {
        "dailyLimitExceeded",
        "rateLimitExceeded",
        "userRateLimitExceeded",
    }:
        raise GoogleDriveRateLimitError("Google Drive rate limit exceeded; retry later.") from cause
    raise GoogleDriveError("Google Drive request failed.") from cause


def _bounded_response_body(response: httpx.Response, limit: int) -> bytes:
    body = _BoundedBytesIO(limit)
    for chunk in response.iter_bytes(chunk_size=min(_EXPORT_CHUNK_BYTES, limit)):
        remaining = limit - body.tell()
        if remaining <= 0:
            break
        body.write(chunk[:remaining])
        if len(chunk) > remaining:
            break
    return body.getvalue()


def _revoke_google_token(credential: CredentialEnvelope) -> None:
    token = credential.values.get("refresh_token") or credential.values.get("token")
    if not isinstance(token, str) or not token:
        return
    request = urllib.request.Request(
        "https://oauth2.googleapis.com/revoke",
        data=urllib.parse.urlencode({"token": token}).encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
            if response.status != 200:
                raise GoogleDriveError("Google rejected the Drive token revocation.")
    except (OSError, urllib.error.HTTPError, urllib.error.URLError) as exc:
        raise GoogleDriveError("Google Drive token revocation failed.") from exc


class GoogleDriveDocsProvider(BaseConnectorProvider):
    manifest = manifest

    def __init__(
        self,
        client_factory: DriveClientFactory = _GoogleApiDriveClient,
        token_revoker: TokenRevoker = _revoke_google_token,
    ) -> None:
        self._client_factory = client_factory
        self._token_revoker = token_revoker

    def enabled(self) -> bool:
        return enabled()

    async def begin_auth(
        self, context: ConnectorOperationContext, callback_url: str
    ) -> ConnectorAuthStart:
        return await asyncio.to_thread(self._begin_auth, callback_url)

    @staticmethod
    def _begin_auth(callback_url: str) -> ConnectorAuthStart:
        flow = _flow(callback_url)
        auth_url, state = flow.authorization_url(  # type: ignore[attr-defined]
            access_type="offline",
            prompt="consent",
            include_granted_scopes="false",
        )
        return ConnectorAuthStart(str(auth_url), str(state))

    async def complete_auth(
        self,
        context: ConnectorOperationContext,
        callback_url: str,
        parameters: dict[str, str],
    ) -> ConnectorSetupResult:
        return await asyncio.to_thread(
            self._complete_auth,
            callback_url,
            parameters,
        )

    @staticmethod
    def _complete_auth(
        callback_url: str,
        parameters: dict[str, str],
    ) -> ConnectorSetupResult:
        code = parameters.get("code", "").strip()
        if not code:
            raise ValueError("missing authorization code")
        flow = _flow(callback_url)
        flow.fetch_token(code=code)  # type: ignore[attr-defined]
        credentials = flow.credentials  # type: ignore[attr-defined]
        values = json.loads(credentials.to_json())
        if not isinstance(values, dict):
            raise ValueError("Google Drive credentials did not serialize to an object")
        return ConnectorSetupResult(
            binding=ConnectorBindingDraft(display_name="Google Drive / Docs"),
            credential=CredentialEnvelope(kind="oauth", values=values),
            status=ConnectorBindingStatus.connected,
        )

    async def list_resources(self, context: ConnectorOperationContext) -> ConnectorResourceResult:
        return await asyncio.to_thread(self._list_resources, context)

    def _list_resources(self, context: ConnectorOperationContext) -> ConnectorResourceResult:
        client = self._client(context)
        resources: list[ConnectorResourceDraft] = []
        page_token: str | None = None
        while True:
            page = client.list_files(page_token=page_token)
            for file in page.files:
                if file.trashed or not _is_selectable(file):
                    continue
                resources.append(
                    ConnectorResourceDraft(
                        external_id=file.id,
                        kind="folder" if file.mime_type == GOOGLE_FOLDER_MIME_TYPE else "file",
                        display_name=file.name,
                        url=_source_url(file),
                        config=_file_config(file),
                    )
                )
            page_token = page.next_page_token
            if page_token is None:
                break
        resources.sort(
            key=lambda item: (item.kind != "folder", item.display_name, item.external_id)
        )
        return ConnectorResourceResult(
            tuple(resources),
            ConnectorResourceRefreshMode.authoritative,
        )

    async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
        return await asyncio.to_thread(self._sync, context)

    def _sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
        if context.binding is None:
            raise GoogleDriveError("Google Drive binding is unavailable.")
        client = self._client(context)
        if not context.resources:
            return ConnectorSyncResult(state=self._state_update(context, client))
        cursors = _resource_cursors(context.resources, context.cursors)
        if len(cursors) != len(context.resources):
            return self._full_resync(context, client)
        try:
            return self._incremental_sync(context, client, cursors)
        except GoogleDriveCursorInvalidError:
            return self._full_resync(context, client)

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        return await asyncio.to_thread(self._health, context)

    def _health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        now = datetime.now(UTC)
        if not self.enabled():
            return ConnectorHealth(
                ConnectorHealthStatus.degraded,
                now,
                "Google Drive / Docs is configured but disabled.",
            )
        if context.credential is None:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                now,
                "Google Drive credentials are missing.",
            )
        try:
            self._client(context).check_health()
        except GoogleDriveRateLimitError as exc:
            return ConnectorHealth(
                ConnectorHealthStatus.degraded,
                now,
                str(exc),
                retryable=True,
            )
        except ConnectorAuthenticationError as exc:
            return ConnectorHealth(ConnectorHealthStatus.error, now, str(exc))
        except GoogleDriveError as exc:
            return ConnectorHealth(
                ConnectorHealthStatus.error,
                now,
                str(exc),
                retryable=True,
            )
        return ConnectorHealth(ConnectorHealthStatus.healthy, now)

    async def revoke(self, context: ConnectorOperationContext) -> None:
        if context.credential is not None:
            await asyncio.to_thread(self._token_revoker, context.credential)

    def _incremental_sync(
        self,
        context: ConnectorOperationContext,
        client: DriveClient,
        cursors: Mapping[str, ConnectorCursor],
    ) -> ConnectorSyncResult:
        resources_by_id = {item.id: item for item in context.resources}
        resources_by_cursor: dict[str, list[ConnectorResource]] = {}
        for resource_id, cursor in cursors.items():
            resources_by_cursor.setdefault(cursor.value, []).append(resources_by_id[resource_id])

        changed: dict[str, DriveChange] = {}
        cursor_updates: list[ConnectorCursorUpdate] = []
        for token in sorted(resources_by_cursor):
            changes, new_token = _collect_changes(client, token)
            for change in changes:
                changed[change.file_id] = change
            for resource in resources_by_cursor[token]:
                cursor_updates.append(_cursor_update(resource, new_token))

        existing = {item.external_id: item for item in context.items}
        if _requires_full_resync(changed.values(), existing):
            return self._full_resync(context, client)

        changes_out: list[ConnectorChange] = []
        selected_file_ids, selected_folder_ids = _selected_ids(context.resources)
        ancestry_cache: dict[str, bool] = {}
        for file_id in sorted(changed):
            observed = changed[file_id]
            current = client.get_file(file_id)
            if current is None or current.trashed:
                if file_id in existing:
                    changes_out.append(self._delete_change(context, file_id, observed.file))
                continue
            if current.mime_type == GOOGLE_FOLDER_MIME_TYPE:
                return self._full_resync(context, client)
            selected = file_id in selected_file_ids or _under_selected_folder(
                current,
                selected_folder_ids,
                client,
                ancestry_cache,
            )
            if not selected or current.mime_type not in SUPPORTED_FILE_MIME_TYPES:
                if file_id in existing:
                    changes_out.append(self._delete_change(context, file_id, current))
                continue
            upsert_change = self._upsert_if_changed(
                context,
                client,
                current,
                existing.get(file_id),
            )
            if upsert_change is not None:
                changes_out.append(upsert_change)

        return ConnectorSyncResult(
            tuple(changes_out),
            self._state_update(context, client, tuple(cursor_updates)),
        )

    def _full_resync(
        self,
        context: ConnectorOperationContext,
        client: DriveClient,
    ) -> ConnectorSyncResult:
        start_token = client.get_start_page_token()
        discovered = _crawl_selected(context.resources, client)
        existing = {item.external_id: item for item in context.items}
        changes: list[ConnectorChange] = []
        for file_id in sorted(discovered):
            change = self._upsert_if_changed(
                context,
                client,
                discovered[file_id],
                existing.get(file_id),
            )
            if change is not None:
                changes.append(change)
        for file_id in sorted(set(existing) - set(discovered)):
            changes.append(self._delete_change(context, file_id, None))
        cursor_updates = tuple(
            _cursor_update(resource, start_token) for resource in context.resources
        )
        return ConnectorSyncResult(
            tuple(changes),
            self._state_update(context, client, cursor_updates),
        )

    def _upsert_if_changed(
        self,
        context: ConnectorOperationContext,
        client: DriveClient,
        file: DriveFile,
        existing: ConnectorItem | None,
    ) -> ConnectorChange | None:
        revision = _revision(file)
        source_url = _source_url(file)
        if (
            existing is not None
            and existing.config.get("revision") == revision
            and existing.display_name == file.name
            and existing.url == source_url
        ):
            return None
        content, mime_type = client.read_content(file)
        assert context.binding is not None
        return ConnectorChange(
            ConnectorChangeKind.upsert,
            ConnectorProvenance(
                connector_id=GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
                binding_id=context.binding.id,
                external_resource_id=file.id,
                source_url=source_url,
                revision=revision,
            ),
            title=file.name,
            content=content,
            mime_type=mime_type,
            taint=ContentTaint.tainted,
        )

    @staticmethod
    def _delete_change(
        context: ConnectorOperationContext,
        file_id: str,
        file: DriveFile | None,
    ) -> ConnectorChange:
        assert context.binding is not None
        return ConnectorChange(
            ConnectorChangeKind.delete,
            ConnectorProvenance(
                connector_id=GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
                binding_id=context.binding.id,
                external_resource_id=file_id,
                source_url=_source_url(file) if file is not None else None,
                revision=_revision(file) if file is not None else "deleted",
            ),
            taint=ContentTaint.tainted,
        )

    @staticmethod
    def _state_update(
        context: ConnectorOperationContext,
        client: DriveClient,
        cursors: tuple[ConnectorCursorUpdate, ...] = (),
    ) -> ConnectorStateUpdate:
        credential = client.credential
        credential_update = (
            ConnectorCredentialUpdate(credential, context.credential_version)
            if context.credential is not None and credential.values != context.credential.values
            else None
        )
        return ConnectorStateUpdate(
            credential=credential_update,
            cursor_updates=cursors,
        )

    def _client(self, context: ConnectorOperationContext) -> DriveClient:
        if context.credential is None:
            raise ConnectorAuthenticationError("Google Drive credentials are missing.")
        return self._client_factory(context.credential)


def _collect_changes(client: DriveClient, token: str) -> tuple[tuple[DriveChange, ...], str]:
    changes: list[DriveChange] = []
    page_token = token
    while True:
        page = client.list_changes(page_token)
        changes.extend(page.changes)
        if page.next_page_token is not None:
            page_token = page.next_page_token
            continue
        if page.new_start_page_token is None:
            raise GoogleDriveError("Google Drive changes response omitted its next cursor.")
        return tuple(changes), page.new_start_page_token


def _crawl_selected(
    resources: tuple[ConnectorResource, ...],
    client: DriveClient,
) -> dict[str, DriveFile]:
    found: dict[str, DriveFile] = {}
    folders: deque[str] = deque()
    visited_folders: set[str] = set()
    for resource in resources:
        if resource.kind == "folder":
            folders.append(resource.external_id)
            continue
        file = client.get_file(resource.external_id)
        if file is not None and not file.trashed and file.mime_type in SUPPORTED_FILE_MIME_TYPES:
            found[file.id] = file
    while folders:
        folder_id = folders.popleft()
        if folder_id in visited_folders:
            continue
        visited_folders.add(folder_id)
        page_token: str | None = None
        while True:
            page = client.list_files(page_token=page_token, parent_id=folder_id)
            for file in page.files:
                if file.trashed:
                    continue
                if file.mime_type == GOOGLE_FOLDER_MIME_TYPE:
                    folders.append(file.id)
                elif file.mime_type in SUPPORTED_FILE_MIME_TYPES:
                    found[file.id] = file
            page_token = page.next_page_token
            if page_token is None:
                break
    return found


def _selected_ids(resources: Iterable[ConnectorResource]) -> tuple[set[str], set[str]]:
    files: set[str] = set()
    folders: set[str] = set()
    for resource in resources:
        (folders if resource.kind == "folder" else files).add(resource.external_id)
    return files, folders


def _under_selected_folder(
    file: DriveFile,
    selected_folders: set[str],
    client: DriveClient,
    cache: dict[str, bool],
) -> bool:
    pending = deque(file.parents)
    visited: set[str] = set()
    while pending:
        parent_id = pending.popleft()
        if parent_id in selected_folders:
            for item in visited:
                cache[item] = True
            return True
        if parent_id in cache:
            if cache[parent_id]:
                for item in visited:
                    cache[item] = True
                return True
            continue
        if parent_id in visited:
            continue
        visited.add(parent_id)
        parent = client.get_file(parent_id)
        if parent is not None:
            pending.extend(parent.parents)
    for item in visited:
        cache[item] = False
    return False


def _requires_full_resync(
    changes: Iterable[DriveChange],
    existing: Mapping[str, ConnectorItem],
) -> bool:
    for change in changes:
        if change.file is not None and change.file.mime_type == GOOGLE_FOLDER_MIME_TYPE:
            return True
        if change.removed and change.file_id not in existing:
            return True
    return False


def _resource_cursors(
    resources: Iterable[ConnectorResource],
    cursors: Iterable[ConnectorCursor],
) -> dict[str, ConnectorCursor]:
    resource_ids = {item.id for item in resources}
    return {
        cursor.resource_id: cursor
        for cursor in cursors
        if cursor.resource_id in resource_ids
        and cursor.resource_id is not None
        and cursor.stream == _CURSOR_STREAM
    }


def _cursor_update(resource: ConnectorResource, value: str) -> ConnectorCursorUpdate:
    return ConnectorCursorUpdate(
        stream=_CURSOR_STREAM,
        value=value,
        resource_id=resource.id,
        revision="drive-v3",
    )


def _is_selectable(file: DriveFile) -> bool:
    return file.mime_type == GOOGLE_FOLDER_MIME_TYPE or file.mime_type in SUPPORTED_FILE_MIME_TYPES


def _source_url(file: DriveFile) -> str:
    if file.source_url:
        return file.source_url
    if file.mime_type == GOOGLE_DOC_MIME_TYPE:
        return f"https://docs.google.com/document/d/{urllib.parse.quote(file.id, safe='')}/edit"
    return f"https://drive.google.com/open?id={urllib.parse.quote(file.id, safe='')}"


def _revision(file: DriveFile) -> str:
    return json.dumps(
        {
            "modified_time": file.modified_time,
            "name": file.name,
            "version": file.version,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _file_config(file: DriveFile) -> dict[str, Any]:
    return {
        "mime_type": file.mime_type,
        "modified_time": file.modified_time,
        "parents": list(file.parents),
        "size": file.size,
        "version": file.version,
    }


def _normalize_text(raw: bytes) -> str:
    if len(raw) > _MAX_CONTENT_BYTES:
        raise GoogleDriveContentTooLargeError(
            "Google Drive text content exceeds the 1 MiB Knowledge limit."
        )
    content = raw.decode("utf-8-sig", errors="replace").replace("\x00", "")
    content = content.replace("\r\n", "\n").replace("\r", "\n")
    content = content if content else "\n"
    if len(content.encode("utf-8")) > _MAX_CONTENT_BYTES:
        raise GoogleDriveContentTooLargeError(
            "Google Drive normalized text exceeds the 1 MiB Knowledge limit."
        )
    return content


def _mapping_list(value: object, label: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise GoogleDriveError(f"{label} response was invalid.")
    return list(value)


def _required_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise GoogleDriveError(f"{label} was missing.")
    return value


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_size(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    return None


def factory() -> GoogleDriveDocsProvider:
    return GoogleDriveDocsProvider()


__all__ = [
    "DriveChange",
    "DriveChangePage",
    "DriveClient",
    "DriveFile",
    "DriveFilePage",
    "GOOGLE_DOC_MIME_TYPE",
    "GOOGLE_DRIVE_DOCS_CONNECTOR_ID",
    "GOOGLE_DRIVE_DOCS_SCOPES",
    "GOOGLE_FOLDER_MIME_TYPE",
    "GoogleDriveContentTooLargeError",
    "GoogleDriveCursorInvalidError",
    "GoogleDriveDocsProvider",
    "GoogleDriveError",
    "GoogleDriveRateLimitError",
    "MARKDOWN_MIME_TYPE",
    "SUPPORTED_FILE_MIME_TYPES",
    "TEXT_MIME_TYPE",
    "availability",
    "enabled",
    "factory",
    "manifest",
]
