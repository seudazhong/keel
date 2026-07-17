"""Provider-local Google Drive/Docs sync and Knowledge lifecycle contracts."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Mapping
from email.message import Message
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from keel_core.connector_contracts import (
    ConnectorAuthenticationError,
    ConnectorBinding,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorChangeKind,
    ConnectorCursor,
    ConnectorHealthStatus,
    ConnectorItem,
    ConnectorOperationContext,
    ConnectorResource,
    ConnectorResourceDraft,
    ConnectorTargetKind,
)
from keel_core.connector_credentials import ConnectorCredentialStore, CredentialEnvelope
from keel_core.connector_providers import google_drive_docs
from keel_core.connector_providers.google_drive_docs import (
    GOOGLE_DOC_MIME_TYPE,
    GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
    GOOGLE_DRIVE_DOCS_SCOPES,
    TEXT_MIME_TYPE,
    DriveChange,
    DriveChangePage,
    DriveFile,
    DriveFilePage,
    GoogleDriveContentTooLargeError,
    GoogleDriveCursorInvalidError,
    GoogleDriveDocsProvider,
    GoogleDriveError,
    GoogleDriveRateLimitError,
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

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "connectors" / "google_drive_docs"


def _load_fixture(name: str) -> dict[str, Any]:
    value = json.loads((_FIXTURES / name).read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _drive_file(value: Mapping[str, Any]) -> DriveFile:
    return DriveFile(
        id=str(value["id"]),
        name=str(value["name"]),
        mime_type=str(value["mime_type"]),
        parents=tuple(str(item) for item in value.get("parents", [])),
        source_url=str(value["source_url"]) if value.get("source_url") else None,
        modified_time=str(value["modified_time"]) if value.get("modified_time") else None,
        version=str(value["version"]) if value.get("version") else None,
    )


class FakeDriveClient:
    def __init__(
        self,
        fixture: dict[str, Any],
        *,
        credential: CredentialEnvelope | None = None,
    ) -> None:
        self.files = {
            file.id: file
            for raw in fixture.get("files", [])
            if isinstance(raw, dict)
            for file in (_drive_file(raw),)
        }
        self.content = {
            str(key): str(value).encode() for key, value in dict(fixture.get("content", {})).items()
        }
        self.start_page_token = str(fixture.get("start_page_token", "cursor-1"))
        self.change_pages: dict[str, DriveChangePage] = {}
        self.invalid_tokens: set[str] = set()
        self.list_calls: list[tuple[str | None, str | None]] = []
        self.change_calls: list[str] = []
        self.read_calls: list[str] = []
        self.health_error: Exception | None = None
        self._credential = credential or CredentialEnvelope(
            "oauth",
            {"refresh_token": "drive-refresh", "token": "drive-access"},
        )

    @property
    def credential(self) -> CredentialEnvelope:
        return self._credential

    def rotate(self, token: str) -> None:
        self._credential = CredentialEnvelope(
            "oauth",
            {"refresh_token": "drive-refresh", "token": token},
        )

    def list_files(
        self,
        *,
        page_token: str | None = None,
        parent_id: str | None = None,
    ) -> DriveFilePage:
        self.list_calls.append((page_token, parent_id))
        files = sorted(self.files.values(), key=lambda item: item.id)
        if parent_id is not None:
            return DriveFilePage(tuple(file for file in files if parent_id in file.parents))
        midpoint = max(1, len(files) // 2)
        if page_token is None:
            return DriveFilePage(tuple(files[:midpoint]), "files-page-2")
        if page_token == "files-page-2":
            return DriveFilePage(tuple(files[midpoint:]))
        raise AssertionError(f"unexpected files page token: {page_token}")

    def get_file(self, file_id: str) -> DriveFile | None:
        return self.files.get(file_id)

    def get_start_page_token(self) -> str:
        return self.start_page_token

    def list_changes(self, page_token: str) -> DriveChangePage:
        self.change_calls.append(page_token)
        if page_token in self.invalid_tokens:
            raise GoogleDriveCursorInvalidError("expired")
        return self.change_pages[page_token]

    def read_content(self, file: DriveFile) -> tuple[str, str]:
        self.read_calls.append(file.id)
        mime_type = TEXT_MIME_TYPE if file.mime_type == GOOGLE_DOC_MIME_TYPE else file.mime_type
        return self.content[file.id].decode().replace("\r\n", "\n"), mime_type

    def check_health(self) -> None:
        if self.health_error is not None:
            raise self.health_error


def _binding(scope_id: str = "scope:a") -> ConnectorBinding:
    return ConnectorBinding(
        id="binding-drive",
        scope_id=scope_id,
        connector_id=GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        status=ConnectorBindingStatus.connected,
    )


def _resource(
    *,
    external_id: str = "root",
    kind: str = "folder",
    scope_id: str = "scope:a",
) -> ConnectorResource:
    return ConnectorResource(
        id=f"resource-{external_id}",
        scope_id=scope_id,
        connector_id=GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        binding_id="binding-drive",
        external_id=external_id,
        kind=kind,
        display_name=external_id,
        selected=True,
    )


def _item(file: DriveFile, *, revision: str = "old") -> ConnectorItem:
    return ConnectorItem(
        id=f"item-{file.id}",
        scope_id="scope:a",
        connector_id=GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        binding_id="binding-drive",
        external_id=file.id,
        kind="knowledge_document",
        display_name=file.name,
        url=file.source_url,
        destination_kind=ConnectorTargetKind.knowledge,
        destination_target_id="kb-1",
        destination_id=f"knowledge-{file.id}",
        config={"revision": revision},
    )


def _context(
    client: FakeDriveClient,
    *,
    resources: tuple[ConnectorResource, ...] | None = None,
    items: tuple[ConnectorItem, ...] = (),
    cursors: tuple[ConnectorCursor, ...] = (),
) -> ConnectorOperationContext:
    return ConnectorOperationContext(
        scope_id="scope:a",
        connector_id=GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        binding=_binding(),
        credential=CredentialEnvelope(
            "oauth",
            {"refresh_token": "drive-refresh", "token": "drive-access"},
        ),
        credential_version=1,
        resources=resources or (_resource(),),
        items=items,
        cursors=cursors,
    )


class EnabledGoogleDriveProvider(GoogleDriveDocsProvider):
    def enabled(self) -> bool:
        return True


def test_manifest_is_read_only_independent_and_requires_knowledge() -> None:
    assert google_drive_docs.manifest.id == GOOGLE_DRIVE_DOCS_CONNECTOR_ID
    assert google_drive_docs.manifest.scopes == GOOGLE_DRIVE_DOCS_SCOPES
    assert GOOGLE_DRIVE_DOCS_SCOPES == ("https://www.googleapis.com/auth/drive.readonly",)
    assert all("gmail" not in scope for scope in google_drive_docs.manifest.scopes)
    assert [field.kind for field in google_drive_docs.manifest.target_fields] == [
        ConnectorTargetKind.knowledge
    ]
    assert google_drive_docs.manifest.actions == ()


def test_builtin_registry_discovers_and_creates_google_drive_docs() -> None:
    registry = discover_connector_registry()
    assert GOOGLE_DRIVE_DOCS_CONNECTOR_ID in {item.id for item in registry.manifests()}
    assert registry.create(GOOGLE_DRIVE_DOCS_CONNECTOR_ID).manifest is google_drive_docs.manifest


def test_removed_change_accepts_partial_google_file_metadata() -> None:
    file = DriveFile.from_change_api({"id": "deleted-1"}, "deleted-1")
    assert file.id == "deleted-1"
    assert file.name == "deleted-1"
    assert file.mime_type == ""


class _FakeGoogleCredentials:
    def __init__(self, *, valid: bool) -> None:
        self.valid = valid
        self.refresh_token = "refresh-token"
        self.token = "refreshed-token"
        self.refresh_requests: list[object] = []

    def refresh(self, request: object) -> None:
        self.refresh_requests.append(request)
        self.valid = True

    def to_json(self) -> str:
        return json.dumps(
            {
                "refresh_token": self.refresh_token,
                "token": "refreshed-token",
            }
        )


def _library_client(
    monkeypatch: pytest.MonkeyPatch,
    service: object,
    *,
    valid: bool,
) -> tuple[Any, _FakeGoogleCredentials, dict[str, Any]]:
    import google.auth.transport.requests as google_requests
    import google.oauth2.credentials as google_credentials
    import googleapiclient.discovery as google_discovery

    credentials = _FakeGoogleCredentials(valid=valid)
    request_marker = object()
    seen: dict[str, Any] = {}

    def from_authorized_user_info(
        values: dict[str, Any],
        scopes: list[str],
    ) -> _FakeGoogleCredentials:
        seen["credential_values"] = values
        seen["scopes"] = scopes
        return credentials

    def build(name: str, version: str, **kwargs: Any) -> object:
        seen["build"] = (name, version, kwargs)
        return service

    monkeypatch.setattr(
        google_credentials.Credentials,
        "from_authorized_user_info",
        staticmethod(from_authorized_user_info),
    )
    monkeypatch.setattr(google_requests, "Request", lambda: request_marker)
    monkeypatch.setattr(google_discovery, "build", build)
    client = google_drive_docs._GoogleApiDriveClient(
        CredentialEnvelope(
            "oauth",
            {"refresh_token": "refresh-token", "token": "expired-token"},
        )
    )
    seen["request_marker"] = request_marker
    return client, credentials, seen


def test_google_client_refreshes_credentials_and_builds_drive_v3(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = object()
    client, credentials, seen = _library_client(
        monkeypatch,
        service,
        valid=False,
    )

    assert credentials.refresh_requests == [seen["request_marker"]]
    assert seen["credential_values"] == {
        "refresh_token": "refresh-token",
        "token": "expired-token",
    }
    assert seen["scopes"] == list(GOOGLE_DRIVE_DOCS_SCOPES)
    name, version, kwargs = seen["build"]
    assert (name, version) == ("drive", "v3")
    assert kwargs == {
        "credentials": credentials,
        "cache_discovery": False,
    }
    assert client.credential.values["token"] == "refreshed-token"
    authorization_checked: list[bool] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer refreshed-token"
        authorization_checked.append(True)
        return httpx.Response(200, content=b"refreshed export")

    mock = httpx.MockTransport(handler)
    client._export_transport._client_factory = lambda: httpx.Client(transport=mock)
    content, mime_type = client.read_content(DriveFile("doc-1", "Doc", GOOGLE_DOC_MIME_TYPE))
    assert (content, mime_type) == ("refreshed export", TEXT_MIME_TYPE)
    assert authorization_checked == [True]


class _ExecutableRequest:
    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.retries: list[int] = []

    def execute(self, *, num_retries: int) -> dict[str, Any]:
        self.retries.append(num_retries)
        return self.response


class _DriveService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.executed: list[_ExecutableRequest] = []

    def files(self) -> _DriveService:
        return self

    def changes(self) -> _DriveService:
        return self

    def about(self) -> _DriveService:
        return self

    def _request(self, operation: str, response: dict[str, Any], **kwargs: Any) -> object:
        self.calls.append((operation, kwargs))
        request = _ExecutableRequest(response)
        self.executed.append(request)
        return request

    def list(self, **kwargs: Any) -> object:
        if "pageToken" in kwargs and "includeRemoved" in kwargs:
            return self._request(
                "changes.list",
                {
                    "changes": [
                        {
                            "fileId": "doc-1",
                            "removed": False,
                            "file": {
                                "id": "doc-1",
                                "name": "Doc",
                                "mimeType": GOOGLE_DOC_MIME_TYPE,
                            },
                        }
                    ],
                    "newStartPageToken": "cursor-2",
                },
                **kwargs,
            )
        return self._request(
            "files.list",
            {
                "files": [
                    {
                        "id": "txt-1",
                        "name": "Notes.txt",
                        "mimeType": TEXT_MIME_TYPE,
                        "size": "12",
                    }
                ]
            },
            **kwargs,
        )

    def get(self, **kwargs: Any) -> object:
        if "fileId" in kwargs:
            return self._request(
                "files.get",
                {
                    "id": kwargs["fileId"],
                    "name": "Notes.txt",
                    "mimeType": TEXT_MIME_TYPE,
                    "size": "12",
                },
                **kwargs,
            )
        return self._request("about.get", {}, **kwargs)

    def getStartPageToken(self, **kwargs: Any) -> object:
        return self._request(
            "changes.getStartPageToken",
            {"startPageToken": "cursor-1"},
            **kwargs,
        )


def test_google_client_wires_paginated_drive_requests_without_sdk_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _DriveService()
    client, credentials, _ = _library_client(monkeypatch, service, valid=True)

    page = client.list_files(page_token="files-page", parent_id="folder'one")
    file = client.get_file("txt-1")
    start_token = client.get_start_page_token()
    changes = client.list_changes("cursor-1")
    client.check_health()

    assert credentials.refresh_requests == []
    assert page.files[0].size == 12
    assert file is not None and file.size == 12
    assert start_token == "cursor-1"
    assert changes.new_start_page_token == "cursor-2"
    assert all(request.retries == [0] for request in service.executed)
    calls = dict(service.calls)
    assert calls["files.list"]["pageSize"] == 1000
    assert calls["files.list"]["pageToken"] == "files-page"
    assert calls["files.list"]["q"] == "'folder\\'one' in parents and trashed = false"
    assert calls["files.list"]["supportsAllDrives"] is True
    assert calls["changes.list"]["includeRemoved"] is True
    assert calls["changes.list"]["supportsAllDrives"] is True
    assert calls["files.get"]["supportsAllDrives"] is True


class _MediaResponse(dict[str, str]):
    status = 206


class _StreamingHttp:
    def __init__(self, total_size: int) -> None:
        self.total_size = total_size
        self.bytes_returned = 0
        self.calls = 0

    def request(
        self,
        uri: str,
        method: str,
        *,
        headers: dict[str, str],
    ) -> tuple[_MediaResponse, bytes]:
        assert uri == "https://drive.test/media"
        assert method == "GET"
        self.calls += 1
        raw_range = headers["range"].removeprefix("bytes=")
        start_text, end_text = raw_range.split("-", 1)
        start = int(start_text)
        requested_end = int(end_text)
        end = min(requested_end, self.total_size - 1)
        content = b"x" * (end - start + 1)
        self.bytes_returned += len(content)
        return (
            _MediaResponse(
                {
                    "content-range": f"bytes {start}-{end}/{self.total_size}",
                    "content-length": str(len(content)),
                }
            ),
            content,
        )


class _StreamingRequest:
    def __init__(self, total_size: int) -> None:
        self.http = _StreamingHttp(total_size)
        self.uri = "https://drive.test/media"
        self.headers: dict[str, str] = {}


class _MediaFiles:
    def __init__(self, request: _StreamingRequest) -> None:
        self.request = request
        self.get_media_calls = 0

    def get_media(self, **kwargs: Any) -> _StreamingRequest:
        self.get_media_calls += 1
        return self.request


class _MediaService:
    def __init__(self, request: _StreamingRequest) -> None:
        self.files_api = _MediaFiles(request)

    def files(self) -> _MediaFiles:
        return self.files_api


def test_raw_file_size_preflight_rejects_before_request_or_buffering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _StreamingRequest(2_000_000)
    service = _MediaService(request)
    client, _, _ = _library_client(monkeypatch, service, valid=True)
    file = DriveFile(
        id="large-text",
        name="large.txt",
        mime_type=TEXT_MIME_TYPE,
        size=1_048_577,
    )

    with pytest.raises(GoogleDriveContentTooLargeError, match="1 MiB"):
        client.read_content(file)
    assert service.files_api.get_media_calls == 0
    assert request.http.calls == 0
    assert request.http.bytes_returned == 0


def test_google_doc_export_aborts_bounded_stream_without_full_buffering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    total_size = 4_000_000
    stream = _TrackingExportStream(total_size)
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["no_range"] = "range" not in request.headers
        return httpx.Response(200, stream=stream)

    service = _MediaService(_StreamingRequest(1))
    client, _, _ = _library_client(monkeypatch, service, valid=True)
    client._export_transport = _export_transport(handler)
    file = DriveFile(
        id="large-doc",
        name="Large Doc",
        mime_type=GOOGLE_DOC_MIME_TYPE,
    )

    with pytest.raises(GoogleDriveContentTooLargeError, match="1 MiB"):
        client.read_content(file)
    assert seen["no_range"] is True
    assert stream.bytes_yielded <= 1_048_576 + 65_536
    assert stream.bytes_yielded < total_size


class _TrackingExportStream(httpx.SyncByteStream):
    def __init__(
        self,
        total_size: int,
        *,
        chunk_size: int = 65_536,
        chunks: tuple[bytes, ...] | None = None,
    ) -> None:
        self.total_size = total_size
        self.chunk_size = chunk_size
        self.chunks = chunks
        self.bytes_yielded = 0

    def __iter__(self) -> Any:
        if self.chunks is not None:
            for chunk in self.chunks:
                self.bytes_yielded += len(chunk)
                yield chunk
            return
        remaining = self.total_size
        while remaining:
            size = min(self.chunk_size, remaining)
            self.bytes_yielded += size
            remaining -= size
            yield b"x" * size


def _export_transport(
    handler: Any,
    *,
    token: str = "direct-export-token",
) -> Any:
    mock = httpx.MockTransport(handler)
    return google_drive_docs._GoogleDriveExportTransport(
        token,
        lambda: httpx.Client(transport=mock),
    )


def test_direct_export_stream_success_uses_fixed_origin_and_bearer_auth() -> None:
    seen: dict[str, Any] = {}
    stream = _TrackingExportStream(
        12,
        chunks=(b"hello ", b"world\n"),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(
            host=request.url.host,
            raw_path=request.url.raw_path,
            mime_type=request.url.params["mimeType"],
            authorized=request.headers["Authorization"] == f"Bearer {token}",
            identity_encoding=request.headers["Accept-Encoding"] == "identity",
            no_range="range" not in request.headers,
        )
        return httpx.Response(200, stream=stream)

    token = "direct-export-secret"
    transport = _export_transport(handler, token=token)
    assert transport.export_text("doc/with space") == b"hello world\n"

    assert seen["host"] == "www.googleapis.com"
    assert seen["raw_path"] == b"/drive/v3/files/doc%2Fwith%20space/export?mimeType=text%2Fplain"
    assert seen["mime_type"] == TEXT_MIME_TYPE
    assert seen["authorized"] is True
    assert seen["identity_encoding"] is True
    assert seen["no_range"] is True
    assert token not in repr(transport)


@pytest.mark.parametrize(
    ("status", "reason", "expected"),
    [
        (401, None, ConnectorAuthenticationError),
        (404, None, google_drive_docs._GoogleNotFoundError),
        (429, None, GoogleDriveRateLimitError),
        (403, "userRateLimitExceeded", GoogleDriveRateLimitError),
        (500, None, GoogleDriveError),
    ],
)
def test_direct_export_status_auth_and_rate_limit_classification(
    status: int,
    reason: str | None,
    expected: type[Exception],
) -> None:
    body = json.dumps(
        {
            "error": {
                "errors": [{"reason": reason}] if reason is not None else [],
            }
        }
    ).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=body)

    token = "status-secret-token"
    transport = _export_transport(handler, token=token)
    with pytest.raises(expected) as raised:
        transport.export_text("doc-1")
    assert token not in str(raised.value)


class _GoogleHttpError(Exception):
    def __init__(self, status: int, reason: str | None = None) -> None:
        super().__init__("sensitive-provider-error")
        self.resp = SimpleNamespace(status=status)
        self.content = json.dumps(
            {
                "error": {
                    "errors": [{"reason": reason}] if reason is not None else [],
                }
            }
        ).encode()


@pytest.mark.parametrize(
    ("status", "reason", "expected"),
    [
        (401, None, ConnectorAuthenticationError),
        (404, None, google_drive_docs._GoogleNotFoundError),
        (410, None, GoogleDriveCursorInvalidError),
        (429, None, GoogleDriveRateLimitError),
        (403, "dailyLimitExceeded", GoogleDriveRateLimitError),
        (403, "rateLimitExceeded", GoogleDriveRateLimitError),
        (403, "userRateLimitExceeded", GoogleDriveRateLimitError),
        (403, "insufficientPermissions", GoogleDriveError),
    ],
)
def test_google_error_classification(
    status: int,
    reason: str | None,
    expected: type[Exception],
) -> None:
    with pytest.raises(expected):
        google_drive_docs._raise_google_error(_GoogleHttpError(status, reason))


class _RevokeResponse:
    def __init__(self, status: int) -> None:
        self.status = status

    def __enter__(self) -> _RevokeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None


def test_google_token_revoke_posts_refresh_token_without_leaking_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    def urlopen(request: Any, *, timeout: int) -> _RevokeResponse:
        seen["request"] = request
        seen["timeout"] = timeout
        return _RevokeResponse(200)

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    secret = "secret-refresh-token"
    google_drive_docs._revoke_google_token(
        CredentialEnvelope(
            "oauth",
            {"refresh_token": secret, "token": "access-token"},
        )
    )

    request = seen["request"]
    assert request.full_url == "https://oauth2.googleapis.com/revoke"
    assert request.get_method() == "POST"
    assert seen["timeout"] == 10
    assert request.data == b"token=secret-refresh-token"
    assert secret not in repr(request.headers)


@pytest.mark.parametrize("failure", ["status", "http"])
def test_google_token_revoke_failures_are_bounded_and_hide_token(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    secret = "never-leak-this-token"

    def urlopen(request: Any, *, timeout: int) -> _RevokeResponse:
        if failure == "status":
            return _RevokeResponse(500)
        raise urllib.error.HTTPError(
            request.full_url,
            503,
            "unavailable",
            Message(),
            None,
        )

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    with pytest.raises(GoogleDriveError) as raised:
        google_drive_docs._revoke_google_token(
            CredentialEnvelope("oauth", {"refresh_token": secret})
        )
    assert secret not in str(raised.value)


async def test_auth_requests_only_drive_scope_without_incremental_grants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    class Flow:
        def authorization_url(self, **kwargs: Any) -> tuple[str, str]:
            seen.update(kwargs)
            return "https://accounts.google.test/auth", "state"

    monkeypatch.setattr(google_drive_docs, "_flow", lambda callback_url: Flow())
    result = await GoogleDriveDocsProvider().begin_auth(
        ConnectorOperationContext("scope:a", GOOGLE_DRIVE_DOCS_CONNECTOR_ID),
        "https://keel.test/callback",
    )
    assert result.state == "state"
    assert seen == {
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "false",
    }


async def test_resource_discovery_is_paginated_and_excludes_binary_formats() -> None:
    fake = FakeDriveClient(_load_fixture("initial.json"))
    provider = EnabledGoogleDriveProvider(lambda credential: fake)
    result = await provider.list_resources(_context(fake))

    assert [item.external_id for item in result.resources] == [
        "nested",
        "root",
        "txt-1",
        "outside",
        "doc-1",
        "md-1",
    ]
    assert "pdf-1" not in {item.external_id for item in result.resources}
    assert "office-1" not in {item.external_id for item in result.resources}
    assert fake.list_calls[:2] == [(None, None), ("files-page-2", None)]


async def test_initial_sync_crawls_nested_roots_with_taint_and_provenance() -> None:
    fake = FakeDriveClient(_load_fixture("initial.json"))
    provider = EnabledGoogleDriveProvider(lambda credential: fake)
    result = await provider.sync(_context(fake))

    assert [change.provenance.external_resource_id for change in result.changes] == [
        "doc-1",
        "md-1",
        "txt-1",
    ]
    assert all(change.kind is ConnectorChangeKind.upsert for change in result.changes)
    assert all(change.taint is ContentTaint.tainted for change in result.changes)
    roadmap = result.changes[0]
    assert roadmap.content == "Roadmap body\nSecond line"
    assert roadmap.provenance.source_url == "https://docs.google.com/document/d/doc-1/edit"
    revision = json.loads(str(roadmap.provenance.revision))
    assert revision == {
        "modified_time": "2026-07-10T10:00:00Z",
        "name": "Roadmap",
        "version": "7",
    }
    assert result.cursor_updates[0].resource_id == "resource-root"
    assert result.cursor_updates[0].value == "cursor-1"
    assert set(fake.read_calls) == {"doc-1", "md-1", "txt-1"}


async def test_initial_sync_keeps_a_cursor_for_each_selected_root() -> None:
    fake = FakeDriveClient(_load_fixture("initial.json"))
    provider = EnabledGoogleDriveProvider(lambda credential: fake)
    resources = (
        _resource(),
        _resource(external_id="outside", kind="file"),
    )
    result = await provider.sync(_context(fake, resources=resources))

    assert {change.provenance.external_resource_id for change in result.changes} == {
        "doc-1",
        "md-1",
        "outside",
        "txt-1",
    }
    assert {(cursor.resource_id, cursor.value) for cursor in result.cursor_updates} == {
        ("resource-root", "cursor-1"),
        ("resource-outside", "cursor-1"),
    }


async def test_changes_cursor_paginates_deduplicates_updates_moves_and_deletes() -> None:
    initial = _load_fixture("initial.json")
    fake = FakeDriveClient(initial)
    doc = fake.files["doc-1"]
    fake.files["doc-1"] = DriveFile(
        id=doc.id,
        name="Roadmap renamed",
        mime_type=doc.mime_type,
        parents=doc.parents,
        source_url=doc.source_url,
        modified_time="2026-07-18T01:00:00Z",
        version="8",
    )
    fake.content["doc-1"] = b"Updated roadmap"
    moved = DriveFile(
        id="moved-1",
        name="Moved.txt",
        mime_type=TEXT_MIME_TYPE,
        parents=("other-root",),
        source_url="https://drive.google.com/file/d/moved-1/view",
        modified_time="2026-07-18T01:01:00Z",
        version="4",
    )
    fake.files[moved.id] = moved
    fake.content[moved.id] = b"Moved away"
    incoming = DriveFile(
        id="incoming-1",
        name="Incoming.md",
        mime_type="text/markdown",
        parents=("root",),
        source_url="https://drive.google.com/file/d/incoming-1/view",
        modified_time="2026-07-18T01:02:00Z",
        version="1",
    )
    fake.files[incoming.id] = incoming
    fake.content[incoming.id] = b"# Moved in"
    fixture = _load_fixture("changes.json")
    pages = fixture["pages"]
    assert isinstance(pages, dict)
    for token, raw in pages.items():
        assert isinstance(raw, dict)
        changes = tuple(
            DriveChange(
                str(item["file_id"]),
                bool(item["removed"]),
            )
            for item in raw["changes"]
        )
        fake.change_pages[str(token)] = DriveChangePage(
            changes,
            str(raw["next_page_token"]) if raw.get("next_page_token") else None,
            str(raw["new_start_page_token"]) if raw.get("new_start_page_token") else None,
        )
    last_page = fake.change_pages["cursor-1-page-2"]
    fake.change_pages["cursor-1-page-2"] = DriveChangePage(
        last_page.changes + (DriveChange("incoming-1", False),),
        last_page.next_page_token,
        last_page.new_start_page_token,
    )
    deleted = DriveFile(
        "deleted-1",
        "Deleted.txt",
        TEXT_MIME_TYPE,
        ("root",),
        "https://drive.google.com/file/d/deleted-1/view",
        "2026-07-12T00:00:00Z",
        "1",
    )
    cursor = ConnectorCursor(
        id="cursor-row",
        scope_id="scope:a",
        connector_id=GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        binding_id="binding-drive",
        stream="drive_changes",
        value="cursor-1",
        resource_id="resource-root",
    )
    provider = EnabledGoogleDriveProvider(lambda credential: fake)
    result = await provider.sync(
        _context(
            fake,
            items=(_item(doc), _item(deleted), _item(moved)),
            cursors=(cursor,),
        )
    )

    by_id = {change.provenance.external_resource_id: change for change in result.changes}
    assert by_id["doc-1"].kind is ConnectorChangeKind.upsert
    assert by_id["doc-1"].title == "Roadmap renamed"
    assert by_id["deleted-1"].kind is ConnectorChangeKind.delete
    assert by_id["moved-1"].kind is ConnectorChangeKind.delete
    assert by_id["incoming-1"].kind is ConnectorChangeKind.upsert
    assert list(by_id).count("doc-1") == 1
    assert fake.change_calls == ["cursor-1", "cursor-1-page-2"]
    assert result.cursor_updates[0].value == "cursor-2"


async def test_invalid_cursor_runs_one_controlled_full_resync() -> None:
    fake = FakeDriveClient(_load_fixture("initial.json"))
    fake.invalid_tokens.add("expired")
    stale = DriveFile(
        "stale",
        "Stale.txt",
        TEXT_MIME_TYPE,
        ("root",),
        "https://drive.google.com/file/d/stale/view",
        "2026-07-01T00:00:00Z",
        "1",
    )
    cursor = ConnectorCursor(
        id="cursor-row",
        scope_id="scope:a",
        connector_id=GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        binding_id="binding-drive",
        stream="drive_changes",
        value="expired",
        resource_id="resource-root",
    )
    provider = EnabledGoogleDriveProvider(lambda credential: fake)
    result = await provider.sync(_context(fake, items=(_item(stale),), cursors=(cursor,)))

    assert fake.change_calls == ["expired"]
    assert result.cursor_updates[0].value == "cursor-1"
    assert any(
        change.kind is ConnectorChangeKind.delete
        and change.provenance.external_resource_id == "stale"
        for change in result.changes
    )


async def test_sync_reports_credential_refresh_and_rate_limit_health() -> None:
    fake = FakeDriveClient(_load_fixture("initial.json"))
    fake.rotate("rotated-access")
    provider = EnabledGoogleDriveProvider(lambda credential: fake)
    result = await provider.sync(_context(fake))
    assert result.state.credential is not None
    assert result.state.credential.expected_version == 1
    assert result.state.credential.credential.values["token"] == "rotated-access"

    fake.health_error = GoogleDriveRateLimitError("retry later")
    health = await provider.health(_context(fake))
    assert health.status is ConnectorHealthStatus.degraded
    assert health.retryable is True


async def test_service_requires_explicit_knowledge_target_before_provider_work() -> None:
    fake = FakeDriveClient(_load_fixture("initial.json"))
    provider = EnabledGoogleDriveProvider(lambda credential: fake)
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
    )
    await repository.upsert_resources(
        GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        binding.id,
        (ConnectorResourceDraft("root", "folder", "Root", selected=True),),
    )
    credentials = ConnectorCredentialStore(
        InMemoryTokenStore("scope:a", EnvelopeCipher("drive-test-key"))
    )
    await credentials.put(
        GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        CredentialEnvelope("oauth", {"refresh_token": "drive-refresh"}),
    )
    service = ConnectorService(
        ConnectorRegistry(
            (
                ConnectorRegistration(
                    google_drive_docs.manifest,
                    lambda: provider,
                    "tests.google_drive_docs",
                ),
            )
        ),
        repository,
        credentials=credentials,
    )

    with pytest.raises(RuntimeError, match="requires configured targets: knowledge"):
        await service.sync(GOOGLE_DRIVE_DOCS_CONNECTOR_ID, binding.id)
    assert fake.read_calls == []


async def test_scope_bound_credentials_are_isolated_from_other_scopes_and_gmail() -> None:
    store_a = ConnectorCredentialStore(InMemoryTokenStore("scope:a", EnvelopeCipher("scope-a-key")))
    store_b = ConnectorCredentialStore(InMemoryTokenStore("scope:b", EnvelopeCipher("scope-b-key")))
    await store_a.put(
        GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        CredentialEnvelope("oauth", {"refresh_token": "drive-only"}),
    )
    await store_a.put(
        "gmail",
        CredentialEnvelope("oauth", {"refresh_token": "gmail-only"}),
    )

    drive = await store_a.get(GOOGLE_DRIVE_DOCS_CONNECTOR_ID)
    gmail = await store_a.get("gmail")
    assert drive is not None and drive.values["refresh_token"] == "drive-only"
    assert gmail is not None and gmail.values["refresh_token"] == "gmail-only"
    assert await store_b.get(GOOGLE_DRIVE_DOCS_CONNECTOR_ID) is None
    with pytest.raises(ValueError, match="crosses its scope or provider"):
        ConnectorOperationContext(
            scope_id="scope:b",
            connector_id=GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
            binding=_binding("scope:a"),
            resources=(_resource(scope_id="scope:a"),),
        )


class FakeKnowledge:
    def __init__(self) -> None:
        self.documents: dict[str, str] = {}
        self.hidden: set[str] = set()
        self.purge_jobs: list[str] = []
        self._idempotency: dict[str, Any] = {}

    async def create_document(
        self,
        kb_id: str,
        command: CreateKnowledgeDocumentCommand,
        idempotency_key: str,
    ) -> Any:
        if idempotency_key in self._idempotency:
            return self._idempotency[idempotency_key]
        document_id = f"document-{len(self.documents) + 1}"
        self.documents[document_id] = command.content
        result = SimpleNamespace(document=SimpleNamespace(id=document_id))
        self._idempotency[idempotency_key] = result
        return result

    async def update_document(
        self,
        kb_id: str,
        document_id: str,
        command: UpdateKnowledgeDocumentCommand,
        idempotency_key: str,
    ) -> Any:
        if idempotency_key not in self._idempotency:
            self.documents[document_id] = command.content
            self._idempotency[idempotency_key] = object()
        return self._idempotency[idempotency_key]

    async def delete_document(
        self,
        kb_id: str,
        document_id: str,
        command: DeleteKnowledgeCommand,
        idempotency_key: str,
    ) -> Any:
        if idempotency_key not in self._idempotency:
            self.hidden.add(document_id)
            self.purge_jobs.append(document_id)
            self._idempotency[idempotency_key] = object()
        return self._idempotency[idempotency_key]


async def _knowledge_service(
    fake: FakeDriveClient,
    knowledge: FakeKnowledge,
    revoked: list[str],
) -> tuple[ConnectorService, InMemoryConnectorRepository, str]:
    provider = EnabledGoogleDriveProvider(
        lambda credential: fake,
        lambda credential: revoked.append(str(credential.values.get("refresh_token"))),
    )
    repository = InMemoryConnectorRepository("scope:a")
    binding = await repository.upsert_binding(
        GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
    )
    await repository.upsert_resources(
        GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        binding.id,
        (ConnectorResourceDraft("root", "folder", "Root", selected=True),),
    )
    await repository.replace_targets(
        GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        binding.id,
        {ConnectorTargetKind.knowledge: "kb-1"},
    )
    credentials = ConnectorCredentialStore(
        InMemoryTokenStore("scope:a", EnvelopeCipher("drive-test-key"))
    )
    await credentials.put(
        GOOGLE_DRIVE_DOCS_CONNECTOR_ID,
        CredentialEnvelope("oauth", {"refresh_token": "drive-refresh"}),
    )
    sink = DurableConnectorChangeSink(repository, knowledge=knowledge)
    service = ConnectorService(
        ConnectorRegistry(
            (
                ConnectorRegistration(
                    google_drive_docs.manifest,
                    lambda: provider,
                    "tests.google_drive_docs",
                ),
            )
        ),
        repository,
        credentials=credentials,
        change_sink=sink,
        purge_sink=sink,
    )
    return service, repository, binding.id


async def test_external_delete_hides_and_queues_durable_knowledge_purge() -> None:
    fake = FakeDriveClient(_load_fixture("initial.json"))
    knowledge = FakeKnowledge()
    service, repository, binding_id = await _knowledge_service(fake, knowledge, [])
    assert await service.sync(GOOGLE_DRIVE_DOCS_CONNECTOR_ID, binding_id) == 3
    mapped = {
        item.external_id: item
        for item in await repository.list_items(GOOGLE_DRIVE_DOCS_CONNECTOR_ID)
    }
    document_id = str(mapped["doc-1"].destination_id)

    fake.files.pop("doc-1")
    fake.change_pages["cursor-1"] = DriveChangePage(
        (DriveChange("doc-1", True),),
        new_start_page_token="cursor-2",
    )
    assert await service.sync(GOOGLE_DRIVE_DOCS_CONNECTOR_ID, binding_id) == 1
    assert document_id in knowledge.hidden
    assert document_id in knowledge.purge_jobs
    assert "doc-1" not in {
        item.external_id for item in await repository.list_items(GOOGLE_DRIVE_DOCS_CONNECTOR_ID)
    }


@pytest.mark.parametrize("purge", [False, True])
async def test_revoke_retain_or_purge_and_never_resurrects(purge: bool) -> None:
    fake = FakeDriveClient(_load_fixture("initial.json"))
    knowledge = FakeKnowledge()
    revoked: list[str] = []
    service, repository, binding_id = await _knowledge_service(fake, knowledge, revoked)
    assert await service.sync(GOOGLE_DRIVE_DOCS_CONNECTOR_ID, binding_id) == 3
    imported_ids = {
        str(item.destination_id)
        for item in await repository.list_items(GOOGLE_DRIVE_DOCS_CONNECTOR_ID)
    }

    assert await service.revoke(GOOGLE_DRIVE_DOCS_CONNECTOR_ID, purge=purge) is True
    assert revoked == ["drive-refresh"]
    if purge:
        assert knowledge.hidden == imported_ids
        assert set(knowledge.purge_jobs) == imported_ids
    else:
        assert knowledge.hidden == set()
        assert set(knowledge.documents) == imported_ids
    with pytest.raises(LookupError, match="not configured"):
        await service.sync(GOOGLE_DRIVE_DOCS_CONNECTOR_ID, binding_id)
