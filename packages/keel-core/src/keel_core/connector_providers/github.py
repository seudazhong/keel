"""GitHub App collaboration connector.

The connector stays deliberately repository-local: GitHub App installation metadata and
selected repositories are durable, while installation access tokens are minted just in time
and discarded after each operation.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlsplit

import httpx
import jwt

from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAction,
    ConnectorActionApproval,
    ConnectorActionContext,
    ConnectorActionIdempotency,
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
    ConnectorEvent,
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorIngressRequest,
    ConnectorIngressResponse,
    ConnectorIngressResult,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorProvenance,
    ConnectorResource,
    ConnectorResourceDraft,
    ConnectorResourceResult,
    ConnectorSetupArtifact,
    ConnectorSetupArtifactKind,
    ConnectorSetupField,
    ConnectorSetupResult,
)
from keel_core.connector_credentials import CredentialEnvelope
from keel_core.connectors import ActionFn
from keel_core.protocols import ToolContext

GITHUB_CONNECTOR_ID = "github"
_CREDENTIAL_KIND = "github_app"
_DEFAULT_API_BASE = "https://api.github.com"
_DEFAULT_WEB_BASE = "https://github.com"
_API_VERSION = "2022-11-28"
_MAX_PAGES = 10
_COMMENT_RECONCILIATION_PAGES = 3
_COMMENT_RECONCILIATION_WINDOW = timedelta(days=7)
_IDEMPOTENCY_PREFIX = "<!-- keel-idempotency:"
_REF_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PERMISSION_LEVELS = {"none": 0, "read": 1, "write": 2, "admin": 3}


class GitHubConnectorError(RuntimeError):
    """A sanitized GitHub connector failure."""


class GitHubRateLimitError(GitHubConnectorError):
    """GitHub rejected a request because its rate limit was exhausted."""


class GitHubPermissionError(GitHubConnectorError):
    """The installation or selected repository does not grant an operation."""


class GitHubInstallationError(ConnectorAuthenticationError):
    """The GitHub App installation is absent, revoked, or unusable."""


class RawCredentialStore(Protocol):
    async def get(self, connector_id: str) -> str | None: ...


SecretResolver = Callable[[str], str]
Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class GitHubAppConfig:
    app_id: str
    client_id: str
    app_slug: str
    private_key_secret_ref: str
    webhook_secret_ref: str
    api_base_url: str = _DEFAULT_API_BASE
    web_base_url: str = _DEFAULT_WEB_BASE

    @classmethod
    def from_values(cls, values: Mapping[str, Any]) -> GitHubAppConfig:
        def required(name: str) -> str:
            value = values.get(name)
            if not isinstance(value, str) or not value.strip():
                raise GitHubInstallationError(f"GitHub App credential field {name!r} is missing")
            return value.strip()

        app_id = required("app_id")
        if not app_id.isdigit():
            raise ValueError("GitHub App id must be numeric")
        app_slug = required("app_slug")
        if any(not (char.isalnum() or char == "-") for char in app_slug):
            raise ValueError("GitHub App slug contains unsupported characters")
        api_base = _validated_base_url(
            str(values.get("api_base_url") or _DEFAULT_API_BASE),
            field="GitHub API base URL",
        )
        web_base = _validated_base_url(
            str(values.get("web_base_url") or _DEFAULT_WEB_BASE),
            field="GitHub web base URL",
        )
        return cls(
            app_id=app_id,
            client_id=required("client_id"),
            app_slug=app_slug,
            private_key_secret_ref=required("private_key_secret_ref"),
            webhook_secret_ref=required("webhook_secret_ref"),
            api_base_url=api_base,
            web_base_url=web_base,
        )

    def credential_values(self) -> dict[str, str]:
        return {
            "app_id": self.app_id,
            "client_id": self.client_id,
            "app_slug": self.app_slug,
            "private_key_secret_ref": self.private_key_secret_ref,
            "webhook_secret_ref": self.webhook_secret_ref,
            "api_base_url": self.api_base_url,
            "web_base_url": self.web_base_url,
        }


@dataclass(frozen=True, slots=True)
class GitHubInstallationToken:
    value: str
    expires_at: datetime
    permissions: dict[str, str]


@dataclass(frozen=True, slots=True)
class GitHubPage:
    items: list[dict[str, Any]]
    next_url: str | None


def _validated_base_url(value: str, *, field: str) -> str:
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{field} must be an HTTPS origin or base path")
    return value.strip().rstrip("/")


def resolve_secret_reference(reference: str) -> str:
    """Resolve an operator-controlled environment or absolute-file secret reference."""

    kind, separator, value = reference.partition(":")
    if not separator or not value:
        raise GitHubInstallationError("secret references must use env:NAME or file:ABSOLUTE_PATH")
    if kind == "env":
        if _REF_PATTERN.fullmatch(value) is None:
            raise GitHubInstallationError("GitHub secret environment reference is invalid")
        secret = os.environ.get(value)
        if secret is None or not secret:
            raise GitHubInstallationError(f"GitHub secret environment reference {value!r} is unset")
        return secret.replace("\\n", "\n") if "-----BEGIN" in secret else secret
    if kind == "file":
        path = Path(value)
        if not path.is_absolute():
            raise GitHubInstallationError("GitHub secret file reference must be absolute")
        try:
            secret = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise GitHubInstallationError("GitHub secret file reference is unreadable") from exc
        if not secret:
            raise GitHubInstallationError("GitHub secret file reference is empty")
        return secret
    raise GitHubInstallationError("unsupported GitHub secret reference kind")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _parse_datetime(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise GitHubInstallationError(f"GitHub response omitted {field}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise GitHubInstallationError(f"GitHub returned an invalid {field}") from exc
    if parsed.tzinfo is None:
        raise GitHubInstallationError(f"GitHub returned a timezone-free {field}")
    return parsed.astimezone(UTC)


def _object(value: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GitHubConnectorError(f"GitHub returned an invalid {field}")
    return cast(dict[str, Any], value)


def _objects(value: Any, *, field: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise GitHubConnectorError(f"GitHub returned invalid {field}")
    return cast(list[dict[str, Any]], value)


def _text(value: Any, default: str = "") -> str:
    return value if isinstance(value, str) else default


def _integer(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise GitHubConnectorError(f"GitHub returned an invalid {field}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise GitHubConnectorError(f"GitHub returned an invalid {field}") from exc


def _response_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return "request failed"
    if isinstance(payload, dict) and isinstance(payload.get("message"), str):
        return str(payload["message"])[:300]
    return "request failed"


def _raise_for_response(response: httpx.Response, *, installation_request: bool = False) -> None:
    if response.is_success:
        return
    message = _response_message(response)
    remaining = response.headers.get("x-ratelimit-remaining")
    if response.status_code == 429 or (
        response.status_code == 403 and (remaining == "0" or "rate limit" in message.lower())
    ):
        retry = response.headers.get("retry-after")
        reset = response.headers.get("x-ratelimit-reset")
        detail = f" retry_after={retry}" if retry else (f" reset={reset}" if reset else "")
        raise GitHubRateLimitError(f"GitHub rate limit exceeded.{detail}")
    if installation_request and response.status_code in {401, 404}:
        raise GitHubInstallationError("GitHub App installation is revoked, uninstalled, or missing")
    if response.status_code == 401:
        raise GitHubInstallationError("GitHub rejected the App or installation credentials")
    if response.status_code in {403, 404}:
        raise GitHubPermissionError(
            f"GitHub permission or selected-repository access denied: {message}"
        )
    if response.status_code == 422:
        raise GitHubPermissionError(f"GitHub rejected the repository-scoped operation: {message}")
    raise GitHubConnectorError(f"GitHub API request failed ({response.status_code}): {message}")


def _permissions(value: Any) -> dict[str, str]:
    raw = _object(value, field="installation permissions")
    result: dict[str, str] = {}
    for name, level in raw.items():
        if isinstance(name, str) and isinstance(level, str):
            result[name] = level
    return result


def _require_permission(permissions: Mapping[str, str], name: str, required: str) -> None:
    actual = permissions.get(name, "none")
    if _PERMISSION_LEVELS.get(actual, 0) < _PERMISSION_LEVELS[required]:
        raise GitHubPermissionError(
            f"GitHub App installation permission {name!r} requires {required}; current={actual}"
        )


class GitHubAPI:
    def __init__(
        self,
        config: GitHubAppConfig,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        secret_resolver: SecretResolver = resolve_secret_reference,
        now: Clock = _utc_now,
    ) -> None:
        self.config = config
        self._transport = transport
        self._resolve_secret = secret_resolver
        self._now = now

    def _api_url(self, path: str) -> str:
        return f"{self.config.api_base_url}/{path.lstrip('/')}"

    def _app_jwt(self) -> str:
        now = self._now().astimezone(UTC)
        private_key = self._resolve_secret(self.config.private_key_secret_ref)
        encoded = jwt.encode(
            {
                "iat": int((now - timedelta(seconds=60)).timestamp()),
                "exp": int((now + timedelta(minutes=9)).timestamp()),
                "iss": self.config.app_id,
            },
            private_key,
            algorithm="RS256",
        )
        return str(encoded)

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=self._transport,
            timeout=httpx.Timeout(20.0),
            follow_redirects=False,
            headers={
                "accept": "application/vnd.github+json",
                "x-github-api-version": _API_VERSION,
                "user-agent": "keel-github-connector",
            },
        )

    async def app_request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        installation_request: bool = False,
    ) -> httpx.Response:
        async with self._client() as client:
            response = await client.request(
                method,
                self._api_url(path),
                headers={"authorization": f"Bearer {self._app_jwt()}"},
                json=json_body,
            )
        _raise_for_response(response, installation_request=installation_request)
        return response

    async def installation(self, installation_id: str) -> dict[str, Any]:
        response = await self.app_request(
            "GET",
            f"/app/installations/{quote(installation_id, safe='')}",
            installation_request=True,
        )
        return _object(response.json(), field="installation")

    async def mint_installation_token(
        self,
        installation_id: str,
        *,
        repository_ids: tuple[int, ...] = (),
        required_permissions: Mapping[str, str] | None = None,
    ) -> GitHubInstallationToken:
        body: dict[str, Any] = {}
        if repository_ids:
            body["repository_ids"] = list(repository_ids)
        if required_permissions:
            body["permissions"] = dict(required_permissions)
        response = await self.app_request(
            "POST",
            f"/app/installations/{quote(installation_id, safe='')}/access_tokens",
            json_body=body,
            installation_request=True,
        )
        payload = _object(response.json(), field="installation token")
        token = _text(payload.get("token"))
        if not token:
            raise GitHubInstallationError("GitHub returned an empty installation token")
        expires_at = _parse_datetime(payload.get("expires_at"), field="token expiry")
        if expires_at <= self._now().astimezone(UTC):
            raise GitHubInstallationError("GitHub returned an expired installation token")
        permissions = _permissions(payload.get("permissions", {}))
        for name, level in (required_permissions or {}).items():
            _require_permission(permissions, name, level)
        return GitHubInstallationToken(token, expires_at, permissions)

    async def installation_request(
        self,
        token: GitHubInstallationToken,
        method: str,
        path_or_url: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: Mapping[str, str | int] | None = None,
    ) -> httpx.Response:
        if token.expires_at <= self._now().astimezone(UTC):
            raise GitHubInstallationError("GitHub installation token expired before use")
        url = path_or_url if path_or_url.startswith("https://") else self._api_url(path_or_url)
        self._validate_page_url(url)
        async with self._client() as client:
            response = await client.request(
                method,
                url,
                headers={"authorization": f"Bearer {token.value}"},
                json=json_body,
                params=params,
            )
        _raise_for_response(response)
        return response

    async def paginate(
        self,
        token: GitHubInstallationToken,
        path: str,
        *,
        item_key: str | None = None,
        params: Mapping[str, str | int] | None = None,
        max_pages: int = _MAX_PAGES,
    ) -> GitHubPage:
        if max_pages < 1 or max_pages > _MAX_PAGES:
            raise ValueError(f"max_pages must be between 1 and {_MAX_PAGES}")
        items: list[dict[str, Any]] = []
        next_url: str | None = path
        request_params = params
        for _ in range(max_pages):
            if next_url is None:
                break
            response = await self.installation_request(
                token,
                "GET",
                next_url,
                params=request_params,
            )
            request_params = None
            payload = response.json()
            raw_items = (
                _object(payload, field="paginated response").get(item_key, [])
                if item_key is not None
                else payload
            )
            items.extend(_objects(raw_items, field="paginated items"))
            next_url = self._next_link(response.headers.get("link"))
        return GitHubPage(items, next_url)

    async def find_recent_comment_marker(
        self,
        token: GitHubInstallationToken,
        path: str,
        marker: str,
    ) -> dict[str, Any] | None:
        since = (
            (self._now().astimezone(UTC) - _COMMENT_RECONCILIATION_WINDOW)
            .isoformat()
            .replace("+00:00", "Z")
        )
        params: dict[str, str | int] = {"per_page": 100, "page": 1, "since": since}
        first = await self.installation_request(token, "GET", path, params=params)
        first_items = _objects(first.json(), field="recent issue comments")
        last_url = self._link(first.headers.get("link"), "last")
        if last_url is None:
            return _find_marker(first_items, marker)
        last_page = self._page_number(last_url)
        first_match = _find_marker(first_items, marker)
        if first_match is not None:
            return first_match
        oldest_page = max(2, last_page - _COMMENT_RECONCILIATION_PAGES + 1)
        for page_number in range(last_page, oldest_page - 1, -1):
            response = await self.installation_request(
                token,
                "GET",
                path,
                params={"per_page": 100, "page": page_number, "since": since},
            )
            match = _find_marker(
                _objects(response.json(), field="recent issue comments"),
                marker,
            )
            if match is not None:
                return match
        return None

    def _next_link(self, header: str | None) -> str | None:
        return self._link(header, "next")

    def _link(self, header: str | None, relation: str) -> str | None:
        if not header:
            return None
        for part in header.split(","):
            section = part.strip()
            if f'rel="{relation}"' not in section:
                continue
            if not section.startswith("<") or ">" not in section:
                raise GitHubConnectorError("GitHub returned an invalid pagination link")
            target = section[1 : section.index(">")]
            resolved = urljoin(f"{self.config.api_base_url}/", target)
            self._validate_page_url(resolved)
            return resolved
        return None

    @staticmethod
    def _page_number(url: str) -> int:
        values = parse_qs(urlsplit(url).query).get("page")
        if values is None or len(values) != 1:
            raise GitHubConnectorError("GitHub pagination link omitted a page number")
        try:
            page = int(values[0])
        except ValueError as exc:
            raise GitHubConnectorError("GitHub pagination link has an invalid page number") from exc
        if page < 1:
            raise GitHubConnectorError("GitHub pagination link has an invalid page number")
        return page

    def _validate_page_url(self, value: str) -> None:
        expected = urlsplit(self.config.api_base_url)
        actual = urlsplit(value)
        if (
            actual.scheme != expected.scheme
            or actual.hostname != expected.hostname
            or actual.port != expected.port
            or actual.username is not None
            or actual.password is not None
        ):
            raise GitHubConnectorError("GitHub pagination attempted to leave the API origin")


def _manifest_action(
    name: str,
    description: str,
    properties: dict[str, Any],
    required: tuple[str, ...],
    *,
    outbound: bool = False,
) -> ConnectorActionManifest:
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
        "required": list(required),
    }
    return ConnectorActionManifest(
        name=name,
        description=description,
        input_schema=schema,
        semantics=(
            ConnectorActionSemantics.outbound if outbound else ConnectorActionSemantics.read
        ),
        idempotency=(
            ConnectorActionIdempotency.required if outbound else ConnectorActionIdempotency.none
        ),
        approval=(ConnectorActionApproval.tainted if outbound else ConnectorActionApproval.none),
    )


_REPOSITORY_PROPERTY = {
    "repository_id": {
        "type": "string",
        "description": "Selected GitHub repository id returned by resource discovery.",
    }
}
_NUMBER_PROPERTY = {"number": {"type": "integer", "minimum": 1}}
_PAGINATION_PROPERTIES = {
    "per_page": {"type": "integer", "minimum": 1, "maximum": 100},
    "max_pages": {"type": "integer", "minimum": 1, "maximum": _MAX_PAGES},
}

REPOSITORY_GET = _manifest_action(
    "github_repository_get",
    "Read metadata for a selected GitHub repository.",
    dict(_REPOSITORY_PROPERTY),
    ("repository_id",),
)
ISSUE_GET = _manifest_action(
    "github_issue_get",
    "Read a GitHub Issue from a selected repository.",
    {**_REPOSITORY_PROPERTY, **_NUMBER_PROPERTY},
    ("repository_id", "number"),
)
PULL_REQUEST_GET = _manifest_action(
    "github_pull_request_get",
    "Read a GitHub pull request from a selected repository.",
    {**_REPOSITORY_PROPERTY, **_NUMBER_PROPERTY},
    ("repository_id", "number"),
)
COMMENTS_LIST = _manifest_action(
    "github_comments_list",
    "List Issue or pull-request conversation comments from a selected repository.",
    {**_REPOSITORY_PROPERTY, **_NUMBER_PROPERTY, **_PAGINATION_PROPERTIES},
    ("repository_id", "number"),
)
COMMIT_STATUS_GET = _manifest_action(
    "github_commit_status_get",
    "Read combined commit status for a ref in a selected repository.",
    {
        **_REPOSITORY_PROPERTY,
        "ref": {"type": "string", "minLength": 1},
        "per_page": _PAGINATION_PROPERTIES["per_page"],
        "page": {"type": "integer", "minimum": 1},
    },
    ("repository_id", "ref"),
)
ISSUE_CREATE = _manifest_action(
    "github_issue_create",
    "Create an Issue in a selected GitHub repository after approval.",
    {
        **_REPOSITORY_PROPERTY,
        "title": {"type": "string", "minLength": 1, "maxLength": 256},
        "body": {"type": "string"},
        "idempotency_key": {"type": "string", "minLength": 1},
    },
    ("repository_id", "title", "idempotency_key"),
    outbound=True,
)
COMMENT_CREATE = _manifest_action(
    "github_comment_create",
    "Create an Issue or pull-request conversation comment after approval.",
    {
        **_REPOSITORY_PROPERTY,
        **_NUMBER_PROPERTY,
        "body": {"type": "string", "minLength": 1},
        "idempotency_key": {"type": "string", "minLength": 1},
    },
    ("repository_id", "number", "body", "idempotency_key"),
    outbound=True,
)

manifest = ConnectorManifest(
    id=GITHUB_CONNECTOR_ID,
    name="GitHub",
    description="Read selected repositories and create approved Issues or comments.",
    icon="🐙",
    auth_kind=ConnectorAuthKind.github_app,
    capabilities=(
        ConnectorCapability.read,
        ConnectorCapability.write,
        ConnectorCapability.webhook,
        ConnectorCapability.resources,
    ),
    scopes=(
        "metadata:read",
        "issues:write",
        "pull_requests:read",
        "statuses:read",
    ),
    setup_fields=(
        ConnectorSetupField("app_id", "GitHub App ID"),
        ConnectorSetupField("client_id", "GitHub App client ID"),
        ConnectorSetupField("app_slug", "GitHub App slug"),
        ConnectorSetupField(
            "private_key_secret_ref",
            "Private-key secret reference",
            help_text="Use env:NAME or file:ABSOLUTE_PATH; the private key is never persisted.",
        ),
        ConnectorSetupField(
            "webhook_secret_ref",
            "Webhook-secret reference",
            help_text="Use env:NAME or file:ABSOLUTE_PATH.",
        ),
        ConnectorSetupField(
            "api_base_url",
            "GitHub API base URL",
            required=False,
            input_type="url",
        ),
        ConnectorSetupField(
            "web_base_url",
            "GitHub web base URL",
            required=False,
            input_type="url",
        ),
    ),
    auth_action=ConnectorAuthAction(
        label="Install GitHub App",
        callback_parameters=(
            ConnectorCallbackParameter("installation_id"),
            ConnectorCallbackParameter("setup_action", required=False),
        ),
        requires_setup=True,
        help_text="Save App configuration, then install it on selected GitHub repositories.",
    ),
    setup_action_label="Save GitHub App configuration",
    resource_label="Repositories",
    actions=(
        REPOSITORY_GET,
        ISSUE_GET,
        PULL_REQUEST_GET,
        COMMENTS_LIST,
        COMMIT_STATUS_GET,
        ISSUE_CREATE,
        COMMENT_CREATE,
    ),
)


def _config_from_context(context: ConnectorOperationContext) -> GitHubAppConfig:
    credential = context.credential
    if credential is None or credential.kind != _CREDENTIAL_KIND:
        raise GitHubInstallationError("GitHub App configuration is missing")
    return GitHubAppConfig.from_values(credential.values)


async def _config_from_store(store: RawCredentialStore) -> GitHubAppConfig:
    encoded = await store.get(GITHUB_CONNECTOR_ID)
    if encoded is None:
        raise GitHubInstallationError("GitHub App configuration is missing")
    try:
        envelope = CredentialEnvelope.parse(encoded)
    except ValueError as exc:
        raise GitHubInstallationError("GitHub App credential envelope is invalid") from exc
    if envelope is None or envelope.kind != _CREDENTIAL_KIND:
        raise GitHubInstallationError("GitHub App credential envelope is invalid")
    return GitHubAppConfig.from_values(envelope.values)


def _installation_id(binding: ConnectorBinding | None) -> str:
    value = None if binding is None else binding.external_account_id
    if not value or not value.isdigit():
        raise GitHubInstallationError("GitHub App installation metadata is missing")
    return value


def _installation_metadata(installation: Mapping[str, Any]) -> dict[str, Any]:
    account = _object(installation.get("account"), field="installation account")
    return {
        "account_login": _text(account.get("login")),
        "account_type": _text(account.get("type")),
        "repository_selection": _text(installation.get("repository_selection")),
        "permissions": _permissions(installation.get("permissions", {})),
        "suspended": installation.get("suspended_at") is not None,
    }


def _resource_coordinates(resource: ConnectorResource) -> tuple[str, str]:
    owner = resource.config.get("owner")
    name = resource.config.get("name")
    if not isinstance(owner, str) or not owner or not isinstance(name, str) or not name:
        raise GitHubPermissionError("selected GitHub repository metadata is incomplete")
    return owner, name


def _repo_id(resource: ConnectorResource) -> int:
    return _integer(resource.external_id, field="selected repository id")


def _positive_int(args: Mapping[str, Any], name: str) -> int:
    value = args.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"{name} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def _bounded_int(
    args: Mapping[str, Any],
    name: str,
    *,
    default: int,
    maximum: int,
) -> int:
    value = args.get(name, default)
    parsed = _positive_int({name: value}, name)
    if parsed > maximum:
        raise ValueError(f"{name} must be at most {maximum}")
    return parsed


def _required_text(args: Mapping[str, Any], name: str, *, max_length: int | None = None) -> str:
    value = args.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must not be blank")
    result = value.strip()
    if max_length is not None and len(result) > max_length:
        raise ValueError(f"{name} exceeds {max_length} characters")
    return result


def _provenance(
    binding: ConnectorBinding,
    external_id: str,
    source_url: str | None,
    revision: str | None,
) -> dict[str, Any]:
    return {
        "connector_id": GITHUB_CONNECTOR_ID,
        "binding_id": binding.id,
        "external_resource_id": external_id,
        "source_url": source_url,
        "revision": revision,
    }


def _render_result(
    binding: ConnectorBinding,
    resource: ConnectorResource,
    kind: str,
    payload: dict[str, Any],
    *,
    external_suffix: str = "",
    next_url: str | None = None,
) -> str:
    source_url = _text(payload.get("html_url")) or resource.url
    revision = _text(payload.get("updated_at")) or _text(payload.get("sha")) or None
    result: dict[str, Any] = {
        "kind": kind,
        "repository": resource.display_name,
        "data": payload,
        "provenance": _provenance(
            binding,
            f"{resource.external_id}{external_suffix}",
            source_url,
            revision,
        ),
    }
    if next_url is not None:
        result["next_page"] = next_url
    return json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _idempotency_marker(key: str, *, namespace: str) -> str:
    digest = hashlib.sha256(f"{namespace}\0{key}".encode()).hexdigest()
    return f"{_IDEMPOTENCY_PREFIX}{digest} -->"


def _append_marker(body: str, marker: str) -> str:
    return f"{body.rstrip()}\n\n{marker}" if body.strip() else marker


def _find_marker(items: list[dict[str, Any]], marker: str) -> dict[str, Any] | None:
    for item in items:
        body = item.get("body")
        if isinstance(body, str) and marker in body:
            return item
    return None


def _sanitize_repository(payload: Mapping[str, Any]) -> dict[str, Any]:
    owner = _object(payload.get("owner"), field="repository owner")
    return {
        "id": _integer(payload.get("id"), field="repository id"),
        "node_id": _text(payload.get("node_id")),
        "name": _text(payload.get("name")),
        "full_name": _text(payload.get("full_name")),
        "private": bool(payload.get("private")),
        "html_url": _text(payload.get("html_url")),
        "description": payload.get("description"),
        "default_branch": _text(payload.get("default_branch")),
        "archived": bool(payload.get("archived")),
        "disabled": bool(payload.get("disabled")),
        "visibility": _text(payload.get("visibility")),
        "owner": {
            "id": owner.get("id"),
            "login": _text(owner.get("login")),
            "type": _text(owner.get("type")),
        },
        "permissions": payload.get("permissions", {}),
        "updated_at": _text(payload.get("updated_at")),
    }


def _sanitize_issue(payload: Mapping[str, Any]) -> dict[str, Any]:
    user = payload.get("user")
    user_object = user if isinstance(user, dict) else {}
    return {
        "id": payload.get("id"),
        "node_id": payload.get("node_id"),
        "number": payload.get("number"),
        "title": payload.get("title"),
        "body": payload.get("body"),
        "state": payload.get("state"),
        "state_reason": payload.get("state_reason"),
        "locked": payload.get("locked"),
        "comments": payload.get("comments"),
        "html_url": payload.get("html_url"),
        "created_at": payload.get("created_at"),
        "updated_at": payload.get("updated_at"),
        "closed_at": payload.get("closed_at"),
        "author": {
            "id": user_object.get("id"),
            "login": user_object.get("login"),
            "type": user_object.get("type"),
        },
        "labels": payload.get("labels", []),
        "assignees": payload.get("assignees", []),
    }


def _sanitize_pull_request(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = _sanitize_issue(payload)
    result.update(
        {
            "draft": payload.get("draft"),
            "merged": payload.get("merged"),
            "mergeable": payload.get("mergeable"),
            "mergeable_state": payload.get("mergeable_state"),
            "head": payload.get("head"),
            "base": payload.get("base"),
            "commits": payload.get("commits"),
            "additions": payload.get("additions"),
            "deletions": payload.get("deletions"),
            "changed_files": payload.get("changed_files"),
        }
    )
    return result


def _sanitize_comment(payload: Mapping[str, Any]) -> dict[str, Any]:
    user = payload.get("user")
    user_object = user if isinstance(user, dict) else {}
    return {
        "id": payload.get("id"),
        "node_id": payload.get("node_id"),
        "body": payload.get("body"),
        "html_url": payload.get("html_url"),
        "issue_url": payload.get("issue_url"),
        "created_at": payload.get("created_at"),
        "updated_at": payload.get("updated_at"),
        "author_association": payload.get("author_association"),
        "author": {
            "id": user_object.get("id"),
            "login": user_object.get("login"),
            "type": user_object.get("type"),
        },
    }


class GitHubProvider(BaseConnectorProvider):
    manifest = manifest

    def __init__(
        self,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        secret_resolver: SecretResolver = resolve_secret_reference,
        now: Clock = _utc_now,
    ) -> None:
        self._transport = transport
        self._secret_resolver = secret_resolver
        self._now = now

    def _api(self, config: GitHubAppConfig) -> GitHubAPI:
        return GitHubAPI(
            config,
            transport=self._transport,
            secret_resolver=self._secret_resolver,
            now=self._now,
        )

    async def setup(
        self,
        context: ConnectorOperationContext,
        values: dict[str, str],
    ) -> ConnectorSetupResult:
        config = GitHubAppConfig.from_values(values)
        self._secret_resolver(config.private_key_secret_ref)
        self._secret_resolver(config.webhook_secret_ref)
        artifacts: list[ConnectorSetupArtifact] = []
        if context.callback_base_url:
            artifacts.extend(
                (
                    ConnectorSetupArtifact(
                        ConnectorSetupArtifactKind.url,
                        "GitHub App setup URL",
                        f"{context.callback_base_url}/callback",
                    ),
                    ConnectorSetupArtifact(
                        ConnectorSetupArtifactKind.url,
                        "GitHub App webhook URL",
                        f"{context.callback_base_url}/webhook",
                    ),
                )
            )
        return ConnectorSetupResult(
            ConnectorBindingDraft(display_name=f"GitHub App: {config.app_slug}"),
            CredentialEnvelope(_CREDENTIAL_KIND, config.credential_values()),
            tuple(artifacts),
            status=ConnectorBindingStatus.configured,
        )

    async def begin_auth(
        self,
        context: ConnectorOperationContext,
        callback_url: str,
    ) -> ConnectorAuthStart:
        config = _config_from_context(context)
        state = secrets.token_urlsafe(32)
        install_url = (
            f"{config.web_base_url}/apps/{quote(config.app_slug, safe='-')}/installations/new?"
            f"{urlencode({'state': state})}"
        )
        return ConnectorAuthStart(install_url, state)

    async def complete_auth(
        self,
        context: ConnectorOperationContext,
        callback_url: str,
        parameters: dict[str, str],
    ) -> ConnectorSetupResult:
        config = _config_from_context(context)
        installation_id = parameters.get("installation_id", "").strip()
        if not installation_id.isdigit():
            raise ValueError("GitHub callback omitted a numeric installation_id")
        installation = await self._api(config).installation(installation_id)
        metadata = _installation_metadata(installation)
        if metadata["suspended"]:
            raise GitHubInstallationError("GitHub App installation is suspended")
        account = _object(installation.get("account"), field="installation account")
        account_login = _text(account.get("login")) or installation_id
        account_id = str(_integer(account.get("id"), field="installation account id"))
        return ConnectorSetupResult(
            ConnectorBindingDraft(
                display_name=f"GitHub: {account_login}",
                external_account_id=installation_id,
                external_tenant_id=account_id,
                metadata=metadata,
            ),
            CredentialEnvelope(_CREDENTIAL_KIND, config.credential_values()),
        )

    async def list_resources(
        self,
        context: ConnectorOperationContext,
    ) -> ConnectorResourceResult:
        config = _config_from_context(context)
        installation_id = _installation_id(context.binding)
        api = self._api(config)
        token = await api.mint_installation_token(installation_id)
        page = await api.paginate(
            token,
            "/installation/repositories",
            item_key="repositories",
            params={"per_page": 100},
        )
        resources: list[ConnectorResourceDraft] = []
        for raw in page.items:
            repository = _sanitize_repository(raw)
            repo_id = str(repository["id"])
            full_name = _text(repository.get("full_name"))
            owner_data = _object(repository.get("owner"), field="repository owner")
            owner = _text(owner_data.get("login"))
            name = _text(repository.get("name"))
            if not full_name or not owner or not name:
                raise GitHubConnectorError("GitHub repository metadata is incomplete")
            resources.append(
                ConnectorResourceDraft(
                    external_id=repo_id,
                    kind="repository",
                    display_name=full_name,
                    url=_text(repository.get("html_url")) or None,
                    config={
                        "owner": owner,
                        "name": name,
                        "private": bool(repository.get("private")),
                        "default_branch": _text(repository.get("default_branch")),
                        "archived": bool(repository.get("archived")),
                        "visibility": _text(repository.get("visibility")),
                    },
                )
            )
        return ConnectorResourceResult(tuple(resources))

    async def health(self, context: ConnectorOperationContext) -> ConnectorHealth:
        checked_at = self._now().astimezone(UTC)
        if context.binding is None or context.credential is None:
            return ConnectorHealth(
                ConnectorHealthStatus.unconfigured,
                checked_at,
                "GitHub App setup or installation is incomplete.",
            )
        try:
            config = _config_from_context(context)
            installation_id = _installation_id(context.binding)
            api = self._api(config)
            installation = await api.installation(installation_id)
            metadata = _installation_metadata(installation)
            if metadata["suspended"]:
                raise GitHubInstallationError("GitHub App installation is suspended")
            selected = tuple(item for item in context.resources if item.selected)
            repository_ids = tuple(_repo_id(item) for item in selected)
            if len(repository_ids) > 500:
                raise GitHubPermissionError(
                    "GitHub health validation supports at most 500 selected repositories"
                )
            if repository_ids:
                token = await api.mint_installation_token(
                    installation_id,
                    repository_ids=repository_ids,
                    required_permissions={
                        "issues": "write",
                        "pull_requests": "read",
                        "statuses": "read",
                    },
                )
                page = await api.paginate(
                    token,
                    "/installation/repositories",
                    item_key="repositories",
                    params={"per_page": 100},
                )
                accessible = {
                    _integer(item.get("id"), field="repository id") for item in page.items
                }
                missing = sorted(set(repository_ids) - accessible)
                if missing:
                    raise GitHubPermissionError(
                        "GitHub installation no longer grants selected repositories: "
                        + ", ".join(str(item) for item in missing)
                    )
        except (GitHubConnectorError, ConnectorAuthenticationError) as exc:
            return ConnectorHealth(ConnectorHealthStatus.error, checked_at, str(exc))
        return ConnectorHealth(
            ConnectorHealthStatus.healthy,
            checked_at,
            f"GitHub installation and {len([r for r in context.resources if r.selected])} "
            "selected repositories are accessible.",
        )

    async def revoke(self, context: ConnectorOperationContext) -> None:
        if context.binding is None:
            return
        config = _config_from_context(context)
        installation_id = _installation_id(context.binding)
        await self._api(config).app_request(
            "DELETE",
            f"/app/installations/{quote(installation_id, safe='')}",
            installation_request=True,
        )

    async def ingress(
        self,
        context: ConnectorOperationContext,
        request: ConnectorIngressRequest,
    ) -> ConnectorIngressResult:
        if request.method.upper() != "POST":
            raise ValueError("GitHub webhook ingress requires POST")
        config = _config_from_context(context)
        secret = self._secret_resolver(config.webhook_secret_ref).encode("utf-8")
        supplied = request.headers.get("x-hub-signature-256", "")
        expected = "sha256=" + hmac.new(secret, request.body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(supplied, expected):
            raise GitHubInstallationError("invalid GitHub webhook signature")
        delivery_id = request.headers.get("x-github-delivery", "").strip()
        if not delivery_id:
            raise ValueError("GitHub webhook delivery id is missing")
        event_name = request.headers.get("x-github-event", "").strip()
        try:
            payload = json.loads(request.body)
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValueError("GitHub webhook payload is invalid JSON") from exc
        payload_object = _object(payload, field="webhook payload")
        repository = _object(payload_object.get("repository"), field="webhook repository")
        repository_id = str(_integer(repository.get("id"), field="repository id"))
        selected = next(
            (
                item
                for item in context.resources
                if item.selected and item.external_id == repository_id
            ),
            None,
        )
        changes: tuple[ConnectorChange, ...] = ()
        if selected is not None and event_name in {"issues", "pull_request", "issue_comment"}:
            changes = (self._webhook_change(context, payload_object, event_name, delivery_id),)
        return ConnectorIngressResult(
            ConnectorIngressResponse(
                status_code=202,
                content_type="application/json",
                headers={"cache-control": "no-store"},
                body=b'{"accepted":true}',
            ),
            delivery_id=delivery_id,
            payload_hash=hashlib.sha256(request.body).hexdigest(),
            changes=changes,
        )

    def _webhook_change(
        self,
        context: ConnectorOperationContext,
        payload: dict[str, Any],
        event_name: str,
        delivery_id: str,
    ) -> ConnectorChange:
        assert context.binding is not None
        repository = _object(payload.get("repository"), field="webhook repository")
        repository_id = str(_integer(repository.get("id"), field="repository id"))
        action = _text(payload.get("action"))
        object_name = {
            "issues": "issue",
            "pull_request": "pull_request",
            "issue_comment": "comment",
        }[event_name]
        source = _object(payload.get(object_name), field=f"webhook {object_name}")
        if event_name == "issues":
            sanitized = _sanitize_issue(source)
        elif event_name == "pull_request":
            sanitized = _sanitize_pull_request(source)
        else:
            sanitized = _sanitize_comment(source)
        issue = payload.get("issue")
        number = (
            issue.get("number")
            if event_name == "issue_comment" and isinstance(issue, dict)
            else source.get("number")
        )
        external_id = f"{repository_id}:{object_name}:{number or source.get('id', delivery_id)}"
        source_url = _text(source.get("html_url")) or _text(repository.get("html_url")) or None
        revision = _text(source.get("updated_at")) or None
        provenance = ConnectorProvenance(
            GITHUB_CONNECTOR_ID,
            context.binding.id,
            external_id,
            source_url=source_url,
            revision=revision,
            event_id=delivery_id,
        )
        event_payload = {
            "action": action,
            "repository": {
                "id": repository.get("id"),
                "full_name": repository.get("full_name"),
                "html_url": repository.get("html_url"),
            },
            object_name: sanitized,
            "sender": payload.get("sender", {}),
        }
        event = ConnectorEvent(
            f"github.{event_name}.{action or 'unknown'}",
            provenance,
            event_payload,
        )
        return ConnectorChange(
            ConnectorChangeKind.event,
            provenance,
            title=f"GitHub {event_name}: {action or 'unknown'}",
            event=event,
        )

    def build_actions(
        self,
        context: ConnectorActionContext,
    ) -> tuple[ConnectorAction, ...]:
        store = context.credential_store
        if store is None or not hasattr(store, "get"):
            raise RuntimeError("encrypted connector credential storage is unavailable")
        credential_store = cast(RawCredentialStore, store)
        return (
            ConnectorAction(
                REPOSITORY_GET,
                self._repository_get_action(context, credential_store),
            ),
            ConnectorAction(ISSUE_GET, self._issue_get_action(context, credential_store)),
            ConnectorAction(
                PULL_REQUEST_GET,
                self._pull_request_get_action(context, credential_store),
            ),
            ConnectorAction(
                COMMENTS_LIST,
                self._comments_list_action(context, credential_store),
            ),
            ConnectorAction(
                COMMIT_STATUS_GET,
                self._commit_status_get_action(context, credential_store),
            ),
            ConnectorAction(
                ISSUE_CREATE,
                self._issue_create_action(context, credential_store),
            ),
            ConnectorAction(
                COMMENT_CREATE,
                self._comment_create_action(context, credential_store),
            ),
        )

    async def _action_state(
        self,
        context: ConnectorActionContext,
        store: RawCredentialStore,
        repository_id: str,
        required_permissions: Mapping[str, str],
    ) -> tuple[GitHubAPI, GitHubInstallationToken, ConnectorBinding, ConnectorResource]:
        state = await context.load_state(GITHUB_CONNECTOR_ID)
        binding = state.binding
        if binding is None or binding.status not in {
            ConnectorBindingStatus.connected,
            ConnectorBindingStatus.degraded,
        }:
            raise GitHubInstallationError("GitHub connector is not connected")
        resource = await context.require_selected_resource(GITHUB_CONNECTOR_ID, repository_id)
        config = await _config_from_store(store)
        token = await self._api(config).mint_installation_token(
            _installation_id(binding),
            repository_ids=(_repo_id(resource),),
            required_permissions=required_permissions,
        )
        return self._api(config), token, binding, resource

    def _repository_get_action(
        self,
        context: ConnectorActionContext,
        store: RawCredentialStore,
    ) -> ActionFn:
        async def action(args: dict[str, Any], tool_context: ToolContext) -> str:
            repository_id = _required_text(args, "repository_id")
            api, token, binding, resource = await self._action_state(
                context,
                store,
                repository_id,
                {},
            )
            owner, name = _resource_coordinates(resource)
            response = await api.installation_request(
                token,
                "GET",
                f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}",
            )
            payload = _sanitize_repository(_object(response.json(), field="repository"))
            return _render_result(binding, resource, "repository", payload)

        return action

    def _issue_get_action(
        self,
        context: ConnectorActionContext,
        store: RawCredentialStore,
    ) -> ActionFn:
        async def action(args: dict[str, Any], tool_context: ToolContext) -> str:
            repository_id = _required_text(args, "repository_id")
            number = _positive_int(args, "number")
            api, token, binding, resource = await self._action_state(
                context,
                store,
                repository_id,
                {"issues": "read"},
            )
            owner, name = _resource_coordinates(resource)
            response = await api.installation_request(
                token,
                "GET",
                f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}/issues/{number}",
            )
            payload = _object(response.json(), field="issue")
            if "pull_request" in payload:
                raise ValueError("requested number is a pull request, not an Issue")
            return _render_result(
                binding,
                resource,
                "issue",
                _sanitize_issue(payload),
                external_suffix=f":issue:{number}",
            )

        return action

    def _pull_request_get_action(
        self,
        context: ConnectorActionContext,
        store: RawCredentialStore,
    ) -> ActionFn:
        async def action(args: dict[str, Any], tool_context: ToolContext) -> str:
            repository_id = _required_text(args, "repository_id")
            number = _positive_int(args, "number")
            api, token, binding, resource = await self._action_state(
                context,
                store,
                repository_id,
                {"pull_requests": "read"},
            )
            owner, name = _resource_coordinates(resource)
            response = await api.installation_request(
                token,
                "GET",
                f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}/pulls/{number}",
            )
            payload = _sanitize_pull_request(_object(response.json(), field="pull request"))
            return _render_result(
                binding,
                resource,
                "pull_request",
                payload,
                external_suffix=f":pull_request:{number}",
            )

        return action

    def _comments_list_action(
        self,
        context: ConnectorActionContext,
        store: RawCredentialStore,
    ) -> ActionFn:
        async def action(args: dict[str, Any], tool_context: ToolContext) -> str:
            repository_id = _required_text(args, "repository_id")
            number = _positive_int(args, "number")
            per_page = _bounded_int(args, "per_page", default=100, maximum=100)
            max_pages = _bounded_int(args, "max_pages", default=3, maximum=_MAX_PAGES)
            api, token, binding, resource = await self._action_state(
                context,
                store,
                repository_id,
                {"issues": "read"},
            )
            owner, name = _resource_coordinates(resource)
            page = await api.paginate(
                token,
                f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}/issues/{number}/comments",
                params={"per_page": per_page},
                max_pages=max_pages,
            )
            payload = {
                "number": number,
                "comments": [_sanitize_comment(item) for item in page.items],
            }
            return _render_result(
                binding,
                resource,
                "comments",
                payload,
                external_suffix=f":issue_or_pull_request:{number}:comments",
                next_url=page.next_url,
            )

        return action

    def _commit_status_get_action(
        self,
        context: ConnectorActionContext,
        store: RawCredentialStore,
    ) -> ActionFn:
        async def action(args: dict[str, Any], tool_context: ToolContext) -> str:
            repository_id = _required_text(args, "repository_id")
            ref = _required_text(args, "ref", max_length=200)
            per_page = _bounded_int(args, "per_page", default=100, maximum=100)
            page_number = _positive_int({"page": args.get("page", 1)}, "page")
            api, token, binding, resource = await self._action_state(
                context,
                store,
                repository_id,
                {"statuses": "read"},
            )
            owner, name = _resource_coordinates(resource)
            response = await api.installation_request(
                token,
                "GET",
                f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}/commits/"
                f"{quote(ref, safe='')}/status",
                params={"per_page": per_page, "page": page_number},
            )
            payload = _object(response.json(), field="commit status")
            sanitized = {
                "state": payload.get("state"),
                "sha": payload.get("sha"),
                "total_count": payload.get("total_count"),
                "statuses": payload.get("statuses", []),
                "repository": payload.get("repository", {}),
                "commit_url": payload.get("commit_url"),
                "url": payload.get("url"),
                "html_url": (
                    f"{resource.url}/commit/{quote(_text(payload.get('sha')), safe='')}"
                    if resource.url and _text(payload.get("sha"))
                    else resource.url
                ),
            }
            next_url = api._next_link(response.headers.get("link"))
            return _render_result(
                binding,
                resource,
                "commit_status",
                sanitized,
                external_suffix=f":commit:{ref}:status",
                next_url=next_url,
            )

        return action

    def _issue_create_action(
        self,
        context: ConnectorActionContext,
        store: RawCredentialStore,
    ) -> ActionFn:
        async def action(args: dict[str, Any], tool_context: ToolContext) -> str:
            repository_id = _required_text(args, "repository_id")
            title = _required_text(args, "title", max_length=256)
            body = str(args.get("body", ""))
            key = _required_text(args, "idempotency_key", max_length=512)
            marker = _idempotency_marker(
                key,
                namespace=(f"{tool_context.scope_id}:{GITHUB_CONNECTOR_ID}:issue:{repository_id}"),
            )
            api, token, binding, resource = await self._action_state(
                context,
                store,
                repository_id,
                {"issues": "write"},
            )
            owner, name = _resource_coordinates(resource)
            path = f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}/issues"
            existing_page = await api.paginate(
                token,
                path,
                params={
                    "state": "all",
                    "sort": "created",
                    "direction": "desc",
                    "per_page": 100,
                },
                max_pages=3,
            )
            existing = _find_marker(existing_page.items, marker)
            reconciled = existing is not None
            if existing is None:
                response = await api.installation_request(
                    token,
                    "POST",
                    path,
                    json_body={"title": title, "body": _append_marker(body, marker)},
                )
                existing = _object(response.json(), field="created Issue")
            payload = _sanitize_issue(existing)
            payload["reconciled"] = reconciled
            number = payload.get("number")
            return _render_result(
                binding,
                resource,
                "issue_created",
                payload,
                external_suffix=f":issue:{number}",
            )

        return action

    def _comment_create_action(
        self,
        context: ConnectorActionContext,
        store: RawCredentialStore,
    ) -> ActionFn:
        async def action(args: dict[str, Any], tool_context: ToolContext) -> str:
            repository_id = _required_text(args, "repository_id")
            number = _positive_int(args, "number")
            body = _required_text(args, "body")
            key = _required_text(args, "idempotency_key", max_length=512)
            marker = _idempotency_marker(
                key,
                namespace=(
                    f"{tool_context.scope_id}:{GITHUB_CONNECTOR_ID}:comment:"
                    f"{repository_id}:{number}"
                ),
            )
            api, token, binding, resource = await self._action_state(
                context,
                store,
                repository_id,
                {"issues": "write"},
            )
            owner, name = _resource_coordinates(resource)
            path = f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}/issues/{number}/comments"
            existing = await api.find_recent_comment_marker(token, path, marker)
            reconciled = existing is not None
            if existing is None:
                response = await api.installation_request(
                    token,
                    "POST",
                    path,
                    json_body={"body": _append_marker(body, marker)},
                )
                existing = _object(response.json(), field="created comment")
            payload = _sanitize_comment(existing)
            payload["reconciled"] = reconciled
            return _render_result(
                binding,
                resource,
                "comment_created",
                payload,
                external_suffix=f":issue_or_pull_request:{number}:comment:{payload.get('id')}",
            )

        return action


def factory() -> GitHubProvider:
    return GitHubProvider()


__all__ = [
    "COMMENT_CREATE",
    "COMMENTS_LIST",
    "COMMIT_STATUS_GET",
    "GITHUB_CONNECTOR_ID",
    "GitHubAPI",
    "GitHubAppConfig",
    "GitHubConnectorError",
    "GitHubInstallationError",
    "GitHubInstallationToken",
    "GitHubPermissionError",
    "GitHubProvider",
    "GitHubRateLimitError",
    "ISSUE_CREATE",
    "ISSUE_GET",
    "PULL_REQUEST_GET",
    "REPOSITORY_GET",
    "factory",
    "manifest",
    "resolve_secret_reference",
]
