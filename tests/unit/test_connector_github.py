"""Provider-local GitHub App connector tests with sanitized HTTP fixtures."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from keel_core.connector_contracts import (
    ConnectorActionApproval,
    ConnectorActionContext,
    ConnectorActionIdempotency,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorHealthStatus,
    ConnectorIngressRequest,
    ConnectorOperationContext,
    ConnectorResourceDraft,
)
from keel_core.connector_credentials import ConnectorCredentialStore, CredentialEnvelope
from keel_core.connector_providers.github import (
    GITHUB_CONNECTOR_ID,
    GitHubAPI,
    GitHubAppConfig,
    GitHubInstallationError,
    GitHubPermissionError,
    GitHubProvider,
    GitHubRateLimitError,
    manifest,
)
from keel_core.connector_registry import ConnectorRegistration, ConnectorRegistry
from keel_core.connector_repository import InMemoryConnectorRepository
from keel_core.connector_service import ConnectorChangeSink, ConnectorService
from keel_core.connectors import ConnectorTool
from keel_core.digest import digest_permissions, digest_registry
from keel_core.outbox import InMemoryOutboundStore
from keel_core.protocols import ToolContext
from keel_core.secrets import EnvelopeCipher
from keel_core.tokens import InMemoryTokenStore
from keel_core.types import ContentTaint, PermissionDecision

NOW = datetime(2026, 7, 18, 3, 19, 39, tzinfo=UTC)
INSTALLATION_ID = "77"
REPOSITORY_ID = "101"
OTHER_REPOSITORY_ID = "202"


def _private_key() -> tuple[str, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode("ascii")
    public_pem = (
        key.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode("ascii")
    )
    return private_pem, public_pem


def _config_values() -> dict[str, str]:
    return {
        "app_id": "1234",
        "client_id": "Iv1.sanitized",
        "app_slug": "keel-sanitized",
        "private_key_secret_ref": "env:TEST_GITHUB_PRIVATE_KEY",
        "webhook_secret_ref": "env:TEST_GITHUB_WEBHOOK_SECRET",
        "api_base_url": "https://api.github.test",
        "web_base_url": "https://github.test",
    }


def _installation() -> dict[str, Any]:
    return {
        "id": int(INSTALLATION_ID),
        "account": {"id": 9001, "login": "octo-org", "type": "Organization"},
        "repository_selection": "selected",
        "permissions": {
            "issues": "write",
            "pull_requests": "read",
            "statuses": "read",
            "metadata": "read",
        },
        "suspended_at": None,
    }


def _repository(repository_id: int = 101, name: str = "demo") -> dict[str, Any]:
    return {
        "id": repository_id,
        "node_id": f"R_{repository_id}",
        "name": name,
        "full_name": f"octo-org/{name}",
        "private": True,
        "html_url": f"https://github.test/octo-org/{name}",
        "description": "sanitized",
        "default_branch": "main",
        "archived": False,
        "disabled": False,
        "visibility": "private",
        "owner": {"id": 9001, "login": "octo-org", "type": "Organization"},
        "permissions": {"admin": False, "push": True, "pull": True},
        "updated_at": "2026-07-18T01:00:00Z",
    }


def _token(
    *,
    permissions: dict[str, str] | None = None,
    expires_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "token": "ghs_ephemeral_sanitized",
        "expires_at": (expires_at or NOW + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "permissions": permissions
        or {
            "issues": "write",
            "pull_requests": "read",
            "statuses": "read",
            "metadata": "read",
        },
    }


def _response(
    request: httpx.Request,
    status: int,
    payload: Any,
    *,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return httpx.Response(status, json=payload, headers=headers, request=request)


def _registry(provider: GitHubProvider) -> ConnectorRegistry:
    return ConnectorRegistry(
        (
            ConnectorRegistration(
                manifest,
                lambda: provider,
                "tests.github",
            ),
        )
    )


def _stores() -> tuple[InMemoryTokenStore, ConnectorCredentialStore]:
    raw = InMemoryTokenStore("scope:github", EnvelopeCipher("sanitized-key"))
    return raw, ConnectorCredentialStore(raw)


def _credential() -> CredentialEnvelope:
    return CredentialEnvelope("github_app", _config_values())


async def _connected_state(
    repository: InMemoryConnectorRepository,
    credentials: ConnectorCredentialStore,
    *,
    selected: bool = True,
    other_selected: bool = False,
) -> None:
    binding = await repository.upsert_binding(
        GITHUB_CONNECTOR_ID,
        ConnectorBindingDraft(
            display_name="GitHub: octo-org",
            external_account_id=INSTALLATION_ID,
            external_tenant_id="9001",
            metadata={
                "account_login": "octo-org",
                "repository_selection": "selected",
                "permissions": _installation()["permissions"],
            },
        ),
        ConnectorBindingStatus.connected,
    )
    await repository.upsert_resources(
        GITHUB_CONNECTOR_ID,
        binding.id,
        (
            ConnectorResourceDraft(
                REPOSITORY_ID,
                "repository",
                "octo-org/demo",
                "https://github.test/octo-org/demo",
                selected=selected,
                config={"owner": "octo-org", "name": "demo", "default_branch": "main"},
            ),
            ConnectorResourceDraft(
                OTHER_REPOSITORY_ID,
                "repository",
                "octo-org/other",
                "https://github.test/octo-org/other",
                selected=other_selected,
                config={"owner": "octo-org", "name": "other", "default_branch": "main"},
            ),
        ),
    )
    await credentials.put(GITHUB_CONNECTOR_ID, _credential())


@pytest.fixture
def github_secrets(monkeypatch: pytest.MonkeyPatch) -> str:
    private_key, public_key = _private_key()
    monkeypatch.setenv("TEST_GITHUB_PRIVATE_KEY", private_key)
    monkeypatch.setenv("TEST_GITHUB_WEBHOOK_SECRET", "webhook-sanitized")
    return public_key


async def test_staged_install_persists_metadata_and_discovers_paginated_repositories(
    github_secrets: str,
) -> None:
    token_requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == f"/app/installations/{INSTALLATION_ID}" and request.method == "GET":
            return _response(request, 200, _installation())
        if path == f"/app/installations/{INSTALLATION_ID}/access_tokens":
            token_requests.append(json.loads(request.content or b"{}"))
            return _response(request, 201, _token())
        if path == "/installation/repositories":
            page = parse_qs(request.url.query.decode()).get("page", ["1"])[0]
            if page == "1":
                return _response(
                    request,
                    200,
                    {"repositories": [_repository()]},
                    headers={
                        "link": (
                            "<https://api.github.test/installation/repositories?"
                            'per_page=100&page=2>; rel="next"'
                        )
                    },
                )
            return _response(request, 200, {"repositories": [_repository(202, "other")]})
        raise AssertionError(f"unexpected GitHub request: {request.method} {request.url}")

    provider = GitHubProvider(
        transport=httpx.MockTransport(handler),
        now=lambda: NOW,
    )
    raw, credentials = _stores()
    repository = InMemoryConnectorRepository("scope:github")
    service = ConnectorService(_registry(provider), repository, credentials=credentials)

    setup = await service.setup(
        GITHUB_CONNECTOR_ID,
        _config_values(),
        callback_base_url="https://keel.test/v1/connectors/github",
    )
    assert setup.binding.status is ConnectorBindingStatus.configured
    assert [artifact.label for artifact in setup.artifacts] == [
        "GitHub App setup URL",
        "GitHub App webhook URL",
    ]
    stored = await credentials.get(GITHUB_CONNECTOR_ID)
    assert stored is not None
    assert stored.values["private_key_secret_ref"] == "env:TEST_GITHUB_PRIVATE_KEY"
    assert "token" not in stored.values and "installation_token" not in stored.values

    start = await service.begin_auth(
        GITHUB_CONNECTOR_ID,
        "https://keel.test/v1/connectors/github/callback",
    )
    assert start.url.startswith("https://github.test/apps/keel-sanitized/installations/new?")
    assert parse_qs(start.url.split("?", 1)[1])["state"] == [start.state]
    connected = await service.complete_auth(
        GITHUB_CONNECTOR_ID,
        "https://keel.test/v1/connectors/github/callback",
        {"installation_id": INSTALLATION_ID, "state": start.state},
    )
    assert connected.binding.status is ConnectorBindingStatus.connected
    assert connected.binding.external_account_id == INSTALLATION_ID
    assert connected.binding.metadata["account_login"] == "octo-org"

    resources = await service.refresh_resources(GITHUB_CONNECTOR_ID)
    assert [item["external_id"] for item in resources] == [REPOSITORY_ID, OTHER_REPOSITORY_ID]
    assert all(item["selected"] is False for item in resources)
    await service.select_resources(GITHUB_CONNECTOR_ID, {REPOSITORY_ID})
    selected = await repository.list_resources(GITHUB_CONNECTOR_ID, selected_only=True)
    assert [(item.external_id, item.config["default_branch"]) for item in selected] == [
        (REPOSITORY_ID, "main")
    ]
    assert token_requests == [{}]
    assert "ghs_ephemeral_sanitized" not in (await raw.get(GITHUB_CONNECTOR_ID) or "")


async def test_jit_token_jwt_and_expiry_are_fail_closed(github_secrets: str) -> None:
    public_key = github_secrets
    app_jwts: list[str] = []
    expired = True

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal expired
        authorization = request.headers["authorization"]
        app_jwts.append(authorization.removeprefix("Bearer "))
        expiry = NOW - timedelta(seconds=1) if expired else NOW + timedelta(minutes=30)
        return _response(request, 201, _token(expires_at=expiry))

    api = GitHubAPI(
        GitHubAppConfig.from_values(_config_values()),
        transport=httpx.MockTransport(handler),
        now=lambda: NOW,
    )
    with pytest.raises(GitHubInstallationError, match="expired"):
        await api.mint_installation_token(INSTALLATION_ID, repository_ids=(101,))
    claims = jwt.decode(
        app_jwts[0],
        public_key,
        algorithms=["RS256"],
        options={"verify_aud": False, "verify_exp": False, "verify_iat": False},
    )
    assert claims["iss"] == "1234"
    assert claims["exp"] - claims["iat"] == 600

    expired = False
    token = await api.mint_installation_token(
        INSTALLATION_ID,
        repository_ids=(101,),
        required_permissions={"issues": "write"},
    )
    assert token.expires_at == NOW + timedelta(minutes=30)


async def test_actions_enforce_selected_repository_and_read_results_are_tainted(
    github_secrets: str,
) -> None:
    scoped_repository_ids: list[list[int]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/access_tokens"):
            body = json.loads(request.content)
            scoped_repository_ids.append(body["repository_ids"])
            return _response(request, 201, _token())
        if request.url.path == "/repos/octo-org/demo":
            return _response(request, 200, _repository())
        raise AssertionError(f"unexpected GitHub request: {request.method} {request.url}")

    provider = GitHubProvider(transport=httpx.MockTransport(handler), now=lambda: NOW)
    raw, credentials = _stores()
    repository = InMemoryConnectorRepository("scope:github")
    await _connected_state(repository, credentials)
    action_context = ConnectorActionContext.with_repository(
        "scope:github",
        repository,
        credential_store=raw,
    )
    actions = _registry(provider).build_actions(action_context)
    by_name = {item.manifest.name: item for item in actions}
    tool = ConnectorTool(
        name="github_repository_get",
        description="read",
        action=by_name["github_repository_get"].action,
    )
    result = await tool.run(
        {"repository_id": REPOSITORY_ID},
        ToolContext(scope_id="scope:github", session_id="session"),
    )
    assert result.taint is ContentTaint.tainted
    rendered = json.loads(result.output)
    assert rendered["provenance"]["connector_id"] == GITHUB_CONNECTOR_ID
    assert rendered["provenance"]["external_resource_id"] == REPOSITORY_ID
    assert scoped_repository_ids == [[101]]

    with pytest.raises(PermissionError, match="not selected"):
        await by_name["github_repository_get"].action(
            {"repository_id": OTHER_REPOSITORY_ID},
            ToolContext(scope_id="scope:github", session_id="session"),
        )
    assert scoped_repository_ids == [[101]]


class _Sink(ConnectorChangeSink):
    def __init__(self) -> None:
        self.changes: list[Any] = []

    async def apply(self, change: Any) -> None:
        self.changes.append(change)


async def test_webhook_signature_selection_replay_and_redelivery(
    github_secrets: str,
) -> None:
    provider = GitHubProvider(now=lambda: NOW)
    _, credentials = _stores()
    repository = InMemoryConnectorRepository("scope:github")
    await _connected_state(repository, credentials)
    sink = _Sink()
    service = ConnectorService(
        _registry(provider),
        repository,
        credentials=credentials,
        change_sink=sink,
    )
    payload = {
        "action": "opened",
        "repository": _repository(),
        "issue": {
            "id": 501,
            "number": 9,
            "title": "Sanitized Issue",
            "body": "untrusted",
            "state": "open",
            "html_url": "https://github.test/octo-org/demo/issues/9",
            "updated_at": "2026-07-18T02:00:00Z",
            "user": {"id": 7, "login": "octocat", "type": "User"},
        },
        "sender": {"id": 7, "login": "octocat"},
    }
    body = json.dumps(payload, separators=(",", ":")).encode()
    signature = (
        "sha256="
        + hmac.new(
            b"webhook-sanitized",
            body,
            hashlib.sha256,
        ).hexdigest()
    )
    request = ConnectorIngressRequest(
        "POST",
        {},
        {
            "x-hub-signature-256": signature,
            "x-github-delivery": "delivery-1",
            "x-github-event": "issues",
        },
        body,
        "https://keel.test/v1/connectors/github/webhook",
    )
    first = await service.ingress(GITHUB_CONNECTOR_ID, request)
    replay = await service.ingress(GITHUB_CONNECTOR_ID, request)
    assert (first.accepted, first.changes) == (True, 1)
    assert (replay.accepted, replay.changes) == (False, 0)
    assert len(sink.changes) == 1
    assert sink.changes[0].event.taint is ContentTaint.tainted

    tampered = body.replace(b"Sanitized", b"Changed")
    tampered_signature = (
        "sha256="
        + hmac.new(
            b"webhook-sanitized",
            tampered,
            hashlib.sha256,
        ).hexdigest()
    )
    with pytest.raises(ValueError, match="different payload"):
        await service.ingress(
            GITHUB_CONNECTOR_ID,
            ConnectorIngressRequest(
                "POST",
                {},
                {
                    "x-hub-signature-256": tampered_signature,
                    "x-github-delivery": "delivery-1",
                    "x-github-event": "issues",
                },
                tampered,
                "https://keel.test/v1/connectors/github/webhook",
            ),
        )

    unselected = {**payload, "repository": _repository(202, "other")}
    unselected_body = json.dumps(unselected, separators=(",", ":")).encode()
    unselected_signature = (
        "sha256="
        + hmac.new(
            b"webhook-sanitized",
            unselected_body,
            hashlib.sha256,
        ).hexdigest()
    )
    outcome = await service.ingress(
        GITHUB_CONNECTOR_ID,
        ConnectorIngressRequest(
            "POST",
            {},
            {
                "x-hub-signature-256": unselected_signature,
                "x-github-delivery": "delivery-2",
                "x-github-event": "issues",
            },
            unselected_body,
            "https://keel.test/v1/connectors/github/webhook",
        ),
    )
    assert (outcome.accepted, outcome.changes) == (True, 0)
    assert len(sink.changes) == 1

    with pytest.raises(GitHubInstallationError, match="signature"):
        await service.ingress(
            GITHUB_CONNECTOR_ID,
            ConnectorIngressRequest(
                "POST",
                {},
                {
                    "x-hub-signature-256": "sha256=invalid",
                    "x-github-delivery": "delivery-3",
                    "x-github-event": "issues",
                },
                body,
                "https://keel.test/v1/connectors/github/webhook",
            ),
        )


async def test_pagination_rate_limit_and_permission_errors_are_explicit(
    github_secrets: str,
) -> None:
    mode = "rate"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/access_tokens"):
            permissions = _token()["permissions"]
            if mode == "permission":
                permissions = {**permissions, "issues": "read"}
            return _response(request, 201, _token(permissions=permissions))
        if request.url.path.endswith("/issues/9/comments"):
            return _response(
                request,
                403,
                {"message": "API rate limit exceeded"},
                headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "1784319999"},
            )
        raise AssertionError(f"unexpected GitHub request: {request.method} {request.url}")

    provider = GitHubProvider(transport=httpx.MockTransport(handler), now=lambda: NOW)
    raw, credentials = _stores()
    repository = InMemoryConnectorRepository("scope:github")
    await _connected_state(repository, credentials)
    actions = _registry(provider).build_actions(
        ConnectorActionContext.with_repository(
            "scope:github",
            repository,
            credential_store=raw,
        )
    )
    by_name = {item.manifest.name: item for item in actions}
    with pytest.raises(GitHubRateLimitError, match="rate limit"):
        await by_name["github_comments_list"].action(
            {"repository_id": REPOSITORY_ID, "number": 9, "max_pages": 2},
            ToolContext(scope_id="scope:github", session_id="session"),
        )

    mode = "permission"
    with pytest.raises(GitHubPermissionError, match="requires write"):
        await by_name["github_comment_create"].action(
            {
                "repository_id": REPOSITORY_ID,
                "number": 9,
                "body": "approved",
                "idempotency_key": "permission-test",
            },
            ToolContext(scope_id="scope:github", session_id="session"),
        )


async def test_outbound_approval_idempotency_and_provider_reconciliation(
    github_secrets: str,
) -> None:
    comments: list[dict[str, Any]] = []
    comment_posts = 0
    issue_posts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal comment_posts, issue_posts
        path = request.url.path
        if path.endswith("/access_tokens"):
            return _response(request, 201, _token())
        if path == "/repos/octo-org/demo/issues/9/comments" and request.method == "GET":
            return _response(request, 200, comments)
        if path == "/repos/octo-org/demo/issues/9/comments" and request.method == "POST":
            comment_posts += 1
            created = {
                "id": 8001,
                "node_id": "IC_8001",
                "body": json.loads(request.content)["body"],
                "html_url": "https://github.test/octo-org/demo/issues/9#issuecomment-8001",
                "issue_url": "https://api.github.test/repos/octo-org/demo/issues/9",
                "created_at": "2026-07-18T03:00:00Z",
                "updated_at": "2026-07-18T03:00:00Z",
                "user": {"id": 1, "login": "keel-app", "type": "Bot"},
            }
            comments.append(created)
            raise httpx.ReadTimeout("response lost after provider accepted", request=request)
        if path == "/repos/octo-org/demo/issues" and request.method == "GET":
            return _response(request, 200, [])
        if path == "/repos/octo-org/demo/issues" and request.method == "POST":
            issue_posts += 1
            payload = json.loads(request.content)
            return _response(
                request,
                201,
                {
                    "id": 6001,
                    "number": 10,
                    "title": payload["title"],
                    "body": payload["body"],
                    "state": "open",
                    "html_url": "https://github.test/octo-org/demo/issues/10",
                    "created_at": "2026-07-18T03:01:00Z",
                    "updated_at": "2026-07-18T03:01:00Z",
                    "user": {"id": 1, "login": "keel-app", "type": "Bot"},
                },
            )
        raise AssertionError(f"unexpected GitHub request: {request.method} {request.url}")

    provider = GitHubProvider(transport=httpx.MockTransport(handler), now=lambda: NOW)
    raw, credentials = _stores()
    repository = InMemoryConnectorRepository("scope:github")
    await _connected_state(repository, credentials)
    actions = _registry(provider).build_actions(
        ConnectorActionContext.with_repository(
            "scope:github",
            repository,
            credential_store=raw,
            idempotency_store=InMemoryOutboundStore(),
        )
    )
    outbound = {
        action.manifest.name: action.manifest
        for action in actions
        if action.manifest.approval is ConnectorActionApproval.tainted
    }
    assert set(outbound) == {"github_issue_create", "github_comment_create"}
    assert all(
        item.idempotency is ConnectorActionIdempotency.required for item in outbound.values()
    )
    permissions = digest_permissions(actions)
    tainted_context = ToolContext(
        scope_id="scope:github",
        session_id="session",
        content_taint=ContentTaint.tainted,
    )
    assert (
        permissions.evaluate("github_issue_create", {}, tainted_context) is PermissionDecision.ask
    )
    assert (
        permissions.evaluate("github_comment_create", {}, tainted_context) is PermissionDecision.ask
    )

    tools = digest_registry(
        connector_actions=actions,
        idempotency_store=InMemoryOutboundStore(),
    )
    comment_tool = tools.get("github_comment_create")
    issue_tool = tools.get("github_issue_create")
    assert comment_tool is not None and issue_tool is not None
    comment_args = {
        "repository_id": REPOSITORY_ID,
        "number": 9,
        "body": "approved comment",
        "idempotency_key": "comment-key",
    }
    with pytest.raises(httpx.ReadTimeout):
        await comment_tool.run(comment_args, tainted_context)
    reconciled = await comment_tool.run(comment_args, tainted_context)
    assert json.loads(reconciled.output)["data"]["reconciled"] is True
    assert comment_posts == 1

    issue_args = {
        "repository_id": REPOSITORY_ID,
        "title": "Approved Issue",
        "body": "approved body",
        "idempotency_key": "issue-key",
    }
    created = await issue_tool.run(issue_args, tainted_context)
    replayed = await issue_tool.run(issue_args, tainted_context)
    assert created.output == replayed.output
    assert issue_posts == 1


async def test_revoked_installation_and_permission_shrink_fail_closed(
    github_secrets: str,
) -> None:
    mode = "revoked"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/app/installations/{INSTALLATION_ID}":
            if mode == "revoked":
                return _response(request, 404, {"message": "Not Found"})
            return _response(request, 200, _installation())
        if request.url.path.endswith("/access_tokens"):
            if mode == "revoked":
                return _response(request, 404, {"message": "Not Found"})
            permissions = {
                "issues": "read",
                "pull_requests": "read",
                "statuses": "read",
            }
            return _response(request, 201, _token(permissions=permissions))
        raise AssertionError(f"unexpected GitHub request: {request.method} {request.url}")

    provider = GitHubProvider(transport=httpx.MockTransport(handler), now=lambda: NOW)
    raw, credentials = _stores()
    repository = InMemoryConnectorRepository("scope:github")
    await _connected_state(repository, credentials)
    binding = await repository.get_binding(GITHUB_CONNECTOR_ID)
    resources = tuple(await repository.list_resources(GITHUB_CONNECTOR_ID))
    assert binding is not None
    context = ConnectorOperationContext(
        "scope:github",
        GITHUB_CONNECTOR_ID,
        binding=binding,
        credential=_credential(),
        credential_version=1,
        resources=resources,
    )
    revoked = await provider.health(context)
    assert revoked.status is ConnectorHealthStatus.error
    assert "revoked" in (revoked.message or "")

    actions = _registry(provider).build_actions(
        ConnectorActionContext.with_repository(
            "scope:github",
            repository,
            credential_store=raw,
        )
    )
    repository_get = next(
        action for action in actions if action.manifest.name == "github_repository_get"
    )
    with pytest.raises(GitHubInstallationError, match="revoked"):
        await repository_get.action(
            {"repository_id": REPOSITORY_ID},
            ToolContext(scope_id="scope:github", session_id="session"),
        )

    mode = "permission"
    shrunk = await provider.health(context)
    assert shrunk.status is ConnectorHealthStatus.error
    assert "requires write" in (shrunk.message or "")
