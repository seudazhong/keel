"""Managed-project REST API: auth/role errors, CRUD, webhook HMAC (M3.7)."""

from __future__ import annotations

import hashlib
import hmac

import pytest
from fastapi.testclient import TestClient
from oidc_helpers import make_rsa_key, sign_token

from keel_core.identity import (
    IdentityService,
    InMemoryIdentityStore,
    OIDCConfig,
    OIDCVerifier,
    StaticJWKSProvider,
)
from keel_core.projects import InMemoryProjectStore, ProjectService
from keel_server.app import create_app

_ISSUER = "https://issuer.example"
_AUD = "keel"
_KEY = make_rsa_key()
_WEBHOOK_SECRET = "test-webhook-secret"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("KEEL_GITHUB_WEBHOOK_SECRET", _WEBHOOK_SECRET)
    from keel_core.config import get_settings

    get_settings.cache_clear()
    app = create_app()
    identity_store = InMemoryIdentityStore()
    app.state.identity = IdentityService(identity_store, allow_jit_provisioning=True)
    app.state.oidc_verifier = OIDCVerifier(
        OIDCConfig(issuer=_ISSUER, audiences=frozenset({_AUD})),
        StaticJWKSProvider([_KEY.jwk]),
    )
    app.state.auth_required = False
    app.state.api_keys = {}
    app.state.projects = ProjectService(InMemoryProjectStore(), identity_store)
    return TestClient(app)


def _auth(subject: str, org: str | None = None) -> dict[str, str]:
    token = sign_token(
        _KEY, issuer=_ISSUER, audience=_AUD, subject=subject, email=f"{subject}@example.com"
    )
    headers = {"Authorization": f"Bearer {token}"}
    if org is not None:
        headers["X-Keel-Org"] = org
    return headers


def _login(client: TestClient, subject: str) -> str:
    resp = client.get("/v1/identity/me", headers=_auth(subject))
    assert resp.status_code == 200, resp.text
    return resp.json()["user"]["id"]


def _new_org(client: TestClient, subject: str, slug: str) -> str:
    resp = client.post(
        "/v1/identity/organizations",
        headers=_auth(subject),
        json={"slug": slug, "display_name": slug.title()},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["organization"]["id"]


def test_project_crud_and_role_errors(client: TestClient) -> None:
    _login(client, "alice")
    bob = _login(client, "bob")
    org = _new_org(client, "alice", "acme")
    # Add bob as a viewer (read only).
    add = client.post(
        "/v1/identity/members",
        headers=_auth("alice", org),
        json={"user_id": bob, "role": "viewer"},
    )
    assert add.status_code in (200, 201), add.text

    # Owner creates a project.
    created = client.post(
        "/v1/projects",
        headers=_auth("alice", org),
        json={"slug": "web-app", "display_name": "Web App"},
    )
    assert created.status_code == 201, created.text
    project_id = created.json()["id"]

    # Viewer (bob) cannot create (needs write) -> 403.
    denied = client.post(
        "/v1/projects",
        headers=_auth("bob", org),
        json={"slug": "other-app", "display_name": "Other"},
    )
    assert denied.status_code == 403

    # Viewer can list/get (read).
    listed = client.get("/v1/projects", headers=_auth("bob", org))
    assert listed.status_code == 200 and len(listed.json()) == 1
    got = client.get(f"/v1/projects/{project_id}", headers=_auth("bob", org))
    assert got.status_code == 200

    # A non-member cannot access the org at all.
    _login(client, "carol")
    forbidden = client.get("/v1/projects", headers=_auth("carol", org))
    assert forbidden.status_code in (403, 404)

    # Archive (owner) then it drops from the active list.
    version = created.json()["version"]
    archived = client.post(
        f"/v1/projects/{project_id}/archive",
        headers=_auth("alice", org),
        json={"expected_version": version},
    )
    assert archived.status_code == 200
    assert archived.json()["status"] == "archived"


def test_project_requires_org_header(client: TestClient) -> None:
    _login(client, "alice")
    _new_org(client, "alice", "acme")
    # No X-Keel-Org header -> require_org fails.
    resp = client.get("/v1/projects", headers=_auth("alice"))
    assert resp.status_code in (400, 403, 404)


def _sign(body: bytes) -> str:
    return "sha256=" + hmac.new(_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()


def test_webhook_hmac_and_replay(client: TestClient) -> None:
    import json

    payload = {"zen": "keep it simple", "hook_id": 1}
    body = json.dumps(payload).encode()

    # Bad signature -> 401.
    bad = client.post(
        "/v1/projects/github/webhook",
        headers={
            "X-Hub-Signature-256": "sha256=deadbeef",
            "X-GitHub-Delivery": "d-1",
            "X-GitHub-Event": "ping",
        },
        content=body,
    )
    assert bad.status_code == 401

    # Good signature -> 202 accepted.
    headers = {
        "X-Hub-Signature-256": _sign(body),
        "X-GitHub-Delivery": "d-1",
        "X-GitHub-Event": "ping",
        "Content-Type": "application/json",
    }
    ok = client.post("/v1/projects/github/webhook", headers=headers, content=body)
    assert ok.status_code == 202, ok.text

    # Replay of the same delivery id -> duplicate no-op.
    replay = client.post("/v1/projects/github/webhook", headers=headers, content=body)
    assert replay.status_code == 202
    assert replay.json()["status"] == "duplicate"


def test_webhook_unknown_event_skipped(client: TestClient) -> None:
    body = b"{}"
    headers = {
        "X-Hub-Signature-256": _sign(body),
        "X-GitHub-Delivery": "d-evt",
        "X-GitHub-Event": "deployment",  # not on the allowlist
        "Content-Type": "application/json",
    }
    resp = client.post("/v1/projects/github/webhook", headers=headers, content=body)
    assert resp.status_code == 202 and resp.json()["status"] == "skipped"
