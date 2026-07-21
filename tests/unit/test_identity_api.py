"""Identity REST API: OIDC actor, org selection, agents, grants, auth/status (M3.6)."""

from __future__ import annotations

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
from keel_server.app import create_app

_ISSUER = "https://issuer.example"
_AUD = "keel"
_KEY = make_rsa_key()


@pytest.fixture
def client() -> TestClient:
    app = create_app()
    app.state.identity = IdentityService(InMemoryIdentityStore(), allow_jit_provisioning=True)
    app.state.oidc_verifier = OIDCVerifier(
        OIDCConfig(issuer=_ISSUER, audiences=frozenset({_AUD})),
        StaticJWKSProvider([_KEY.jwk]),
    )
    app.state.auth_required = False
    app.state.api_keys = {}
    # No `with` -> the app lifespan (real runtime/sandbox) is not started; identity state
    # is injected directly, mirroring the other server unit tests.
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


def test_me_requires_valid_token(client: TestClient) -> None:
    # A malformed bearer JWT is rejected (fails OIDC verification).
    bad = client.get("/v1/identity/me", headers={"Authorization": "Bearer a.b.c"})
    assert bad.status_code == 401
    # In cloud mode a no-credential request cannot fall back to the local operator.
    app = create_app()
    app.state.identity = IdentityService(InMemoryIdentityStore())
    app.state.oidc_verifier = None
    app.state.auth_required = True  # cloud mode: fail closed
    app.state.api_keys = {}
    cloud = TestClient(app)
    assert cloud.get("/v1/identity/me").status_code in (401, 403, 503)


def test_full_org_agent_grant_flow(client: TestClient) -> None:
    _login(client, "alice")
    bob = _login(client, "bob")

    created = client.post(
        "/v1/identity/organizations",
        headers=_auth("alice"),
        json={"slug": "acme", "display_name": "Acme"},
    )
    assert created.status_code == 201, created.text
    org_id = created.json()["organization"]["id"]

    # Alice (owner) adds Bob as a member.
    added = client.post(
        "/v1/identity/members",
        headers=_auth("alice", org_id),
        json={"user_id": bob, "role": "member"},
    )
    assert added.status_code == 201, added.text

    # Alice creates a shared team agent.
    team = client.post(
        "/v1/identity/agents",
        headers=_auth("alice", org_id),
        json={"kind": "team", "name": "Shared", "persona": "top secret"},
    )
    assert team.status_code == 201, team.text
    team_id = team.json()["id"]

    # Bob (member) has no Agent Access edge yet: cannot see or select the team agent.
    listed = client.get("/v1/identity/agents", headers=_auth("bob", org_id))
    assert listed.status_code == 200
    assert listed.json() == []
    denied_select = client.post(
        f"/v1/identity/agents/{team_id}/select", headers=_auth("bob", org_id)
    )
    assert denied_select.status_code == 404

    # A hidden Agent is not disclosed through its access-management endpoint.
    self_grant = client.post(
        f"/v1/identity/agents/{team_id}/access",
        headers=_auth("bob", org_id),
        json={"principal_type": "user", "principal_id": bob, "level": "use"},
    )
    assert self_grant.status_code == 404

    # Alice (owner) grants Bob 'use' Agent Access.
    access = client.post(
        f"/v1/identity/agents/{team_id}/access",
        headers=_auth("alice", org_id),
        json={"principal_type": "user", "principal_id": bob, "level": "use"},
    )
    assert access.status_code == 201, access.text

    # Bob (member) now sees + can select the team agent.
    listed = client.get("/v1/identity/agents", headers=_auth("bob", org_id))
    assert listed.status_code == 200
    assert {a["id"] for a in listed.json()} == {team_id}
    selected = client.post(f"/v1/identity/agents/{team_id}/select", headers=_auth("bob", org_id))
    assert selected.status_code == 200
    visible_but_not_manager = client.post(
        f"/v1/identity/agents/{team_id}/access",
        headers=_auth("bob", org_id),
        json={"principal_type": "user", "principal_id": bob, "level": "manage"},
    )
    assert visible_but_not_manager.status_code == 403

    # Alice can list the access edges; revoking takes effect immediately.
    edges = client.get(f"/v1/identity/agents/{team_id}/access", headers=_auth("alice", org_id))
    assert edges.status_code == 200
    assert {e["principal_id"] for e in edges.json()} == {bob}
    revoke_access = client.request(
        "DELETE",
        f"/v1/identity/agents/{team_id}/access",
        headers=_auth("alice", org_id),
        params={"principal_type": "user", "principal_id": bob},
    )
    assert revoke_access.status_code == 200
    assert revoke_access.json()["status"] == "revoked"
    denied_after_revoke = client.post(
        f"/v1/identity/agents/{team_id}/select", headers=_auth("bob", org_id)
    )
    assert denied_after_revoke.status_code == 404

    channel_id = "slack:T1/C2"
    channel_access = client.post(
        f"/v1/identity/agents/{team_id}/access",
        headers=_auth("alice", org_id),
        json={"principal_type": "channel", "principal_id": channel_id, "level": "use"},
    )
    assert channel_access.status_code == 201
    channel_revoke = client.request(
        "DELETE",
        f"/v1/identity/agents/{team_id}/access",
        headers=_auth("alice", org_id),
        params={"principal_type": "channel", "principal_id": channel_id},
    )
    assert channel_revoke.status_code == 200
    assert channel_revoke.json()["status"] == "revoked"

    # Bob (member) cannot grant — needs admin/owner.
    denied = client.post(
        "/v1/identity/grants",
        headers=_auth("bob", org_id),
        json={
            "agent_id": team_id,
            "resource_type": "kb",
            "resource_id": "kb1",
            "capability": "read",
        },
    )
    assert denied.status_code == 403

    # Alice (owner) grants, lists, revokes.
    granted = client.post(
        "/v1/identity/grants",
        headers=_auth("alice", org_id),
        json={
            "agent_id": team_id,
            "resource_type": "kb",
            "resource_id": "kb1",
            "capability": "read",
        },
    )
    assert granted.status_code == 201, granted.text
    grant_id = granted.json()["id"]
    grants = client.get(
        "/v1/identity/grants", headers=_auth("alice", org_id), params={"agent_id": team_id}
    )
    assert [g["id"] for g in grants.json()] == [grant_id]
    revoked = client.request(
        "DELETE", f"/v1/identity/grants/{grant_id}", headers=_auth("alice", org_id)
    )
    assert revoked.status_code == 200
    assert revoked.json()["status"] == "revoked"


def test_personal_agent_isolation_via_api(client: TestClient) -> None:
    _login(client, "alice")
    bob = _login(client, "bob")
    org_id = client.post(
        "/v1/identity/organizations",
        headers=_auth("alice"),
        json={"slug": "acme", "display_name": "Acme"},
    ).json()["organization"]["id"]
    client.post(
        "/v1/identity/members",
        headers=_auth("alice", org_id),
        json={"user_id": bob, "role": "member"},
    )
    personal = client.post(
        "/v1/identity/agents",
        headers=_auth("bob", org_id),
        json={"kind": "personal", "name": "BobBot"},
    )
    assert personal.status_code == 201
    pid = personal.json()["id"]
    # A different member (carol) cannot see or fetch it.
    carol = _login(client, "carol")
    client.post(
        "/v1/identity/members",
        headers=_auth("alice", org_id),
        json={"user_id": carol, "role": "member"},
    )
    visible = client.get("/v1/identity/agents", headers=_auth("carol", org_id))
    assert pid not in {a["id"] for a in visible.json()}
    fetched = client.get(f"/v1/identity/agents/{pid}", headers=_auth("carol", org_id))
    assert fetched.status_code == 404


def test_org_spoofing_and_missing_org(client: TestClient) -> None:
    _login(client, "alice")
    _login(client, "bob")
    org_id = client.post(
        "/v1/identity/organizations",
        headers=_auth("alice"),
        json={"slug": "acme", "display_name": "Acme"},
    ).json()["organization"]["id"]
    # Bob is not a member -> the org is invisible (404), never leaked as 403.
    spoof = client.get("/v1/identity/agents", headers=_auth("bob", org_id))
    assert spoof.status_code == 404
    # Missing org selection header -> 400.
    missing = client.get("/v1/identity/agents", headers=_auth("alice"))
    assert missing.status_code == 400


def test_last_owner_protection_via_api(client: TestClient) -> None:
    alice = _login(client, "alice")
    org_id = client.post(
        "/v1/identity/organizations",
        headers=_auth("alice"),
        json={"slug": "acme", "display_name": "Acme"},
    ).json()["organization"]["id"]
    resp = client.request("DELETE", f"/v1/identity/members/{alice}", headers=_auth("alice", org_id))
    assert resp.status_code == 409


def test_local_operator_without_oidc() -> None:
    """Open/local mode maps the single operator to a durable user (no OIDC configured)."""
    app = create_app()
    app.state.identity = IdentityService(InMemoryIdentityStore())
    app.state.oidc_verifier = None
    app.state.auth_required = False
    app.state.api_keys = {}
    local = TestClient(app)
    me = local.get("/v1/identity/me")
    assert me.status_code == 200
    assert me.json()["user"]["display_name"] == "Local Operator"
    created = local.post(
        "/v1/identity/organizations", json={"slug": "home", "display_name": "Home"}
    )
    assert created.status_code == 201
