"""R1B adversarial integration tests: Agent Access edges + session ownership/visibility.

Covers the acceptance scenarios in the task brief end-to-end against a real Postgres
substrate (migration 0025): discover/use/manage tiers, revocation at admission, session
ownership independent of Agent access, agent_members/explicit visibility, cross-org
non-disclosure, and admission idempotency/atomicity.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import httpx
import jwt
import pytest
import pytest_asyncio
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from httpx import ASGITransport
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.identity import (
    AuditAction,
    IdentityService,
    InMemoryAuditSink,
    NotFoundError,
    OIDCConfig,
    OIDCVerifier,
    PostgresIdentityStore,
    StaticJWKSProvider,
)
from keel_core.loop import admit
from keel_core.scoping import derive_agent_scope
from keel_core.session_visibility import SessionVisibility, ensure_session_identity
from keel_core.state import PostgresEventStore
from keel_server.app import create_app

pytestmark = pytest.mark.integration

_ISSUER = "https://issuer.example"
_AUD = "keel"


@dataclass
class _SigningKey:
    kid: str
    private_pem: bytes
    jwk: dict[str, Any]


def _make_rsa_key(kid: str = "rsa-1") -> _SigningKey:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    jwk.update({"kid": kid, "alg": "RS256", "use": "sig"})
    return _SigningKey(kid=kid, private_pem=pem, jwk=jwk)


def _sign_token(key: _SigningKey, *, subject: str, email: str | None = None) -> str:
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": _ISSUER,
        "sub": subject,
        "aud": _AUD,
        "iat": now,
        "exp": now + 300,
    }
    if email:
        claims["email"] = email
        claims["email_verified"] = True
    return jwt.encode(claims, key.private_pem, algorithm="RS256", headers={"kid": key.kid})


_KEY = _make_rsa_key()


def _auth(subject: str, org: str | None = None, agent: str | None = None) -> dict[str, str]:
    token = _sign_token(_KEY, subject=subject, email=f"{subject}@example.com")
    headers = {"Authorization": f"Bearer {token}"}
    if org is not None:
        headers["X-Keel-Org"] = org
    if agent is not None:
        headers["X-Keel-Agent"] = agent
    return headers


@pytest_asyncio.fixture
async def client_and_engine(
    migrated_db: AsyncEngine,
) -> AsyncIterator[tuple[httpx.AsyncClient, AsyncEngine]]:
    app = create_app()
    audit = InMemoryAuditSink()
    app.state.identity = IdentityService(
        PostgresIdentityStore(migrated_db), audit=audit, allow_jit_provisioning=True
    )
    app.state.r1b_audit = audit
    app.state.oidc_verifier = OIDCVerifier(
        OIDCConfig(issuer=_ISSUER, audiences=frozenset({_AUD})),
        StaticJWKSProvider([_KEY.jwk]),
    )
    app.state.auth_required = False
    app.state.api_keys = {}
    app.state.engine = migrated_db

    async def _enqueue(_kind: str, _run_id: str, _scope: str) -> None:
        return None

    app.state.enqueue = _enqueue
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, migrated_db


async def _login(client: httpx.AsyncClient, subject: str) -> str:
    resp = await client.get("/v1/identity/me", headers=_auth(subject))
    assert resp.status_code == 200, resp.text
    return resp.json()["user"]["id"]


async def _create_org(client: httpx.AsyncClient, owner_subject: str, slug: str) -> str:
    resp = await client.post(
        "/v1/identity/organizations",
        headers=_auth(owner_subject),
        json={"slug": slug, "display_name": slug},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["organization"]["id"]


async def _create_team_agent(client: httpx.AsyncClient, owner_subject: str, org_id: str) -> str:
    resp = await client.post(
        "/v1/identity/agents",
        headers=_auth(owner_subject, org_id),
        json={"kind": "team", "name": f"Team-{uuid.uuid4().hex[:8]}", "persona": "helpful"},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["id"]


async def _grant_access(
    client: httpx.AsyncClient,
    owner_subject: str,
    org_id: str,
    agent_id: str,
    principal_id: str,
    level: str,
) -> httpx.Response:
    return await client.post(
        f"/v1/identity/agents/{agent_id}/access",
        headers=_auth(owner_subject, org_id),
        json={"principal_type": "user", "principal_id": principal_id, "level": level},
    )


async def _send_message(
    client: httpx.AsyncClient, subject: str, org_id: str, agent_id: str, session_id: str
) -> httpx.Response:
    return await client.post(
        f"/v1/sessions/{session_id}/messages",
        headers=_auth(subject, org_id, agent_id),
        json={"content": "hello"},
    )


# --- 1/2/3: discover / use / manage tiers, org membership alone is insufficient --------


async def test_member_without_access_edge_cannot_discover_or_use_team_agent(
    client_and_engine: tuple[httpx.AsyncClient, AsyncEngine],
) -> None:
    client, _engine = client_and_engine
    await _login(client, "alice-t1")
    org_id = await _create_org(client, "alice-t1", f"acme-{uuid.uuid4().hex[:8]}")
    bob = await _login(client, "bob-t1")
    add = await client.post(
        "/v1/identity/members",
        headers=_auth("alice-t1", org_id),
        json={"user_id": bob, "role": "member"},
    )
    assert add.status_code == 201, add.text
    agent_id = await _create_team_agent(client, "alice-t1", org_id)

    # Bare org membership no longer discovers the team Agent.
    listed = await client.get("/v1/identity/agents", headers=_auth("bob-t1", org_id))
    assert listed.status_code == 200
    assert listed.json() == []

    select = await client.post(
        f"/v1/identity/agents/{agent_id}/select", headers=_auth("bob-t1", org_id)
    )
    assert select.status_code == 404  # hidden, not merely forbidden (no disclosure)

    session_id = f"s-{uuid.uuid4().hex}"
    sent = await _send_message(client, "bob-t1", org_id, agent_id, session_id)
    assert sent.status_code == 404


async def test_discover_only_cannot_use(
    client_and_engine: tuple[httpx.AsyncClient, AsyncEngine],
) -> None:
    client, _engine = client_and_engine
    await _login(client, "alice-t2")
    org_id = await _create_org(client, "alice-t2", f"acme-{uuid.uuid4().hex[:8]}")
    bob = await _login(client, "bob-t2")
    await client.post(
        "/v1/identity/members",
        headers=_auth("alice-t2", org_id),
        json={"user_id": bob, "role": "member"},
    )
    agent_id = await _create_team_agent(client, "alice-t2", org_id)
    granted = await _grant_access(client, "alice-t2", org_id, agent_id, bob, "discover")
    assert granted.status_code == 201, granted.text

    listed = await client.get("/v1/identity/agents", headers=_auth("bob-t2", org_id))
    assert {a["id"] for a in listed.json()} == {agent_id}

    select = await client.post(
        f"/v1/identity/agents/{agent_id}/select", headers=_auth("bob-t2", org_id)
    )
    assert select.status_code == 403  # visible, but discover does not imply use

    session_id = f"s-{uuid.uuid4().hex}"
    sent = await _send_message(client, "bob-t2", org_id, agent_id, session_id)
    assert sent.status_code == 403


async def test_use_cannot_manage(
    client_and_engine: tuple[httpx.AsyncClient, AsyncEngine],
) -> None:
    client, _engine = client_and_engine
    await _login(client, "alice-t3")
    org_id = await _create_org(client, "alice-t3", f"acme-{uuid.uuid4().hex[:8]}")
    bob = await _login(client, "bob-t3")
    await client.post(
        "/v1/identity/members",
        headers=_auth("alice-t3", org_id),
        json={"user_id": bob, "role": "member"},
    )
    agent_id = await _create_team_agent(client, "alice-t3", org_id)
    granted = await _grant_access(client, "alice-t3", org_id, agent_id, bob, "use")
    assert granted.status_code == 201, granted.text

    # use grants selection/admission...
    session_id = f"s-{uuid.uuid4().hex}"
    sent = await _send_message(client, "bob-t3", org_id, agent_id, session_id)
    assert sent.status_code == 202, sent.text

    # ...but not management of the Agent or its access list.
    update = await client.patch(
        f"/v1/identity/agents/{agent_id}",
        headers=_auth("bob-t3", org_id),
        json={"expected_version": 1, "persona": "hacked"},
    )
    assert update.status_code == 403

    carol = await _login(client, "carol-t3")
    escalate = await _grant_access(client, "bob-t3", org_id, agent_id, carol, "manage")
    assert escalate.status_code == 403


async def test_revoked_access_fails_admission_and_worker_claim(
    client_and_engine: tuple[httpx.AsyncClient, AsyncEngine],
) -> None:
    client, _engine = client_and_engine
    await _login(client, "alice-t4")
    org_id = await _create_org(client, "alice-t4", f"acme-{uuid.uuid4().hex[:8]}")
    bob = await _login(client, "bob-t4")
    await client.post(
        "/v1/identity/members",
        headers=_auth("alice-t4", org_id),
        json={"user_id": bob, "role": "member"},
    )
    agent_id = await _create_team_agent(client, "alice-t4", org_id)
    assert (
        await _grant_access(client, "alice-t4", org_id, agent_id, bob, "use")
    ).status_code == 201

    # Works while active.
    ok = await _send_message(client, "bob-t4", org_id, agent_id, f"s-{uuid.uuid4().hex}")
    assert ok.status_code == 202

    revoke = await client.request(
        "DELETE",
        f"/v1/identity/agents/{agent_id}/access",
        headers=_auth("alice-t4", org_id),
        params={"principal_type": "user", "principal_id": bob},
    )
    assert revoke.status_code == 200
    assert revoke.json()["status"] == "revoked"

    # Fails at (re-)admission — no existence disclosure.
    denied = await _send_message(client, "bob-t4", org_id, agent_id, f"s-{uuid.uuid4().hex}")
    assert denied.status_code == 404

    # The worker's claim-time re-check (IdentityService.select_agent) fails closed too — this
    # is the *exact* call keel_worker.runs._visibility_check performs before executing a
    # claimed run.
    identity: IdentityService = client._transport.app.state.identity  # type: ignore[attr-defined]
    with pytest.raises(NotFoundError):
        await identity.select_agent(org_id, bob, agent_id)


# --- 5: session ownership is independent of Agent access -------------------------------


async def test_private_session_hidden_from_other_agent_user(
    client_and_engine: tuple[httpx.AsyncClient, AsyncEngine],
) -> None:
    client, _engine = client_and_engine
    await _login(client, "alice-t5")
    org_id = await _create_org(client, "alice-t5", f"acme-{uuid.uuid4().hex[:8]}")
    bob = await _login(client, "bob-t5")
    carol = await _login(client, "carol-t5")
    for uid in (bob, carol):
        await client.post(
            "/v1/identity/members",
            headers=_auth("alice-t5", org_id),
            json={"user_id": uid, "role": "member"},
        )
    agent_id = await _create_team_agent(client, "alice-t5", org_id)
    for uid in (bob, carol):
        assert (
            await _grant_access(client, "alice-t5", org_id, agent_id, uid, "use")
        ).status_code == 201

    session_id = f"s-{uuid.uuid4().hex}"
    admitted = await _send_message(client, "bob-t5", org_id, agent_id, session_id)
    assert admitted.status_code == 202
    run_id = admitted.json()["run_id"]

    # Bob (owner) sees his own session.
    bob_list = await client.get("/v1/sessions", headers=_auth("bob-t5", org_id, agent_id))
    assert session_id in {s["id"] for s in bob_list.json()}
    bob_history = await client.get(
        f"/v1/sessions/{session_id}/history", headers=_auth("bob-t5", org_id, agent_id)
    )
    assert bob_history.status_code == 200

    # Carol shares the SAME team Agent (same scope) but must not see Bob's private session.
    carol_list = await client.get("/v1/sessions", headers=_auth("carol-t5", org_id, agent_id))
    assert session_id not in {s["id"] for s in carol_list.json()}
    carol_history = await client.get(
        f"/v1/sessions/{session_id}/history", headers=_auth("carol-t5", org_id, agent_id)
    )
    assert carol_history.status_code == 404
    carol_events = await client.get(
        f"/v1/sessions/{session_id}/events", headers=_auth("carol-t5", org_id, agent_id)
    )
    assert carol_events.status_code == 404
    carol_write = await _send_message(client, "carol-t5", org_id, agent_id, session_id)
    assert carol_write.status_code == 404
    carol_interrupt = await client.post(
        f"/v1/runs/{run_id}/interrupt", headers=_auth("carol-t5", org_id, agent_id)
    )
    assert carol_interrupt.status_code == 404
    carol_steer = await client.post(
        f"/v1/runs/{run_id}/steer",
        headers=_auth("carol-t5", org_id, agent_id),
        json={"text": "inject"},
    )
    assert carol_steer.status_code == 404


# --- 6: agent_members visibility only for Agent users -----------------------------------


async def test_agent_members_visibility_readable_only_by_agent_users(
    client_and_engine: tuple[httpx.AsyncClient, AsyncEngine],
) -> None:
    client, engine = client_and_engine
    await _login(client, "alice-t6")
    org_id = await _create_org(client, "alice-t6", f"acme-{uuid.uuid4().hex[:8]}")
    bob = await _login(client, "bob-t6")
    dave = await _login(client, "dave-t6")
    for uid in (bob, dave):
        await client.post(
            "/v1/identity/members",
            headers=_auth("alice-t6", org_id),
            json={"user_id": uid, "role": "member"},
        )
    agent_id = await _create_team_agent(client, "alice-t6", org_id)
    # Only Bob gets Agent Access; Dave remains a bare member with none.
    assert (
        await _grant_access(client, "alice-t6", org_id, agent_id, bob, "use")
    ).status_code == 201

    scope_id = derive_agent_scope(org_id, agent_id)
    session_id = f"s-{uuid.uuid4().hex}"
    await ensure_session_identity(
        engine,
        scope_id,
        session_id,
        org_id=org_id,
        owner_user_id=None,
        visibility=SessionVisibility.agent_members,
    )
    # A durable event must exist for the session to be "in scope" for the history endpoint;
    # this mirrors how a group-channel/system session actually gets its content (unrelated to
    # the visibility decision itself, which is exercised below).
    await admit(PostgresEventStore(engine, scope_id), session_id, scope_id, "hello")

    # Bob (has Agent Access) can read it.
    bob_history = await client.get(
        f"/v1/sessions/{session_id}/history", headers=_auth("bob-t6", org_id, agent_id)
    )
    assert bob_history.status_code == 200

    # Dave has no Agent Access at all: he cannot even select the Agent, so he never reaches
    # the session (404 from endpoint auth, before the visibility check runs).
    dave_history = await client.get(
        f"/v1/sessions/{session_id}/history", headers=_auth("dave-t6", org_id, agent_id)
    )
    assert dave_history.status_code == 404


# --- 7: explicit share works and revocation removes access ------------------------------


async def test_explicit_share_grant_and_revoke(
    client_and_engine: tuple[httpx.AsyncClient, AsyncEngine],
) -> None:
    client, _engine = client_and_engine
    await _login(client, "alice-t7")
    org_id = await _create_org(client, "alice-t7", f"acme-{uuid.uuid4().hex[:8]}")
    bob = await _login(client, "bob-t7")
    carol = await _login(client, "carol-t7")
    for uid in (bob, carol):
        await client.post(
            "/v1/identity/members",
            headers=_auth("alice-t7", org_id),
            json={"user_id": uid, "role": "member"},
        )
    agent_id = await _create_team_agent(client, "alice-t7", org_id)
    for uid in (bob, carol):
        assert (
            await _grant_access(client, "alice-t7", org_id, agent_id, uid, "use")
        ).status_code == 201

    session_id = f"s-{uuid.uuid4().hex}"
    assert (await _send_message(client, "bob-t7", org_id, agent_id, session_id)).status_code == 202

    # Carol cannot read Bob's private session yet.
    denied = await client.get(
        f"/v1/sessions/{session_id}/history", headers=_auth("carol-t7", org_id, agent_id)
    )
    assert denied.status_code == 404

    # Bob (owner) switches visibility to explicit and shares with Carol.
    vis = await client.patch(
        f"/v1/sessions/{session_id}/visibility",
        headers=_auth("bob-t7", org_id, agent_id),
        json={"visibility": "explicit"},
    )
    assert vis.status_code == 200, vis.text
    share = await client.post(
        f"/v1/sessions/{session_id}/shares",
        headers=_auth("bob-t7", org_id, agent_id),
        json={"user_id": carol},
    )
    assert share.status_code == 201, share.text

    allowed = await client.get(
        f"/v1/sessions/{session_id}/history", headers=_auth("carol-t7", org_id, agent_id)
    )
    assert allowed.status_code == 200

    # Carol can currently see the session (via her active share) but is not the owner and
    # holds no manage authority: she cannot grant a share to a third party.
    dave = await _login(client, "dave-t7")
    await client.post(
        "/v1/identity/members",
        headers=_auth("alice-t7", org_id),
        json={"user_id": dave, "role": "member"},
    )
    carol_grants_dave = await client.post(
        f"/v1/sessions/{session_id}/shares",
        headers=_auth("carol-t7", org_id, agent_id),
        json={"user_id": dave},
    )
    assert carol_grants_dave.status_code == 403

    # Revoking the share removes access again.
    revoke = await client.request(
        "DELETE",
        f"/v1/sessions/{session_id}/shares/{carol}",
        headers=_auth("bob-t7", org_id, agent_id),
    )
    assert revoke.status_code == 200
    audit: InMemoryAuditSink = client._transport.app.state.r1b_audit  # type: ignore[attr-defined]
    actions = {event.action for event in audit.events}
    assert {
        AuditAction.session_visibility_changed,
        AuditAction.session_share_granted,
        AuditAction.session_share_revoked,
    } <= actions
    denied_again = await client.get(
        f"/v1/sessions/{session_id}/history", headers=_auth("carol-t7", org_id, agent_id)
    )
    assert denied_again.status_code == 404

    # Carol can no longer even see the session, so a self-share attempt fails closed with the
    # same non-disclosing 404 (never distinguishing "forbidden" from "does not exist").
    self_share = await client.post(
        f"/v1/sessions/{session_id}/shares",
        headers=_auth("carol-t7", org_id, agent_id),
        json={"user_id": carol},
    )
    assert self_share.status_code == 404


# --- 8: cross-org ids fail without disclosure -------------------------------------------


async def test_cross_org_agent_and_session_ids_fail_without_disclosure(
    client_and_engine: tuple[httpx.AsyncClient, AsyncEngine],
) -> None:
    client, _engine = client_and_engine
    await _login(client, "alice-t8")
    org_a = await _create_org(client, "alice-t8", f"acme-{uuid.uuid4().hex[:8]}")
    agent_a = await _create_team_agent(client, "alice-t8", org_a)

    await _login(client, "dana-t8")
    org_b = await _create_org(client, "dana-t8", f"globex-{uuid.uuid4().hex[:8]}")

    # Alice (org A owner) cannot see org B's agent by id under her own org header.
    cross = await client.get(f"/v1/identity/agents/{agent_a}", headers=_auth("dana-t8", org_b))
    assert cross.status_code == 404

    # Dana is not a member of org A at all: selecting org A is denied, not disclosed.
    no_member = await client.get(
        "/v1/identity/organizations/current", headers=_auth("dana-t8", org_a)
    )
    assert no_member.status_code == 404


# --- 10: retry/crash does not create an ownerless or ambiguous session ------------------


async def test_ensure_session_identity_first_writer_wins(
    client_and_engine: tuple[httpx.AsyncClient, AsyncEngine],
) -> None:
    client, engine = client_and_engine
    alice = await _login(client, "alice-t10")
    org_id = await _create_org(client, "alice-t10", f"acme-{uuid.uuid4().hex[:8]}")
    bob = await _login(client, "bob-t10")
    agent_id = await _create_team_agent(client, "alice-t10", org_id)
    scope_id = derive_agent_scope(org_id, agent_id)
    session_id = f"s-{uuid.uuid4().hex}"

    first = await ensure_session_identity(
        engine,
        scope_id,
        session_id,
        org_id=org_id,
        owner_user_id=alice,
        visibility=SessionVisibility.private,
    )
    assert first.owner_user_id == alice

    # A "retry" that recomputes different values (simulating a hypothetical race / a second
    # admitter) must NOT overwrite the original owner — first-writer-wins, atomic.
    retried = await ensure_session_identity(
        engine,
        scope_id,
        session_id,
        org_id=org_id,
        owner_user_id=bob,
        visibility=SessionVisibility.agent_members,
    )
    assert retried.owner_user_id == alice
    assert retried.visibility is SessionVisibility.private


# --- 9: IM channel/session semantics (private 1:1 vs group channel) ---------------------


async def test_im_admission_wires_private_and_group_session_identity(
    client_and_engine: tuple[httpx.AsyncClient, AsyncEngine],
) -> None:
    """DurableImIngress.admit (keel_server.gateway.durable) sets session identity/visibility
    exactly per the R1B contract: a private 1:1 chat belongs to the run-as user; a group
    channel has no single owner and is bound to the channel identity with agent_members
    visibility (readable only by principals holding an active Agent Access edge)."""
    from keel_core.im_routing import (
        ImChannelMapping,
        ImChatKind,
        ImMappingStatus,
        ImProvider,
        ImReplyPolicy,
        PostgresImMappingStore,
        PostgresImProvisioner,
        PostgresImRouteIndex,
    )
    from keel_core.session_visibility import get_session_identity
    from keel_server.gateway.durable import DurableImIngress, ImInbound

    client, engine = client_and_engine
    alice = await _login(client, "alice-t9")
    org_id = await _create_org(client, "alice-t9", f"acme-{uuid.uuid4().hex[:8]}")
    agent_id = await _create_team_agent(client, "alice-t9", org_id)
    scope_id = derive_agent_scope(org_id, agent_id)

    provisioner = PostgresImProvisioner(engine)
    private_mapping = await provisioner.provision(
        ImChannelMapping(
            id=f"map-{uuid.uuid4().hex[:8]}",
            org_id=org_id,
            provider=ImProvider.telegram,
            external_bot_id="bot-9",
            external_chat_id="private-chat",
            chat_kind=ImChatKind.personal,
            agent_id=agent_id,
            scope_id=scope_id,
            policy=ImReplyPolicy(reply_enabled=True),
            status=ImMappingStatus.active,
            created_by=alice,
            run_as_user_id=alice,
        )
    )
    group_mapping = await provisioner.provision(
        ImChannelMapping(
            id=f"map-{uuid.uuid4().hex[:8]}",
            org_id=org_id,
            provider=ImProvider.telegram,
            external_bot_id="bot-9",
            external_chat_id="group-chat",
            chat_kind=ImChatKind.group,
            agent_id=agent_id,
            scope_id=scope_id,
            policy=ImReplyPolicy(reply_enabled=True),
            status=ImMappingStatus.active,
            created_by=alice,
            run_as_user_id=alice,
        )
    )

    route_index = PostgresImRouteIndex(engine)
    for mapping in (private_mapping, group_mapping):
        await route_index.put(mapping.route_entry())
    mapping_store = PostgresImMappingStore(engine, org_id)

    def _run_service(scope: str) -> Any:
        from keel_core.approvals import PostgresApprovalStore
        from keel_core.loop import admit as _admit
        from keel_core.run_service import DurableRunService
        from keel_core.runs import PostgresRunStore
        from keel_core.state import PostgresEventStore as _PgEventStore

        async def _enqueue(_run_id: str) -> None:
            return None

        return DurableRunService(
            run_store=PostgresRunStore(engine, scope),
            event_store=_PgEventStore(engine, scope),
            approvals=PostgresApprovalStore(engine, scope),
            scope_id=scope,
            enqueue=_enqueue,
            admit_fn=_admit,
        )

    ingress = DurableImIngress(
        route_index=route_index,
        mapping_store_factory=lambda _org: mapping_store,
        run_service_factory=_run_service,
        cloud_mode=True,
        default_model="gpt-4o-mini",
        engine=engine,
    )

    private_inbound = ImInbound(
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="private-chat",
        external_message_id="m-1",
        chat_kind=ImChatKind.personal,
        text="hello privately",
    )
    group_inbound = ImInbound(
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="group-chat",
        external_message_id="m-2",
        chat_kind=ImChatKind.group,
        text="hello group",
    )
    assert await ingress.admit(private_inbound) is not None
    assert await ingress.admit(group_inbound) is not None

    private_identity = await get_session_identity(engine, scope_id, private_inbound.session_id())
    assert private_identity is not None
    assert private_identity.owner_user_id == alice
    assert private_identity.channel_provider == "telegram"
    assert private_identity.visibility is SessionVisibility.private

    group_identity = await get_session_identity(engine, scope_id, group_inbound.session_id())
    assert group_identity is not None
    assert group_identity.owner_user_id is None
    assert group_identity.channel_provider == "telegram"
    assert group_identity.channel_external_id == "group-chat"
    assert group_identity.visibility is SessionVisibility.agent_members
