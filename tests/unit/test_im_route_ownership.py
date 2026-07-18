"""Global IM route ownership: insert/claim, platform-admin provisioning, single-winner races.

Covers the route-ownership hardening (WS-E/J): a global ``(provider, bot, chat)`` route belongs
to exactly one org/mapping (insert/claim, never last-writer-wins); provisioning a route is
**platform-admin-only** (a global machine admin credential), while an ordinary OIDC/org admin may
manage but never first-claim; a concurrent cross-org race yields exactly one winner with no orphan
loser; a non-owner cannot revoke the winner's route; and the webhook still resolves the winner.
Also asserts id-less provider updates are rejected rather than collapsed onto one dedupe key.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from oidc_helpers import make_rsa_key, sign_token

from keel_core.identity import (
    AuditAction,
    IdentityService,
    InMemoryAuditSink,
    InMemoryIdentityStore,
    OIDCConfig,
    OIDCVerifier,
    StaticJWKSProvider,
)
from keel_core.identity.models import AgentKind
from keel_core.im_routing import (
    ImChannelMapping,
    ImChatKind,
    ImProvider,
    InMemoryImMappingStore,
    InMemoryImProvisioner,
    InMemoryImRouteIndex,
    RouteConflictError,
    route_key,
)
from keel_server.app import create_app
from keel_server.auth import parse_api_keys
from keel_server.gateway.durable import parse_onebot_inbound, parse_telegram_inbound

_ISSUER = "https://issuer.example"
_AUD = "keel"
_KEY = make_rsa_key()


def _run[T](coro: Awaitable[T]) -> T:
    return asyncio.run(coro)  # type: ignore[arg-type]


def _mapping(org_id: str, mapping_id: str, *, chat: str = "4242") -> ImChannelMapping:
    agent_id = "agent-1"
    return ImChannelMapping(
        id=mapping_id,
        org_id=org_id,
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id=chat,
        chat_kind=ImChatKind.group,
        agent_id=agent_id,
        scope_id=f"agent:{org_id}/{agent_id}",
        created_by="admin",
    )


# --------------------------------------------------------------------------- core claim/provision


async def test_claim_is_single_owner_and_same_mapping_idempotent() -> None:
    index = InMemoryImRouteIndex()
    m1 = _mapping("org-a", "m1")
    await index.claim(m1.route_entry())
    # Re-claiming the same mapping is idempotent (refreshes its opaque row, no conflict).
    await index.claim(m1.route_entry())
    # A different org/mapping for the same chat is refused and never overwrites the owner.
    m2 = _mapping("org-b", "m2")
    with pytest.raises(RouteConflictError):
        await index.claim(m2.route_entry())
    found = await index.lookup(route_key("telegram", "bot-9", "4242"))
    assert found is not None and found.org_id == "org-a" and found.mapping_id == "m1"


async def test_concurrent_cross_org_race_has_exactly_one_winner_no_orphan() -> None:
    mappings = InMemoryImMappingStore()
    index = InMemoryImRouteIndex()
    provisioner = InMemoryImProvisioner(mappings, index)

    results = await asyncio.gather(
        provisioner.provision(_mapping("org-a", "m-a")),
        provisioner.provision(_mapping("org-b", "m-b")),
        return_exceptions=True,
    )
    winners = [r for r in results if isinstance(r, ImChannelMapping)]
    losers = [r for r in results if isinstance(r, Exception)]
    assert len(winners) == 1 and len(losers) == 1
    assert isinstance(losers[0], RouteConflictError)
    winner = winners[0]

    # The loser left no active orphan mapping (compensated away).
    surviving = await mappings.list_for_org("org-a") + await mappings.list_for_org("org-b")
    assert [m.id for m in surviving] == [winner.id]
    # The webhook route resolves the winner only.
    entry = await index.lookup(route_key("telegram", "bot-9", "4242"))
    assert entry is not None and entry.mapping_id == winner.id and entry.org_id == winner.org_id


async def test_provision_conflict_leaves_existing_route_and_owner_untouched() -> None:
    mappings = InMemoryImMappingStore()
    index = InMemoryImRouteIndex()
    provisioner = InMemoryImProvisioner(mappings, index)
    first = await provisioner.provision(_mapping("org-a", "m-a"))
    with pytest.raises(RouteConflictError):
        await provisioner.provision(_mapping("org-b", "m-b"))
    # org-b's mapping was compensated; org-a still owns the route.
    assert await mappings.list_for_org("org-b") == []
    entry = await index.lookup(route_key("telegram", "bot-9", "4242"))
    assert entry is not None and entry.mapping_id == first.id


# --------------------------------------------------------------------------- API: platform admin


def _seeded_client() -> tuple[TestClient, IdentityService]:
    svc = IdentityService(
        InMemoryIdentityStore(), audit=InMemoryAuditSink(), allow_jit_provisioning=True
    )
    app = create_app()
    app.state.identity = svc
    app.state.oidc_verifier = OIDCVerifier(
        OIDCConfig(issuer=_ISSUER, audiences=frozenset({_AUD})),
        StaticJWKSProvider([_KEY.jwk]),
    )
    app.state.auth_required = True  # cloud mode
    app.state.api_keys = parse_api_keys("gkey:admin:global")
    app.state.engine = None
    # No `with` -> lifespan (runtime/sandbox) is not started; state is injected directly.
    return TestClient(app), svc


def _oidc(subject: str, org: str | None = None) -> dict[str, str]:
    token = sign_token(
        _KEY, issuer=_ISSUER, audience=_AUD, subject=subject, email=f"{subject}@example.com"
    )
    headers = {"Authorization": f"Bearer {token}"}
    if org is not None:
        headers["X-Keel-Org"] = org
    return headers


def _seed_org_agent(svc: IdentityService, *, slug: str) -> tuple[str, str]:
    owner = _run(svc.store.create_user(display_name="Owner", email=None))
    org = _run(svc.create_org(owner.id, slug=slug, display_name=slug.title()))
    agent = _run(
        svc.create_agent(org.org_id, owner.id, kind=AgentKind.team, name="Support", persona="")
    )
    return org.org_id, agent.id


_CHAT_BODY = {
    "provider": "telegram",
    "external_bot_id": "bot-9",
    "external_chat_id": "4242",
    "chat_kind": "group",
}


def test_global_admin_provisions_route_and_is_audited() -> None:
    client, svc = _seeded_client()
    org_id, agent_id = _seed_org_agent(svc, slug="acme")

    resp = client.post(
        "/v1/im/mappings",
        headers={"X-API-Key": "gkey", "X-Keel-Org": org_id},
        json={**_CHAT_BODY, "agent_id": agent_id},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["org_id"] == org_id and body["agent_id"] == agent_id
    assert body["status"] == "active"

    sink = svc.audit
    assert isinstance(sink, InMemoryAuditSink)
    assert AuditAction.im_route_claimed in {e.action for e in sink.events}


def test_oidc_org_owner_cannot_first_claim_a_chat() -> None:
    client, _svc = _seeded_client()
    # Alice logs in (JIT), owns a new org, and owns an Agent in it.
    assert client.get("/v1/identity/me", headers=_oidc("alice")).status_code == 200
    org = client.post(
        "/v1/identity/organizations",
        headers=_oidc("alice"),
        json={"slug": "acme", "display_name": "Acme"},
    )
    org_id = org.json()["organization"]["id"]
    agent = client.post(
        "/v1/identity/agents",
        headers=_oidc("alice", org_id),
        json={"kind": "team", "name": "Support", "persona": ""},
    )
    agent_id = agent.json()["id"]

    # An OIDC org owner may manage but can never first-claim an arbitrary chat: 403.
    resp = client.post(
        "/v1/im/mappings",
        headers=_oidc("alice", org_id),
        json={**_CHAT_BODY, "agent_id": agent_id},
    )
    assert resp.status_code == 403, resp.text


def test_conflicting_claim_is_opaque_409() -> None:
    client, svc = _seeded_client()
    org_a, agent_a = _seed_org_agent(svc, slug="acme")
    org_b, agent_b = _seed_org_agent(svc, slug="globex")

    first = client.post(
        "/v1/im/mappings",
        headers={"X-API-Key": "gkey", "X-Keel-Org": org_a},
        json={**_CHAT_BODY, "agent_id": agent_a},
    )
    assert first.status_code == 201, first.text
    # A second org claiming the same chat fails closed with an opaque 409 (owner not disclosed).
    conflict = client.post(
        "/v1/im/mappings",
        headers={"X-API-Key": "gkey", "X-Keel-Org": org_b},
        json={**_CHAT_BODY, "agent_id": agent_b},
    )
    assert conflict.status_code == 409, conflict.text
    assert org_a not in conflict.text  # the owning org is never leaked


def test_non_owner_cannot_revoke_winners_route() -> None:
    client, svc = _seeded_client()
    org_a, agent_a = _seed_org_agent(svc, slug="acme")

    claim = client.post(
        "/v1/im/mappings",
        headers={"X-API-Key": "gkey", "X-Keel-Org": org_a},
        json={**_CHAT_BODY, "agent_id": agent_a},
    )
    assert claim.status_code == 201, claim.text
    mapping_id = claim.json()["id"]

    # Bob owns a *different* org and tries to revoke org A's mapping — denied (404), route intact.
    assert client.get("/v1/identity/me", headers=_oidc("bob")).status_code == 200
    org_b = client.post(
        "/v1/identity/organizations",
        headers=_oidc("bob"),
        json={"slug": "globex", "display_name": "Globex"},
    ).json()["organization"]["id"]
    denied = client.post(f"/v1/im/mappings/{mapping_id}/revoke", headers=_oidc("bob", org_b))
    assert denied.status_code == 404, denied.text

    index = cast(FastAPI, client.app).state.im_route_index
    entry = _run(index.lookup(route_key("telegram", "bot-9", "4242")))
    assert entry is not None and entry.org_id == org_a and entry.mapping_id == mapping_id


def test_duplicate_same_chat_claim_is_refused_and_route_intact() -> None:
    client, svc = _seeded_client()
    org_a, agent_a = _seed_org_agent(svc, slug="acme")

    claim = client.post(
        "/v1/im/mappings",
        headers={"X-API-Key": "gkey", "X-Keel-Org": org_a},
        json={**_CHAT_BODY, "agent_id": agent_a},
    )
    assert claim.status_code == 201, claim.text
    mapping_id = claim.json()["id"]

    # A second claim for the same chat (even by the same org, a fresh mapping row) is refused —
    # the route already has a single owner — and the existing route is left untouched.
    again = client.post(
        "/v1/im/mappings",
        headers={"X-API-Key": "gkey", "X-Keel-Org": org_a},
        json={**_CHAT_BODY, "agent_id": agent_a},
    )
    assert again.status_code == 409, again.text
    index = cast(FastAPI, client.app).state.im_route_index
    entry = _run(index.lookup(route_key("telegram", "bot-9", "4242")))
    assert entry is not None and entry.mapping_id == mapping_id


# --------------------------------------------------------------------------- id-less dedupe


def test_telegram_idless_update_is_rejected() -> None:
    # A private chat always wakes, but with no stable message_id the update must be rejected
    # rather than admitted with an empty id (which would collapse unrelated messages).
    payload = {"message": {"text": "hi", "chat": {"id": 5, "type": "private"}}}
    got = parse_telegram_inbound(payload, bot_id="mybot", bot_username="mybot", prefixes=())
    assert got is None
    # With a stable id it parses.
    ok = {"message": {"message_id": 11, "text": "hi", "chat": {"id": 5, "type": "private"}}}
    assert parse_telegram_inbound(ok, bot_id="mybot", bot_username="mybot", prefixes=()) is not None


def test_onebot_idless_message_is_rejected() -> None:
    payload = {
        "post_type": "message",
        "message_type": "private",
        "user_id": 7,
        "self_id": 100,
        "raw_message": "hi",
    }  # no message_id
    assert parse_onebot_inbound(payload, self_id=100, prefixes=("/keel",)) is None
