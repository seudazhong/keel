"""IdentityService: provisioning, org selection, roles, agents, grants, audit (M3.6)."""

from __future__ import annotations

import pytest

from keel_core.errors import PermissionDenied
from keel_core.identity import (
    AgentAccessLevel,
    AgentAccessPrincipalType,
    AgentKind,
    Capability,
    ConflictError,
    IdentityService,
    InMemoryAuditSink,
    InMemoryIdentityStore,
    LastOwnerError,
    MembershipRole,
    NotFoundError,
    OIDCClaims,
    OptimisticConcurrencyError,
)


def _claims(subject: str, email: str | None = "u@example.com") -> OIDCClaims:
    return OIDCClaims(
        issuer="https://issuer.example",
        subject=subject,
        audience=("keel",),
        email=email,
        email_verified=email is not None,
        expires_at=0,
        issued_at=0,
    )


async def _svc(*, jit: bool = False):
    store = InMemoryIdentityStore()
    audit = InMemoryAuditSink()
    return IdentityService(store, audit=audit, allow_jit_provisioning=jit), store, audit


async def test_jit_provisioning_disabled_by_default() -> None:
    svc, _store, _audit = await _svc(jit=False)
    with pytest.raises(NotFoundError):
        await svc.resolve_oidc_user(_claims("new-subject"))


async def test_jit_provisioning_when_enabled() -> None:
    svc, store, _audit = await _svc(jit=True)
    user = await svc.resolve_oidc_user(_claims("new-subject", "jit@example.com"))
    assert user.email == "jit@example.com"
    # A second login resolves the same durable user (idempotent link).
    again = await svc.resolve_oidc_user(_claims("new-subject", "jit@example.com"))
    assert again.id == user.id


async def test_org_selection_rejects_non_member_and_spoofing() -> None:
    svc, store, _audit = await _svc()
    alice = await store.create_user(display_name="Alice", email="a@x.com")
    bob = await store.create_user(display_name="Bob", email="b@x.com")
    ctx = await svc.create_org(alice.id, slug="acme", display_name="Acme")
    # Bob is not a member: same 'not found' as an unknown org (no disclosure).
    with pytest.raises(NotFoundError):
        await svc.select_org(bob.id, ctx.org_id)
    with pytest.raises(NotFoundError):
        await svc.select_org(alice.id, "org_does_not_exist")
    # Alice (owner) resolves by id and by slug.
    assert (await svc.select_org(alice.id, ctx.org_id)).org_id == ctx.org_id
    assert (await svc.select_org(alice.id, "acme")).org_id == ctx.org_id


async def test_last_owner_protection() -> None:
    svc, store, _audit = await _svc()
    alice = await store.create_user(display_name="Alice", email="a@x.com")
    bob = await store.create_user(display_name="Bob", email="b@x.com")
    ctx = await svc.create_org(alice.id, slug="acme", display_name="Acme")
    org = ctx.org_id
    # Cannot demote or remove the sole owner.
    with pytest.raises(LastOwnerError):
        await svc.change_member_role(org, alice.id, alice.id, MembershipRole.member)
    with pytest.raises(LastOwnerError):
        await svc.remove_member(org, alice.id, alice.id)
    # Add a second owner, then the first can be demoted.
    await svc.add_member(org, alice.id, bob.id, MembershipRole.owner)
    demoted = await svc.change_member_role(org, alice.id, alice.id, MembershipRole.member)
    assert demoted.role is MembershipRole.member


async def test_only_owner_grants_owner_role() -> None:
    svc, store, _audit = await _svc()
    alice = await store.create_user(display_name="Alice", email="a@x.com")
    bob = await store.create_user(display_name="Bob", email="b@x.com")
    carol = await store.create_user(display_name="Carol", email="c@x.com")
    ctx = await svc.create_org(alice.id, slug="acme", display_name="Acme")
    org = ctx.org_id
    await svc.add_member(org, alice.id, bob.id, MembershipRole.admin)
    # An admin cannot mint an owner.
    with pytest.raises(PermissionDenied):
        await svc.add_member(org, bob.id, carol.id, MembershipRole.owner)


async def test_member_cannot_manage_members() -> None:
    svc, store, _audit = await _svc()
    alice = await store.create_user(display_name="Alice", email="a@x.com")
    bob = await store.create_user(display_name="Bob", email="b@x.com")
    carol = await store.create_user(display_name="Carol", email="c@x.com")
    ctx = await svc.create_org(alice.id, slug="acme", display_name="Acme")
    org = ctx.org_id
    await svc.add_member(org, alice.id, bob.id, MembershipRole.member)
    with pytest.raises(PermissionDenied):
        await svc.add_member(org, bob.id, carol.id, MembershipRole.member)


async def test_personal_agent_isolation_and_team_sharing() -> None:
    svc, store, _audit = await _svc()
    alice = await store.create_user(display_name="Alice", email="a@x.com")
    bob = await store.create_user(display_name="Bob", email="b@x.com")
    carol = await store.create_user(display_name="Carol", email="c@x.com")
    ctx = await svc.create_org(alice.id, slug="acme", display_name="Acme")
    org = ctx.org_id
    await svc.add_member(org, alice.id, bob.id, MembershipRole.member)
    await svc.add_member(org, alice.id, carol.id, MembershipRole.member)
    team = await svc.create_agent(org, alice.id, kind=AgentKind.team, name="Shared")
    personal = await svc.create_agent(org, bob.id, kind=AgentKind.personal, name="BobBot")
    # A peer member cannot even see Bob's personal agent (hidden -> not found).
    with pytest.raises(NotFoundError):
        await svc.get_agent(org, carol.id, personal.id)
    with pytest.raises(NotFoundError):
        await svc.list_agent_access(org, carol.id, agent_id=personal.id)
    with pytest.raises(NotFoundError):
        await svc.grant_agent_access(
            org,
            carol.id,
            agent_id=personal.id,
            principal_type=AgentAccessPrincipalType.user,
            principal_id=carol.id,
            level=AgentAccessLevel.discover,
        )
    with pytest.raises(NotFoundError):
        await svc.revoke_agent_access(
            org,
            carol.id,
            agent_id=personal.id,
            principal_type=AgentAccessPrincipalType.user,
            principal_id=carol.id,
        )
    # R1B: bare org membership does not grant team-Agent discovery — Carol has no edge yet.
    assert {a.id for a in await svc.list_visible_agents(org, carol.id)} == set()
    with pytest.raises(NotFoundError):
        await svc.select_agent(org, bob.id, team.id)
    # The org owner can see Bob's personal agent (for management) but cannot silently *use* it.
    with pytest.raises(PermissionDenied):
        await svc.select_agent(org, alice.id, personal.id)
    # Alice (admin/owner) grants Bob 'use' Agent Access on the team Agent.
    await svc.grant_agent_access(
        org,
        alice.id,
        agent_id=team.id,
        principal_type=AgentAccessPrincipalType.user,
        principal_id=bob.id,
        level=AgentAccessLevel.use,
    )
    # Team agent is now usable by Bob (member with an active access edge).
    used = await svc.select_agent(org, bob.id, team.id)
    assert used.id == team.id
    # Carol still has no edge: neither discover nor use.
    assert {a.id for a in await svc.list_visible_agents(org, carol.id)} == set()
    with pytest.raises(NotFoundError):
        await svc.select_agent(org, carol.id, team.id)
    # The owner uses their own personal agent.
    assert (await svc.select_agent(org, bob.id, personal.id)).id == personal.id
    # Revoking Bob's access takes effect immediately.
    await svc.revoke_agent_access(
        org,
        alice.id,
        agent_id=team.id,
        principal_type=AgentAccessPrincipalType.user,
        principal_id=bob.id,
    )
    with pytest.raises(NotFoundError):
        await svc.select_agent(org, bob.id, team.id)


async def test_optimistic_concurrency_on_agent_update() -> None:
    svc, store, _audit = await _svc()
    alice = await store.create_user(display_name="Alice", email="a@x.com")
    ctx = await svc.create_org(alice.id, slug="acme", display_name="Acme")
    org = ctx.org_id
    agent = await svc.create_agent(org, alice.id, kind=AgentKind.team, name="Bot")
    updated = await svc.update_agent(org, alice.id, agent.id, expected_version=1, persona="hi")
    assert updated.version == 2
    # A second writer with the stale version loses the race.
    with pytest.raises(OptimisticConcurrencyError):
        await svc.update_agent(org, alice.id, agent.id, expected_version=1, persona="stale")


async def test_duplicate_agent_name_conflict() -> None:
    svc, store, _audit = await _svc()
    alice = await store.create_user(display_name="Alice", email="a@x.com")
    ctx = await svc.create_org(alice.id, slug="acme", display_name="Acme")
    org = ctx.org_id
    await svc.create_agent(org, alice.id, kind=AgentKind.team, name="Bot")
    with pytest.raises(ConflictError):
        await svc.create_agent(org, alice.id, kind=AgentKind.team, name="bot")


async def test_grant_flow_and_audit_has_no_sensitive_detail() -> None:
    svc, store, audit = await _svc()
    alice = await store.create_user(display_name="Alice", email="a@x.com")
    bob = await store.create_user(display_name="Bob", email="b@x.com")
    ctx = await svc.create_org(alice.id, slug="acme", display_name="Acme")
    org = ctx.org_id
    await svc.add_member(org, alice.id, bob.id, MembershipRole.member)
    agent = await svc.create_agent(
        org, alice.id, kind=AgentKind.team, name="Bot", persona="secret system prompt"
    )
    # Member cannot grant.
    with pytest.raises(PermissionDenied):
        await svc.grant_resource(
            org,
            bob.id,
            agent_id=agent.id,
            resource_type="kb",
            resource_id="kb1",
            capability=Capability.read,
        )
    grant = await svc.grant_resource(
        org,
        alice.id,
        agent_id=agent.id,
        resource_type="kb",
        resource_id="kb1",
        capability=Capability.read,
    )
    assert grant.capability is Capability.read
    listed = await svc.list_grants(org, alice.id, agent_id=agent.id)
    assert [g.id for g in listed] == [grant.id]
    revoked = await svc.revoke_grant(org, alice.id, grant.id)
    assert not revoked.is_active
    # No audit record leaks the persona / prompt text or any forbidden key.
    for event in audit.events:
        assert "secret system prompt" not in str(dict(event.details))
        assert "persona" not in event.details


async def test_cross_org_grant_impossible_via_service() -> None:
    svc, store, _audit = await _svc()
    alice = await store.create_user(display_name="Alice", email="a@x.com")
    org_a = await svc.create_org(alice.id, slug="acme", display_name="Acme")
    org_b = await svc.create_org(alice.id, slug="beta", display_name="Beta")
    agent_b = await svc.create_agent(org_b.org_id, alice.id, kind=AgentKind.team, name="Bee")
    # Operating in org A, granting for an agent from org B -> the agent is not found in A.
    with pytest.raises(NotFoundError):
        await svc.grant_resource(
            org_a.org_id,
            alice.id,
            agent_id=agent_b.id,
            resource_type="kb",
            resource_id="kb1",
            capability=Capability.read,
        )
