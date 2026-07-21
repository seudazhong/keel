"""Authorization composition: ownership, membership roles, grants, intersection (M3.6)."""

from __future__ import annotations

from dataclasses import replace

from keel_core.identity import (
    Agent,
    AgentAccess,
    AgentAccessLevel,
    AgentAccessPrincipalType,
    AgentKind,
    AuthorizationService,
    Capability,
    GrantStatus,
    Membership,
    MembershipRole,
    MembershipStatus,
    ResourceGrant,
)

AUTHZ = AuthorizationService()

ORG = "org_a"
OTHER_ORG = "org_b"
ALICE = "usr_alice"
BOB = "usr_bob"


def _member(user: str, role: MembershipRole, org: str = ORG) -> Membership:
    return Membership(id=f"mem_{user}", org_id=org, user_id=user, role=role)


def _agent(kind: AgentKind, owner: str, org: str = ORG) -> Agent:
    return Agent(id="agt_1", org_id=org, kind=kind, owner_user_id=owner, name="A")


def _grant(cap: Capability, agent_id: str = "agt_1", org: str = ORG) -> ResourceGrant:
    return ResourceGrant(
        id=f"grt_{cap}",
        org_id=org,
        agent_id=agent_id,
        resource_type="kb",
        resource_id="kb1",
        capability=cap,
        grantor_user_id=ALICE,
    )


def _access(
    user: str,
    level: AgentAccessLevel,
    agent_id: str = "agt_1",
    org: str = ORG,
    principal_type: AgentAccessPrincipalType = AgentAccessPrincipalType.user,
) -> AgentAccess:
    return AgentAccess(
        id=f"aac_{user}",
        org_id=org,
        agent_id=agent_id,
        principal_type=principal_type,
        principal_id=user,
        level=level,
        grantor_user_id=ALICE,
    )


def test_personal_agent_private_to_owner() -> None:
    agent = _agent(AgentKind.personal, BOB)
    bob = _member(BOB, MembershipRole.member)
    alice_admin = _member(ALICE, MembershipRole.admin)
    alice_member = _member(ALICE, MembershipRole.member)
    # Owner uses it; a non-owner member cannot even view it.
    assert AUTHZ.can_use_agent(BOB, bob, agent)
    assert not AUTHZ.can_view_agent(ALICE, alice_member, agent)
    # An admin can view (manage) but must not silently *use* another's personal agent.
    assert AUTHZ.can_view_agent(ALICE, alice_admin, agent)
    assert not AUTHZ.can_use_agent(ALICE, alice_admin, agent)
    assert AUTHZ.can_manage_agent(ALICE, alice_admin, agent)


def test_team_agent_requires_explicit_access_edge() -> None:
    """R1B: bare org membership no longer implies team-Agent discovery/use."""
    agent = _agent(AgentKind.team, ALICE)
    viewer = _member(BOB, MembershipRole.viewer)
    member = _member(BOB, MembershipRole.member)
    # No Agent Access edge at all: viewer AND member are both denied.
    assert not AUTHZ.can_view_agent(BOB, viewer, agent)
    assert not AUTHZ.can_view_agent(BOB, member, agent)
    assert not AUTHZ.can_use_agent(BOB, member, agent)


def test_team_agent_access_levels_are_ordered() -> None:
    agent = _agent(AgentKind.team, ALICE)
    member = _member(BOB, MembershipRole.member)
    discover_only = [_access(BOB, AgentAccessLevel.discover)]
    use_edge = [_access(BOB, AgentAccessLevel.use)]
    manage_edge = [_access(BOB, AgentAccessLevel.manage)]
    # discover: view yes, use/manage no.
    assert AUTHZ.can_view_agent(BOB, member, agent, discover_only)
    assert not AUTHZ.can_use_agent(BOB, member, agent, discover_only)
    assert not AUTHZ.can_manage_agent(BOB, member, agent, discover_only)
    # use: view+use yes, manage no.
    assert AUTHZ.can_view_agent(BOB, member, agent, use_edge)
    assert AUTHZ.can_use_agent(BOB, member, agent, use_edge)
    assert not AUTHZ.can_manage_agent(BOB, member, agent, use_edge)
    # manage: all three yes (manage implies use implies discover).
    assert AUTHZ.can_view_agent(BOB, member, agent, manage_edge)
    assert AUTHZ.can_use_agent(BOB, member, agent, manage_edge)
    assert AUTHZ.can_manage_agent(BOB, member, agent, manage_edge)


def test_team_agent_revoked_access_edge_denies() -> None:
    agent = _agent(AgentKind.team, ALICE)
    member = _member(BOB, MembershipRole.member)
    revoked = replace(_access(BOB, AgentAccessLevel.use), status=GrantStatus.revoked)
    assert not AUTHZ.can_view_agent(BOB, member, agent, [revoked])
    assert not AUTHZ.can_use_agent(BOB, member, agent, [revoked])


def test_team_agent_channel_principal_does_not_grant_user_access() -> None:
    """A channel-principal edge never authorizes a human actor directly."""
    agent = _agent(AgentKind.team, ALICE)
    member = _member(BOB, MembershipRole.member)
    channel_edge = [
        _access(
            BOB,
            AgentAccessLevel.manage,
            principal_type=AgentAccessPrincipalType.channel,
        )
    ]
    assert not AUTHZ.can_view_agent(BOB, member, agent, channel_edge)


def test_org_admin_keeps_administrative_manage_path() -> None:
    """Org admin/owner retains an implicit manage-equivalent path on team Agents."""
    agent = _agent(AgentKind.team, ALICE)
    admin = _member(BOB, MembershipRole.admin)
    assert AUTHZ.can_view_agent(BOB, admin, agent)
    assert AUTHZ.can_use_agent(BOB, admin, agent)
    assert AUTHZ.can_manage_agent(BOB, admin, agent)


def test_no_membership_denies_everything() -> None:
    agent = _agent(AgentKind.team, ALICE)
    revoked = Membership(
        id="mem_x",
        org_id=ORG,
        user_id=BOB,
        role=MembershipRole.admin,
        status=MembershipStatus.revoked,
    )
    assert not AUTHZ.can_view_agent(BOB, None, agent)
    assert not AUTHZ.can_view_agent(BOB, revoked, agent)


def test_capability_intersection_of_role_and_grant() -> None:
    agent = _agent(AgentKind.team, ALICE)
    member = _member(BOB, MembershipRole.member)  # {read, use}
    use_edge = [_access(BOB, AgentAccessLevel.use)]
    grants = [_grant(Capability.read), _grant(Capability.write)]
    effective = AUTHZ.effective_resource_capabilities(
        BOB, member, agent, grants, "kb", "kb1", use_edge
    )
    # Grant offers read+write, but the member only holds read/use -> write is dropped.
    assert effective == {Capability.read}
    assert AUTHZ.can_agent_access_resource(
        BOB, member, agent, grants, "kb", "kb1", Capability.read, use_edge
    )
    assert not AUTHZ.can_agent_access_resource(
        BOB, member, agent, grants, "kb", "kb1", Capability.write, use_edge
    )


def test_admin_gets_full_intersection() -> None:
    agent = _agent(AgentKind.team, ALICE)
    admin = _member(BOB, MembershipRole.admin)  # {read,use,write,manage}
    grants = [_grant(Capability.write)]
    assert AUTHZ.can_agent_access_resource(BOB, admin, agent, grants, "kb", "kb1", Capability.write)


def test_grant_requires_manage_and_no_cross_org() -> None:
    same_org_agent = _agent(AgentKind.team, ALICE, org=ORG)
    cross_org_agent = _agent(AgentKind.team, ALICE, org=OTHER_ORG)
    admin = _member(ALICE, MembershipRole.admin, org=ORG)
    member = _member(ALICE, MembershipRole.member, org=ORG)
    assert AUTHZ.can_grant_resource(admin, same_org_agent, Capability.read)
    assert not AUTHZ.can_grant_resource(member, same_org_agent, Capability.read)
    # Cross-org grant denied even for an admin.
    assert not AUTHZ.can_grant_resource(admin, cross_org_agent, Capability.read)


def test_grant_on_unrelated_resource_is_empty() -> None:
    agent = _agent(AgentKind.team, ALICE)
    grants = [_grant(Capability.read)]  # for kb/kb1
    assert AUTHZ.granted_capabilities(agent, grants, "kb", "OTHER") == frozenset()
