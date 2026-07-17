"""In-memory store guard behaviour: last-owner + grantor revalidation (M3.6 review #2/#9).

The durable Postgres store enforces these atomically under locks (see the integration
suite); here we assert the same fail-closed semantics in the dependency-free store so the
guards are exercised without a database.
"""

from __future__ import annotations

import pytest

from keel_core.errors import PermissionDenied
from keel_core.identity import (
    AgentKind,
    Capability,
    InMemoryIdentityStore,
    LastOwnerError,
    MembershipRole,
)


async def _org_with_owner() -> tuple[InMemoryIdentityStore, str, str]:
    store = InMemoryIdentityStore()
    owner = await store.create_user(display_name="Owner", email="o@x.com")
    org = await store.create_org(slug="acme", display_name="Acme")
    await store.create_membership(org_id=org.id, user_id=owner.id, role=MembershipRole.owner)
    return store, org.id, owner.id


async def test_guard_blocks_sole_owner_demotion() -> None:
    store, org_id, owner_id = await _org_with_owner()
    with pytest.raises(LastOwnerError):
        await store.update_membership_role(
            org_id, owner_id, MembershipRole.member, guard_last_owner=True
        )
    with pytest.raises(LastOwnerError):
        await store.revoke_membership(org_id, owner_id, guard_last_owner=True)


async def test_guard_allows_demotion_when_another_owner_exists() -> None:
    store, org_id, owner_id = await _org_with_owner()
    second = await store.create_user(display_name="Second", email="s@x.com")
    await store.create_membership(org_id=org_id, user_id=second.id, role=MembershipRole.owner)
    demoted = await store.update_membership_role(
        org_id, owner_id, MembershipRole.member, guard_last_owner=True
    )
    assert demoted is not None and demoted.role is MembershipRole.member


async def test_create_grant_revalidates_grantor_membership() -> None:
    store, org_id, owner_id = await _org_with_owner()
    admin = await store.create_user(display_name="Admin", email="a@x.com")
    await store.create_membership(org_id=org_id, user_id=admin.id, role=MembershipRole.admin)
    agent = await store.create_agent(
        org_id=org_id,
        kind=AgentKind.team,
        owner_user_id=owner_id,
        name="Bot",
    )
    # The admin is demoted/revoked before the grant is attempted.
    await store.revoke_membership(org_id, admin.id)
    with pytest.raises(PermissionDenied):
        await store.create_grant(
            org_id=org_id,
            agent_id=agent.id,
            resource_type="kb",
            resource_id="kb1",
            capability=Capability.read,
            grantor_user_id=admin.id,
        )


async def test_revoke_grant_revalidates_actor_membership() -> None:
    store, org_id, owner_id = await _org_with_owner()
    agent = await store.create_agent(
        org_id=org_id, kind=AgentKind.team, owner_user_id=owner_id, name="Bot"
    )
    grant = await store.create_grant(
        org_id=org_id,
        agent_id=agent.id,
        resource_type="kb",
        resource_id="kb1",
        capability=Capability.read,
        grantor_user_id=owner_id,
    )
    member = await store.create_user(display_name="Member", email="m@x.com")
    await store.create_membership(org_id=org_id, user_id=member.id, role=MembershipRole.member)
    with pytest.raises(PermissionDenied):
        await store.revoke_grant(org_id, grant.id, actor_user_id=member.id)


async def test_create_membership_revalidates_actor() -> None:
    store, org_id, owner_id = await _org_with_owner()
    admin = await store.create_user(display_name="Admin", email="a@x.com")
    await store.create_membership(org_id=org_id, user_id=admin.id, role=MembershipRole.admin)
    target = await store.create_user(display_name="T", email="t@x.com")
    await store.revoke_membership(org_id, admin.id)
    with pytest.raises(PermissionDenied):
        await store.create_membership(
            org_id=org_id,
            user_id=target.id,
            role=MembershipRole.member,
            revalidate_actor_user_id=admin.id,
        )


async def test_owner_only_role_change_requires_owner_actor() -> None:
    store, org_id, owner_id = await _org_with_owner()
    admin = await store.create_user(display_name="Admin", email="a@x.com")
    await store.create_membership(org_id=org_id, user_id=admin.id, role=MembershipRole.admin)
    target = await store.create_user(display_name="T", email="t@x.com")
    await store.create_membership(org_id=org_id, user_id=target.id, role=MembershipRole.member)
    with pytest.raises(PermissionDenied):
        await store.update_membership_role(
            org_id,
            target.id,
            MembershipRole.owner,
            revalidate_actor_user_id=admin.id,
            require_owner_actor=True,
        )
