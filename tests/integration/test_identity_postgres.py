"""Durable identity: Postgres store, RLS under keel_runtime, concurrency, erasure (M3.6)."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.identity import (
    AgentKind,
    Capability,
    ConflictError,
    MembershipRole,
    OptimisticConcurrencyError,
    PostgresIdentityStore,
)
from keel_core.identity.purge import purge_organization, purge_user

pytestmark = pytest.mark.integration


async def _seed_org(store: PostgresIdentityStore, slug: str):
    owner = await store.create_user(display_name="Owner", email=f"owner-{slug}@x.com")
    org = await store.create_org(slug=slug, display_name=slug.title())
    await store.create_membership(org_id=org.id, user_id=owner.id, role=MembershipRole.owner)
    return owner, org


async def test_store_roundtrip(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    agent = await store.create_agent(
        org_id=org.id, kind=AgentKind.team, owner_user_id=owner.id, name="Bot", persona="p"
    )
    grant = await store.create_grant(
        org_id=org.id,
        agent_id=agent.id,
        resource_type="kb",
        resource_id="kb1",
        capability=Capability.read,
        grantor_user_id=owner.id,
    )
    assert (await store.get_agent(org.id, agent.id)).name == "Bot"  # type: ignore[union-attr]
    assert [g.id for g in await store.list_grants(org.id)] == [grant.id]
    membership = await store.get_membership(org.id, owner.id)
    assert membership is not None and membership.role is MembershipRole.owner
    # A user can enumerate its own memberships (self-service, across orgs).
    assert [m.org_id for m in await store.list_memberships_for_user(owner.id)] == [org.id]


async def test_oidc_link_idempotent(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    user = await store.create_user(display_name="U", email="u@x.com")
    first = await store.link_identity(
        user_id=user.id, issuer="https://iss", subject="sub-1", email="u@x.com"
    )
    again = await store.link_identity(
        user_id=user.id, issuer="https://iss", subject="sub-1", email="u@x.com"
    )
    assert first.id == again.id
    assert (await store.get_identity("https://iss", "sub-1")).user_id == user.id  # type: ignore[union-attr]


async def _runtime_role_available(engine: AsyncEngine) -> bool:
    async with engine.connect() as conn:
        found = await conn.scalar(text("SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime'"))
    return bool(found)


async def test_rls_isolates_orgs_under_runtime_role(migrated_db: AsyncEngine) -> None:
    if not await _runtime_role_available(migrated_db):
        pytest.skip("keel_runtime role not provisioned in this database")
    store = PostgresIdentityStore(migrated_db)
    owner_a, org_a = await _seed_org(store, "orga")
    owner_b, org_b = await _seed_org(store, "orgb")
    await store.create_agent(
        org_id=org_a.id, kind=AgentKind.team, owner_user_id=owner_a.id, name="Aagent"
    )
    await store.create_agent(
        org_id=org_b.id, kind=AgentKind.team, owner_user_id=owner_b.id, name="Bagent"
    )

    async with migrated_db.connect() as conn:
        await conn.execute(text("SET ROLE keel_runtime"))
        await conn.execute(text("SELECT set_config('app.org_id', :o, false)"), {"o": org_a.id})
        rows_a = (await conn.execute(text("SELECT org_id FROM agents"))).fetchall()
        assert {r[0] for r in rows_a} == {org_a.id}

        await conn.execute(text("SELECT set_config('app.org_id', :o, false)"), {"o": org_b.id})
        rows_b = (await conn.execute(text("SELECT org_id FROM agents"))).fetchall()
        assert {r[0] for r in rows_b} == {org_b.id}  # org A invisible

        await conn.execute(text("SELECT set_config('app.org_id', '', false)"))
        rows_none = (await conn.execute(text("SELECT org_id FROM agents"))).fetchall()
        assert rows_none == []  # deny-by-default with no org selected
        await conn.execute(text("RESET ROLE"))


async def test_membership_self_read_rls(migrated_db: AsyncEngine) -> None:
    if not await _runtime_role_available(migrated_db):
        pytest.skip("keel_runtime role not provisioned in this database")
    store = PostgresIdentityStore(migrated_db)
    owner_a, org_a = await _seed_org(store, "orga")
    # The same user also belongs to a second org.
    org_b = await store.create_org(slug="orgb", display_name="B")
    await store.create_membership(org_id=org_b.id, user_id=owner_a.id, role=MembershipRole.member)

    async with migrated_db.connect() as conn:
        await conn.execute(text("SET ROLE keel_runtime"))
        # Only the user GUC is set (no org selected): the self-read branch returns the
        # user's memberships across both orgs.
        await conn.execute(text("SELECT set_config('app.user_id', :u, false)"), {"u": owner_a.id})
        await conn.execute(text("SELECT set_config('app.org_id', '', false)"))
        rows = (await conn.execute(text("SELECT org_id FROM memberships"))).fetchall()
        assert {r[0] for r in rows} == {org_a.id, org_b.id}
        await conn.execute(text("RESET ROLE"))


async def test_cross_org_grant_fk_rejected(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner_a, org_a = await _seed_org(store, "orga")
    owner_b, org_b = await _seed_org(store, "orgb")
    agent_a = await store.create_agent(
        org_id=org_a.id, kind=AgentKind.team, owner_user_id=owner_a.id, name="Aagent"
    )
    # Claiming org B while referencing an agent from org A must fail the composite FK.
    with pytest.raises(ConflictError):
        await store.create_grant(
            org_id=org_b.id,
            agent_id=agent_a.id,
            resource_type="kb",
            resource_id="kb1",
            capability=Capability.read,
            grantor_user_id=owner_b.id,
        )


async def test_duplicate_membership_conflict(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    user = await store.create_user(display_name="X", email="x@x.com")
    await store.create_membership(org_id=org.id, user_id=user.id, role=MembershipRole.member)
    with pytest.raises(ConflictError):
        await store.create_membership(org_id=org.id, user_id=user.id, role=MembershipRole.viewer)


async def test_concurrent_membership_writes(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    user = await store.create_user(display_name="X", email="x@x.com")

    async def add() -> object:
        try:
            return await store.create_membership(
                org_id=org.id, user_id=user.id, role=MembershipRole.member
            )
        except ConflictError as exc:
            return exc

    results = await asyncio.gather(add(), add())
    conflicts = [r for r in results if isinstance(r, ConflictError)]
    assert len(conflicts) == 1  # the unique (org_id, user_id) index admits exactly one


async def test_concurrent_agent_update_optimistic(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    agent = await store.create_agent(
        org_id=org.id, kind=AgentKind.team, owner_user_id=owner.id, name="Bot"
    )

    async def bump(persona: str) -> object:
        try:
            return await store.update_agent(org.id, agent.id, expected_version=1, persona=persona)
        except OptimisticConcurrencyError as exc:
            return exc

    results = await asyncio.gather(bump("a"), bump("b"))
    losers = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    assert len(losers) == 1  # exactly one writer wins the version race


async def test_concurrent_grant_is_idempotent(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    agent = await store.create_agent(
        org_id=org.id, kind=AgentKind.team, owner_user_id=owner.id, name="Bot"
    )

    async def grant() -> object:
        return await store.create_grant(
            org_id=org.id,
            agent_id=agent.id,
            resource_type="kb",
            resource_id="kb1",
            capability=Capability.read,
            grantor_user_id=owner.id,
        )

    await asyncio.gather(grant(), grant())
    grants = await store.list_grants(org.id)
    assert len(grants) == 1  # ON CONFLICT collapses to one active grant


async def test_organization_erasure_is_complete(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    agent = await store.create_agent(
        org_id=org.id, kind=AgentKind.team, owner_user_id=owner.id, name="Bot"
    )
    await store.create_grant(
        org_id=org.id,
        agent_id=agent.id,
        resource_type="kb",
        resource_id="kb1",
        capability=Capability.read,
        grantor_user_id=owner.id,
    )
    result = await purge_organization(migrated_db, org.id)
    assert result.organization == 1
    assert result.agents == 1 and result.resource_grants == 1 and result.memberships == 1
    # Everything org-owned is gone; the (global) user identity survives.
    assert await store.get_org(org.id) is None
    assert await store.get_agent(org.id, agent.id) is None
    assert await store.list_grants(org.id) == []
    assert (await store.get_user(owner.id)) is not None


async def test_user_erasure_cascades(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    await store.link_identity(
        user_id=owner.id, issuer="https://iss", subject="sub-owner", email="owner@x.com"
    )
    agent = await store.create_agent(
        org_id=org.id, kind=AgentKind.personal, owner_user_id=owner.id, name="Priv"
    )
    await store.create_grant(
        org_id=org.id,
        agent_id=agent.id,
        resource_type="kb",
        resource_id="kb1",
        capability=Capability.read,
        grantor_user_id=owner.id,
    )
    result = await purge_user(migrated_db, owner.id)
    assert result.user == 1
    assert result.oidc_identities == 1 and result.agents == 1
    assert result.memberships == 1 and result.resource_grants == 1
    assert await store.get_user(owner.id) is None
    assert await store.get_identity("https://iss", "sub-owner") is None
    assert await store.get_agent(org.id, agent.id) is None
    assert await store.get_membership(org.id, owner.id) is None
