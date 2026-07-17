"""Durable identity: Postgres store, RLS under keel_runtime, concurrency, erasure (M3.6).

Includes the M3.6 security-review regression coverage: SELECT-only membership self-read RLS
(adversarial ``keel_runtime`` mutation attempts), atomic last-owner protection under
concurrent demotions, atomic grantor revalidation for grants, and the user-erasure org-orphan
policy (block vs. archive).
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.errors import PermissionDenied
from keel_core.identity import (
    AgentKind,
    Capability,
    ConflictError,
    LastOwnerError,
    MembershipRole,
    OptimisticConcurrencyError,
    PostgresIdentityStore,
)
from keel_core.identity.purge import (
    UserErasureBlockedError,
    purge_organization,
    purge_user,
)

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


# --- #1 membership self-read is SELECT-only (adversarial keel_runtime) ----------------


async def test_membership_self_read_is_select_only(migrated_db: AsyncEngine) -> None:
    if not await _runtime_role_available(migrated_db):
        pytest.skip("keel_runtime role not provisioned in this database")
    store = PostgresIdentityStore(migrated_db)
    owner_a, org_a = await _seed_org(store, "orga")
    org_b = await store.create_org(slug="orgb", display_name="B")
    await store.create_membership(org_id=org_b.id, user_id=owner_a.id, role=MembershipRole.member)

    async with migrated_db.connect() as conn:
        await conn.execute(text("SET ROLE keel_runtime"))
        # An attacker holding only ``app.user_id`` (no operating org selected).
        await conn.execute(text("SELECT set_config('app.user_id', :u, false)"), {"u": owner_a.id})
        await conn.execute(text("SELECT set_config('app.org_id', '', false)"))
        # SELECT self-read still works (the legitimate pre-org "list my orgs").
        rows = (await conn.execute(text("SELECT org_id FROM memberships"))).fetchall()
        assert {r[0] for r in rows} == {org_a.id, org_b.id}
        # But UPDATE/DELETE cannot ride the self-read branch: zero cross-org rows are
        # mutable without the correct ``app.org_id`` (mutations are org-isolation only).
        upd = await conn.execute(
            text("UPDATE memberships SET role = 'owner' WHERE user_id = :u"),
            {"u": owner_a.id},
        )
        assert upd.rowcount == 0
        dele = await conn.execute(
            text("DELETE FROM memberships WHERE user_id = :u"), {"u": owner_a.id}
        )
        assert dele.rowcount == 0
        await conn.execute(text("RESET ROLE"))
    # Nothing was mutated: the member role in org B is intact.
    membership = await store.get_membership(org_b.id, owner_a.id)
    assert membership is not None and membership.role is MembershipRole.member


async def test_membership_mutation_requires_operating_org(migrated_db: AsyncEngine) -> None:
    if not await _runtime_role_available(migrated_db):
        pytest.skip("keel_runtime role not provisioned in this database")
    store = PostgresIdentityStore(migrated_db)
    owner_a, org_a = await _seed_org(store, "orga")
    other = await store.create_user(display_name="Other", email="other@x.com")
    await store.create_membership(org_id=org_a.id, user_id=other.id, role=MembershipRole.member)

    async with migrated_db.connect() as conn:
        await conn.execute(text("SET ROLE keel_runtime"))
        # Selecting org A: a mutation inside the operating org is permitted (the policy is
        # not a blanket deny — it is scoped to ``app.org_id``).
        await conn.execute(text("SELECT set_config('app.org_id', :o, false)"), {"o": org_a.id})
        await conn.execute(text("SELECT set_config('app.user_id', '', false)"))
        upd = await conn.execute(
            text("UPDATE memberships SET updated_at = now() WHERE user_id = :u"),
            {"u": other.id},
        )
        assert upd.rowcount == 1
        await conn.execute(text("RESET ROLE"))
    # The connection is closed without commit, so the probe update is rolled back.


# --- #2 atomic last-owner protection under concurrent demotions ----------------------


async def test_concurrent_owner_demotion_keeps_one_owner(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner_a, org = await _seed_org(store, "acme")
    owner_b = await store.create_user(display_name="B", email="b@x.com")
    await store.create_membership(org_id=org.id, user_id=owner_b.id, role=MembershipRole.owner)

    async def demote(user_id: str) -> object:
        try:
            return await store.update_membership_role(
                org.id, user_id, MembershipRole.member, guard_last_owner=True
            )
        except LastOwnerError as exc:
            return exc

    results = await asyncio.gather(demote(owner_a.id), demote(owner_b.id))
    blocked = [r for r in results if isinstance(r, LastOwnerError)]
    # The org-row lock serializes the two demotions: exactly one is refused so an active
    # owner always remains (two concurrent demotions cannot both win).
    assert len(blocked) == 1
    assert await store.count_active_owners(org.id) >= 1


async def test_concurrent_owner_removal_keeps_one_owner(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner_a, org = await _seed_org(store, "acme")
    owner_b = await store.create_user(display_name="B", email="b@x.com")
    await store.create_membership(org_id=org.id, user_id=owner_b.id, role=MembershipRole.owner)

    async def remove(user_id: str) -> object:
        try:
            return await store.revoke_membership(org.id, user_id, guard_last_owner=True)
        except LastOwnerError as exc:
            return exc

    results = await asyncio.gather(remove(owner_a.id), remove(owner_b.id))
    assert len([r for r in results if isinstance(r, LastOwnerError)]) == 1
    assert await store.count_active_owners(org.id) >= 1


# --- #9 atomic grantor revalidation --------------------------------------------------


async def test_grant_blocked_after_admin_revoked(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    admin = await store.create_user(display_name="Admin", email="admin@x.com")
    await store.create_membership(org_id=org.id, user_id=admin.id, role=MembershipRole.admin)
    agent = await store.create_agent(
        org_id=org.id, kind=AgentKind.team, owner_user_id=owner.id, name="Bot"
    )
    # The admin loses their admin membership, then attempts to grant.
    await store.revoke_membership(org.id, admin.id)
    with pytest.raises(PermissionDenied):
        await store.create_grant(
            org_id=org.id,
            agent_id=agent.id,
            resource_type="kb",
            resource_id="kb1",
            capability=Capability.read,
            grantor_user_id=admin.id,
        )


async def test_concurrent_demote_and_grant_are_serialized(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    admin = await store.create_user(display_name="Admin", email="admin@x.com")
    await store.create_membership(org_id=org.id, user_id=admin.id, role=MembershipRole.admin)
    agent = await store.create_agent(
        org_id=org.id, kind=AgentKind.team, owner_user_id=owner.id, name="Bot"
    )

    async def grant() -> object:
        try:
            return await store.create_grant(
                org_id=org.id,
                agent_id=agent.id,
                resource_type="kb",
                resource_id="kb1",
                capability=Capability.read,
                grantor_user_id=admin.id,
            )
        except PermissionDenied as exc:
            return exc

    async def demote() -> object:
        return await store.update_membership_role(
            org.id, admin.id, MembershipRole.member, guard_last_owner=False
        )

    grant_result, _ = await asyncio.gather(grant(), demote())
    # The membership row lock serializes the two: the grant either committed while the admin
    # was still authorized, or it was atomically refused — never committed after demotion.
    active_grants = [g for g in await store.list_grants(org.id) if g.is_active]
    if isinstance(grant_result, PermissionDenied):
        assert active_grants == []
    else:
        # Grant won the race (committed before demotion took the write lock).
        assert len(active_grants) == 1


# --- adjacent: member management is atomic with actor revalidation -------------------


async def test_member_add_blocked_after_admin_revoked(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    admin = await store.create_user(display_name="Admin", email="admin@x.com")
    await store.create_membership(org_id=org.id, user_id=admin.id, role=MembershipRole.admin)
    target = await store.create_user(display_name="T", email="t@x.com")
    # The admin loses their membership; a member-add that revalidates the actor is refused.
    await store.revoke_membership(org.id, admin.id)
    with pytest.raises(PermissionDenied):
        await store.create_membership(
            org_id=org.id,
            user_id=target.id,
            role=MembershipRole.member,
            revalidate_actor_user_id=admin.id,
        )


async def test_role_change_requires_owner_actor_after_demotion(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    admin = await store.create_user(display_name="Admin", email="admin@x.com")
    await store.create_membership(org_id=org.id, user_id=admin.id, role=MembershipRole.admin)
    target = await store.create_user(display_name="T", email="t@x.com")
    await store.create_membership(org_id=org.id, user_id=target.id, role=MembershipRole.member)
    # An admin cannot mint an owner even if a stale check passed: require_owner_actor fails.
    with pytest.raises(PermissionDenied):
        await store.update_membership_role(
            org.id,
            target.id,
            MembershipRole.owner,
            revalidate_actor_user_id=admin.id,
            require_owner_actor=True,
        )


# --- #3 user erasure never orphans an active organization ----------------------------


async def test_user_erasure_blocked_when_org_has_other_members(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    member = await store.create_user(display_name="M", email="m@x.com")
    await store.create_membership(org_id=org.id, user_id=member.id, role=MembershipRole.member)
    with pytest.raises(UserErasureBlockedError) as exc:
        await purge_user(migrated_db, owner.id)
    assert org.id in exc.value.blocking_org_ids
    # Nothing was deleted — the whole erasure aborted atomically.
    assert await store.get_user(owner.id) is not None
    assert await store.get_membership(org.id, owner.id) is not None


async def test_user_erasure_archives_solely_owned_empty_org(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    result = await purge_user(migrated_db, owner.id)
    assert result.user == 1
    assert result.archived_organizations == 1
    # The org is retained but archived (lifecycle-honest): never a live ownerless tenant.
    archived = await store.get_org(org.id)
    assert archived is not None and archived.status.value == "archived"
    assert await store.get_membership(org.id, owner.id) is None


async def test_user_erasure_allowed_when_another_owner_exists(migrated_db: AsyncEngine) -> None:
    store = PostgresIdentityStore(migrated_db)
    owner, org = await _seed_org(store, "acme")
    other = await store.create_user(display_name="O2", email="o2@x.com")
    await store.create_membership(org_id=org.id, user_id=other.id, role=MembershipRole.owner)
    result = await purge_user(migrated_db, owner.id)
    assert result.user == 1
    assert result.archived_organizations == 0
    # The org stays active — the co-owner keeps it alive.
    still = await store.get_org(org.id)
    assert still is not None and still.status.value == "active"
    assert await store.count_active_owners(org.id) == 1
