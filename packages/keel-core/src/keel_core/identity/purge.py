"""Durable erasure for identity data — organization + data-subject (M3.6, WS-L).

Two GDPR primitives that complement the M3.5 scope/session/project erasure coordinator
(identity is org-partitioned, not scope-partitioned, so it is erased through this dedicated
path rather than the scope ledger — see ``docs/DATA-LIFECYCLE.md``):

* :meth:`IdentityPurgeRepository.erase_organization` — tenant offboarding. Removes every
  org-owned row (resource grants, Agents, memberships) and the organization itself.
* :meth:`IdentityPurgeRepository.erase_user` — a data subject's erasure. Removes the user's
  OIDC links, the Agents it owns, its memberships across every org, the grants it issued,
  and the user row. Erasure never orphans an active organization: it is **blocked** (with
  :class:`UserErasureBlockedError`, atomically, deleting nothing) when the user is the sole
  active owner of an active org that still has other active members, and it atomically
  **archives** an active org the user solely owns and is the only active member of. All row
  deletes cascade from ``users`` via ``ON DELETE CASCADE``; the explicit, ordered deletes
  make the operation observable + idempotent.

Every method sets ``app.org_id`` (org path) so Postgres RLS is engaged when erasure runs
under the non-owner runtime role. Identity is not event-sourced, so no projection rebuild
can resurrect an erased identity row (there is no event source to replay from).
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.errors import KeelError

_SET_ORG = text("SELECT set_config('app.org_id', :org, true)")


class UserErasureBlockedError(KeelError):
    """Erasing a user would orphan an active organization it solely owns.

    A user who is the *sole active owner* of an active org that still has **other** active
    members cannot be erased until ownership is transferred (or those members are removed):
    silently deleting the owner would leave a live, ownerless tenant. The blocking org ids
    are exposed so an operator can act. No rows are deleted when this is raised (the whole
    erasure is atomic — it either fully proceeds or fully aborts).
    """

    def __init__(self, blocking_org_ids: tuple[str, ...]) -> None:
        self.blocking_org_ids = blocking_org_ids
        super().__init__(
            "user erasure blocked: transfer ownership of solely-owned active organizations "
            f"first: {', '.join(blocking_org_ids)}"
        )


@dataclass(frozen=True)
class OrganizationErasureResult:
    resource_grants: int
    agents: int
    memberships: int
    organization: int

    @property
    def total(self) -> int:
        return self.resource_grants + self.agents + self.memberships + self.organization


@dataclass(frozen=True)
class UserErasureResult:
    oidc_identities: int
    agents: int
    memberships: int
    resource_grants: int
    user: int
    archived_organizations: int = 0

    @property
    def total(self) -> int:
        return (
            self.oidc_identities + self.agents + self.memberships + self.resource_grants + self.user
        )


async def purge_organization(engine: AsyncEngine, org_id: str) -> OrganizationErasureResult:
    """Erase all rows owned by an organization (idempotent). Returns per-store counts."""
    async with engine.begin() as conn:
        await conn.execute(_SET_ORG, {"org": org_id})
        grants = await conn.execute(
            text("DELETE FROM resource_grants WHERE org_id = :org"), {"org": org_id}
        )
        agents = await conn.execute(text("DELETE FROM agents WHERE org_id = :org"), {"org": org_id})
        members = await conn.execute(
            text("DELETE FROM memberships WHERE org_id = :org"), {"org": org_id}
        )
        org = await conn.execute(text("DELETE FROM organizations WHERE id = :org"), {"org": org_id})
    return OrganizationErasureResult(
        resource_grants=int(grants.rowcount or 0),
        agents=int(agents.rowcount or 0),
        memberships=int(members.rowcount or 0),
        organization=int(org.rowcount or 0),
    )


# Active orgs the user solely owns (no OTHER active owner), split by whether any OTHER
# active member remains. ``other_members`` > 0 -> a transfer target exists, so blocking is
# the honest policy; ``other_members`` == 0 -> nobody else to own it, so the org is safely
# archived (lifecycle-honest: retained as archived, never a live ownerless tenant).
_SOLELY_OWNED_ACTIVE_ORGS = text(
    "SELECT m.org_id AS org_id, "
    "  (SELECT count(*) FROM memberships mm "
    "     WHERE mm.org_id = m.org_id AND mm.status = 'active' AND mm.user_id <> :user) "
    "     AS other_members "
    "FROM memberships m "
    "JOIN organizations o ON o.id = m.org_id "
    "WHERE m.user_id = :user AND m.status = 'active' AND m.role = 'owner' "
    "  AND o.status = 'active' "
    "  AND NOT EXISTS ("
    "    SELECT 1 FROM memberships o2 "
    "    WHERE o2.org_id = m.org_id AND o2.status = 'active' AND o2.role = 'owner' "
    "      AND o2.user_id <> :user"
    "  ) "
    "FOR UPDATE OF m"
)


async def purge_user(engine: AsyncEngine, user_id: str) -> UserErasureResult:
    """Erase a user's global identity + every org-owned row it owns/issued (idempotent).

    Never orphans an active organization: if the user is the sole active owner of an active
    org that still has other active members, the whole erasure is aborted with
    :class:`UserErasureBlockedError` (ownership must be transferred first). An active org the
    user solely owns *and* is the only active member of is atomically archived as part of the
    same transaction (nobody else can own it), so lifecycle stays honest and no live tenant
    is left ownerless.
    """
    async with engine.begin() as conn:
        rows = (await conn.execute(_SOLELY_OWNED_ACTIVE_ORGS, {"user": user_id})).mappings().all()
        blocking = tuple(r["org_id"] for r in rows if int(r["other_members"]) > 0)
        if blocking:
            raise UserErasureBlockedError(blocking)
        archivable = [r["org_id"] for r in rows if int(r["other_members"]) == 0]
        archived = 0
        for org_id in archivable:
            result = await conn.execute(
                text(
                    "UPDATE organizations SET status = 'archived', archived_at = now(), "
                    "updated_at = now() WHERE id = :org AND status = 'active'"
                ),
                {"org": org_id},
            )
            archived += int(result.rowcount or 0)
        # Grants issued by the user, and grants bound to Agents the user owns.
        grants = await conn.execute(
            text(
                "DELETE FROM resource_grants g "
                "WHERE g.grantor_user_id = :user "
                "OR g.agent_id IN (SELECT id FROM agents WHERE owner_user_id = :user)"
            ),
            {"user": user_id},
        )
        agents = await conn.execute(
            text("DELETE FROM agents WHERE owner_user_id = :user"), {"user": user_id}
        )
        members = await conn.execute(
            text("DELETE FROM memberships WHERE user_id = :user"), {"user": user_id}
        )
        oidc = await conn.execute(
            text("DELETE FROM oidc_identities WHERE user_id = :user"), {"user": user_id}
        )
        user = await conn.execute(text("DELETE FROM users WHERE id = :user"), {"user": user_id})
    return UserErasureResult(
        oidc_identities=int(oidc.rowcount or 0),
        agents=int(agents.rowcount or 0),
        memberships=int(members.rowcount or 0),
        resource_grants=int(grants.rowcount or 0),
        user=int(user.rowcount or 0),
        archived_organizations=archived,
    )


class IdentityPurgeRepository:
    """Durable identity erasure (organization offboarding + data-subject erasure)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def erase_organization(self, org_id: str) -> OrganizationErasureResult:
        return await purge_organization(self._engine, org_id)

    async def erase_user(self, user_id: str) -> UserErasureResult:
        return await purge_user(self._engine, user_id)


__all__ = [
    "IdentityPurgeRepository",
    "OrganizationErasureResult",
    "UserErasureBlockedError",
    "UserErasureResult",
    "purge_organization",
    "purge_user",
]
