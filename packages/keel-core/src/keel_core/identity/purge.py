"""Durable erasure for identity data — organization + data-subject (M3.6, WS-L).

Two GDPR primitives that complement the M3.5 scope/session/project erasure coordinator
(identity is org-partitioned, not scope-partitioned, so it is erased through this dedicated
path rather than the scope ledger — see ``docs/DATA-LIFECYCLE.md``):

* :meth:`IdentityPurgeRepository.erase_organization` — tenant offboarding. Removes every
  org-owned row (resource grants, Agents, memberships) and the organization itself.
* :meth:`IdentityPurgeRepository.erase_user` — a data subject's erasure. Removes the user's
  OIDC links, the Agents it owns, its memberships across every org, the grants it issued,
  and the user row. All of these cascade from ``users`` via ``ON DELETE CASCADE``; the
  explicit, ordered deletes make the operation observable + idempotent.

Every method sets ``app.org_id`` (org path) so Postgres RLS is engaged when erasure runs
under the non-owner runtime role. Identity is not event-sourced, so no projection rebuild
can resurrect an erased identity row (there is no event source to replay from).
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_SET_ORG = text("SELECT set_config('app.org_id', :org, true)")


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


async def purge_user(engine: AsyncEngine, user_id: str) -> UserErasureResult:
    """Erase a user's global identity + every org-owned row it owns/issued (idempotent)."""
    async with engine.begin() as conn:
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
    "UserErasureResult",
    "purge_organization",
    "purge_user",
]
