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
  **archives** an active org the user solely owns and is the only active member of.

Both primitives execute the durable ``keel_erase_organization`` / ``keel_erase_user``
``SECURITY DEFINER`` functions installed by migration 0013 rather than issuing the deletes
directly. This is what makes erasure correct under production RLS: user erasure is inherently
cross-tenant (a user belongs to many orgs) and must delete the global ``users`` row, but the
non-bypass ``keel_runtime`` role under ``FORCE ROW LEVEL SECURITY`` cannot enumerate rows
across orgs — so a direct enumeration would silently skip the sole-owner guard and cascade an
active org into an ownerless state. The definer functions (owned by the ``keel_maintenance``
role, EXECUTE revoked from ``keel_runtime``) enumerate + lock the affected orgs, enforce the
block/archive semantics, and purge every identity row atomically, while normal runtime
principals are denied both the functions and DELETE on the global identity tables.

Identity is not event-sourced, so no projection rebuild can resurrect an erased identity row.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.errors import KeelError


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
        row = (
            (
                await conn.execute(
                    text("SELECT * FROM keel_erase_organization(:org)"),
                    {"org": org_id},
                )
            )
            .mappings()
            .one()
        )
    return OrganizationErasureResult(
        resource_grants=int(row["deleted_grants"] or 0),
        agents=int(row["deleted_agents"] or 0),
        memberships=int(row["deleted_memberships"] or 0),
        organization=int(row["deleted_org"] or 0),
    )


async def purge_user(engine: AsyncEngine, user_id: str) -> UserErasureResult:
    """Erase a user's global identity + every org-owned row it owns/issued (idempotent).

    Never orphans an active organization: if the user is the sole active owner of an active
    org that still has other active members, the whole erasure is aborted with
    :class:`UserErasureBlockedError` (ownership must be transferred first). An active org the
    user solely owns *and* is the only active member of is atomically archived as part of the
    same transaction (nobody else can own it), so lifecycle stays honest and no live tenant
    is left ownerless. The block/archive decision and the purge run inside the durable
    ``keel_erase_user`` ``SECURITY DEFINER`` function so they are correct under production RLS.
    """
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text("SELECT * FROM keel_erase_user(:user)"),
                    {"user": user_id},
                )
            )
            .mappings()
            .one()
        )
    blocked = tuple(row["blocked_org_ids"] or ())
    if blocked:
        raise UserErasureBlockedError(blocked)
    return UserErasureResult(
        oidc_identities=int(row["deleted_oidc"] or 0),
        agents=int(row["deleted_agents"] or 0),
        memberships=int(row["deleted_memberships"] or 0),
        resource_grants=int(row["deleted_grants"] or 0),
        user=int(row["deleted_users"] or 0),
        archived_organizations=int(row["archived_orgs"] or 0),
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
