"""Durable + in-memory identity repositories (M3.6, WS-L).

One :class:`IdentityStore` protocol over users, OIDC links, organizations, memberships,
Agents, and resource grants, with:

* :class:`InMemoryIdentityStore` — a faithful, dependency-free implementation for unit
  tests and the ``lite`` profile (enforces the same uniqueness + optimistic-concurrency
  rules the schema does), and
* :class:`PostgresIdentityStore` — the durable implementation. Tenant-owned reads/writes
  set the ``app.org_id`` (and, for user self-service, ``app.user_id``) GUCs so Postgres RLS
  is engaged as defense-in-depth (ADR-0009 / DESIGN-REVIEW G16).

Repositories are intentionally thin: validation, authorization, last-owner protection, and
audit live in :mod:`keel_core.identity.service`.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.errors import PermissionDenied
from keel_core.identity.models import (
    ADMIN_ROLES,
    Agent,
    AgentKind,
    AgentStatus,
    Capability,
    ConflictError,
    GrantStatus,
    LastOwnerError,
    Membership,
    MembershipRole,
    MembershipStatus,
    OIDCIdentity,
    OptimisticConcurrencyError,
    Organization,
    OrganizationStatus,
    ResourceGrant,
    User,
    UserStatus,
    new_agent_id,
    new_grant_id,
    new_membership_id,
    new_oidc_id,
    new_org_id,
    new_user_id,
)

_SET_ORG = text("SELECT set_config('app.org_id', :org, true)")
_SET_USER = text("SELECT set_config('app.user_id', :user, true)")


def _now() -> datetime:
    return datetime.now(UTC)


@runtime_checkable
class IdentityStore(Protocol):
    """Durable seam for identity persistence."""

    # users
    async def create_user(self, *, display_name: str, email: str | None) -> User: ...
    async def get_user(self, user_id: str) -> User | None: ...
    async def get_user_by_email(self, email: str) -> User | None: ...
    async def update_user(
        self, user_id: str, *, display_name: str | None = None, email: str | None = None
    ) -> User | None: ...
    async def soft_delete_user(self, user_id: str) -> User | None: ...

    # oidc links
    async def get_identity(self, issuer: str, subject: str) -> OIDCIdentity | None: ...
    async def link_identity(
        self, *, user_id: str, issuer: str, subject: str, email: str | None
    ) -> OIDCIdentity: ...
    async def touch_identity_login(self, identity_id: str) -> None: ...
    async def list_identities_for_user(self, user_id: str) -> list[OIDCIdentity]: ...

    # organizations
    async def create_org(self, *, slug: str, display_name: str) -> Organization: ...
    async def get_org(self, org_id: str) -> Organization | None: ...
    async def get_org_by_slug(self, slug: str) -> Organization | None: ...
    async def archive_org(self, org_id: str) -> Organization | None: ...

    # memberships
    async def create_membership(
        self,
        *,
        org_id: str,
        user_id: str,
        role: MembershipRole,
        revalidate_actor_user_id: str | None = None,
    ) -> Membership: ...
    async def get_membership(self, org_id: str, user_id: str) -> Membership | None: ...
    async def list_memberships(self, org_id: str) -> list[Membership]: ...
    async def list_memberships_for_user(self, user_id: str) -> list[Membership]: ...
    async def update_membership_role(
        self,
        org_id: str,
        user_id: str,
        role: MembershipRole,
        *,
        revalidate_actor_user_id: str | None = None,
    ) -> Membership | None: ...
    async def revoke_membership(
        self,
        org_id: str,
        user_id: str,
        *,
        revalidate_actor_user_id: str | None = None,
    ) -> Membership | None: ...
    async def count_active_owners(self, org_id: str) -> int: ...

    # agents
    async def create_agent(
        self,
        *,
        org_id: str,
        kind: AgentKind,
        owner_user_id: str,
        name: str,
        persona: str = "",
    ) -> Agent: ...
    async def get_agent(self, org_id: str, agent_id: str) -> Agent | None: ...
    async def list_agents(self, org_id: str) -> list[Agent]: ...
    async def update_agent(
        self,
        org_id: str,
        agent_id: str,
        *,
        expected_version: int,
        name: str | None = None,
        persona: str | None = None,
    ) -> Agent | None: ...
    async def archive_agent(
        self, org_id: str, agent_id: str, *, expected_version: int
    ) -> Agent | None: ...

    # grants
    async def create_grant(
        self,
        *,
        org_id: str,
        agent_id: str,
        resource_type: str,
        resource_id: str,
        capability: Capability,
        grantor_user_id: str,
    ) -> ResourceGrant: ...
    async def get_grant(self, org_id: str, grant_id: str) -> ResourceGrant | None: ...
    async def list_grants(
        self, org_id: str, *, agent_id: str | None = None
    ) -> list[ResourceGrant]: ...
    async def list_grants_for_resource(
        self, org_id: str, resource_type: str, resource_id: str
    ) -> list[ResourceGrant]: ...
    async def revoke_grant(
        self, org_id: str, grant_id: str, *, actor_user_id: str
    ) -> ResourceGrant | None: ...


# --- In-memory implementation --------------------------------------------------------


class InMemoryIdentityStore:
    """Non-durable identity store (unit tests / lite profile)."""

    def __init__(self) -> None:
        self._users: dict[str, User] = {}
        self._identities: dict[str, OIDCIdentity] = {}
        self._orgs: dict[str, Organization] = {}
        self._memberships: dict[str, Membership] = {}
        self._agents: dict[str, Agent] = {}
        self._grants: dict[str, ResourceGrant] = {}

    # users
    async def create_user(self, *, display_name: str, email: str | None) -> User:
        if email is not None:
            for user in self._users.values():
                if user.email == email and user.status is not UserStatus.deleted:
                    raise ConflictError("a user with this email already exists")
        now = _now()
        user = User(
            id=new_user_id(),
            display_name=display_name,
            email=email,
            created_at=now,
            updated_at=now,
        )
        self._users[user.id] = user
        return user

    async def get_user(self, user_id: str) -> User | None:
        return self._users.get(user_id)

    async def get_user_by_email(self, email: str) -> User | None:
        for user in self._users.values():
            if user.email == email and user.status is not UserStatus.deleted:
                return user
        return None

    async def update_user(
        self, user_id: str, *, display_name: str | None = None, email: str | None = None
    ) -> User | None:
        user = self._users.get(user_id)
        if user is None:
            return None
        if email is not None and email != user.email:
            for other in self._users.values():
                if other.email == email and other.status is not UserStatus.deleted:
                    raise ConflictError("a user with this email already exists")
        updated = replace(
            user,
            display_name=display_name if display_name is not None else user.display_name,
            email=email if email is not None else user.email,
            updated_at=_now(),
        )
        self._users[user_id] = updated
        return updated

    async def soft_delete_user(self, user_id: str) -> User | None:
        user = self._users.get(user_id)
        if user is None:
            return None
        now = _now()
        updated = replace(
            user, status=UserStatus.deleted, email=None, deleted_at=now, updated_at=now
        )
        self._users[user_id] = updated
        return updated

    # oidc
    async def get_identity(self, issuer: str, subject: str) -> OIDCIdentity | None:
        for identity in self._identities.values():
            if identity.issuer == issuer and identity.subject == subject:
                return identity
        return None

    async def link_identity(
        self, *, user_id: str, issuer: str, subject: str, email: str | None
    ) -> OIDCIdentity:
        existing = await self.get_identity(issuer, subject)
        if existing is not None:
            if existing.user_id != user_id:
                raise ConflictError("this OIDC identity is linked to another user")
            return existing
        now = _now()
        identity = OIDCIdentity(
            id=new_oidc_id(),
            user_id=user_id,
            issuer=issuer,
            subject=subject,
            email=email,
            created_at=now,
            updated_at=now,
        )
        self._identities[identity.id] = identity
        return identity

    async def touch_identity_login(self, identity_id: str) -> None:
        identity = self._identities.get(identity_id)
        if identity is not None:
            self._identities[identity_id] = replace(identity, last_login_at=_now())

    async def list_identities_for_user(self, user_id: str) -> list[OIDCIdentity]:
        return [i for i in self._identities.values() if i.user_id == user_id]

    # organizations
    async def create_org(self, *, slug: str, display_name: str) -> Organization:
        for org in self._orgs.values():
            if org.slug == slug:
                raise ConflictError("an organization with this slug already exists")
        now = _now()
        org = Organization(
            id=new_org_id(),
            slug=slug,
            display_name=display_name,
            created_at=now,
            updated_at=now,
        )
        self._orgs[org.id] = org
        return org

    async def get_org(self, org_id: str) -> Organization | None:
        return self._orgs.get(org_id)

    async def get_org_by_slug(self, slug: str) -> Organization | None:
        for org in self._orgs.values():
            if org.slug == slug:
                return org
        return None

    async def archive_org(self, org_id: str) -> Organization | None:
        org = self._orgs.get(org_id)
        if org is None:
            return None
        now = _now()
        updated = replace(org, status=OrganizationStatus.archived, archived_at=now, updated_at=now)
        self._orgs[org_id] = updated
        return updated

    # memberships
    def _membership_key(self, org_id: str, user_id: str) -> str | None:
        for key, membership in self._memberships.items():
            if membership.org_id == org_id and membership.user_id == user_id:
                return key
        return None

    async def create_membership(
        self,
        *,
        org_id: str,
        user_id: str,
        role: MembershipRole,
        revalidate_actor_user_id: str | None = None,
    ) -> Membership:
        if revalidate_actor_user_id is not None:
            # Inviting an owner requires the actor to itself be an owner (derived from the
            # requested role, not a stale service decision).
            self._require_manager(org_id, revalidate_actor_user_id, role is MembershipRole.owner)
        if self._membership_key(org_id, user_id) is not None:
            raise ConflictError("membership already exists")
        now = _now()
        membership = Membership(
            id=new_membership_id(),
            org_id=org_id,
            user_id=user_id,
            role=role,
            created_at=now,
            updated_at=now,
        )
        self._memberships[membership.id] = membership
        return membership

    async def get_membership(self, org_id: str, user_id: str) -> Membership | None:
        key = self._membership_key(org_id, user_id)
        return None if key is None else self._memberships[key]

    async def list_memberships(self, org_id: str) -> list[Membership]:
        return [m for m in self._memberships.values() if m.org_id == org_id]

    async def list_memberships_for_user(self, user_id: str) -> list[Membership]:
        return [m for m in self._memberships.values() if m.user_id == user_id]

    async def update_membership_role(
        self,
        org_id: str,
        user_id: str,
        role: MembershipRole,
        *,
        revalidate_actor_user_id: str | None = None,
    ) -> Membership | None:
        key = self._membership_key(org_id, user_id)
        if key is None:
            return None
        current = self._memberships[key]
        if current.status is not MembershipStatus.active:
            return None
        # Owner-affecting transitions (this row is, or becomes, an owner) are derived from
        # the *current* target row, never from a stale caller decision.
        owner_change = MembershipRole.owner in (current.role, role)
        if revalidate_actor_user_id is not None:
            self._require_manager(org_id, revalidate_actor_user_id, owner_change)
        # Last-owner protection: demoting the final active owner is refused.
        if current.role is MembershipRole.owner and role is not MembershipRole.owner:
            if self._other_active_owners(org_id, user_id) == 0:
                raise LastOwnerError("an organization must retain at least one active owner")
        updated = replace(current, role=role, updated_at=_now())
        self._memberships[key] = updated
        return updated

    async def revoke_membership(
        self,
        org_id: str,
        user_id: str,
        *,
        revalidate_actor_user_id: str | None = None,
    ) -> Membership | None:
        key = self._membership_key(org_id, user_id)
        if key is None:
            return None
        current = self._memberships[key]
        if current.status is not MembershipStatus.active:
            return None
        # Removing an owner is an owner-affecting transition, derived from the target row.
        removes_owner = current.role is MembershipRole.owner
        if revalidate_actor_user_id is not None:
            self._require_manager(org_id, revalidate_actor_user_id, removes_owner)
        if removes_owner and self._other_active_owners(org_id, user_id) == 0:
            raise LastOwnerError("an organization must retain at least one active owner")
        now = _now()
        updated = replace(
            current,
            status=MembershipStatus.revoked,
            revoked_at=now,
            updated_at=now,
        )
        self._memberships[key] = updated
        return updated

    def _other_active_owners(self, org_id: str, exclude_user_id: str) -> int:
        return sum(
            1
            for m in self._memberships.values()
            if m.org_id == org_id
            and m.user_id != exclude_user_id
            and m.role is MembershipRole.owner
            and m.status is MembershipStatus.active
        )

    def _require_manager(self, org_id: str, actor_user_id: str, require_owner: bool) -> None:
        membership = self._active_manage_membership(org_id, actor_user_id)
        if membership is None or (require_owner and membership.role is not MembershipRole.owner):
            raise PermissionDenied(
                "this operation requires an active org "
                + ("owner" if require_owner else "admin/owner")
                + " membership"
            )

    def _active_manage_membership(self, org_id: str, user_id: str) -> Membership | None:
        key = self._membership_key(org_id, user_id)
        if key is None:
            return None
        membership = self._memberships[key]
        if membership.status is not MembershipStatus.active or membership.role not in ADMIN_ROLES:
            return None
        return membership

    async def count_active_owners(self, org_id: str) -> int:
        return sum(
            1
            for m in self._memberships.values()
            if m.org_id == org_id
            and m.role is MembershipRole.owner
            and m.status is MembershipStatus.active
        )

    # agents
    async def create_agent(
        self,
        *,
        org_id: str,
        kind: AgentKind,
        owner_user_id: str,
        name: str,
        persona: str = "",
    ) -> Agent:
        for agent in self._agents.values():
            if (
                agent.org_id == org_id
                and agent.status is AgentStatus.active
                and agent.name.lower() == name.lower()
            ):
                raise ConflictError("an active agent with this name already exists")
        now = _now()
        agent = Agent(
            id=new_agent_id(),
            org_id=org_id,
            kind=kind,
            owner_user_id=owner_user_id,
            name=name,
            persona=persona,
            created_at=now,
            updated_at=now,
        )
        self._agents[agent.id] = agent
        return agent

    async def get_agent(self, org_id: str, agent_id: str) -> Agent | None:
        agent = self._agents.get(agent_id)
        if agent is None or agent.org_id != org_id:
            return None
        return agent

    async def list_agents(self, org_id: str) -> list[Agent]:
        return [
            a
            for a in self._agents.values()
            if a.org_id == org_id and a.status is AgentStatus.active
        ]

    async def update_agent(
        self,
        org_id: str,
        agent_id: str,
        *,
        expected_version: int,
        name: str | None = None,
        persona: str | None = None,
    ) -> Agent | None:
        agent = await self.get_agent(org_id, agent_id)
        if agent is None or agent.status is not AgentStatus.active:
            return None
        if agent.version != expected_version:
            raise OptimisticConcurrencyError("agent was modified concurrently")
        if name is not None and name.lower() != agent.name.lower():
            for other in self._agents.values():
                if (
                    other.org_id == org_id
                    and other.id != agent_id
                    and other.status is AgentStatus.active
                    and other.name.lower() == name.lower()
                ):
                    raise ConflictError("an active agent with this name already exists")
        updated = replace(
            agent,
            name=name if name is not None else agent.name,
            persona=persona if persona is not None else agent.persona,
            version=agent.version + 1,
            updated_at=_now(),
        )
        self._agents[agent_id] = updated
        return updated

    async def archive_agent(
        self, org_id: str, agent_id: str, *, expected_version: int
    ) -> Agent | None:
        agent = await self.get_agent(org_id, agent_id)
        if agent is None or agent.status is not AgentStatus.active:
            return None
        if agent.version != expected_version:
            raise OptimisticConcurrencyError("agent was modified concurrently")
        now = _now()
        updated = replace(
            agent,
            status=AgentStatus.archived,
            version=agent.version + 1,
            archived_at=now,
            updated_at=now,
        )
        self._agents[agent_id] = updated
        return updated

    # grants
    async def create_grant(
        self,
        *,
        org_id: str,
        agent_id: str,
        resource_type: str,
        resource_id: str,
        capability: Capability,
        grantor_user_id: str,
    ) -> ResourceGrant:
        # Re-validate the grantor's authority here (defense-in-depth mirror of the durable
        # store's transactional recheck): a grantor whose admin/owner membership has been
        # revoked concurrently must not be able to commit a grant.
        if self._active_manage_membership(org_id, grantor_user_id) is None:
            raise PermissionDenied("granting requires an active org admin/owner membership")
        for grant in self._grants.values():
            if (
                grant.org_id == org_id
                and grant.agent_id == agent_id
                and grant.resource_type == resource_type
                and grant.resource_id == resource_id
                and grant.capability is capability
            ):
                if grant.is_active:
                    return grant
                # Re-activate a previously revoked identical grant.
                reactivated = replace(
                    grant,
                    status=GrantStatus.active,
                    grantor_user_id=grantor_user_id,
                    revoked_at=None,
                    updated_at=_now(),
                )
                self._grants[grant.id] = reactivated
                return reactivated
        now = _now()
        grant = ResourceGrant(
            id=new_grant_id(),
            org_id=org_id,
            agent_id=agent_id,
            resource_type=resource_type,
            resource_id=resource_id,
            capability=capability,
            grantor_user_id=grantor_user_id,
            created_at=now,
            updated_at=now,
        )
        self._grants[grant.id] = grant
        return grant

    async def get_grant(self, org_id: str, grant_id: str) -> ResourceGrant | None:
        grant = self._grants.get(grant_id)
        if grant is None or grant.org_id != org_id:
            return None
        return grant

    async def list_grants(self, org_id: str, *, agent_id: str | None = None) -> list[ResourceGrant]:
        return [
            g
            for g in self._grants.values()
            if g.org_id == org_id and (agent_id is None or g.agent_id == agent_id)
        ]

    async def list_grants_for_resource(
        self, org_id: str, resource_type: str, resource_id: str
    ) -> list[ResourceGrant]:
        return [
            g
            for g in self._grants.values()
            if g.org_id == org_id
            and g.resource_type == resource_type
            and g.resource_id == resource_id
        ]

    async def revoke_grant(
        self, org_id: str, grant_id: str, *, actor_user_id: str
    ) -> ResourceGrant | None:
        grant = await self.get_grant(org_id, grant_id)
        if grant is None:
            return None
        if self._active_manage_membership(org_id, actor_user_id) is None:
            raise PermissionDenied("revoking requires an active org admin/owner membership")
        now = _now()
        updated = replace(grant, status=GrantStatus.revoked, revoked_at=now, updated_at=now)
        self._grants[grant_id] = updated
        return updated


# --- Postgres implementation ---------------------------------------------------------

_USER_COLS = "id, email, display_name, status, created_at, updated_at, deleted_at"
_OIDC_COLS = "id, user_id, issuer, subject, email, created_at, updated_at, last_login_at"
_ORG_COLS = "id, slug, display_name, status, created_at, updated_at, archived_at"
_MEMBER_COLS = "id, org_id, user_id, role, status, created_at, updated_at, revoked_at"
_AGENT_COLS = (
    "id, org_id, kind, owner_user_id, name, persona, status, version, "
    "created_at, updated_at, archived_at"
)
_GRANT_COLS = (
    "id, org_id, agent_id, resource_type, resource_id, capability, grantor_user_id, "
    "status, created_at, updated_at, revoked_at"
)


def _to_user(row: Any) -> User:
    return User(
        id=row["id"],
        display_name=row["display_name"],
        email=row["email"],
        status=UserStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        deleted_at=row["deleted_at"],
    )


def _to_identity(row: Any) -> OIDCIdentity:
    return OIDCIdentity(
        id=row["id"],
        user_id=row["user_id"],
        issuer=row["issuer"],
        subject=row["subject"],
        email=row["email"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        last_login_at=row["last_login_at"],
    )


def _to_org(row: Any) -> Organization:
    return Organization(
        id=row["id"],
        slug=row["slug"],
        display_name=row["display_name"],
        status=OrganizationStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        archived_at=row["archived_at"],
    )


def _to_membership(row: Any) -> Membership:
    return Membership(
        id=row["id"],
        org_id=row["org_id"],
        user_id=row["user_id"],
        role=MembershipRole(row["role"]),
        status=MembershipStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        revoked_at=row["revoked_at"],
    )


def _to_agent(row: Any) -> Agent:
    return Agent(
        id=row["id"],
        org_id=row["org_id"],
        kind=AgentKind(row["kind"]),
        owner_user_id=row["owner_user_id"],
        name=row["name"],
        persona=row["persona"],
        status=AgentStatus(row["status"]),
        version=int(row["version"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        archived_at=row["archived_at"],
    )


def _to_grant(row: Any) -> ResourceGrant:
    return ResourceGrant(
        id=row["id"],
        org_id=row["org_id"],
        agent_id=row["agent_id"],
        resource_type=row["resource_type"],
        resource_id=row["resource_id"],
        capability=Capability(row["capability"]),
        grantor_user_id=row["grantor_user_id"],
        status=GrantStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        revoked_at=row["revoked_at"],
    )


class PostgresIdentityStore:
    """Durable identity store; tenant-owned access sets ``app.org_id``/``app.user_id``."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    # --- users (global) --------------------------------------------------------------
    async def create_user(self, *, display_name: str, email: str | None) -> User:
        user_id = new_user_id()
        try:
            async with self._engine.begin() as conn:
                row = (
                    (
                        await conn.execute(
                            text(
                                "INSERT INTO users (id, email, display_name) "
                                "VALUES (:id, :email, :name) "
                                f"RETURNING {_USER_COLS}"
                            ),
                            {"id": user_id, "email": email, "name": display_name},
                        )
                    )
                    .mappings()
                    .one()
                )
        except IntegrityError as exc:
            raise ConflictError("a user with this email already exists") from exc
        return _to_user(row)

    async def get_user(self, user_id: str) -> User | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(f"SELECT {_USER_COLS} FROM users WHERE id = :id"),
                        {"id": user_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_user(row)

    async def get_user_by_email(self, email: str) -> User | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_USER_COLS} FROM users "
                            "WHERE email = :email AND deleted_at IS NULL"
                        ),
                        {"email": email},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_user(row)

    async def update_user(
        self, user_id: str, *, display_name: str | None = None, email: str | None = None
    ) -> User | None:
        try:
            async with self._engine.begin() as conn:
                row = (
                    (
                        await conn.execute(
                            text(
                                "UPDATE users SET "
                                "display_name = COALESCE(:name, display_name), "
                                "email = CASE WHEN :set_email THEN :email ELSE email END, "
                                "updated_at = now() "
                                "WHERE id = :id AND status <> 'deleted' "
                                f"RETURNING {_USER_COLS}"
                            ),
                            {
                                "id": user_id,
                                "name": display_name,
                                "set_email": email is not None,
                                "email": email,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except IntegrityError as exc:
            raise ConflictError("a user with this email already exists") from exc
        return None if row is None else _to_user(row)

    async def soft_delete_user(self, user_id: str) -> User | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE users SET status = 'deleted', email = NULL, "
                            "deleted_at = now(), updated_at = now() "
                            f"WHERE id = :id RETURNING {_USER_COLS}"
                        ),
                        {"id": user_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_user(row)

    # --- oidc (global) ---------------------------------------------------------------
    async def get_identity(self, issuer: str, subject: str) -> OIDCIdentity | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_OIDC_COLS} FROM oidc_identities "
                            "WHERE issuer = :iss AND subject = :sub"
                        ),
                        {"iss": issuer, "sub": subject},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_identity(row)

    async def link_identity(
        self, *, user_id: str, issuer: str, subject: str, email: str | None
    ) -> OIDCIdentity:
        identity_id = new_oidc_id()
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO oidc_identities (id, user_id, issuer, subject, email) "
                    "VALUES (:id, :user, :iss, :sub, :email) "
                    "ON CONFLICT (issuer, subject) DO NOTHING"
                ),
                {
                    "id": identity_id,
                    "user": user_id,
                    "iss": issuer,
                    "sub": subject,
                    "email": email,
                },
            )
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_OIDC_COLS} FROM oidc_identities "
                            "WHERE issuer = :iss AND subject = :sub"
                        ),
                        {"iss": issuer, "sub": subject},
                    )
                )
                .mappings()
                .one()
            )
        identity = _to_identity(row)
        if identity.user_id != user_id:
            raise ConflictError("this OIDC identity is linked to another user")
        return identity

    async def touch_identity_login(self, identity_id: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE oidc_identities SET last_login_at = now(), updated_at = now() "
                    "WHERE id = :id"
                ),
                {"id": identity_id},
            )

    async def list_identities_for_user(self, user_id: str) -> list[OIDCIdentity]:
        async with self._engine.begin() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_OIDC_COLS} FROM oidc_identities "
                            "WHERE user_id = :user ORDER BY created_at"
                        ),
                        {"user": user_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_identity(row) for row in rows]

    # --- organizations (global) ------------------------------------------------------
    async def create_org(self, *, slug: str, display_name: str) -> Organization:
        org_id = new_org_id()
        try:
            async with self._engine.begin() as conn:
                row = (
                    (
                        await conn.execute(
                            text(
                                "INSERT INTO organizations (id, slug, display_name) "
                                "VALUES (:id, :slug, :name) "
                                f"RETURNING {_ORG_COLS}"
                            ),
                            {"id": org_id, "slug": slug, "name": display_name},
                        )
                    )
                    .mappings()
                    .one()
                )
        except IntegrityError as exc:
            raise ConflictError("an organization with this slug already exists") from exc
        return _to_org(row)

    async def get_org(self, org_id: str) -> Organization | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(f"SELECT {_ORG_COLS} FROM organizations WHERE id = :id"),
                        {"id": org_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_org(row)

    async def get_org_by_slug(self, slug: str) -> Organization | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(f"SELECT {_ORG_COLS} FROM organizations WHERE slug = :slug"),
                        {"slug": slug},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_org(row)

    async def archive_org(self, org_id: str) -> Organization | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE organizations SET status = 'archived', "
                            "archived_at = now(), updated_at = now() "
                            f"WHERE id = :id RETURNING {_ORG_COLS}"
                        ),
                        {"id": org_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_org(row)

    # --- memberships (tenant-owned) --------------------------------------------------
    async def _require_active_manager(
        self, conn: Any, org_id: str, user_id: str, *, require_owner: bool = False
    ) -> None:
        """Re-check + ``FOR SHARE`` row-lock the actor's admin/owner membership in a txn.

        Used by the grant paths (which acquire a *single* membership lock and no org lock, so
        they cannot participate in a lock cycle with the org-first membership mutations). The
        ``FOR SHARE`` lock blocks a concurrent role demotion/revocation (which needs a
        ``FOR UPDATE`` write lock on the same row) until this transaction commits; if the
        demotion has already committed, the filtered row no longer matches and the operation
        is refused, so a demoted admin cannot commit a grant afterward.
        """
        roles = "('owner')" if require_owner else "('owner', 'admin')"
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT role FROM memberships "
                        "WHERE org_id = :org AND user_id = :user AND status = 'active' "
                        f"AND role IN {roles} FOR SHARE"
                    ),
                    {"org": org_id, "user": user_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise PermissionDenied(
                "this operation requires an active org "
                + ("owner" if require_owner else "admin/owner")
                + " membership"
            )

    async def _lock_org_row(self, conn: Any, org_id: str) -> None:
        """Serialize every owner-affecting membership mutation on ``org_id``.

        A stable ``FOR UPDATE`` lock on the (single) organization row is the FIRST lock a
        membership mutation takes. Because all membership mutations acquire this lock before
        any membership-row lock, two concurrent mutations on the same org cannot interleave
        their per-row locks in opposite orders, so cross-owner demotions/removals can neither
        deadlock nor both observe two owners and both proceed.
        """
        await conn.execute(
            text("SELECT id FROM organizations WHERE id = :org FOR UPDATE"),
            {"org": org_id},
        )

    async def _lock_membership_rows(self, conn: Any, org_id: str, user_ids: set[str]) -> None:
        """``FOR UPDATE``-lock the given membership rows in a deterministic (sorted) order.

        Locking actor + target rows in a stable order is defense-in-depth against deadlock on
        top of the org lock (which already fully serializes these mutations). Missing rows
        simply take no lock; the subsequent role/target reads then decide the outcome.
        """
        for uid in sorted(user_ids):
            await conn.execute(
                text(
                    "SELECT id FROM memberships WHERE org_id = :org AND user_id = :user FOR UPDATE"
                ),
                {"org": org_id, "user": uid},
            )

    async def _assert_active_manager(
        self, conn: Any, org_id: str, user_id: str, *, require_owner: bool
    ) -> None:
        """Authorize the actor from its (already locked) membership row — no extra lock."""
        roles = "('owner')" if require_owner else "('owner', 'admin')"
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT role FROM memberships "
                        "WHERE org_id = :org AND user_id = :user AND status = 'active' "
                        f"AND role IN {roles}"
                    ),
                    {"org": org_id, "user": user_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise PermissionDenied(
                "this operation requires an active org "
                + ("owner" if require_owner else "admin/owner")
                + " membership"
            )

    async def _read_active_target_role(self, conn: Any, org_id: str, user_id: str) -> str | None:
        """Return the target's current active role (from its locked row), else ``None``."""
        role = await conn.scalar(
            text(
                "SELECT role FROM memberships "
                "WHERE org_id = :org AND user_id = :user AND status = 'active'"
            ),
            {"org": org_id, "user": user_id},
        )
        return None if role is None else str(role)

    async def _require_other_active_owner(
        self, conn: Any, org_id: str, exclude_user_id: str
    ) -> None:
        """Refuse a mutation that would leave ``org_id`` with zero active owners."""
        remaining = await conn.scalar(
            text(
                "SELECT count(*) FROM memberships "
                "WHERE org_id = :org AND role = 'owner' AND status = 'active' "
                "AND user_id <> :user"
            ),
            {"org": org_id, "user": exclude_user_id},
        )
        if int(remaining or 0) == 0:
            raise LastOwnerError("an organization must retain at least one active owner")

    async def create_membership(
        self,
        *,
        org_id: str,
        user_id: str,
        role: MembershipRole,
        revalidate_actor_user_id: str | None = None,
    ) -> Membership:
        membership_id = new_membership_id()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                await conn.execute(_SET_USER, {"user": user_id})
                if revalidate_actor_user_id is not None:
                    # Lock the org first, then the actor row, before authorizing: an owner
                    # invite requires an owner actor (derived from the requested role), and a
                    # concurrently-demoted actor can no longer authorize the invite.
                    await self._lock_org_row(conn, org_id)
                    await self._lock_membership_rows(conn, org_id, {revalidate_actor_user_id})
                    await self._assert_active_manager(
                        conn,
                        org_id,
                        revalidate_actor_user_id,
                        require_owner=role is MembershipRole.owner,
                    )
                row = (
                    (
                        await conn.execute(
                            text(
                                "INSERT INTO memberships (id, org_id, user_id, role) "
                                "VALUES (:id, :org, :user, :role) "
                                f"RETURNING {_MEMBER_COLS}"
                            ),
                            {
                                "id": membership_id,
                                "org": org_id,
                                "user": user_id,
                                "role": role.value,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
        except IntegrityError as exc:
            raise ConflictError("membership already exists") from exc
        return _to_membership(row)

    async def get_membership(self, org_id: str, user_id: str) -> Membership | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            await conn.execute(_SET_USER, {"user": user_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_MEMBER_COLS} FROM memberships "
                            "WHERE org_id = :org AND user_id = :user"
                        ),
                        {"org": org_id, "user": user_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_membership(row)

    async def list_memberships(self, org_id: str) -> list[Membership]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_MEMBER_COLS} FROM memberships "
                            "WHERE org_id = :org ORDER BY created_at"
                        ),
                        {"org": org_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_membership(row) for row in rows]

    async def list_memberships_for_user(self, user_id: str) -> list[Membership]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_USER, {"user": user_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_MEMBER_COLS} FROM memberships "
                            "WHERE user_id = :user ORDER BY created_at"
                        ),
                        {"user": user_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_membership(row) for row in rows]

    async def update_membership_role(
        self,
        org_id: str,
        user_id: str,
        role: MembershipRole,
        *,
        revalidate_actor_user_id: str | None = None,
    ) -> Membership | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            # Lock ordering: organization row first, then the actor + target membership rows
            # in a deterministic (sorted) order. The org lock serializes owner-affecting
            # mutations on this org; the sorted membership locks are defense-in-depth.
            await self._lock_org_row(conn, org_id)
            lock_users = {user_id}
            if revalidate_actor_user_id is not None:
                lock_users.add(revalidate_actor_user_id)
            await self._lock_membership_rows(conn, org_id, lock_users)
            # The target's CURRENT (locked) role is the sole source of truth for whether this
            # is an owner-affecting transition — never a stale service-level read.
            target_role = await self._read_active_target_role(conn, org_id, user_id)
            if target_role is None:
                return None
            owner_change = "owner" in (target_role, role.value)
            if revalidate_actor_user_id is not None:
                await self._assert_active_manager(
                    conn, org_id, revalidate_actor_user_id, require_owner=owner_change
                )
            # Last-owner protection: demoting the final active owner is refused, decided from
            # the owner count read under the same lock.
            if target_role == "owner" and role is not MembershipRole.owner:
                await self._require_other_active_owner(conn, org_id, user_id)
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE memberships SET role = :role, updated_at = now() "
                            "WHERE org_id = :org AND user_id = :user AND status = 'active' "
                            f"RETURNING {_MEMBER_COLS}"
                        ),
                        {"org": org_id, "user": user_id, "role": role.value},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_membership(row)

    async def revoke_membership(
        self,
        org_id: str,
        user_id: str,
        *,
        revalidate_actor_user_id: str | None = None,
    ) -> Membership | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            await self._lock_org_row(conn, org_id)
            lock_users = {user_id}
            if revalidate_actor_user_id is not None:
                lock_users.add(revalidate_actor_user_id)
            await self._lock_membership_rows(conn, org_id, lock_users)
            target_role = await self._read_active_target_role(conn, org_id, user_id)
            if target_role is None:
                return None
            removes_owner = target_role == "owner"
            if revalidate_actor_user_id is not None:
                await self._assert_active_manager(
                    conn, org_id, revalidate_actor_user_id, require_owner=removes_owner
                )
            if removes_owner:
                await self._require_other_active_owner(conn, org_id, user_id)
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE memberships SET status = 'revoked', revoked_at = now(), "
                            "updated_at = now() "
                            "WHERE org_id = :org AND user_id = :user AND status = 'active' "
                            f"RETURNING {_MEMBER_COLS}"
                        ),
                        {"org": org_id, "user": user_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_membership(row)

    async def count_active_owners(self, org_id: str) -> int:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            count = await conn.scalar(
                text(
                    "SELECT count(*) FROM memberships "
                    "WHERE org_id = :org AND role = 'owner' AND status = 'active'"
                ),
                {"org": org_id},
            )
        return int(count or 0)

    # --- agents (tenant-owned) -------------------------------------------------------
    async def create_agent(
        self,
        *,
        org_id: str,
        kind: AgentKind,
        owner_user_id: str,
        name: str,
        persona: str = "",
    ) -> Agent:
        agent_id = new_agent_id()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                row = (
                    (
                        await conn.execute(
                            text(
                                "INSERT INTO agents "
                                "(id, org_id, kind, owner_user_id, name, persona) "
                                "VALUES (:id, :org, :kind, :owner, :name, :persona) "
                                f"RETURNING {_AGENT_COLS}"
                            ),
                            {
                                "id": agent_id,
                                "org": org_id,
                                "kind": kind.value,
                                "owner": owner_user_id,
                                "name": name,
                                "persona": persona,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
        except IntegrityError as exc:
            raise ConflictError("an active agent with this name already exists") from exc
        return _to_agent(row)

    async def get_agent(self, org_id: str, agent_id: str) -> Agent | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(f"SELECT {_AGENT_COLS} FROM agents WHERE org_id = :org AND id = :id"),
                        {"org": org_id, "id": agent_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_agent(row)

    async def list_agents(self, org_id: str) -> list[Agent]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_AGENT_COLS} FROM agents "
                            "WHERE org_id = :org AND status = 'active' ORDER BY created_at"
                        ),
                        {"org": org_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_agent(row) for row in rows]

    async def update_agent(
        self,
        org_id: str,
        agent_id: str,
        *,
        expected_version: int,
        name: str | None = None,
        persona: str | None = None,
    ) -> Agent | None:
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                current = (
                    (
                        await conn.execute(
                            text(
                                "SELECT version, status FROM agents "
                                "WHERE org_id = :org AND id = :id"
                            ),
                            {"org": org_id, "id": agent_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if current is None or current["status"] != "active":
                    return None
                if int(current["version"]) != expected_version:
                    raise OptimisticConcurrencyError("agent was modified concurrently")
                row = (
                    (
                        await conn.execute(
                            text(
                                "UPDATE agents SET "
                                "name = COALESCE(:name, name), "
                                "persona = COALESCE(:persona, persona), "
                                "version = version + 1, updated_at = now() "
                                "WHERE org_id = :org AND id = :id AND version = :ver "
                                "AND status = 'active' "
                                f"RETURNING {_AGENT_COLS}"
                            ),
                            {
                                "org": org_id,
                                "id": agent_id,
                                "name": name,
                                "persona": persona,
                                "ver": expected_version,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
        except IntegrityError as exc:
            raise ConflictError("an active agent with this name already exists") from exc
        if row is None:
            raise OptimisticConcurrencyError("agent was modified concurrently")
        return _to_agent(row)

    async def archive_agent(
        self, org_id: str, agent_id: str, *, expected_version: int
    ) -> Agent | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            current = (
                (
                    await conn.execute(
                        text("SELECT version, status FROM agents WHERE org_id = :org AND id = :id"),
                        {"org": org_id, "id": agent_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if current is None or current["status"] != "active":
                return None
            if int(current["version"]) != expected_version:
                raise OptimisticConcurrencyError("agent was modified concurrently")
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE agents SET status = 'archived', version = version + 1, "
                            "archived_at = now(), updated_at = now() "
                            "WHERE org_id = :org AND id = :id AND version = :ver "
                            "AND status = 'active' "
                            f"RETURNING {_AGENT_COLS}"
                        ),
                        {"org": org_id, "id": agent_id, "ver": expected_version},
                    )
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise OptimisticConcurrencyError("agent was modified concurrently")
        return _to_agent(row)

    # --- grants (tenant-owned) -------------------------------------------------------
    async def create_grant(
        self,
        *,
        org_id: str,
        agent_id: str,
        resource_type: str,
        resource_id: str,
        capability: Capability,
        grantor_user_id: str,
    ) -> ResourceGrant:
        grant_id = new_grant_id()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                await self._require_active_manager(conn, org_id, grantor_user_id)
                await conn.execute(
                    text(
                        "INSERT INTO resource_grants "
                        "(id, org_id, agent_id, resource_type, resource_id, capability, "
                        "grantor_user_id) "
                        "VALUES (:id, :org, :agent, :rtype, :rid, :cap, :grantor) "
                        "ON CONFLICT (org_id, agent_id, resource_type, resource_id, capability) "
                        "DO UPDATE SET status = 'active', revoked_at = NULL, "
                        "grantor_user_id = EXCLUDED.grantor_user_id, updated_at = now()"
                    ),
                    {
                        "id": grant_id,
                        "org": org_id,
                        "agent": agent_id,
                        "rtype": resource_type,
                        "rid": resource_id,
                        "cap": capability.value,
                        "grantor": grantor_user_id,
                    },
                )
                row = (
                    (
                        await conn.execute(
                            text(
                                f"SELECT {_GRANT_COLS} FROM resource_grants "
                                "WHERE org_id = :org AND agent_id = :agent "
                                "AND resource_type = :rtype AND resource_id = :rid "
                                "AND capability = :cap"
                            ),
                            {
                                "org": org_id,
                                "agent": agent_id,
                                "rtype": resource_type,
                                "rid": resource_id,
                                "cap": capability.value,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
        except IntegrityError as exc:
            raise ConflictError("grant references an agent outside this organization") from exc
        return _to_grant(row)

    async def get_grant(self, org_id: str, grant_id: str) -> ResourceGrant | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_GRANT_COLS} FROM resource_grants "
                            "WHERE org_id = :org AND id = :id"
                        ),
                        {"org": org_id, "id": grant_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_grant(row)

    async def list_grants(self, org_id: str, *, agent_id: str | None = None) -> list[ResourceGrant]:
        sql = f"SELECT {_GRANT_COLS} FROM resource_grants WHERE org_id = :org"
        params: dict[str, Any] = {"org": org_id}
        if agent_id is not None:
            sql += " AND agent_id = :agent"
            params["agent"] = agent_id
        sql += " ORDER BY created_at"
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (await conn.execute(text(sql), params)).mappings().all()
        return [_to_grant(row) for row in rows]

    async def list_grants_for_resource(
        self, org_id: str, resource_type: str, resource_id: str
    ) -> list[ResourceGrant]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_GRANT_COLS} FROM resource_grants "
                            "WHERE org_id = :org AND resource_type = :rtype "
                            "AND resource_id = :rid ORDER BY created_at"
                        ),
                        {"org": org_id, "rtype": resource_type, "rid": resource_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_grant(row) for row in rows]

    async def revoke_grant(
        self, org_id: str, grant_id: str, *, actor_user_id: str
    ) -> ResourceGrant | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            await self._require_active_manager(conn, org_id, actor_user_id)
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE resource_grants SET status = 'revoked', revoked_at = now(), "
                            "updated_at = now() "
                            "WHERE org_id = :org AND id = :id AND status = 'active' "
                            f"RETURNING {_GRANT_COLS}"
                        ),
                        {"org": org_id, "id": grant_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_grant(row)


__all__ = ["IdentityStore", "InMemoryIdentityStore", "PostgresIdentityStore"]
