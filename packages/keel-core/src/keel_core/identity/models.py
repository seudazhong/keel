"""Identity domain models: users, orgs, memberships, Agents, grants (M3.6, WS-L).

Pure, transport-free dataclasses + enums + validators + errors, mirroring the durable
schema in migration ``0013_identity_agents_grants``. Identifiers are stable, prefixed hex
strings (``usr_``/``org_``/``mem_``/``agt_``/``grt_``/``oid_``) so a caller can tell a
resource's kind from its id and ids never collide across kinds.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from keel_core.errors import KeelError

# --- Identifiers ---------------------------------------------------------------------

_ID_HEX_CHARS = 32
_USER_PREFIX = "usr_"
_ORG_PREFIX = "org_"
_MEMBERSHIP_PREFIX = "mem_"
_AGENT_PREFIX = "agt_"
_GRANT_PREFIX = "grt_"
_OIDC_PREFIX = "oid_"
_AGENT_ACCESS_PREFIX = "aac_"

type UserId = str
type OrganizationId = str
type MembershipId = str
type AgentId = str
type GrantId = str
type OIDCIdentityId = str
type AgentAccessId = str


def _new_id(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex}"


def new_user_id() -> UserId:
    return _new_id(_USER_PREFIX)


def new_org_id() -> OrganizationId:
    return _new_id(_ORG_PREFIX)


def new_membership_id() -> MembershipId:
    return _new_id(_MEMBERSHIP_PREFIX)


def new_agent_id() -> AgentId:
    return _new_id(_AGENT_PREFIX)


def new_grant_id() -> GrantId:
    return _new_id(_GRANT_PREFIX)


def new_oidc_id() -> OIDCIdentityId:
    return _new_id(_OIDC_PREFIX)


def new_agent_access_id() -> AgentAccessId:
    return _new_id(_AGENT_ACCESS_PREFIX)


# --- Errors --------------------------------------------------------------------------


class IdentityError(KeelError):
    """Base class for identity-domain errors."""


class IdentityValidationError(IdentityError):
    """A supplied identity value (name/slug/email/role) is invalid."""


class LastOwnerError(IdentityError):
    """An org would be left with no active owner (last-owner protection, fail closed)."""


class OptimisticConcurrencyError(IdentityError):
    """A durable update lost an optimistic-concurrency race (stale ``version``)."""


class ConflictError(IdentityError):
    """A uniqueness constraint (slug/email/name/membership) was violated."""


class NotFoundError(IdentityError):
    """A referenced identity resource does not exist (or is not visible)."""


class CrossOrgError(IdentityError):
    """A resource from one org was referenced from another (confused-deputy defense)."""


# --- Enumerations --------------------------------------------------------------------


class UserStatus(StrEnum):
    active = "active"
    suspended = "suspended"
    deleted = "deleted"


class OrganizationStatus(StrEnum):
    active = "active"
    archived = "archived"


class MembershipRole(StrEnum):
    """Ordered org RBAC tiers; a higher tier's capabilities include the lower tiers'."""

    viewer = "viewer"
    member = "member"
    admin = "admin"
    owner = "owner"


class MembershipStatus(StrEnum):
    active = "active"
    revoked = "revoked"


class AgentKind(StrEnum):
    personal = "personal"
    team = "team"


class AgentStatus(StrEnum):
    active = "active"
    archived = "archived"


class GrantStatus(StrEnum):
    active = "active"
    revoked = "revoked"


class AgentAccessPrincipalType(StrEnum):
    """What kind of principal an :class:`AgentAccess` edge binds — a durable user or a
    durable channel identity (e.g. an IM chat/room), never a bare org membership."""

    user = "user"
    channel = "channel"


class AgentAccessLevel(StrEnum):
    """Ordered team-Agent access tiers; a higher tier's capabilities include the lower
    tiers' (``manage`` implies ``use`` implies ``discover``)."""

    discover = "discover"
    use = "use"
    manage = "manage"


# AgentAccessLevel -> its rank for "at least" comparisons. Never compare the enum members
# directly (StrEnum ordering is lexical, not the intended tier order).
_AGENT_ACCESS_LEVEL_RANK: dict[AgentAccessLevel, int] = {
    AgentAccessLevel.discover: 0,
    AgentAccessLevel.use: 1,
    AgentAccessLevel.manage: 2,
}


def agent_access_level_at_least(level: AgentAccessLevel, minimum: AgentAccessLevel) -> bool:
    """Whether ``level`` implies at least ``minimum`` (``manage`` implies ``use``/``discover``)."""
    return _AGENT_ACCESS_LEVEL_RANK[level] >= _AGENT_ACCESS_LEVEL_RANK[minimum]


class Capability(StrEnum):
    """A capability an actor/Agent may hold on the org or a resource.

    Ordered for readability only; authorization treats capabilities as a **set**, never a
    scalar comparison (contrast the coarse endpoint role tiers in ``keel_server.auth``).
    """

    read = "read"
    use = "use"
    write = "write"
    manage = "manage"


# Membership role -> the capabilities it confers on the org. Fail-closed default (a role
# not listed here confers nothing).
ROLE_CAPABILITIES: dict[MembershipRole, frozenset[Capability]] = {
    MembershipRole.viewer: frozenset({Capability.read}),
    MembershipRole.member: frozenset({Capability.read, Capability.use}),
    MembershipRole.admin: frozenset(
        {Capability.read, Capability.use, Capability.write, Capability.manage}
    ),
    MembershipRole.owner: frozenset(
        {Capability.read, Capability.use, Capability.write, Capability.manage}
    ),
}

# Roles that can administer an org (manage members, agents, grants) and are subject to
# last-owner protection.
ADMIN_ROLES: frozenset[MembershipRole] = frozenset({MembershipRole.owner, MembershipRole.admin})


def capabilities_for_role(role: MembershipRole) -> frozenset[Capability]:
    return ROLE_CAPABILITIES.get(role, frozenset())


# --- Validation ----------------------------------------------------------------------

_SLUG_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,38}[a-z0-9])$")
_AGENT_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,62}[A-Za-z0-9]$")
_DISPLAY_NAME_MAX = 200
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_EMAIL_MAX = 320


def validate_org_slug(value: str) -> str:
    """Normalize + validate an org slug (lower, 3-40 chars, ``a-z0-9-``, no edge dash)."""
    slug = value.strip().lower()
    if not _SLUG_RE.match(slug):
        raise IdentityValidationError(
            "organization slug must be 3-40 chars of a-z, 0-9 or '-' (no leading/trailing '-')"
        )
    return slug


def validate_display_name(value: str) -> str:
    name = value.strip()
    if not name or len(name) > _DISPLAY_NAME_MAX:
        raise IdentityValidationError(f"display name must be 1-{_DISPLAY_NAME_MAX} characters")
    return name


def validate_agent_name(value: str) -> str:
    name = value.strip()
    if not _AGENT_NAME_RE.match(name):
        raise IdentityValidationError(
            "agent name must be 2-64 chars of letters, digits, space, '_', '.' or '-'"
        )
    return name


_PRINCIPAL_ID_MAX = 300


def validate_principal_id(value: str) -> str:
    """Validate an :class:`AgentAccess` principal id (a user id or an opaque channel key).

    Bounded, non-blank, and traversal-free (no control characters) — a caller-composed
    channel key (e.g. ``"slack:T1/C2"``) must never smuggle whitespace/newlines into an
    audit log or SQL parameter."""
    principal = value.strip()
    if not principal or len(principal) > _PRINCIPAL_ID_MAX or any(ord(c) < 0x20 for c in principal):
        raise IdentityValidationError(f"principal id must be 1-{_PRINCIPAL_ID_MAX} characters")
    return principal


def normalize_email(value: str | None) -> str | None:
    """Lower-case + validate an optional email; ``None``/blank -> ``None``."""
    if value is None:
        return None
    email = value.strip().lower()
    if not email:
        return None
    if len(email) > _EMAIL_MAX or not _EMAIL_RE.match(email):
        raise IdentityValidationError("email is not a valid address")
    return email


# --- Records -------------------------------------------------------------------------


@dataclass(frozen=True)
class User:
    """A durable human identity (global; may belong to many orgs)."""

    id: UserId
    display_name: str
    email: str | None = None
    status: UserStatus = UserStatus.active
    created_at: datetime | None = None
    updated_at: datetime | None = None
    deleted_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.status is UserStatus.active


@dataclass(frozen=True)
class OIDCIdentity:
    """A global ``(issuer, subject) -> user`` link from an external OIDC provider."""

    id: OIDCIdentityId
    user_id: UserId
    issuer: str
    subject: str
    email: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_login_at: datetime | None = None


@dataclass(frozen=True)
class Organization:
    """The tenant root (global lookup table)."""

    id: OrganizationId
    slug: str
    display_name: str
    status: OrganizationStatus = OrganizationStatus.active
    created_at: datetime | None = None
    updated_at: datetime | None = None
    archived_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.status is OrganizationStatus.active


@dataclass(frozen=True)
class Membership:
    """The user<->org RBAC edge (tenant-owned; carries ``org_id``)."""

    id: MembershipId
    org_id: OrganizationId
    user_id: UserId
    role: MembershipRole
    status: MembershipStatus = MembershipStatus.active
    created_at: datetime | None = None
    updated_at: datetime | None = None
    revoked_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.status is MembershipStatus.active

    @property
    def capabilities(self) -> frozenset[Capability]:
        if not self.is_active:
            return frozenset()
        return capabilities_for_role(self.role)


@dataclass(frozen=True)
class Agent:
    """A persisted personal/team Agent owned by a user inside an org (tenant-owned)."""

    id: AgentId
    org_id: OrganizationId
    kind: AgentKind
    owner_user_id: UserId
    name: str
    persona: str = ""
    status: AgentStatus = AgentStatus.active
    version: int = 1
    created_at: datetime | None = None
    updated_at: datetime | None = None
    archived_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.status is AgentStatus.active


@dataclass(frozen=True)
class ResourceGrant:
    """An explicit ``(org, agent, resource, capability)`` grant (tenant-owned)."""

    id: GrantId
    org_id: OrganizationId
    agent_id: AgentId
    resource_type: str
    resource_id: str
    capability: Capability
    grantor_user_id: UserId
    status: GrantStatus = GrantStatus.active
    created_at: datetime | None = None
    updated_at: datetime | None = None
    revoked_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.status is GrantStatus.active


@dataclass(frozen=True)
class AgentAccess:
    """An explicit ``(org, agent, principal) -> level`` edge for a **team** Agent.

    First-class replacement for "bare org membership implies team-Agent access": a member/
    viewer's org role alone no longer discovers or uses a team Agent — an active edge here
    is required (R1B). ``principal_type`` is ``user`` (a durable :class:`User`) or ``channel``
    (an opaque, caller-defined channel identity string, e.g. an IM chat/room key); exactly one
    row exists per ``(org, agent, principal_type, principal_id)`` — granting again updates the
    ``level``/reactivates rather than creating a duplicate edge. Personal Agents never carry
    these edges (they stay private to their owner via :class:`Agent.kind`)."""

    id: AgentAccessId
    org_id: OrganizationId
    agent_id: AgentId
    principal_type: AgentAccessPrincipalType
    principal_id: str
    level: AgentAccessLevel
    grantor_user_id: UserId
    status: GrantStatus = GrantStatus.active
    created_at: datetime | None = None
    updated_at: datetime | None = None
    revoked_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.status is GrantStatus.active

    def level_at_least(self, minimum: AgentAccessLevel) -> bool:
        return self.is_active and agent_access_level_at_least(self.level, minimum)


__all__ = [
    "ADMIN_ROLES",
    "ROLE_CAPABILITIES",
    "Agent",
    "AgentAccess",
    "AgentAccessId",
    "AgentAccessLevel",
    "AgentAccessPrincipalType",
    "AgentId",
    "AgentKind",
    "AgentStatus",
    "Capability",
    "CrossOrgError",
    "GrantId",
    "GrantStatus",
    "IdentityError",
    "IdentityValidationError",
    "LastOwnerError",
    "ConflictError",
    "NotFoundError",
    "Membership",
    "MembershipId",
    "MembershipRole",
    "MembershipStatus",
    "OIDCIdentity",
    "OIDCIdentityId",
    "OptimisticConcurrencyError",
    "Organization",
    "OrganizationId",
    "OrganizationStatus",
    "ResourceGrant",
    "User",
    "UserId",
    "UserStatus",
    "agent_access_level_at_least",
    "capabilities_for_role",
    "new_agent_access_id",
    "new_agent_id",
    "new_grant_id",
    "new_membership_id",
    "new_oidc_id",
    "new_org_id",
    "new_user_id",
    "normalize_email",
    "validate_agent_name",
    "validate_display_name",
    "validate_org_slug",
    "validate_principal_id",
]
