"""Identity service: provisioning, org selection, and authorized CRUD (M3.6, WS-L).

The service is the single place that composes the repository (:class:`IdentityStore`), the
fine-grained :class:`AuthorizationService`, input validation, last-owner protection,
optimistic concurrency, and audit. Transport (FastAPI) and the durable schema stay out of
it, so it is fully exercisable with the in-memory store.

Provisioning policy (documented): an OIDC subject is resolved to a durable user by its
``(issuer, subject)`` link. When ``allow_jit`` is enabled a first-seen subject is
provisioned just-in-time (a new user + link); otherwise an unlinked subject is rejected and
must be linked explicitly (:meth:`link_identity`). A verified subject alone never grants org
access — the caller must additionally select an org the user is an active member of.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from keel_core.errors import PermissionDenied
from keel_core.identity.audit import AuditAction, AuditEvent, AuditSink, LoggingAuditSink
from keel_core.identity.authz import AuthorizationService
from keel_core.identity.models import (
    Agent,
    AgentKind,
    Capability,
    ConflictError,
    IdentityValidationError,
    Membership,
    MembershipRole,
    NotFoundError,
    Organization,
    OrganizationStatus,
    ResourceGrant,
    User,
    normalize_email,
    validate_agent_name,
    validate_display_name,
    validate_org_slug,
)
from keel_core.identity.oidc import OIDCClaims
from keel_core.identity.store import IdentityStore

# A stable synthetic identity for the single-operator local/self-hosted profile, so local
# identity reads/writes still bind to a durable user (never the ambient ``web:local`` scope).
LOCAL_ISSUER = "local"
LOCAL_SUBJECT = "operator"
LOCAL_ORG_SLUG = "local"
LOCAL_ORG_NAME = "Personal workspace"


@dataclass(frozen=True)
class OrgContext:
    """A user's resolved, authorized position inside one organization."""

    organization: Organization
    membership: Membership

    @property
    def org_id(self) -> str:
        return self.organization.id

    @property
    def capabilities(self) -> frozenset[Capability]:
        return self.membership.capabilities


class IdentityService:
    """Authorized identity + Agent + grant operations over an :class:`IdentityStore`."""

    def __init__(
        self,
        store: IdentityStore,
        *,
        authz: AuthorizationService | None = None,
        audit: AuditSink | None = None,
        allow_jit_provisioning: bool = False,
    ) -> None:
        self._store = store
        self._authz = authz or AuthorizationService()
        self._audit = audit or LoggingAuditSink()
        self._allow_jit = allow_jit_provisioning
        self._local_user_lock = asyncio.Lock()
        self._local_org_lock = asyncio.Lock()

    @property
    def store(self) -> IdentityStore:
        return self._store

    @property
    def authz(self) -> AuthorizationService:
        return self._authz

    @property
    def audit(self) -> AuditSink:
        return self._audit

    # --- provisioning ----------------------------------------------------------------
    async def resolve_oidc_user(self, claims: OIDCClaims) -> User:
        """Resolve a verified OIDC subject to a durable user (JIT or explicit-link)."""
        identity = await self._store.get_identity(claims.issuer, claims.subject)
        if identity is not None:
            user = await self._store.get_user(identity.user_id)
            if user is None or not user.is_active:
                raise NotFoundError("linked user is not active")
            await self._store.touch_identity_login(identity.id)
            return user
        if not self._allow_jit:
            raise NotFoundError(
                "OIDC subject is not linked to a user; explicit linking is required"
            )
        display = claims.email or f"{claims.issuer}#{claims.subject}"
        email = normalize_email(claims.email) if claims.email_verified else None
        user = await self._store.create_user(
            display_name=validate_display_name(display[:200]), email=email
        )
        await self._store.link_identity(
            user_id=user.id, issuer=claims.issuer, subject=claims.subject, email=email
        )
        self._audit.record(
            AuditEvent(AuditAction.user_provisioned, user.id, None, user.id, {"jit": "true"})
        )
        return user

    async def link_identity(self, user_id: str, claims: OIDCClaims) -> None:
        """Explicitly link a verified OIDC subject to an existing user."""
        user = await self._store.get_user(user_id)
        if user is None or not user.is_active:
            raise NotFoundError("user not found")
        email = normalize_email(claims.email) if claims.email_verified else None
        await self._store.link_identity(
            user_id=user_id, issuer=claims.issuer, subject=claims.subject, email=email
        )

    async def ensure_local_user(self, *, display_name: str = "Local Operator") -> User:
        """Get-or-create the durable local-operator user (single-operator local profile)."""
        async with self._local_user_lock:
            identity = await self._store.get_identity(LOCAL_ISSUER, LOCAL_SUBJECT)
            if identity is not None:
                user = await self._store.get_user(identity.user_id)
                if user is not None and user.is_active:
                    return user
            user = await self._store.create_user(
                display_name=validate_display_name(display_name), email=None
            )
            try:
                await self._store.link_identity(
                    user_id=user.id, issuer=LOCAL_ISSUER, subject=LOCAL_SUBJECT, email=None
                )
            except ConflictError:
                identity = await self._store.get_identity(LOCAL_ISSUER, LOCAL_SUBJECT)
                linked = (
                    await self._store.get_user(identity.user_id) if identity is not None else None
                )
                if linked is not None and linked.is_active:
                    return linked
                raise
            return user

    async def ensure_local_org(self, user_id: str) -> OrgContext:
        """Get-or-create the local preview user's stable personal organization."""
        async with self._local_org_lock:
            org = await self._store.get_org_by_slug(LOCAL_ORG_SLUG)
            if org is not None and org.status is OrganizationStatus.active:
                membership = await self._store.get_membership(org.id, user_id)
                if membership is None or not membership.is_active:
                    try:
                        membership = await self._store.create_membership(
                            org_id=org.id,
                            user_id=user_id,
                            role=MembershipRole.owner,
                        )
                    except ConflictError:
                        membership = await self._store.get_membership(org.id, user_id)
                if membership is not None and membership.is_active:
                    return OrgContext(organization=org, membership=membership)
            try:
                return await self.create_org(
                    user_id,
                    slug=LOCAL_ORG_SLUG,
                    display_name=LOCAL_ORG_NAME,
                )
            except ConflictError:
                return await self.select_org(user_id, LOCAL_ORG_SLUG)

    # --- org selection ---------------------------------------------------------------
    async def select_org(self, user_id: str, org_ref: str) -> OrgContext:
        """Resolve an org (by id or slug) the user is an active member of (reject spoofing)."""
        org = await self._store.get_org(org_ref)
        if org is None:
            org = await self._store.get_org_by_slug(org_ref)
        if org is None or org.status is not OrganizationStatus.active:
            raise NotFoundError("organization not found")
        membership = await self._store.get_membership(org.id, user_id)
        if membership is None or not membership.is_active:
            # Do not disclose org existence to a non-member: same error as unknown org.
            raise NotFoundError("organization not found")
        return OrgContext(organization=org, membership=membership)

    async def list_orgs_for_user(self, user_id: str) -> list[tuple[Organization, Membership]]:
        memberships = await self._store.list_memberships_for_user(user_id)
        result: list[tuple[Organization, Membership]] = []
        for membership in memberships:
            if not membership.is_active:
                continue
            org = await self._store.get_org(membership.org_id)
            if org is not None and org.status is OrganizationStatus.active:
                result.append((org, membership))
        return result

    async def create_org(self, actor_user_id: str, *, slug: str, display_name: str) -> OrgContext:
        """Create an org; the creating user becomes its first active owner."""
        clean_slug = validate_org_slug(slug)
        clean_name = validate_display_name(display_name)
        org = await self._store.create_org(slug=clean_slug, display_name=clean_name)
        membership = await self._store.create_membership(
            org_id=org.id, user_id=actor_user_id, role=MembershipRole.owner
        )
        self._audit.record(
            AuditEvent(AuditAction.org_created, actor_user_id, org.id, org.id, {"slug": clean_slug})
        )
        return OrgContext(organization=org, membership=membership)

    # --- membership management -------------------------------------------------------
    async def _require_actor_membership(self, org_id: str, actor_user_id: str) -> Membership:
        membership = await self._store.get_membership(org_id, actor_user_id)
        if membership is None or not membership.is_active:
            raise PermissionDenied("no active membership in this organization")
        return membership

    async def add_member(
        self, org_id: str, actor_user_id: str, target_user_id: str, role: MembershipRole
    ) -> Membership:
        actor = await self._require_actor_membership(org_id, actor_user_id)
        if not self._authz.can_manage_members(actor):
            raise PermissionDenied("managing members requires org admin/owner")
        # Only an owner may create another owner (admins cannot escalate to owner).
        if role is MembershipRole.owner and actor.role is not MembershipRole.owner:
            raise PermissionDenied("only an owner may grant the owner role")
        target = await self._store.get_user(target_user_id)
        if target is None or not target.is_active:
            raise NotFoundError("target user not found")
        membership = await self._store.create_membership(
            org_id=org_id,
            user_id=target_user_id,
            role=role,
            revalidate_actor_user_id=actor_user_id,
        )
        self._audit.record(
            AuditEvent(
                AuditAction.member_added,
                actor_user_id,
                org_id,
                target_user_id,
                {"role": role.value},
            )
        )
        return membership

    async def change_member_role(
        self, org_id: str, actor_user_id: str, target_user_id: str, new_role: MembershipRole
    ) -> Membership:
        actor = await self._require_actor_membership(org_id, actor_user_id)
        if not self._authz.can_manage_members(actor):
            raise PermissionDenied("managing members requires org admin/owner")
        current = await self._store.get_membership(org_id, target_user_id)
        if current is None or not current.is_active:
            raise NotFoundError("target membership not found")
        owner_change = MembershipRole.owner in (current.role, new_role)
        if owner_change and actor.role is not MembershipRole.owner:
            raise PermissionDenied("only an owner may change owner assignments")
        # Last-owner protection and the owner-actor requirement are enforced atomically by
        # the store from the target row + owner count read under the org lock (never from
        # this possibly-stale service read); a concurrent second demotion or a target that
        # became the final owner after this read cannot slip through.
        updated = await self._store.update_membership_role(
            org_id,
            target_user_id,
            new_role,
            revalidate_actor_user_id=actor_user_id,
        )
        if updated is None:
            raise NotFoundError("target membership not found")
        self._audit.record(
            AuditEvent(
                AuditAction.member_role_changed,
                actor_user_id,
                org_id,
                target_user_id,
                {"from": current.role.value, "to": new_role.value},
            )
        )
        return updated

    async def remove_member(
        self, org_id: str, actor_user_id: str, target_user_id: str
    ) -> Membership:
        actor = await self._require_actor_membership(org_id, actor_user_id)
        if not self._authz.can_manage_members(actor):
            raise PermissionDenied("managing members requires org admin/owner")
        current = await self._store.get_membership(org_id, target_user_id)
        if current is None or not current.is_active:
            raise NotFoundError("target membership not found")
        removes_owner = current.role is MembershipRole.owner
        if removes_owner and actor.role is not MembershipRole.owner:
            raise PermissionDenied("only an owner may remove an owner")
        # The store re-derives whether an owner is being removed from the target's locked row
        # and enforces last-owner protection + the owner-actor requirement atomically, so a
        # stale admin operation cannot remove a user who became the final owner.
        revoked = await self._store.revoke_membership(
            org_id,
            target_user_id,
            revalidate_actor_user_id=actor_user_id,
        )
        if revoked is None:
            raise NotFoundError("target membership not found")
        self._audit.record(
            AuditEvent(AuditAction.member_removed, actor_user_id, org_id, target_user_id, {})
        )
        return revoked

    async def list_members(self, org_id: str, actor_user_id: str) -> list[Membership]:
        await self._require_actor_membership(org_id, actor_user_id)
        return await self._store.list_memberships(org_id)

    # --- agents ----------------------------------------------------------------------
    async def create_agent(
        self,
        org_id: str,
        actor_user_id: str,
        *,
        kind: AgentKind,
        name: str,
        persona: str = "",
    ) -> Agent:
        actor = await self._require_actor_membership(org_id, actor_user_id)
        decision = self._authz.can_create_agent(actor, kind)
        if not decision:
            raise PermissionDenied(decision.reason)
        clean_name = validate_agent_name(name)
        if len(persona) > 20_000:
            raise IdentityValidationError("persona is too long")
        agent = await self._store.create_agent(
            org_id=org_id,
            kind=kind,
            owner_user_id=actor_user_id,
            name=clean_name,
            persona=persona,
        )
        self._audit.record(
            AuditEvent(
                AuditAction.agent_created,
                actor_user_id,
                org_id,
                agent.id,
                {"kind": kind.value, "name": clean_name},
            )
        )
        return agent

    async def list_visible_agents(self, org_id: str, actor_user_id: str) -> list[Agent]:
        actor = await self._require_actor_membership(org_id, actor_user_id)
        agents = await self._store.list_agents(org_id)
        return [
            agent for agent in agents if self._authz.can_view_agent(actor_user_id, actor, agent)
        ]

    async def get_agent(self, org_id: str, actor_user_id: str, agent_id: str) -> Agent:
        actor = await self._require_actor_membership(org_id, actor_user_id)
        agent = await self._store.get_agent(org_id, agent_id)
        if agent is None:
            raise NotFoundError("agent not found")
        if not self._authz.can_view_agent(actor_user_id, actor, agent):
            # Hide existence of a private personal agent.
            raise NotFoundError("agent not found")
        return agent

    async def update_agent(
        self,
        org_id: str,
        actor_user_id: str,
        agent_id: str,
        *,
        expected_version: int,
        name: str | None = None,
        persona: str | None = None,
    ) -> Agent:
        actor = await self._require_actor_membership(org_id, actor_user_id)
        agent = await self._store.get_agent(org_id, agent_id)
        if agent is None:
            raise NotFoundError("agent not found")
        decision = self._authz.can_manage_agent(actor_user_id, actor, agent)
        if not decision:
            # Preserve privacy of a personal agent the actor can't even see.
            if not self._authz.can_view_agent(actor_user_id, actor, agent):
                raise NotFoundError("agent not found")
            raise PermissionDenied(decision.reason)
        clean_name = validate_agent_name(name) if name is not None else None
        if persona is not None and len(persona) > 20_000:
            raise IdentityValidationError("persona is too long")
        updated = await self._store.update_agent(
            org_id,
            agent_id,
            expected_version=expected_version,
            name=clean_name,
            persona=persona,
        )
        if updated is None:
            raise NotFoundError("agent not found")
        self._audit.record(
            AuditEvent(AuditAction.agent_updated, actor_user_id, org_id, agent_id, {})
        )
        return updated

    async def archive_agent(
        self, org_id: str, actor_user_id: str, agent_id: str, *, expected_version: int
    ) -> Agent:
        actor = await self._require_actor_membership(org_id, actor_user_id)
        agent = await self._store.get_agent(org_id, agent_id)
        if agent is None:
            raise NotFoundError("agent not found")
        decision = self._authz.can_manage_agent(actor_user_id, actor, agent)
        if not decision:
            if not self._authz.can_view_agent(actor_user_id, actor, agent):
                raise NotFoundError("agent not found")
            raise PermissionDenied(decision.reason)
        archived = await self._store.archive_agent(
            org_id, agent_id, expected_version=expected_version
        )
        if archived is None:
            raise NotFoundError("agent not found")
        self._audit.record(
            AuditEvent(AuditAction.agent_archived, actor_user_id, org_id, agent_id, {})
        )
        return archived

    async def select_agent(self, org_id: str, actor_user_id: str, agent_id: str) -> Agent:
        """Compatibility bridge: resolve a persisted Agent the actor selects for a run.

        Enforces *use* authorization now so a future durable-run integration can bind the
        selected Agent without re-deriving the access decision.
        """
        actor = await self._require_actor_membership(org_id, actor_user_id)
        agent = await self._store.get_agent(org_id, agent_id)
        if agent is None:
            raise NotFoundError("agent not found")
        decision = self._authz.can_use_agent(actor_user_id, actor, agent)
        if not decision:
            if not self._authz.can_view_agent(actor_user_id, actor, agent):
                raise NotFoundError("agent not found")
            raise PermissionDenied(decision.reason)
        return agent

    async def resolve_machine_binding(
        self, org_ref: str, agent_ref: str
    ) -> tuple[Organization, Agent]:
        """Resolve a *configured* machine credential's org+Agent binding (no membership).

        A machine credential is provisioned by a trusted operator, not a human org member, so
        its org/Agent binding is authoritative configuration rather than a membership decision.
        It still fails closed: the org and the Agent must both exist and be active, else the
        credential resolves to nothing — never an ambient or cross-tenant scope. ``org_ref`` is
        matched by id or slug; ``agent_ref`` is the persisted Agent id within that org.
        """
        org = await self._store.get_org(org_ref)
        if org is None:
            org = await self._store.get_org_by_slug(org_ref)
        if org is None or org.status is not OrganizationStatus.active:
            raise NotFoundError("organization not found")
        agent = await self._store.get_agent(org.id, agent_ref)
        if agent is None or not agent.is_active:
            raise NotFoundError("agent not found")
        return org, agent

    async def authorize_im_run_as(
        self, org: Organization, agent: Agent, run_as_user_id: str
    ) -> Membership:
        """Validate the *run-as* org member a platform admin selected for an IM channel mapping.

        The IM run executes under ``run_as_user_id`` (never the platform admin that provisions the
        mapping), so that user must be an **active member** of ``org`` and independently authorized
        to *use* the selected Agent: a **personal** Agent requires its owner, a **team** Agent
        requires the member's ``use`` capability. Fails closed with :class:`PermissionDenied` (a
        non-member, revoked member, or an unauthorized member) or :class:`NotFoundError` — so a
        mapping can only ever run as a member entitled to the Agent, and a later revocation /
        member removal makes the worker's re-check fail the run closed.
        """
        membership = await self._store.get_membership(org.id, run_as_user_id)
        if membership is None or not membership.is_active:
            raise PermissionDenied("run-as user is not an active member of the organization")
        decision = self._authz.can_use_agent(run_as_user_id, membership, agent)
        if not decision:
            raise PermissionDenied(decision.reason)
        return membership

    # --- grants ----------------------------------------------------------------------
    async def grant_resource(
        self,
        org_id: str,
        actor_user_id: str,
        *,
        agent_id: str,
        resource_type: str,
        resource_id: str,
        capability: Capability,
    ) -> ResourceGrant:
        actor = await self._require_actor_membership(org_id, actor_user_id)
        agent = await self._store.get_agent(org_id, agent_id)
        if agent is None:
            raise NotFoundError("agent not found")
        decision = self._authz.can_grant_resource(actor, agent, capability)
        if not decision:
            raise PermissionDenied(decision.reason)
        self._validate_resource(resource_type, resource_id)
        grant = await self._store.create_grant(
            org_id=org_id,
            agent_id=agent_id,
            resource_type=resource_type,
            resource_id=resource_id,
            capability=capability,
            grantor_user_id=actor_user_id,
        )
        self._audit.record(
            AuditEvent(
                AuditAction.grant_created,
                actor_user_id,
                org_id,
                grant.id,
                {
                    "agent_id": agent_id,
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "capability": capability.value,
                },
            )
        )
        return grant

    async def list_grants(
        self, org_id: str, actor_user_id: str, *, agent_id: str | None = None
    ) -> list[ResourceGrant]:
        actor = await self._require_actor_membership(org_id, actor_user_id)
        if agent_id is not None:
            agent = await self._store.get_agent(org_id, agent_id)
            if agent is None:
                raise NotFoundError("agent not found")
            can_owner = agent.owner_user_id == actor_user_id
            if not (self._authz.is_org_admin(actor) or can_owner):
                raise PermissionDenied("listing grants requires org admin/owner or agent ownership")
        elif not self._authz.is_org_admin(actor):
            raise PermissionDenied("listing all grants requires org admin/owner")
        return await self._store.list_grants(org_id, agent_id=agent_id)

    # --- durable-run admission resolver seam (R1B) ------------------------------------
    # These two reads back the durable-run *admission* path (web/IM), never the grants
    # management surface: they exist only to embed a snapshot of the Agent's current
    # name/persona/version + its non-secret active grant descriptors into an admitted run's
    # immutable AgentConfigSnapshot. They deliberately skip the owner/admin *listing*
    # authorization :meth:`list_grants` enforces (and the member/private-agent *viewing*
    # authorization :meth:`get_agent` enforces): the caller has already established the run's
    # narrower authority to *use* this Agent (a user's `select_agent`, a machine credential's
    # scoped binding, or an IM channel mapping's admin-provisioned agent_id) before reaching
    # here, so reading its profile/grants for the snapshot never grants new access.

    async def get_agent_for_admission(self, org_id: str, agent_id: str) -> Agent | None:
        """Resolve an Agent's current persisted profile for a run's admission snapshot.

        Returns ``None`` (never raises) when the Agent no longer exists so a caller can fail
        the admission closed with its own error rather than an ambiguous identity exception.
        """
        return await self._store.get_agent(org_id, agent_id)

    async def active_resource_grants(self, org_id: str, agent_id: str) -> list[ResourceGrant]:
        """The currently-active resource grants for ``agent_id`` (admission snapshot use)."""
        grants = await self._store.list_grants(org_id, agent_id=agent_id)
        return [grant for grant in grants if grant.is_active]

    async def revoke_grant(self, org_id: str, actor_user_id: str, grant_id: str) -> ResourceGrant:
        actor = await self._require_actor_membership(org_id, actor_user_id)
        grant = await self._store.get_grant(org_id, grant_id)
        if grant is None:
            raise NotFoundError("grant not found")
        agent = await self._store.get_agent(org_id, grant.agent_id)
        if agent is None:
            raise NotFoundError("grant not found")
        decision = self._authz.can_grant_resource(actor, agent, grant.capability)
        if not decision:
            raise PermissionDenied(decision.reason)
        revoked = await self._store.revoke_grant(org_id, grant_id, actor_user_id=actor_user_id)
        if revoked is None:
            raise NotFoundError("grant not found")
        self._audit.record(
            AuditEvent(AuditAction.grant_revoked, actor_user_id, org_id, grant_id, {})
        )
        return revoked

    def _validate_resource(self, resource_type: str, resource_id: str) -> None:
        if not (1 <= len(resource_type) <= 100) or not resource_type.strip():
            raise IdentityValidationError("resource_type must be 1-100 characters")
        if not (1 <= len(resource_id) <= 200) or not resource_id.strip():
            raise IdentityValidationError("resource_id must be 1-200 characters")

    # --- profile ---------------------------------------------------------------------
    async def get_profile(self, user_id: str) -> tuple[User, list[tuple[Organization, Membership]]]:
        user = await self._store.get_user(user_id)
        if user is None or not user.is_active:
            raise NotFoundError("user not found")
        return user, await self.list_orgs_for_user(user_id)


__all__ = ["IdentityService", "OrgContext"]
