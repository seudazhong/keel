"""Principal / resource / capability authorization for identity + Agents (M3.6, WS-L).

This is the *fine-grained* authorization model that composes three inputs and fails closed:

1. **Membership role** — the capabilities a user holds in an org (``ROLE_CAPABILITIES``).
2. **Agent ownership + kind** — a *personal* Agent is private to its owner (and org
   admins/owners for management); a *team* Agent follows org membership.
3. **Explicit resource grants** — the capabilities an Agent has been granted on a specific
   ``(resource_type, resource_id)``.

An Agent acting on a resource never exceeds *both* the acting user's org capabilities and
the Agent's granted capabilities — i.e. the effective set is their **intersection** (a
confused-deputy / privilege-escalation defense). This is deliberately not the coarse scalar
``role >= minimum`` comparison used for endpoint tiers in ``keel_server.auth``; keep
``DefaultScopeGuard`` for the existing event-core scope checks and use this service for
identity/Agent/grant decisions.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from keel_core.identity.models import (
    ADMIN_ROLES,
    Agent,
    AgentKind,
    Capability,
    Membership,
    ResourceGrant,
    capabilities_for_role,
)


@dataclass(frozen=True)
class AuthzDecision:
    """A fail-closed authorization outcome carrying a non-sensitive reason."""

    allowed: bool
    reason: str

    def __bool__(self) -> bool:
        return self.allowed


_ALLOW = AuthzDecision(True, "ok")


def _deny(reason: str) -> AuthzDecision:
    return AuthzDecision(False, reason)


class AuthorizationService:
    """Stateless authorization decisions over already-loaded identity records."""

    # --- org-level capabilities ------------------------------------------------------
    def org_capabilities(self, membership: Membership | None) -> frozenset[Capability]:
        """The capabilities the actor holds in the org (empty without active membership)."""
        if membership is None or not membership.is_active:
            return frozenset()
        return capabilities_for_role(membership.role)

    def has_org_capability(self, membership: Membership | None, capability: Capability) -> bool:
        return capability in self.org_capabilities(membership)

    def is_org_admin(self, membership: Membership | None) -> bool:
        return membership is not None and membership.is_active and membership.role in ADMIN_ROLES

    # --- agent visibility / use ------------------------------------------------------
    def can_view_agent(
        self, actor_user_id: str, membership: Membership | None, agent: Agent
    ) -> AuthzDecision:
        """Whether the actor may see ``agent`` (list/read)."""
        if membership is None or not membership.is_active:
            return _deny("no active membership in the agent's org")
        if membership.org_id != agent.org_id:
            return _deny("membership org does not match agent org")
        if agent.kind is AgentKind.personal:
            if agent.owner_user_id == actor_user_id or self.is_org_admin(membership):
                return _ALLOW
            return _deny("personal agent is private to its owner")
        # Team agent: any active member with at least read may view.
        if Capability.read in self.org_capabilities(membership):
            return _ALLOW
        return _deny("membership lacks read capability")

    def can_use_agent(
        self, actor_user_id: str, membership: Membership | None, agent: Agent
    ) -> AuthzDecision:
        """Whether the actor may *use* (run/select) ``agent``."""
        view = self.can_view_agent(actor_user_id, membership, agent)
        if not view:
            return view
        if not agent.is_active:
            return _deny("agent is archived")
        if agent.kind is AgentKind.personal:
            # The owner uses their own personal agent regardless of org role tier.
            if agent.owner_user_id == actor_user_id:
                return _ALLOW
            # An admin may manage but not silently *use* another's personal agent.
            return _deny("personal agent is private to its owner")
        if Capability.use in self.org_capabilities(membership):
            return _ALLOW
        return _deny("membership lacks use capability")

    def can_manage_agent(
        self, actor_user_id: str, membership: Membership | None, agent: Agent
    ) -> AuthzDecision:
        """Whether the actor may edit/archive ``agent``."""
        if membership is None or not membership.is_active:
            return _deny("no active membership in the agent's org")
        if membership.org_id != agent.org_id:
            return _deny("membership org does not match agent org")
        if agent.kind is AgentKind.personal and agent.owner_user_id == actor_user_id:
            return _ALLOW
        if self.is_org_admin(membership):
            return _ALLOW
        return _deny("managing this agent requires ownership or org admin/owner")

    def can_create_agent(self, membership: Membership | None, kind: AgentKind) -> AuthzDecision:
        """Whether the actor may create an Agent of ``kind`` in the org."""
        if membership is None or not membership.is_active:
            return _deny("no active membership in the org")
        caps = self.org_capabilities(membership)
        if kind is AgentKind.personal:
            if Capability.use in caps:
                return _ALLOW
            return _deny("creating a personal agent requires at least member")
        if Capability.manage in caps:
            return _ALLOW
        return _deny("creating a team agent requires org admin/owner")

    # --- resource grants -------------------------------------------------------------
    def granted_capabilities(
        self,
        agent: Agent,
        grants: Iterable[ResourceGrant],
        resource_type: str,
        resource_id: str,
    ) -> frozenset[Capability]:
        """Capabilities actively granted to ``agent`` on the exact resource."""
        return frozenset(
            grant.capability
            for grant in grants
            if grant.is_active
            and grant.agent_id == agent.id
            and grant.org_id == agent.org_id
            and grant.resource_type == resource_type
            and grant.resource_id == resource_id
        )

    def effective_resource_capabilities(
        self,
        actor_user_id: str,
        membership: Membership | None,
        agent: Agent,
        grants: Iterable[ResourceGrant],
        resource_type: str,
        resource_id: str,
    ) -> frozenset[Capability]:
        """Intersection of the acting user's org capabilities and the Agent's grants.

        Empty if the actor cannot even use the Agent — an Agent never confers authority
        the acting user does not independently hold (confused-deputy defense).
        """
        if not self.can_use_agent(actor_user_id, membership, agent):
            return frozenset()
        granted = self.granted_capabilities(agent, grants, resource_type, resource_id)
        return granted & self.org_capabilities(membership)

    def can_agent_access_resource(
        self,
        actor_user_id: str,
        membership: Membership | None,
        agent: Agent,
        grants: Iterable[ResourceGrant],
        resource_type: str,
        resource_id: str,
        capability: Capability,
    ) -> AuthzDecision:
        """Whether ``agent``, driven by the actor, may exercise ``capability`` on a resource."""
        use = self.can_use_agent(actor_user_id, membership, agent)
        if not use:
            return use
        effective = self.effective_resource_capabilities(
            actor_user_id, membership, agent, grants, resource_type, resource_id
        )
        if capability in effective:
            return _ALLOW
        granted = self.granted_capabilities(agent, grants, resource_type, resource_id)
        if capability not in granted:
            return _deny("agent has no active grant for this capability on the resource")
        return _deny("acting user lacks this capability in the org")

    def can_grant_resource(
        self, grantor_membership: Membership | None, agent: Agent, capability: Capability
    ) -> AuthzDecision:
        """Whether the grantor may bind ``capability`` for ``agent`` (grant/revoke)."""
        if grantor_membership is None or not grantor_membership.is_active:
            return _deny("no active membership in the org")
        if grantor_membership.org_id != agent.org_id:
            return _deny("cannot grant across organizations")
        caps = self.org_capabilities(grantor_membership)
        if Capability.manage not in caps:
            return _deny("granting resources requires org admin/owner")
        # A grantor cannot confer a capability it does not itself hold.
        if capability not in caps:
            return _deny("grantor cannot confer a capability it does not hold")
        return _ALLOW

    # --- membership administration ---------------------------------------------------
    def can_manage_members(self, membership: Membership | None) -> AuthzDecision:
        if self.is_org_admin(membership):
            return _ALLOW
        return _deny("managing members requires org admin/owner")


__all__ = ["AuthorizationService", "AuthzDecision"]
