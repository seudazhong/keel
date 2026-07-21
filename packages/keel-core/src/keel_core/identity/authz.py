"""Principal / resource / capability authorization for identity + Agents (M3.6, WS-L, R1B).

This is the *fine-grained* authorization model that composes four inputs and fails closed:

1. **Membership role** — the capabilities a user holds in an org (``ROLE_CAPABILITIES``).
2. **Agent ownership + kind** — a *personal* Agent is private to its owner (and org
   admins/owners for management); a *team* Agent requires an explicit :class:`AgentAccess`
   edge (R1B) — bare org membership no longer implies team-Agent discovery/use.
3. **Agent Access edges** — a ``(org, agent, user|channel principal) -> level`` edge whose
   ``discover``/``use``/``manage`` tiers are ordered (a higher tier implies the lower ones).
   An org admin/owner keeps an explicit administrative path that behaves as an implicit
   ``manage`` edge on every team Agent in its org; every other member/viewer needs an active
   edge. Revoking the edge takes effect on the next authorization check (admission, claim,
   or endpoint read) — there is no caching layer to invalidate.
4. **Explicit resource grants** — the capabilities an Agent has been granted on a specific
   ``(resource_type, resource_id)``.

An Agent acting on a resource never exceeds *both* the acting user's org capabilities and
the Agent's granted capabilities — i.e. the effective set is their **intersection** (a
confused-deputy / privilege-escalation defense). This is deliberately not the coarse scalar
``role >= minimum`` comparison used for endpoint tiers in ``keel_server.auth``; keep
``DefaultScopeGuard`` for the existing event-core scope checks and use this service for
identity/Agent/grant decisions.

Session ownership/visibility (:mod:`keel_core.session_visibility`) is a deliberately
*separate* axis: using a team Agent (i.e. passing the checks in this module) never by
itself grants reading another user's private session — see ``can_view_session``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from keel_core.identity.models import (
    ADMIN_ROLES,
    Agent,
    AgentAccess,
    AgentAccessLevel,
    AgentAccessPrincipalType,
    AgentKind,
    Capability,
    Membership,
    ResourceGrant,
    agent_access_level_at_least,
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

    # --- Agent Access edges (team Agents; R1B) ---------------------------------------
    def effective_agent_access_level(
        self, actor_user_id: str, agent: Agent, access_edges: Iterable[AgentAccess]
    ) -> AgentAccessLevel | None:
        """The highest active :class:`AgentAccess` level ``actor_user_id`` holds on ``agent``.

        Only ``user``-principal edges scoped to the *same* org+agent are considered; a
        ``channel``-principal edge never grants a human actor authority here (channel access
        is resolved separately by the IM admission path against the channel's own identity).
        Returns ``None`` with no matching active edge (fails closed — a caller with no edge
        gets no team-Agent discovery/use)."""
        best: AgentAccessLevel | None = None
        for edge in access_edges:
            if not edge.is_active:
                continue
            if edge.org_id != agent.org_id or edge.agent_id != agent.id:
                continue
            if edge.principal_type is not AgentAccessPrincipalType.user:
                continue
            if edge.principal_id != actor_user_id:
                continue
            if best is None or agent_access_level_at_least(edge.level, best):
                best = edge.level
        return best

    # --- agent visibility / use ------------------------------------------------------
    def can_view_agent(
        self,
        actor_user_id: str,
        membership: Membership | None,
        agent: Agent,
        access_edges: Iterable[AgentAccess] = (),
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
        # Team agent: bare org membership no longer implies discovery (R1B). An org
        # admin/owner keeps an explicit administrative path (documented — manage implies
        # discover/use); everyone else needs an active Agent Access edge.
        if self.is_org_admin(membership):
            return _ALLOW
        if self.effective_agent_access_level(actor_user_id, agent, access_edges) is not None:
            return _ALLOW
        return _deny("no active Agent Access edge for this agent")

    def can_use_agent(
        self,
        actor_user_id: str,
        membership: Membership | None,
        agent: Agent,
        access_edges: Iterable[AgentAccess] = (),
    ) -> AuthzDecision:
        """Whether the actor may *use* (run/select) ``agent``."""
        view = self.can_view_agent(actor_user_id, membership, agent, access_edges)
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
        if self.is_org_admin(membership):
            return _ALLOW
        level = self.effective_agent_access_level(actor_user_id, agent, access_edges)
        if level is not None and agent_access_level_at_least(level, AgentAccessLevel.use):
            return _ALLOW
        return _deny("agent access level is discover-only")

    def can_manage_agent(
        self,
        actor_user_id: str,
        membership: Membership | None,
        agent: Agent,
        access_edges: Iterable[AgentAccess] = (),
    ) -> AuthzDecision:
        """Whether the actor may edit/archive ``agent`` (or manage its Agent Access edges)."""
        if membership is None or not membership.is_active:
            return _deny("no active membership in the agent's org")
        if membership.org_id != agent.org_id:
            return _deny("membership org does not match agent org")
        if agent.kind is AgentKind.personal and agent.owner_user_id == actor_user_id:
            return _ALLOW
        if self.is_org_admin(membership):
            return _ALLOW
        if agent.kind is AgentKind.team:
            level = self.effective_agent_access_level(actor_user_id, agent, access_edges)
            if level is not None and agent_access_level_at_least(level, AgentAccessLevel.manage):
                return _ALLOW
        return _deny("managing this agent requires ownership, org admin/owner, or manage access")

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
        access_edges: Iterable[AgentAccess] = (),
    ) -> frozenset[Capability]:
        """Intersection of the acting user's org capabilities and the Agent's grants.

        Empty if the actor cannot even use the Agent — an Agent never confers authority
        the acting user does not independently hold (confused-deputy defense).
        """
        if not self.can_use_agent(actor_user_id, membership, agent, access_edges):
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
        access_edges: Iterable[AgentAccess] = (),
    ) -> AuthzDecision:
        """Whether ``agent``, driven by the actor, may exercise ``capability`` on a resource."""
        use = self.can_use_agent(actor_user_id, membership, agent, access_edges)
        if not use:
            return use
        effective = self.effective_resource_capabilities(
            actor_user_id, membership, agent, grants, resource_type, resource_id, access_edges
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

    def can_manage_agent_access(
        self,
        actor_user_id: str,
        grantor_membership: Membership | None,
        agent: Agent,
        access_edges: Iterable[AgentAccess] = (),
    ) -> AuthzDecision:
        """Whether the actor may grant/revoke/list :class:`AgentAccess` edges on ``agent``.

        Only **team** Agents carry access edges (a personal Agent stays owner-private).
        Org admin/owner keep the administrative path; a delegated ``manage``-level edge
        holder may also administer the Agent's own access list (but never escalate itself
        or another principal beyond ``manage``)."""
        if agent.kind is not AgentKind.team:
            return _deny("only team Agents carry Agent Access edges")
        if grantor_membership is None or not grantor_membership.is_active:
            return _deny("no active membership in the org")
        if grantor_membership.org_id != agent.org_id:
            return _deny("cannot manage access across organizations")
        if self.is_org_admin(grantor_membership):
            return _ALLOW
        level = self.effective_agent_access_level(actor_user_id, agent, access_edges)
        if level is not None and agent_access_level_at_least(level, AgentAccessLevel.manage):
            return _ALLOW
        return _deny("managing Agent Access requires org admin/owner or manage-level access")

    # --- membership administration ---------------------------------------------------
    def can_manage_members(self, membership: Membership | None) -> AuthzDecision:
        if self.is_org_admin(membership):
            return _ALLOW
        return _deny("managing members requires org admin/owner")


__all__ = ["AuthorizationService", "AuthzDecision"]
