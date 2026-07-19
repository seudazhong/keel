"""Concrete :class:`~keel_core.patch.coordinator.PatchAuthorizer` over :class:`ProjectService`.

The single bridge between the patch control plane and the project/identity substrate. It resolves,
for every patch operation, the authorized project↔storage↔remote binding a proposal runs over —
*only* through the public :class:`ProjectService` seams (never a private membership/grant/GitHub
helper), so authorization can never drift from the rest of the platform:

* **capability** — generation/read run == the ``use`` capability; human approval requires
  ``write``. The trusted GitHub writeback (a remote branch + Draft PR side effect) re-verifies
  *both* at job time: the generation **requester** must still hold ``use`` (authoring never
  confers push rights) and the **approver** who granted the decision must still hold ``write``.
  Every seam is re-invocable at worker/decision time so access revoked between request and
  execution fails closed.
* **scope** — the canonical per-Agent scope is derived from the immutable ``org_id`` + selected
  Agent via :func:`keel_core.scoping.derive_agent_scope`; there is no org-first / default
  installation guessing. A request with no explicit Agent is the *actor acting directly* (the
  reserved :data:`~keel_core.patch.coordinator.DEFAULT_PATCH_AGENT_ID` label names the scope,
  never a real Agent principal), so it never requires an Agent grant.
* **remote target** — the writeback target uses the **exact** ``installation_id`` recorded on the
  repository bound to ``project.github_repository_id``; the repository↔project↔installation binding
  and the installation's active state are all re-verified, and the *stored* clone URL is re-pinned
  through :meth:`ProjectService.safe_clone_url` (never trusted as-is). A non-GitHub project resolves
  ``target=None``.

Not-found and unauthorized are kept distinct without leaking a foreign resource's existence:
:meth:`ProjectService.authorize_project` raises :class:`ProjectNotFoundError` for a project that
does not exist *in this org* and :class:`PermissionDenied` for an in-org project the actor/Agent
cannot reach. Both propagate unchanged (they are not patch errors and must never be swallowed).
"""

from __future__ import annotations

from keel_core.identity.models import Capability
from keel_core.projects.models import GitHubRepository, Project
from keel_core.projects.service import ProjectService
from keel_core.scoping import derive_agent_scope

from .coordinator import DEFAULT_PATCH_AGENT_ID, PatchAuthorizer, ProjectBinding
from .errors import PatchValidationError
from .writeback import WritebackTarget


class ProjectServicePatchAuthorizer(PatchAuthorizer):
    """Authorize patch operations and resolve their project binding via :class:`ProjectService`."""

    def __init__(self, service: ProjectService) -> None:
        self._service = service

    # --- capability seams ------------------------------------------------------------
    async def authorize_generation(
        self, org_id: str, actor: str, project_id: str, *, agent_id: str | None, run_id: str
    ) -> ProjectBinding:
        """Authorize controlled generation (``use``) and resolve the run's project binding."""
        project = await self._service.authorize_project(
            org_id,
            actor,
            project_id,
            capability=Capability.use,
            agent_id=self._auth_agent(agent_id),
        )
        return await self._binding_for(org_id, project, agent_id=agent_id, run_id=run_id)

    async def authorize_writeback(
        self,
        org_id: str,
        project_id: str,
        *,
        requester_actor: str,
        approved_by: str,
        agent_id: str | None,
        run_id: str,
    ) -> ProjectBinding:
        """Authorize the trusted GitHub writeback and resolve the remote binding.

        Two independent capabilities are re-verified at job time so access revoked between approval
        and execution fails closed before any push/Draft-PR side effect:

        * the generation **requester** (the proposal's actor/Agent) must still hold ``use`` —
          authoring a proposal never confers push rights, so this is deliberately *not* ``write``;
        * the human **approver** who granted the decision must still hold ``write`` — only an
          approval unlocks the remote branch + Draft PR side effect, and the approver authorizes
          *directly* (a person approves, never an Agent principal).

        The exact GitHub target is resolved only after both checks pass.
        """
        project = await self._service.authorize_project(
            org_id,
            requester_actor,
            project_id,
            capability=Capability.use,
            agent_id=self._auth_agent(agent_id),
        )
        await self._service.authorize_project(
            org_id, approved_by, project_id, capability=Capability.write
        )
        return await self._binding_for(org_id, project, agent_id=agent_id, run_id=run_id)

    async def authorize_read(self, org_id: str, actor: str, project_id: str) -> None:
        """Authorize a read/list/deny/cancel of a project's proposals (``read``)."""
        await self._service.authorize_project(org_id, actor, project_id, capability=Capability.read)

    async def authorize_approval(self, org_id: str, actor: str, project_id: str) -> None:
        """Authorize a human approval decision (``write`` — it unlocks the remote side effect).

        The deciding actor authorizes *directly* (no Agent principal): a person approves, an Agent
        does not. Denials use :meth:`authorize_read`, so a read-only member can decline but never
        approve a proposal that would push to the remote.
        """
        await self._service.authorize_project(
            org_id, actor, project_id, capability=Capability.write
        )

    async def associate_run(
        self, org_id: str, actor: str, project_id: str, run_id: str, *, agent_id: str | None
    ) -> None:
        """Associate the durable patch run with the project (delegates to the public service)."""
        await self._service.associate_run(
            org_id, actor, project_id, run_id, agent_id=self._auth_agent(agent_id)
        )

    # --- binding resolution ----------------------------------------------------------
    async def _binding_for(
        self, org_id: str, project: Project, *, agent_id: str | None, run_id: str
    ) -> ProjectBinding:
        handle = await self._service.get_active_git_handle(org_id, project.id)
        if handle is None:
            raise PatchValidationError("project has no active coding-storage handle")
        target = await self._resolve_target(org_id, project)
        return ProjectBinding(
            project_handle=handle,
            scope_id=derive_agent_scope(org_id, self._scope_agent(agent_id)),
            coding_run_id=run_id,
            target=target,
        )

    async def _resolve_target(self, org_id: str, project: Project) -> WritebackTarget | None:
        """Resolve the exact, re-verified GitHub writeback target, or ``None`` (non-GitHub project).

        Fails closed on any binding inconsistency: an unregistered repo, a repo bound to a
        different project or org, a missing/inactive installation, or an installation belonging to
        another org. The clone URL is re-pinned through the public service accessor, never trusted
        as stored.
        """
        if project.github_repository_id is None:
            return None
        repo = await self._service.store.get_repository_by_repo_id(
            org_id, project.github_repository_id
        )
        if repo is None:
            raise PatchValidationError("project's GitHub repository is not registered for this org")
        if repo.org_id != org_id or repo.project_id != project.id:
            raise PatchValidationError("GitHub repository is not bound to this project")
        installation = await self._service.store.get_installation(org_id, repo.installation_id)
        if installation is None or installation.org_id != org_id:
            raise PatchValidationError("GitHub installation is not registered for this org")
        if not installation.is_active:
            raise PatchValidationError("GitHub installation is not active")
        return self._writeback_target(repo)

    def _writeback_target(self, repo: GitHubRepository) -> WritebackTarget:
        return WritebackTarget(
            full_name=repo.full_name,
            installation_id=repo.installation_id,
            clone_url=self._service.safe_clone_url(repo.clone_url, repo.full_name),
            default_branch=repo.default_branch,
        )

    # --- agent principal / scope label ----------------------------------------------
    @staticmethod
    def _auth_agent(agent_id: str | None) -> str | None:
        """The authorization principal: the reserved patch label means *actor acting directly*.

        Both a raw ``None`` (request never named an Agent) and the canonicalized
        :data:`DEFAULT_PATCH_AGENT_ID` (the label the proposal persists) resolve to a direct actor
        authorization, so re-authorization at generation/writeback time never spuriously requires an
        Agent named ``patch``. A real Agent id authorizes through its grants unchanged.
        """
        if agent_id is None or agent_id == DEFAULT_PATCH_AGENT_ID:
            return None
        return agent_id

    @staticmethod
    def _scope_agent(agent_id: str | None) -> str:
        """The effective Agent label used to derive the canonical per-Agent scope."""
        return agent_id or DEFAULT_PATCH_AGENT_ID


__all__ = ["ProjectServicePatchAuthorizer"]
