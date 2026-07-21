"""Managed-project service: authorized CRUD, GitHub import/sync, worktrees, runs (M3.7, WS-P).

The single place that composes the :class:`~keel_core.projects.store.ProjectStore`, the
fine-grained identity :class:`~keel_core.identity.authz.AuthorizationService` (principal /
resource / capability), input validation, per-org quotas, the coding-storage integration
(:class:`~keel_core.projects.storage.ProjectStorage`), the GitHub App integration, and audit.

Authorization tiers over a project (via the acting user's org capabilities):

* ``read``   — list / get / status / list runs / list grants
* ``use``    — materialize/reclaim a worktree, associate a run
* ``write``  — create / import / update / request sync
* ``manage`` — archive / delete / purge / grant / installations

An Agent driven by an actor never exceeds the intersection of the actor's org capabilities and
the Agent's explicit ``resource_grants`` on the project (reusing the generic grant model, not a
bespoke authority table).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from keel_core.errors import PermissionDenied
from keel_core.identity.authz import AuthorizationService
from keel_core.identity.models import (
    AgentAccessPrincipalType,
    Capability,
    Membership,
    ResourceGrant,
)
from keel_core.identity.store import IdentityStore
from keel_core.projects.audit import (
    LoggingProjectAuditSink,
    ProjectAuditAction,
    ProjectAuditEvent,
    ProjectAuditSink,
)
from keel_core.projects.github.auth import InstallationTokenService
from keel_core.projects.github.client import GitHubClient
from keel_core.projects.github.urls import normalize_clone_url
from keel_core.projects.github.webhooks import WebhookEvent
from keel_core.projects.models import (
    PROJECT_RESOURCE_TYPE,
    GitHubInstallation,
    GitHubRepository,
    InstallationStatus,
    Project,
    ProjectNotFoundError,
    ProjectQuotaExceededError,
    ProjectSource,
    ProjectValidationError,
    ProjectVisibility,
    ProjectWorktree,
    RepoSyncEntry,
    StorageBackend,
    SyncKind,
    SyncStatus,
    WebhookStatus,
    validate_branch,
    validate_display_name,
    validate_project_slug,
)
from keel_core.projects.storage import ProjectStorage, worktree_storage_id
from keel_core.projects.store import ProjectStore


@dataclass(frozen=True)
class GitHubIntegration:
    """Bundles the GitHub App JIT-token service + read client + host allowlist."""

    tokens: InstallationTokenService
    client: GitHubClient
    allowed_hosts: frozenset[str]

    async def resolve_repository(self, installation_id: int, full_name: str) -> dict[str, object]:
        """Mint a JIT installation token and read repository metadata (control-plane only)."""
        token = await self.tokens.get_token(installation_id)
        return await self.client.get_repository(token=token.token, full_name=full_name)

    async def resolve_pull_request(
        self, installation_id: int, full_name: str, number: int
    ) -> dict[str, object]:
        """Mint a JIT installation token and read pull-request metadata (control-plane only).

        The token exists only for this control-plane call and is never handed to the review
        service, the worktree, or the model.
        """
        token = await self.tokens.get_token(installation_id)
        return await self.client.get_pull_request(
            token=token.token, full_name=full_name, number=number
        )

    def safe_clone_url(self, clone_url: str, full_name: str) -> str:
        """Normalize + pin a clone URL to the expected repo on an allow-listed host."""
        return normalize_clone_url(
            clone_url, allowed_hosts=self.allowed_hosts, repo_full_name=full_name
        )


@dataclass(frozen=True)
class WebhookOutcome:
    """The result of processing one webhook delivery."""

    status: WebhookStatus
    org_id: str | None = None
    project_ids: tuple[str, ...] = ()
    reason: str = ""


class ProjectService:
    """Authorized managed-project operations over a :class:`ProjectStore`."""

    def __init__(
        self,
        store: ProjectStore,
        identity: IdentityStore,
        *,
        authz: AuthorizationService | None = None,
        storage: ProjectStorage | None = None,
        github: GitHubIntegration | None = None,
        audit: ProjectAuditSink | None = None,
        enqueue_sync: Callable[[str, str, str | None], Awaitable[None]] | None = None,
    ) -> None:
        self._store = store
        self._identity = identity
        self._authz = authz or AuthorizationService()
        self._storage = storage
        self._github = github
        self._audit = audit or LoggingProjectAuditSink()
        self._enqueue_sync = enqueue_sync

    @property
    def store(self) -> ProjectStore:
        return self._store

    # --- authorization helpers -------------------------------------------------------
    async def _require_membership(self, org_id: str, actor_user_id: str) -> Membership:
        membership = await self._identity.get_membership(org_id, actor_user_id)
        if membership is None or not membership.is_active:
            raise PermissionDenied("no active membership in this organization")
        return membership

    def _require_capability(self, membership: Membership, capability: Capability) -> None:
        if capability not in self._authz.org_capabilities(membership):
            raise PermissionDenied(f"this operation requires the '{capability.value}' capability")

    async def _authorize_resource(
        self,
        org_id: str,
        actor_user_id: str,
        project: Project,
        capability: Capability,
        *,
        agent_id: str | None,
    ) -> None:
        """Authorize an actor (optionally acting through an Agent) on a project resource."""
        membership = await self._require_membership(org_id, actor_user_id)
        if agent_id is None:
            self._require_capability(membership, capability)
            return
        agent = await self._identity.get_agent(org_id, agent_id)
        if agent is None:
            raise ProjectNotFoundError("agent not found")
        grants = await self._identity.list_grants(org_id, agent_id=agent_id)
        access_edges = await self._identity.list_agent_access(
            org_id, principal_type=AgentAccessPrincipalType.user, principal_id=actor_user_id
        )
        decision = self._authz.can_agent_access_resource(
            actor_user_id,
            membership,
            agent,
            grants,
            PROJECT_RESOURCE_TYPE,
            project.id,
            capability,
            access_edges,
        )
        if not decision:
            raise PermissionDenied(decision.reason)

    async def _load_project(self, org_id: str, project_id: str) -> Project:
        project = await self._store.get_project(org_id, project_id)
        if project is None:
            raise ProjectNotFoundError("project not found")
        return project

    def _audit_event(
        self,
        action: ProjectAuditAction,
        actor_user_id: str | None,
        org_id: str | None,
        target_id: str | None,
        details: dict[str, str] | None = None,
    ) -> None:
        self._audit.record(
            ProjectAuditEvent(action, actor_user_id, org_id, target_id, details or {})
        )

    # --- project CRUD ----------------------------------------------------------------
    async def create_project(
        self,
        org_id: str,
        actor_user_id: str,
        *,
        slug: str,
        display_name: str,
        visibility: ProjectVisibility = ProjectVisibility.private,
        default_branch: str = "main",
    ) -> Project:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.write)
        clean_slug = validate_project_slug(slug)
        clean_name = validate_display_name(display_name)
        clean_branch = validate_branch(default_branch)
        await self._enforce_project_quota(org_id)
        project = await self._store.create_project(
            org_id=org_id,
            slug=clean_slug,
            display_name=clean_name,
            source=ProjectSource.blank,
            visibility=visibility,
            default_branch=clean_branch,
            active_git_handle=None,
            storage_backend=StorageBackend.local,
        )
        handle = project.id
        if self._storage is not None:
            self._storage.create_blank(handle, default_branch=clean_branch)
        updated = await self._store.set_active_git_handle(
            org_id, project.id, handle=handle, backend=StorageBackend.local
        )
        result = updated or project
        self._audit_event(
            ProjectAuditAction.project_created,
            actor_user_id,
            org_id,
            project.id,
            {"slug": clean_slug, "source": ProjectSource.blank.value},
        )
        return result

    async def _enforce_project_quota(self, org_id: str) -> None:
        quota = await self._store.get_quota(org_id)
        if await self._store.count_active_projects(org_id) >= quota.max_projects:
            raise ProjectQuotaExceededError("organization project quota exhausted")

    async def get_project(self, org_id: str, actor_user_id: str, project_id: str) -> Project:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.read)
        return await self._load_project(org_id, project_id)

    async def list_projects(
        self, org_id: str, actor_user_id: str, *, include_inactive: bool = False
    ) -> list[Project]:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.read)
        return await self._store.list_projects(org_id, include_inactive=include_inactive)

    async def update_project(
        self,
        org_id: str,
        actor_user_id: str,
        project_id: str,
        *,
        expected_version: int,
        display_name: str | None = None,
        default_branch: str | None = None,
        visibility: ProjectVisibility | None = None,
    ) -> Project:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.write)
        await self._load_project(org_id, project_id)
        clean_name = validate_display_name(display_name) if display_name is not None else None
        clean_branch = validate_branch(default_branch) if default_branch is not None else None
        updated = await self._store.update_project(
            org_id,
            project_id,
            expected_version=expected_version,
            display_name=clean_name,
            default_branch=clean_branch,
            visibility=visibility,
        )
        if updated is None:
            raise ProjectNotFoundError("project not found")
        return updated

    async def archive_project(
        self, org_id: str, actor_user_id: str, project_id: str, *, expected_version: int
    ) -> Project:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.manage)
        await self._load_project(org_id, project_id)
        archived = await self._store.archive_project(
            org_id, project_id, expected_version=expected_version
        )
        if archived is None:
            raise ProjectNotFoundError("project not found")
        self._audit_event(ProjectAuditAction.project_archived, actor_user_id, org_id, project_id)
        return archived

    async def delete_project(
        self, org_id: str, actor_user_id: str, project_id: str, *, expected_version: int
    ) -> Project:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.manage)
        await self._load_project(org_id, project_id)
        deleted = await self._store.soft_delete_project(
            org_id, project_id, expected_version=expected_version
        )
        if deleted is None:
            raise ProjectNotFoundError("project not found")
        self._audit_event(ProjectAuditAction.project_deleted, actor_user_id, org_id, project_id)
        return deleted

    async def purge_project(
        self, org_id: str, project_id: str, *, actor_user_id: str | None = None
    ) -> bool:
        """Hard-purge a project + its worktrees (used by manage delete and lifecycle erasure).

        When ``actor_user_id`` is supplied the caller must hold ``manage``; when ``None`` the
        call is a privileged system/lifecycle purge (no actor authorization).
        """
        if actor_user_id is not None:
            membership = await self._require_membership(org_id, actor_user_id)
            self._require_capability(membership, Capability.manage)
        project = await self._store.get_project(org_id, project_id)
        for worktree in await self._store.list_active_worktrees(org_id, project_id=project_id):
            await self._reclaim_worktree_storage(project, worktree)
            await self._store.reclaim_worktree(org_id, worktree.id)
        purged = await self._store.purge_project(org_id, project_id)
        if purged:
            self._audit_event(ProjectAuditAction.project_purged, actor_user_id, org_id, project_id)
        return purged

    # --- GitHub import / sync --------------------------------------------------------
    async def import_github_project(
        self,
        org_id: str,
        actor_user_id: str,
        *,
        slug: str,
        display_name: str,
        installation_id: int,
        repo_full_name: str,
    ) -> Project:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.write)
        if self._github is None:
            raise ProjectValidationError("GitHub integration is not configured")
        clean_slug = validate_project_slug(slug)
        clean_name = validate_display_name(display_name)
        installation = await self._store.get_installation(org_id, installation_id)
        if installation is None or not installation.is_active:
            raise ProjectNotFoundError("active GitHub installation not found for this org")
        await self._enforce_project_quota(org_id)

        metadata = await self._github.resolve_repository(installation_id, repo_full_name)
        repo_id = metadata.get("id")
        clone_url = metadata.get("clone_url")
        default_branch = metadata.get("default_branch", "main")
        is_private = bool(metadata.get("private", True))
        if not isinstance(repo_id, int) or not isinstance(clone_url, str):
            raise ProjectValidationError("GitHub returned an unexpected repository payload")
        safe_url = self._github.safe_clone_url(clone_url, repo_full_name)
        clean_branch = validate_branch(str(default_branch))

        repository = await self._store.upsert_repository(
            org_id=org_id,
            installation_id=installation_id,
            repo_id=repo_id,
            full_name=repo_full_name,
            default_branch=clean_branch,
            is_private=is_private,
            clone_url=safe_url,
        )
        project = await self._store.create_project(
            org_id=org_id,
            slug=clean_slug,
            display_name=clean_name,
            source=ProjectSource.github,
            visibility=ProjectVisibility.private,
            default_branch=clean_branch,
            active_git_handle=None,
            storage_backend=StorageBackend.local,
            github_repository_id=repo_id,
        )
        entry, _ = await self._store.append_sync(
            org_id=org_id,
            project_id=project.id,
            kind=SyncKind.import_,
            status=SyncStatus.pending,
            git_ref=clean_branch,
        )
        try:
            head: str | None = None
            if self._storage is not None:
                head = self._storage.import_remote(
                    project.id, safe_url, default_branch=clean_branch
                )
            await self._store.set_active_git_handle(
                org_id, project.id, handle=project.id, backend=StorageBackend.local
            )
            await self._store.link_repository_project(org_id, repository.id, project.id)
            await self._store.update_sync_status(
                org_id, entry.id, status=SyncStatus.succeeded, after_sha=head
            )
        except Exception as exc:
            # Import must be atomic: a fetch failure leaves no active project behind.
            await self._store.update_sync_status(
                org_id, entry.id, status=SyncStatus.failed, detail={"error": type(exc).__name__}
            )
            await self._store.purge_project(org_id, project.id)
            raise
        self._audit_event(
            ProjectAuditAction.project_imported,
            actor_user_id,
            org_id,
            project.id,
            {"slug": clean_slug, "repo_id": str(repo_id)},
        )
        refreshed = await self._store.get_project(org_id, project.id)
        return refreshed or project

    async def request_sync(self, org_id: str, actor_user_id: str, project_id: str) -> RepoSyncEntry:
        """Authorize + enqueue a durable fetch of a GitHub-sourced project."""
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.write)
        project = await self._load_project(org_id, project_id)
        if project.source is not ProjectSource.github:
            raise ProjectValidationError("only GitHub-sourced projects can be synced")
        entry, _ = await self._store.append_sync(
            org_id=org_id,
            project_id=project_id,
            kind=SyncKind.fetch,
            status=SyncStatus.pending,
        )
        if self._enqueue_sync is not None:
            await self._enqueue_sync(org_id, project_id, None)
        return entry

    async def sync_project(
        self, org_id: str, project_id: str, *, delivery_id: str | None = None
    ) -> RepoSyncEntry | None:
        """Durably fetch a GitHub-sourced project (called by the sync job / reconciliation).

        Restart-safe + idempotent: a webhook delivery drives at most one ledger row per project
        (``delivery_id`` unique), so a replayed or concurrent sync is a no-op.
        """
        project = await self._store.get_project(org_id, project_id)
        if project is None or project.source is not ProjectSource.github:
            return None
        if project.github_repository_id is None:
            return None
        repository = await self._store.get_repository_by_repo_id(
            org_id, project.github_repository_id
        )
        if repository is None:
            return None
        entry, created = await self._store.append_sync(
            org_id=org_id,
            project_id=project_id,
            kind=SyncKind.webhook_push if delivery_id else SyncKind.fetch,
            status=SyncStatus.pending,
            git_ref=repository.default_branch,
            delivery_id=delivery_id,
        )
        if not created and entry.status is not SyncStatus.pending:
            return entry
        try:
            head: str | None = None
            if self._storage is not None and self._github is not None:
                safe_url = self._github.safe_clone_url(repository.clone_url, repository.full_name)
                head = self._storage.fetch_remote(project.id, safe_url)
            updated = await self._store.update_sync_status(
                org_id, entry.id, status=SyncStatus.succeeded, after_sha=head
            )
            await self._store.upsert_sync_state(
                org_id=org_id,
                repository_id=repository.id,
                last_delivery_id=delivery_id,
                last_synced_sha=head,
                last_synced_ref=repository.default_branch,
            )
        except Exception as exc:
            updated = await self._store.update_sync_status(
                org_id, entry.id, status=SyncStatus.failed, detail={"error": type(exc).__name__}
            )
            raise
        self._audit_event(
            ProjectAuditAction.project_synced,
            None,
            org_id,
            project_id,
            {"delivery": delivery_id or "manual"},
        )
        return updated

    async def list_sync_entries(
        self, org_id: str, actor_user_id: str, project_id: str, *, limit: int = 50
    ) -> list[RepoSyncEntry]:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.read)
        await self._load_project(org_id, project_id)
        return await self._store.list_sync_entries(org_id, project_id, limit=limit)

    # --- worktrees -------------------------------------------------------------------
    async def materialize_worktree(
        self,
        org_id: str,
        actor_user_id: str,
        project_id: str,
        run_id: str,
        *,
        git_ref: str = "HEAD",
        agent_id: str | None = None,
    ) -> ProjectWorktree:
        project = await self._load_project(org_id, project_id)
        await self._authorize_resource(
            org_id, actor_user_id, project, Capability.use, agent_id=agent_id
        )
        existing = await self._store.get_worktree(org_id, project_id, run_id)
        if existing is not None and existing.is_active:
            return existing
        await self._enforce_worktree_quota(org_id)
        ref = git_ref if git_ref == "HEAD" else validate_branch(git_ref)
        coding_run = worktree_storage_id(run_id)
        handle = project.active_git_handle or project.id
        path = ""
        commit: str | None = None
        if self._storage is not None:
            materialized = self._storage.materialize_worktree(handle, coding_run, ref=ref)
            path = materialized.path
            commit = materialized.commit
        worktree = await self._store.create_worktree(
            org_id=org_id,
            project_id=project_id,
            run_id=run_id,
            coding_run_id=coding_run,
            git_ref=ref,
            commit_sha=commit,
            handle_path=path,
        )
        self._audit_event(
            ProjectAuditAction.worktree_materialized,
            actor_user_id,
            org_id,
            worktree.id,
            {"run_id": run_id, "project_id": project_id},
        )
        return worktree

    async def _enforce_worktree_quota(self, org_id: str) -> None:
        quota = await self._store.get_quota(org_id)
        if await self._store.count_active_worktrees(org_id) >= quota.max_active_worktrees:
            raise ProjectQuotaExceededError("organization worktree quota exhausted")

    async def _reclaim_worktree_storage(
        self, project: Project | None, worktree: ProjectWorktree
    ) -> None:
        if self._storage is None:
            return
        handle = (project.active_git_handle if project else None) or worktree.project_id
        try:
            self._storage.remove_worktree(handle, worktree.coding_run_id)
        except Exception:  # noqa: BLE001 - storage cleanup is best-effort
            pass

    async def reclaim_worktree(
        self,
        org_id: str,
        actor_user_id: str,
        project_id: str,
        run_id: str,
        *,
        agent_id: str | None = None,
    ) -> ProjectWorktree:
        project = await self._load_project(org_id, project_id)
        await self._authorize_resource(
            org_id, actor_user_id, project, Capability.use, agent_id=agent_id
        )
        worktree = await self._store.get_worktree(org_id, project_id, run_id)
        if worktree is None:
            raise ProjectNotFoundError("worktree not found")
        await self._reclaim_worktree_storage(project, worktree)
        reclaimed = await self._store.reclaim_worktree(org_id, worktree.id)
        if reclaimed is None:
            raise ProjectNotFoundError("worktree not found")
        self._audit_event(
            ProjectAuditAction.worktree_reclaimed,
            actor_user_id,
            org_id,
            worktree.id,
            {"run_id": run_id},
        )
        return reclaimed

    async def list_worktrees(
        self, org_id: str, actor_user_id: str, project_id: str
    ) -> list[ProjectWorktree]:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.read)
        await self._load_project(org_id, project_id)
        return await self._store.list_active_worktrees(org_id, project_id=project_id)

    # --- run associations ------------------------------------------------------------
    async def authorize_project(
        self,
        org_id: str,
        actor_user_id: str,
        project_id: str,
        *,
        capability: Capability,
        agent_id: str | None = None,
    ) -> Project:
        """Load a project and authorize an explicit ``capability`` on it (fail closed).

        The single public seam a downstream coordinator (review, patch) authorizes against so it
        never reaches into private membership/grant helpers. A project that does not exist *in this
        org* raises :class:`ProjectNotFoundError` (never leaking a foreign project's existence); a
        project the actor/Agent cannot access at ``capability`` raises :class:`PermissionDenied`.
        Re-invocable at worker claim time so access revoked between request and execution fails
        closed.
        """
        project = await self._load_project(org_id, project_id)
        await self._authorize_resource(
            org_id, actor_user_id, project, capability, agent_id=agent_id
        )
        return project

    async def authorize_review(
        self,
        org_id: str,
        actor_user_id: str,
        project_id: str,
        *,
        capability: Capability = Capability.use,
        agent_id: str | None = None,
    ) -> Project:
        """Authorize a read-only review of a project (read+run == the ``use`` capability).

        Returns the project so a caller (the review coordinator) can resolve its authoritative
        coding-storage handle. Re-invoked at worker claim time so access revoked between request
        and execution fails closed — an actor/Agent that lost ``use`` cannot have a review run.
        """
        return await self.authorize_project(
            org_id, actor_user_id, project_id, capability=capability, agent_id=agent_id
        )

    def safe_clone_url(self, clone_url: str, full_name: str) -> str:
        """Re-validate + pin a *stored* clone URL to the expected repo on an allow-listed host.

        The narrow public accessor over :meth:`GitHubIntegration.safe_clone_url` so a downstream
        coordinator never has to reach into the private ``_github`` integration and never trusts a
        persisted URL as-is: it is normalized + pinned again at authorization time. Fails closed
        when no GitHub integration is configured.
        """
        if self._github is None:
            raise ProjectValidationError("GitHub integration is not configured")
        return self._github.safe_clone_url(clone_url, full_name)

    async def get_project_repository(self, org_id: str, project_id: str) -> GitHubRepository | None:
        """The GitHub repository bound to a project (its installation/full_name), or ``None``.

        Used by the read-only review PR resolver to verify the project↔repo binding and mint a
        JIT token for the correct installation before resolving a PR's exact SHAs.
        """
        for repo in await self._store.list_repositories(org_id):
            if repo.project_id == project_id:
                return repo
        return None

    async def get_active_git_handle(self, org_id: str, project_id: str) -> str | None:
        """The project's authoritative coding-storage handle (``active_git_handle`` or its id).

        Used by the review PR ref materializer to fetch a PR's exact commits into the *same*
        authoritative repository the worktree is later materialized from. Returns ``None`` when
        the project does not exist.
        """
        project = await self._store.get_project(org_id, project_id)
        if project is None:
            return None
        return project.active_git_handle or project.id

    async def associate_run(
        self,
        org_id: str,
        actor_user_id: str,
        project_id: str,
        run_id: str,
        *,
        agent_id: str | None = None,
    ) -> tuple[str, bool]:
        project = await self._load_project(org_id, project_id)
        await self._authorize_resource(
            org_id, actor_user_id, project, Capability.use, agent_id=agent_id
        )
        association, created = await self._store.associate_run(
            org_id=org_id, project_id=project_id, run_id=run_id
        )
        if created:
            self._audit_event(
                ProjectAuditAction.run_associated,
                actor_user_id,
                org_id,
                project_id,
                {"run_id": run_id},
            )
        return association.run_id, created

    async def list_project_runs(
        self, org_id: str, actor_user_id: str, project_id: str
    ) -> list[str]:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.read)
        await self._load_project(org_id, project_id)
        return await self._store.list_project_runs(org_id, project_id)

    # --- project Agent grants (reuse generic resource_grants) ------------------------
    async def grant_project(
        self,
        org_id: str,
        actor_user_id: str,
        *,
        project_id: str,
        agent_id: str,
        capability: Capability,
    ) -> ResourceGrant:
        actor = await self._require_membership(org_id, actor_user_id)
        project = await self._load_project(org_id, project_id)
        agent = await self._identity.get_agent(org_id, agent_id)
        if agent is None:
            raise ProjectNotFoundError("agent not found")
        decision = self._authz.can_grant_resource(actor, agent, capability)
        if not decision:
            raise PermissionDenied(decision.reason)
        grant = await self._identity.create_grant(
            org_id=org_id,
            agent_id=agent_id,
            resource_type=PROJECT_RESOURCE_TYPE,
            resource_id=project.id,
            capability=capability,
            grantor_user_id=actor_user_id,
        )
        self._audit_event(
            ProjectAuditAction.project_grant_created,
            actor_user_id,
            org_id,
            grant.id,
            {"agent_id": agent_id, "project_id": project_id, "capability": capability.value},
        )
        return grant

    async def revoke_project_grant(
        self, org_id: str, actor_user_id: str, grant_id: str
    ) -> ResourceGrant:
        actor = await self._require_membership(org_id, actor_user_id)
        grant = await self._identity.get_grant(org_id, grant_id)
        if grant is None or grant.resource_type != PROJECT_RESOURCE_TYPE:
            raise ProjectNotFoundError("project grant not found")
        agent = await self._identity.get_agent(org_id, grant.agent_id)
        if agent is None:
            raise ProjectNotFoundError("project grant not found")
        decision = self._authz.can_grant_resource(actor, agent, grant.capability)
        if not decision:
            raise PermissionDenied(decision.reason)
        revoked = await self._identity.revoke_grant(org_id, grant_id, actor_user_id=actor_user_id)
        if revoked is None:
            raise ProjectNotFoundError("project grant not found")
        self._audit_event(ProjectAuditAction.project_grant_revoked, actor_user_id, org_id, grant_id)
        return revoked

    async def list_project_grants(
        self, org_id: str, actor_user_id: str, project_id: str
    ) -> list[ResourceGrant]:
        actor = await self._require_membership(org_id, actor_user_id)
        if not self._authz.is_org_admin(actor):
            raise PermissionDenied("listing project grants requires org admin/owner")
        await self._load_project(org_id, project_id)
        grants = await self._identity.list_grants(org_id)
        return [
            grant
            for grant in grants
            if grant.resource_type == PROJECT_RESOURCE_TYPE and grant.resource_id == project_id
        ]

    # --- GitHub installations --------------------------------------------------------
    async def link_installation(
        self,
        org_id: str,
        actor_user_id: str,
        *,
        installation_id: int,
        app_id: int,
        account_login: str,
        account_type: str = "Organization",
    ) -> GitHubInstallation:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.manage)
        installation = await self._store.upsert_installation(
            org_id=org_id,
            installation_id=installation_id,
            app_id=app_id,
            account_login=account_login,
            account_type=account_type,
        )
        self._audit_event(
            ProjectAuditAction.installation_linked,
            actor_user_id,
            org_id,
            installation.id,
            {"installation_id": str(installation_id)},
        )
        return installation

    async def list_installations(self, org_id: str, actor_user_id: str) -> list[GitHubInstallation]:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.read)
        return await self._store.list_installations(org_id)

    async def list_repositories(
        self, org_id: str, actor_user_id: str, *, installation_id: int | None = None
    ) -> list[GitHubRepository]:
        membership = await self._require_membership(org_id, actor_user_id)
        self._require_capability(membership, Capability.read)
        return await self._store.list_repositories(org_id, installation_id=installation_id)

    # --- webhook processing ----------------------------------------------------------
    async def process_webhook(self, event: WebhookEvent) -> WebhookOutcome:
        """Process an authenticated, allowlisted webhook (installation-bound to an org).

        Cross-installation defense: the delivery's installation id is bound to exactly one org;
        any repository referenced by the payload is resolved WITHIN that org only, so a payload
        naming a repo id owned by a different installation/org is ignored (not processed).
        """
        if event.installation_id is None:
            if event.event == "ping":
                return WebhookOutcome(status=WebhookStatus.processed, reason="ping")
            return WebhookOutcome(status=WebhookStatus.skipped, reason="no installation binding")
        binding = await self._store.get_installation_binding(event.installation_id)
        if binding is None:
            return WebhookOutcome(status=WebhookStatus.skipped, reason="unknown installation")
        org_id = binding.org_id

        if event.event == "installation":
            await self._handle_installation_action(binding, event)
            return WebhookOutcome(
                status=WebhookStatus.processed, org_id=org_id, reason="installation"
            )

        if binding.status is not InstallationStatus.active:
            return WebhookOutcome(
                status=WebhookStatus.skipped, org_id=org_id, reason="installation not active"
            )

        if event.event in ("push", "repository"):
            project_ids = await self._handle_push(org_id, event)
            self._audit_event(
                ProjectAuditAction.webhook_processed,
                None,
                org_id,
                None,
                {"event": event.event, "delivery": event.delivery_id},
            )
            return WebhookOutcome(
                status=WebhookStatus.processed,
                org_id=org_id,
                project_ids=tuple(project_ids),
            )
        # Allowlisted but not acted on in this phase (e.g. pull_request metadata).
        return WebhookOutcome(status=WebhookStatus.skipped, org_id=org_id, reason=event.event)

    async def _handle_installation_action(
        self, binding: GitHubInstallation, event: WebhookEvent
    ) -> None:
        action = event.action
        if action in ("suspend",):
            await self._store.set_installation_status(
                binding.installation_id, InstallationStatus.suspended
            )
        elif action in ("deleted",):
            await self._store.set_installation_status(
                binding.installation_id, InstallationStatus.deleted
            )
        elif action in ("unsuspend", "new_permissions_accepted", "created"):
            await self._store.set_installation_status(
                binding.installation_id, InstallationStatus.active
            )

    async def _handle_push(self, org_id: str, event: WebhookEvent) -> list[str]:
        project_ids: list[str] = []
        for repo_id in event.repository_ids:
            repository = await self._store.get_repository_by_repo_id(org_id, repo_id)
            if repository is None or repository.project_id is None:
                continue  # cross-installation / unlinked repo id — ignore
            project = await self._store.get_project(org_id, repository.project_id)
            if project is None:
                continue
            entry, created = await self._store.append_sync(
                org_id=org_id,
                project_id=project.id,
                kind=SyncKind.webhook_push,
                status=SyncStatus.pending,
                git_ref=event.ref,
                before_sha=event.before,
                after_sha=event.after,
                delivery_id=event.delivery_id,
            )
            project_ids.append(project.id)
            if created and self._enqueue_sync is not None:
                await self._enqueue_sync(org_id, project.id, event.delivery_id)
        return project_ids


__all__ = ["GitHubIntegration", "ProjectService", "WebhookOutcome"]
