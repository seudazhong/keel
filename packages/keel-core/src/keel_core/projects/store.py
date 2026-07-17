"""Durable + in-memory managed-project repositories (M3.7, WS-P).

One :class:`ProjectStore` protocol over projects, run-scoped worktrees, run associations, the
repo sync ledger, per-org quotas, and the GitHub binding tables, with:

* :class:`InMemoryProjectStore` — a faithful, dependency-free implementation for unit tests
  and the ``lite`` profile (enforces the same uniqueness / optimistic-concurrency / cross-org
  rules the schema does), and
* :class:`PostgresProjectStore` — the durable implementation. Tenant-owned reads/writes set
  the ``app.org_id`` GUC so Postgres RLS is engaged as defense-in-depth (ADR-0009 /
  DESIGN-REVIEW G16). The global installation / webhook-delivery tables are not org-scoped.

Repositories are intentionally thin: validation, authorization, quota enforcement, and audit
live in :mod:`keel_core.projects.service`.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.projects.models import (
    GitHubInstallation,
    GitHubRepository,
    GitHubSyncState,
    InstallationStatus,
    Project,
    ProjectConflictError,
    ProjectOptimisticConcurrencyError,
    ProjectQuota,
    ProjectRun,
    ProjectSource,
    ProjectStatus,
    ProjectVisibility,
    ProjectWorktree,
    RepoSyncEntry,
    StorageBackend,
    SyncKind,
    SyncStatus,
    WebhookDelivery,
    WebhookStatus,
    WorktreeStatus,
    new_installation_id,
    new_project_id,
    new_project_run_id,
    new_repository_id,
    new_sync_entry_id,
    new_sync_state_id,
    new_worktree_id,
)

_SET_ORG = text("SELECT set_config('app.org_id', :org, true)")


def _now() -> datetime:
    return datetime.now(UTC)


@runtime_checkable
class ProjectStore(Protocol):
    """Durable seam for managed-project persistence."""

    # projects
    async def create_project(
        self,
        *,
        org_id: str,
        slug: str,
        display_name: str,
        source: ProjectSource,
        visibility: ProjectVisibility,
        default_branch: str,
        active_git_handle: str | None,
        storage_backend: StorageBackend,
        github_repository_id: int | None = None,
    ) -> Project: ...
    async def get_project(self, org_id: str, project_id: str) -> Project | None: ...
    async def get_project_by_slug(self, org_id: str, slug: str) -> Project | None: ...
    async def list_projects(
        self, org_id: str, *, include_inactive: bool = False
    ) -> list[Project]: ...
    async def count_active_projects(self, org_id: str) -> int: ...
    async def update_project(
        self,
        org_id: str,
        project_id: str,
        *,
        expected_version: int,
        display_name: str | None = None,
        default_branch: str | None = None,
        visibility: ProjectVisibility | None = None,
    ) -> Project | None: ...
    async def set_active_git_handle(
        self,
        org_id: str,
        project_id: str,
        *,
        handle: str,
        backend: StorageBackend,
        github_repository_id: int | None = None,
    ) -> Project | None: ...
    async def archive_project(
        self, org_id: str, project_id: str, *, expected_version: int
    ) -> Project | None: ...
    async def soft_delete_project(
        self, org_id: str, project_id: str, *, expected_version: int
    ) -> Project | None: ...
    async def purge_project(self, org_id: str, project_id: str) -> bool: ...

    # worktrees
    async def create_worktree(
        self,
        *,
        org_id: str,
        project_id: str,
        run_id: str,
        coding_run_id: str,
        git_ref: str,
        commit_sha: str | None,
        handle_path: str,
    ) -> ProjectWorktree: ...
    async def get_worktree(
        self, org_id: str, project_id: str, run_id: str
    ) -> ProjectWorktree | None: ...
    async def list_active_worktrees(
        self, org_id: str, *, project_id: str | None = None
    ) -> list[ProjectWorktree]: ...
    async def count_active_worktrees(self, org_id: str) -> int: ...
    async def reclaim_worktree(self, org_id: str, worktree_id: str) -> ProjectWorktree | None: ...

    # run associations
    async def associate_run(
        self, *, org_id: str, project_id: str, run_id: str
    ) -> tuple[ProjectRun, bool]: ...
    async def get_run_association(self, org_id: str, run_id: str) -> ProjectRun | None: ...
    async def list_project_runs(self, org_id: str, project_id: str) -> list[str]: ...

    # sync ledger
    async def append_sync(
        self,
        *,
        org_id: str,
        project_id: str,
        kind: SyncKind,
        status: SyncStatus,
        git_ref: str | None = None,
        before_sha: str | None = None,
        after_sha: str | None = None,
        delivery_id: str | None = None,
        detail: dict[str, str] | None = None,
    ) -> tuple[RepoSyncEntry, bool]: ...
    async def update_sync_status(
        self,
        org_id: str,
        entry_id: str,
        *,
        status: SyncStatus,
        after_sha: str | None = None,
        detail: dict[str, str] | None = None,
    ) -> RepoSyncEntry | None: ...
    async def list_sync_entries(
        self, org_id: str, project_id: str, *, limit: int = 50
    ) -> list[RepoSyncEntry]: ...

    # quotas
    async def get_quota(self, org_id: str) -> ProjectQuota: ...
    async def set_quota(self, quota: ProjectQuota) -> ProjectQuota: ...

    # github installations (global)
    async def upsert_installation(
        self,
        *,
        org_id: str,
        installation_id: int,
        app_id: int,
        account_login: str,
        account_type: str,
    ) -> GitHubInstallation: ...
    async def get_installation(
        self, org_id: str, installation_id: int
    ) -> GitHubInstallation | None: ...
    async def get_installation_binding(self, installation_id: int) -> GitHubInstallation | None: ...
    async def set_installation_status(
        self, installation_id: int, status: InstallationStatus
    ) -> GitHubInstallation | None: ...
    async def list_installations(self, org_id: str) -> list[GitHubInstallation]: ...

    # github repositories
    async def upsert_repository(
        self,
        *,
        org_id: str,
        installation_id: int,
        repo_id: int,
        full_name: str,
        default_branch: str,
        is_private: bool,
        clone_url: str,
    ) -> GitHubRepository: ...
    async def get_repository(self, org_id: str, repository_id: str) -> GitHubRepository | None: ...
    async def get_repository_by_repo_id(
        self, org_id: str, repo_id: int
    ) -> GitHubRepository | None: ...
    async def link_repository_project(
        self, org_id: str, repository_id: str, project_id: str | None
    ) -> GitHubRepository | None: ...
    async def list_repositories(
        self, org_id: str, *, installation_id: int | None = None
    ) -> list[GitHubRepository]: ...

    # github sync state
    async def upsert_sync_state(
        self,
        *,
        org_id: str,
        repository_id: str,
        last_delivery_id: str | None,
        last_synced_sha: str | None,
        last_synced_ref: str | None,
    ) -> GitHubSyncState: ...
    async def get_sync_state(self, org_id: str, repository_id: str) -> GitHubSyncState | None: ...

    # webhook deliveries (global)
    async def record_delivery(
        self,
        *,
        delivery_id: str,
        event: str,
        installation_id: int | None,
        action: str | None,
    ) -> tuple[WebhookDelivery, bool]: ...
    async def mark_delivery(
        self, delivery_id: str, status: WebhookStatus
    ) -> WebhookDelivery | None: ...
    async def get_delivery(self, delivery_id: str) -> WebhookDelivery | None: ...


class InMemoryProjectStore:
    """Dependency-free, faithful project store for tests and the ``lite`` profile."""

    def __init__(self) -> None:
        self._projects: dict[str, Project] = {}
        self._worktrees: dict[str, ProjectWorktree] = {}
        self._runs: dict[str, ProjectRun] = {}
        self._sync: dict[str, RepoSyncEntry] = {}
        self._quotas: dict[str, ProjectQuota] = {}
        self._installations: dict[str, GitHubInstallation] = {}
        self._repositories: dict[str, GitHubRepository] = {}
        self._sync_state: dict[str, GitHubSyncState] = {}
        self._deliveries: dict[str, WebhookDelivery] = {}

    # --- projects --------------------------------------------------------------------
    async def create_project(
        self,
        *,
        org_id: str,
        slug: str,
        display_name: str,
        source: ProjectSource,
        visibility: ProjectVisibility,
        default_branch: str,
        active_git_handle: str | None,
        storage_backend: StorageBackend,
        github_repository_id: int | None = None,
    ) -> Project:
        for existing in self._projects.values():
            if (
                existing.org_id == org_id
                and existing.slug.lower() == slug.lower()
                and existing.status is ProjectStatus.active
            ):
                raise ProjectConflictError("a project with this slug already exists")
        now = _now()
        project = Project(
            id=new_project_id(),
            org_id=org_id,
            slug=slug,
            display_name=display_name,
            source=source,
            visibility=visibility,
            status=ProjectStatus.active,
            default_branch=default_branch,
            active_git_handle=active_git_handle,
            storage_backend=storage_backend,
            github_repository_id=github_repository_id,
            version=1,
            created_at=now,
            updated_at=now,
        )
        self._projects[project.id] = project
        return project

    def _live_project(self, org_id: str, project_id: str) -> Project | None:
        project = self._projects.get(project_id)
        if project is None or project.org_id != org_id:
            return None
        if project.status is ProjectStatus.deleted:
            return None
        return project

    async def get_project(self, org_id: str, project_id: str) -> Project | None:
        return self._live_project(org_id, project_id)

    async def get_project_by_slug(self, org_id: str, slug: str) -> Project | None:
        for project in self._projects.values():
            if (
                project.org_id == org_id
                and project.slug.lower() == slug.lower()
                and project.status is not ProjectStatus.deleted
            ):
                return project
        return None

    async def list_projects(self, org_id: str, *, include_inactive: bool = False) -> list[Project]:
        result = [
            p
            for p in self._projects.values()
            if p.org_id == org_id
            and p.status is not ProjectStatus.deleted
            and (include_inactive or p.status is ProjectStatus.active)
        ]
        return sorted(result, key=lambda p: p.created_at or _now())

    async def count_active_projects(self, org_id: str) -> int:
        return sum(
            1
            for p in self._projects.values()
            if p.org_id == org_id and p.status is ProjectStatus.active
        )

    def _apply(
        self, org_id: str, project_id: str, expected_version: int, **changes: Any
    ) -> Project | None:
        project = self._live_project(org_id, project_id)
        if project is None:
            return None
        if project.version != expected_version:
            raise ProjectOptimisticConcurrencyError("stale project version")
        updated = replace(project, version=project.version + 1, updated_at=_now(), **changes)
        self._projects[project_id] = updated
        return updated

    async def update_project(
        self,
        org_id: str,
        project_id: str,
        *,
        expected_version: int,
        display_name: str | None = None,
        default_branch: str | None = None,
        visibility: ProjectVisibility | None = None,
    ) -> Project | None:
        changes: dict[str, Any] = {}
        if display_name is not None:
            changes["display_name"] = display_name
        if default_branch is not None:
            changes["default_branch"] = default_branch
        if visibility is not None:
            changes["visibility"] = visibility
        return self._apply(org_id, project_id, expected_version, **changes)

    async def set_active_git_handle(
        self,
        org_id: str,
        project_id: str,
        *,
        handle: str,
        backend: StorageBackend,
        github_repository_id: int | None = None,
    ) -> Project | None:
        project = self._live_project(org_id, project_id)
        if project is None:
            return None
        changes: dict[str, Any] = {"active_git_handle": handle, "storage_backend": backend}
        if github_repository_id is not None:
            changes["github_repository_id"] = github_repository_id
        updated = replace(project, version=project.version + 1, updated_at=_now(), **changes)
        self._projects[project_id] = updated
        return updated

    async def archive_project(
        self, org_id: str, project_id: str, *, expected_version: int
    ) -> Project | None:
        return self._apply(
            org_id,
            project_id,
            expected_version,
            status=ProjectStatus.archived,
            archived_at=_now(),
        )

    async def soft_delete_project(
        self, org_id: str, project_id: str, *, expected_version: int
    ) -> Project | None:
        return self._apply(
            org_id,
            project_id,
            expected_version,
            status=ProjectStatus.deleted,
            deleted_at=_now(),
        )

    async def purge_project(self, org_id: str, project_id: str) -> bool:
        project = self._projects.get(project_id)
        if project is None or project.org_id != org_id:
            return False
        del self._projects[project_id]
        for wid in [
            w.id
            for w in self._worktrees.values()
            if w.project_id == project_id and w.org_id == org_id
        ]:
            del self._worktrees[wid]
        for rid in [
            r.id for r in self._runs.values() if r.project_id == project_id and r.org_id == org_id
        ]:
            del self._runs[rid]
        for sid in [
            s.id for s in self._sync.values() if s.project_id == project_id and s.org_id == org_id
        ]:
            del self._sync[sid]
        for repo in list(self._repositories.values()):
            if repo.org_id == org_id and repo.project_id == project_id:
                self._repositories[repo.id] = replace(repo, project_id=None)
        return True

    # --- worktrees -------------------------------------------------------------------
    async def create_worktree(
        self,
        *,
        org_id: str,
        project_id: str,
        run_id: str,
        coding_run_id: str,
        git_ref: str,
        commit_sha: str | None,
        handle_path: str,
    ) -> ProjectWorktree:
        for existing in self._worktrees.values():
            if (
                existing.org_id == org_id
                and existing.project_id == project_id
                and existing.run_id == run_id
            ):
                if existing.is_active:
                    return existing
                raise ProjectConflictError("worktree already exists for this run")
        worktree = ProjectWorktree(
            id=new_worktree_id(),
            org_id=org_id,
            project_id=project_id,
            run_id=run_id,
            coding_run_id=coding_run_id,
            git_ref=git_ref,
            commit_sha=commit_sha,
            handle_path=handle_path,
            status=WorktreeStatus.active,
            created_at=_now(),
        )
        self._worktrees[worktree.id] = worktree
        return worktree

    async def get_worktree(
        self, org_id: str, project_id: str, run_id: str
    ) -> ProjectWorktree | None:
        for w in self._worktrees.values():
            if w.org_id == org_id and w.project_id == project_id and w.run_id == run_id:
                return w
        return None

    async def list_active_worktrees(
        self, org_id: str, *, project_id: str | None = None
    ) -> list[ProjectWorktree]:
        return [
            w
            for w in self._worktrees.values()
            if w.org_id == org_id
            and w.is_active
            and (project_id is None or w.project_id == project_id)
        ]

    async def count_active_worktrees(self, org_id: str) -> int:
        return sum(1 for w in self._worktrees.values() if w.org_id == org_id and w.is_active)

    async def reclaim_worktree(self, org_id: str, worktree_id: str) -> ProjectWorktree | None:
        w = self._worktrees.get(worktree_id)
        if w is None or w.org_id != org_id:
            return None
        if not w.is_active:
            return w
        reclaimed = replace(w, status=WorktreeStatus.reclaimed, reclaimed_at=_now())
        self._worktrees[worktree_id] = reclaimed
        return reclaimed

    # --- run associations ------------------------------------------------------------
    async def associate_run(
        self, *, org_id: str, project_id: str, run_id: str
    ) -> tuple[ProjectRun, bool]:
        for existing in self._runs.values():
            if existing.org_id == org_id and existing.run_id == run_id:
                if existing.project_id != project_id:
                    raise ProjectConflictError("run is already associated with another project")
                return existing, False
        run = ProjectRun(
            id=new_project_run_id(),
            org_id=org_id,
            project_id=project_id,
            run_id=run_id,
            created_at=_now(),
        )
        self._runs[run.id] = run
        return run, True

    async def get_run_association(self, org_id: str, run_id: str) -> ProjectRun | None:
        for r in self._runs.values():
            if r.org_id == org_id and r.run_id == run_id:
                return r
        return None

    async def list_project_runs(self, org_id: str, project_id: str) -> list[str]:
        return [
            r.run_id
            for r in sorted(self._runs.values(), key=lambda r: r.created_at or _now(), reverse=True)
            if r.org_id == org_id and r.project_id == project_id
        ]

    # --- sync ledger -----------------------------------------------------------------
    async def append_sync(
        self,
        *,
        org_id: str,
        project_id: str,
        kind: SyncKind,
        status: SyncStatus,
        git_ref: str | None = None,
        before_sha: str | None = None,
        after_sha: str | None = None,
        delivery_id: str | None = None,
        detail: dict[str, str] | None = None,
    ) -> tuple[RepoSyncEntry, bool]:
        if delivery_id is not None:
            for existing in self._sync.values():
                if (
                    existing.org_id == org_id
                    and existing.project_id == project_id
                    and existing.delivery_id == delivery_id
                ):
                    return existing, False
        entry = RepoSyncEntry(
            id=new_sync_entry_id(),
            org_id=org_id,
            project_id=project_id,
            kind=kind,
            status=status,
            git_ref=git_ref,
            before_sha=before_sha,
            after_sha=after_sha,
            delivery_id=delivery_id,
            detail=dict(detail or {}),
            created_at=_now(),
            updated_at=_now(),
        )
        self._sync[entry.id] = entry
        return entry, True

    async def update_sync_status(
        self,
        org_id: str,
        entry_id: str,
        *,
        status: SyncStatus,
        after_sha: str | None = None,
        detail: dict[str, str] | None = None,
    ) -> RepoSyncEntry | None:
        entry = self._sync.get(entry_id)
        if entry is None or entry.org_id != org_id:
            return None
        updated = replace(
            entry,
            status=status,
            after_sha=after_sha if after_sha is not None else entry.after_sha,
            detail=dict(detail) if detail is not None else entry.detail,
            updated_at=_now(),
        )
        self._sync[entry_id] = updated
        return updated

    async def list_sync_entries(
        self, org_id: str, project_id: str, *, limit: int = 50
    ) -> list[RepoSyncEntry]:
        entries = [
            e for e in self._sync.values() if e.org_id == org_id and e.project_id == project_id
        ]
        entries.sort(key=lambda e: e.created_at or _now(), reverse=True)
        return entries[:limit]

    # --- quotas ----------------------------------------------------------------------
    async def get_quota(self, org_id: str) -> ProjectQuota:
        return self._quotas.get(org_id, ProjectQuota(org_id=org_id))

    async def set_quota(self, quota: ProjectQuota) -> ProjectQuota:
        stored = replace(quota, updated_at=_now())
        self._quotas[quota.org_id] = stored
        return stored

    # --- github installations (global) ----------------------------------------------
    async def upsert_installation(
        self,
        *,
        org_id: str,
        installation_id: int,
        app_id: int,
        account_login: str,
        account_type: str,
    ) -> GitHubInstallation:
        for existing in self._installations.values():
            if existing.installation_id == installation_id:
                if existing.status is not InstallationStatus.deleted and existing.org_id != org_id:
                    raise ProjectConflictError(
                        "installation is already bound to another organization"
                    )
                updated = replace(
                    existing,
                    org_id=org_id,
                    app_id=app_id,
                    account_login=account_login,
                    account_type=account_type,
                    status=InstallationStatus.active,
                    updated_at=_now(),
                    suspended_at=None,
                    deleted_at=None,
                )
                self._installations[existing.id] = updated
                return updated
        record = GitHubInstallation(
            id=new_installation_id(),
            org_id=org_id,
            installation_id=installation_id,
            app_id=app_id,
            account_login=account_login,
            account_type=account_type,
            status=InstallationStatus.active,
            created_at=_now(),
            updated_at=_now(),
        )
        self._installations[record.id] = record
        return record

    async def get_installation(
        self, org_id: str, installation_id: int
    ) -> GitHubInstallation | None:
        for i in self._installations.values():
            if i.org_id == org_id and i.installation_id == installation_id:
                return i
        return None

    async def get_installation_binding(self, installation_id: int) -> GitHubInstallation | None:
        for i in self._installations.values():
            if i.installation_id == installation_id and i.status is not InstallationStatus.deleted:
                return i
        return None

    async def set_installation_status(
        self, installation_id: int, status: InstallationStatus
    ) -> GitHubInstallation | None:
        for iid, i in self._installations.items():
            if i.installation_id == installation_id:
                updated = replace(
                    i,
                    status=status,
                    updated_at=_now(),
                    suspended_at=_now()
                    if status is InstallationStatus.suspended
                    else i.suspended_at,
                    deleted_at=_now() if status is InstallationStatus.deleted else i.deleted_at,
                )
                self._installations[iid] = updated
                return updated
        return None

    async def list_installations(self, org_id: str) -> list[GitHubInstallation]:
        return [
            i
            for i in self._installations.values()
            if i.org_id == org_id and i.status is not InstallationStatus.deleted
        ]

    # --- github repositories ---------------------------------------------------------
    async def upsert_repository(
        self,
        *,
        org_id: str,
        installation_id: int,
        repo_id: int,
        full_name: str,
        default_branch: str,
        is_private: bool,
        clone_url: str,
    ) -> GitHubRepository:
        for existing in self._repositories.values():
            if existing.org_id == org_id and existing.repo_id == repo_id:
                updated = replace(
                    existing,
                    installation_id=installation_id,
                    full_name=full_name,
                    default_branch=default_branch,
                    is_private=is_private,
                    clone_url=clone_url,
                    updated_at=_now(),
                )
                self._repositories[existing.id] = updated
                return updated
        record = GitHubRepository(
            id=new_repository_id(),
            org_id=org_id,
            installation_id=installation_id,
            repo_id=repo_id,
            full_name=full_name,
            default_branch=default_branch,
            is_private=is_private,
            clone_url=clone_url,
            created_at=_now(),
            updated_at=_now(),
        )
        self._repositories[record.id] = record
        return record

    async def get_repository(self, org_id: str, repository_id: str) -> GitHubRepository | None:
        repo = self._repositories.get(repository_id)
        if repo is None or repo.org_id != org_id:
            return None
        return repo

    async def get_repository_by_repo_id(self, org_id: str, repo_id: int) -> GitHubRepository | None:
        for r in self._repositories.values():
            if r.org_id == org_id and r.repo_id == repo_id:
                return r
        return None

    async def link_repository_project(
        self, org_id: str, repository_id: str, project_id: str | None
    ) -> GitHubRepository | None:
        repo = self._repositories.get(repository_id)
        if repo is None or repo.org_id != org_id:
            return None
        updated = replace(repo, project_id=project_id, updated_at=_now())
        self._repositories[repository_id] = updated
        return updated

    async def list_repositories(
        self, org_id: str, *, installation_id: int | None = None
    ) -> list[GitHubRepository]:
        return [
            r
            for r in self._repositories.values()
            if r.org_id == org_id
            and (installation_id is None or r.installation_id == installation_id)
        ]

    # --- github sync state -----------------------------------------------------------
    async def upsert_sync_state(
        self,
        *,
        org_id: str,
        repository_id: str,
        last_delivery_id: str | None,
        last_synced_sha: str | None,
        last_synced_ref: str | None,
    ) -> GitHubSyncState:
        for existing in self._sync_state.values():
            if existing.org_id == org_id and existing.repository_id == repository_id:
                updated = replace(
                    existing,
                    last_delivery_id=last_delivery_id,
                    last_synced_sha=last_synced_sha,
                    last_synced_ref=last_synced_ref,
                    last_synced_at=_now(),
                    updated_at=_now(),
                )
                self._sync_state[existing.id] = updated
                return updated
        record = GitHubSyncState(
            id=new_sync_state_id(),
            org_id=org_id,
            repository_id=repository_id,
            last_delivery_id=last_delivery_id,
            last_synced_sha=last_synced_sha,
            last_synced_ref=last_synced_ref,
            last_synced_at=_now(),
            updated_at=_now(),
        )
        self._sync_state[record.id] = record
        return record

    async def get_sync_state(self, org_id: str, repository_id: str) -> GitHubSyncState | None:
        for s in self._sync_state.values():
            if s.org_id == org_id and s.repository_id == repository_id:
                return s
        return None

    # --- webhook deliveries (global) -------------------------------------------------
    async def record_delivery(
        self,
        *,
        delivery_id: str,
        event: str,
        installation_id: int | None,
        action: str | None,
    ) -> tuple[WebhookDelivery, bool]:
        existing = self._deliveries.get(delivery_id)
        if existing is not None:
            return existing, False
        record = WebhookDelivery(
            delivery_id=delivery_id,
            event=event,
            installation_id=installation_id,
            action=action,
            status=WebhookStatus.received,
            received_at=_now(),
        )
        self._deliveries[delivery_id] = record
        return record, True

    async def mark_delivery(
        self, delivery_id: str, status: WebhookStatus
    ) -> WebhookDelivery | None:
        existing = self._deliveries.get(delivery_id)
        if existing is None:
            return None
        updated = replace(
            existing,
            status=status,
            processed_at=_now()
            if status in (WebhookStatus.processed, WebhookStatus.skipped, WebhookStatus.failed)
            else existing.processed_at,
        )
        self._deliveries[delivery_id] = updated
        return updated

    async def get_delivery(self, delivery_id: str) -> WebhookDelivery | None:
        return self._deliveries.get(delivery_id)


# --- Postgres row -> model converters ------------------------------------------------

_PROJECT_COLS = (
    "id, org_id, slug, display_name, source, visibility, status, default_branch, "
    "active_git_handle, storage_backend, github_repository_id, version, "
    "created_at, updated_at, archived_at, deleted_at"
)
_WORKTREE_COLS = (
    "id, org_id, project_id, run_id, coding_run_id, git_ref, commit_sha, handle_path, "
    "status, created_at, reclaimed_at"
)
_RUN_COLS = "id, org_id, project_id, run_id, created_at"
_SYNC_COLS = (
    "id, org_id, project_id, kind, status, git_ref, before_sha, after_sha, delivery_id, "
    "detail, created_at, updated_at"
)
_QUOTA_COLS = (
    "org_id, max_projects, max_active_worktrees, max_repository_bytes, created_at, updated_at"
)
_INSTALL_COLS = (
    "id, org_id, installation_id, app_id, account_login, account_type, status, "
    "created_at, updated_at, suspended_at, deleted_at"
)
_REPO_COLS = (
    "id, org_id, installation_id, repo_id, full_name, default_branch, is_private, "
    "clone_url, project_id, created_at, updated_at"
)
_SYNC_STATE_COLS = (
    "id, org_id, repository_id, last_delivery_id, last_synced_sha, last_synced_ref, "
    "last_synced_at, updated_at"
)
_DELIVERY_COLS = "delivery_id, event, installation_id, action, status, received_at, processed_at"


def _to_project(row: Any) -> Project:
    return Project(
        id=row["id"],
        org_id=row["org_id"],
        slug=row["slug"],
        display_name=row["display_name"],
        source=ProjectSource(row["source"]),
        visibility=ProjectVisibility(row["visibility"]),
        status=ProjectStatus(row["status"]),
        default_branch=row["default_branch"],
        active_git_handle=row["active_git_handle"],
        storage_backend=StorageBackend(row["storage_backend"]),
        github_repository_id=row["github_repository_id"],
        version=row["version"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        archived_at=row["archived_at"],
        deleted_at=row["deleted_at"],
    )


def _to_worktree(row: Any) -> ProjectWorktree:
    return ProjectWorktree(
        id=row["id"],
        org_id=row["org_id"],
        project_id=row["project_id"],
        run_id=row["run_id"],
        coding_run_id=row["coding_run_id"],
        git_ref=row["git_ref"],
        commit_sha=row["commit_sha"],
        handle_path=row["handle_path"],
        status=WorktreeStatus(row["status"]),
        created_at=row["created_at"],
        reclaimed_at=row["reclaimed_at"],
    )


def _to_run(row: Any) -> ProjectRun:
    return ProjectRun(
        id=row["id"],
        org_id=row["org_id"],
        project_id=row["project_id"],
        run_id=row["run_id"],
        created_at=row["created_at"],
    )


def _to_sync(row: Any) -> RepoSyncEntry:
    detail = row["detail"] or {}
    return RepoSyncEntry(
        id=row["id"],
        org_id=row["org_id"],
        project_id=row["project_id"],
        kind=SyncKind(row["kind"]),
        status=SyncStatus(row["status"]),
        git_ref=row["git_ref"],
        before_sha=row["before_sha"],
        after_sha=row["after_sha"],
        delivery_id=row["delivery_id"],
        detail={str(k): str(v) for k, v in detail.items()},
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _to_quota(row: Any) -> ProjectQuota:
    return ProjectQuota(
        org_id=row["org_id"],
        max_projects=row["max_projects"],
        max_active_worktrees=row["max_active_worktrees"],
        max_repository_bytes=row["max_repository_bytes"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _to_installation(row: Any) -> GitHubInstallation:
    return GitHubInstallation(
        id=row["id"],
        org_id=row["org_id"],
        installation_id=row["installation_id"],
        app_id=row["app_id"],
        account_login=row["account_login"],
        account_type=row["account_type"],
        status=InstallationStatus(row["status"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        suspended_at=row["suspended_at"],
        deleted_at=row["deleted_at"],
    )


def _to_repository(row: Any) -> GitHubRepository:
    return GitHubRepository(
        id=row["id"],
        org_id=row["org_id"],
        installation_id=row["installation_id"],
        repo_id=row["repo_id"],
        full_name=row["full_name"],
        default_branch=row["default_branch"],
        is_private=row["is_private"],
        clone_url=row["clone_url"],
        project_id=row["project_id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _to_sync_state(row: Any) -> GitHubSyncState:
    return GitHubSyncState(
        id=row["id"],
        org_id=row["org_id"],
        repository_id=row["repository_id"],
        last_delivery_id=row["last_delivery_id"],
        last_synced_sha=row["last_synced_sha"],
        last_synced_ref=row["last_synced_ref"],
        last_synced_at=row["last_synced_at"],
        updated_at=row["updated_at"],
    )


def _to_delivery(row: Any) -> WebhookDelivery:
    return WebhookDelivery(
        delivery_id=row["delivery_id"],
        event=row["event"],
        installation_id=row["installation_id"],
        action=row["action"],
        status=WebhookStatus(row["status"]),
        received_at=row["received_at"],
        processed_at=row["processed_at"],
    )


class PostgresProjectStore:
    """Durable project store; tenant-owned access sets ``app.org_id`` (RLS defense-in-depth)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    # --- projects --------------------------------------------------------------------
    async def create_project(
        self,
        *,
        org_id: str,
        slug: str,
        display_name: str,
        source: ProjectSource,
        visibility: ProjectVisibility,
        default_branch: str,
        active_git_handle: str | None,
        storage_backend: StorageBackend,
        github_repository_id: int | None = None,
    ) -> Project:
        project_id = new_project_id()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                row = (
                    (
                        await conn.execute(
                            text(
                                "INSERT INTO projects (id, org_id, slug, display_name, source, "
                                "visibility, default_branch, active_git_handle, storage_backend, "
                                "github_repository_id) VALUES (:id, :org, :slug, :name, :source, "
                                ":visibility, :branch, :handle, :backend, :repo) "
                                f"RETURNING {_PROJECT_COLS}"
                            ),
                            {
                                "id": project_id,
                                "org": org_id,
                                "slug": slug,
                                "name": display_name,
                                "source": source.value,
                                "visibility": visibility.value,
                                "branch": default_branch,
                                "handle": active_git_handle,
                                "backend": storage_backend.value,
                                "repo": github_repository_id,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
        except IntegrityError as exc:
            raise ProjectConflictError("a project with this slug already exists") from exc
        return _to_project(row)

    async def get_project(self, org_id: str, project_id: str) -> Project | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_PROJECT_COLS} FROM projects "
                            "WHERE id = :id AND org_id = :org AND status <> 'deleted'"
                        ),
                        {"id": project_id, "org": org_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_project(row)

    async def get_project_by_slug(self, org_id: str, slug: str) -> Project | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_PROJECT_COLS} FROM projects "
                            "WHERE org_id = :org AND lower(slug) = lower(:slug) "
                            "AND status <> 'deleted' ORDER BY created_at DESC LIMIT 1"
                        ),
                        {"org": org_id, "slug": slug},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_project(row)

    async def list_projects(self, org_id: str, *, include_inactive: bool = False) -> list[Project]:
        clause = "status = 'active'" if not include_inactive else "status <> 'deleted'"
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_PROJECT_COLS} FROM projects "
                            f"WHERE org_id = :org AND {clause} ORDER BY created_at"
                        ),
                        {"org": org_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_project(r) for r in rows]

    async def count_active_projects(self, org_id: str) -> int:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            value = (
                await conn.execute(
                    text("SELECT count(*) FROM projects WHERE org_id = :org AND status = 'active'"),
                    {"org": org_id},
                )
            ).scalar_one()
        return int(value)

    async def _update_returning(
        self, org_id: str, sql: str, params: dict[str, Any]
    ) -> Project | None:
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                row = (await conn.execute(text(sql), params)).mappings().one_or_none()
        except IntegrityError as exc:
            raise ProjectConflictError("project update violated a constraint") from exc
        return None if row is None else _to_project(row)

    async def update_project(
        self,
        org_id: str,
        project_id: str,
        *,
        expected_version: int,
        display_name: str | None = None,
        default_branch: str | None = None,
        visibility: ProjectVisibility | None = None,
    ) -> Project | None:
        updated = await self._update_returning(
            org_id,
            "UPDATE projects SET "
            "display_name = COALESCE(:name, display_name), "
            "default_branch = COALESCE(:branch, default_branch), "
            "visibility = COALESCE(:visibility, visibility), "
            "version = version + 1, updated_at = now() "
            "WHERE id = :id AND org_id = :org AND status = 'active' AND version = :ver "
            f"RETURNING {_PROJECT_COLS}",
            {
                "id": project_id,
                "org": org_id,
                "ver": expected_version,
                "name": display_name,
                "branch": default_branch,
                "visibility": visibility.value if visibility is not None else None,
            },
        )
        if updated is None:
            await self._raise_if_stale(org_id, project_id, expected_version)
        return updated

    async def _raise_if_stale(self, org_id: str, project_id: str, expected_version: int) -> None:
        project = await self.get_project(org_id, project_id)
        if project is not None and project.version != expected_version:
            raise ProjectOptimisticConcurrencyError("stale project version")

    async def set_active_git_handle(
        self,
        org_id: str,
        project_id: str,
        *,
        handle: str,
        backend: StorageBackend,
        github_repository_id: int | None = None,
    ) -> Project | None:
        return await self._update_returning(
            org_id,
            "UPDATE projects SET active_git_handle = :handle, storage_backend = :backend, "
            "github_repository_id = COALESCE(:repo, github_repository_id), "
            "version = version + 1, updated_at = now() "
            "WHERE id = :id AND org_id = :org AND status <> 'deleted' "
            f"RETURNING {_PROJECT_COLS}",
            {
                "id": project_id,
                "org": org_id,
                "handle": handle,
                "backend": backend.value,
                "repo": github_repository_id,
            },
        )

    async def archive_project(
        self, org_id: str, project_id: str, *, expected_version: int
    ) -> Project | None:
        updated = await self._update_returning(
            org_id,
            "UPDATE projects SET status = 'archived', archived_at = now(), "
            "version = version + 1, updated_at = now() "
            "WHERE id = :id AND org_id = :org AND status = 'active' AND version = :ver "
            f"RETURNING {_PROJECT_COLS}",
            {"id": project_id, "org": org_id, "ver": expected_version},
        )
        if updated is None:
            await self._raise_if_stale(org_id, project_id, expected_version)
        return updated

    async def soft_delete_project(
        self, org_id: str, project_id: str, *, expected_version: int
    ) -> Project | None:
        updated = await self._update_returning(
            org_id,
            "UPDATE projects SET status = 'deleted', deleted_at = now(), "
            "version = version + 1, updated_at = now() "
            "WHERE id = :id AND org_id = :org AND status <> 'deleted' AND version = :ver "
            f"RETURNING {_PROJECT_COLS}",
            {"id": project_id, "org": org_id, "ver": expected_version},
        )
        if updated is None:
            await self._raise_if_stale(org_id, project_id, expected_version)
        return updated

    async def purge_project(self, org_id: str, project_id: str) -> bool:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            result = await conn.execute(
                text("DELETE FROM projects WHERE id = :id AND org_id = :org"),
                {"id": project_id, "org": org_id},
            )
        return bool(result.rowcount)

    # --- worktrees -------------------------------------------------------------------
    async def create_worktree(
        self,
        *,
        org_id: str,
        project_id: str,
        run_id: str,
        coding_run_id: str,
        git_ref: str,
        commit_sha: str | None,
        handle_path: str,
    ) -> ProjectWorktree:
        existing = await self.get_worktree(org_id, project_id, run_id)
        if existing is not None:
            if existing.is_active:
                return existing
            raise ProjectConflictError("worktree already exists for this run")
        worktree_id = new_worktree_id()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                row = (
                    (
                        await conn.execute(
                            text(
                                "INSERT INTO project_worktrees (id, org_id, project_id, run_id, "
                                "coding_run_id, git_ref, commit_sha, handle_path) VALUES "
                                "(:id, :org, :pid, :rid, :crid, :ref, :sha, :path) "
                                f"RETURNING {_WORKTREE_COLS}"
                            ),
                            {
                                "id": worktree_id,
                                "org": org_id,
                                "pid": project_id,
                                "rid": run_id,
                                "crid": coding_run_id,
                                "ref": git_ref,
                                "sha": commit_sha,
                                "path": handle_path,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
        except IntegrityError as exc:
            raise ProjectConflictError("worktree already exists for this run") from exc
        return _to_worktree(row)

    async def get_worktree(
        self, org_id: str, project_id: str, run_id: str
    ) -> ProjectWorktree | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_WORKTREE_COLS} FROM project_worktrees "
                            "WHERE org_id = :org AND project_id = :pid AND run_id = :rid"
                        ),
                        {"org": org_id, "pid": project_id, "rid": run_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_worktree(row)

    async def list_active_worktrees(
        self, org_id: str, *, project_id: str | None = None
    ) -> list[ProjectWorktree]:
        clause = "AND project_id = :pid" if project_id is not None else ""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_WORKTREE_COLS} FROM project_worktrees "
                            f"WHERE org_id = :org AND status = 'active' {clause} "
                            "ORDER BY created_at"
                        ),
                        {"org": org_id, "pid": project_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_worktree(r) for r in rows]

    async def count_active_worktrees(self, org_id: str) -> int:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            value = (
                await conn.execute(
                    text(
                        "SELECT count(*) FROM project_worktrees "
                        "WHERE org_id = :org AND status = 'active'"
                    ),
                    {"org": org_id},
                )
            ).scalar_one()
        return int(value)

    async def reclaim_worktree(self, org_id: str, worktree_id: str) -> ProjectWorktree | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE project_worktrees SET status = 'reclaimed', "
                            "reclaimed_at = now() WHERE id = :id AND org_id = :org "
                            f"RETURNING {_WORKTREE_COLS}"
                        ),
                        {"id": worktree_id, "org": org_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_worktree(row)

    # --- run associations ------------------------------------------------------------
    async def associate_run(
        self, *, org_id: str, project_id: str, run_id: str
    ) -> tuple[ProjectRun, bool]:
        run_row_id = new_project_run_id()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "INSERT INTO project_runs (id, org_id, project_id, run_id) "
                            "VALUES (:id, :org, :pid, :rid) "
                            "ON CONFLICT (org_id, run_id) DO NOTHING "
                            f"RETURNING {_RUN_COLS}"
                        ),
                        {"id": run_row_id, "org": org_id, "pid": project_id, "rid": run_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is not None:
                return _to_run(row), True
            existing = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_RUN_COLS} FROM project_runs "
                            "WHERE org_id = :org AND run_id = :rid"
                        ),
                        {"org": org_id, "rid": run_id},
                    )
                )
                .mappings()
                .one()
            )
        association = _to_run(existing)
        if association.project_id != project_id:
            raise ProjectConflictError("run is already associated with another project")
        return association, False

    async def get_run_association(self, org_id: str, run_id: str) -> ProjectRun | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_RUN_COLS} FROM project_runs "
                            "WHERE org_id = :org AND run_id = :rid"
                        ),
                        {"org": org_id, "rid": run_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_run(row)

    async def list_project_runs(self, org_id: str, project_id: str) -> list[str]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT run_id FROM project_runs "
                            "WHERE org_id = :org AND project_id = :pid ORDER BY created_at DESC"
                        ),
                        {"org": org_id, "pid": project_id},
                    )
                )
                .mappings()
                .all()
            )
        return [r["run_id"] for r in rows]

    # --- sync ledger -----------------------------------------------------------------
    async def append_sync(
        self,
        *,
        org_id: str,
        project_id: str,
        kind: SyncKind,
        status: SyncStatus,
        git_ref: str | None = None,
        before_sha: str | None = None,
        after_sha: str | None = None,
        delivery_id: str | None = None,
        detail: dict[str, str] | None = None,
    ) -> tuple[RepoSyncEntry, bool]:

        entry_id = new_sync_entry_id()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            if delivery_id is not None:
                row = (
                    (
                        await conn.execute(
                            text(
                                "INSERT INTO repo_sync_ledger (id, org_id, project_id, kind, "
                                "status, git_ref, before_sha, after_sha, delivery_id, detail) "
                                "VALUES (:id, :org, :pid, :kind, :status, :ref, :before, :after, "
                                ":delivery, CAST(:detail AS jsonb)) "
                                "ON CONFLICT (org_id, project_id, delivery_id) "
                                "WHERE delivery_id IS NOT NULL DO NOTHING "
                                f"RETURNING {_SYNC_COLS}"
                            ),
                            {
                                "id": entry_id,
                                "org": org_id,
                                "pid": project_id,
                                "kind": kind.value,
                                "status": status.value,
                                "ref": git_ref,
                                "before": before_sha,
                                "after": after_sha,
                                "delivery": delivery_id,
                                "detail": json.dumps(detail or {}),
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is not None:
                    return _to_sync(row), True
                existing = (
                    (
                        await conn.execute(
                            text(
                                f"SELECT {_SYNC_COLS} FROM repo_sync_ledger "
                                "WHERE org_id = :org AND project_id = :pid AND delivery_id = :d"
                            ),
                            {"org": org_id, "pid": project_id, "d": delivery_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                return _to_sync(existing), False
            row = (
                (
                    await conn.execute(
                        text(
                            "INSERT INTO repo_sync_ledger (id, org_id, project_id, kind, status, "
                            "git_ref, before_sha, after_sha, detail) VALUES "
                            "(:id, :org, :pid, :kind, :status, :ref, :before, :after, "
                            "CAST(:detail AS jsonb)) "
                            f"RETURNING {_SYNC_COLS}"
                        ),
                        {
                            "id": entry_id,
                            "org": org_id,
                            "pid": project_id,
                            "kind": kind.value,
                            "status": status.value,
                            "ref": git_ref,
                            "before": before_sha,
                            "after": after_sha,
                            "detail": json.dumps(detail or {}),
                        },
                    )
                )
                .mappings()
                .one()
            )
        return _to_sync(row), True

    async def update_sync_status(
        self,
        org_id: str,
        entry_id: str,
        *,
        status: SyncStatus,
        after_sha: str | None = None,
        detail: dict[str, str] | None = None,
    ) -> RepoSyncEntry | None:

        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE repo_sync_ledger SET status = :status, "
                            "after_sha = COALESCE(:after, after_sha), "
                            "detail = COALESCE(CAST(:detail AS jsonb), detail), "
                            "updated_at = now() WHERE id = :id AND org_id = :org "
                            f"RETURNING {_SYNC_COLS}"
                        ),
                        {
                            "id": entry_id,
                            "org": org_id,
                            "status": status.value,
                            "after": after_sha,
                            "detail": json.dumps(detail) if detail is not None else None,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_sync(row)

    async def list_sync_entries(
        self, org_id: str, project_id: str, *, limit: int = 50
    ) -> list[RepoSyncEntry]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_SYNC_COLS} FROM repo_sync_ledger "
                            "WHERE org_id = :org AND project_id = :pid "
                            "ORDER BY created_at DESC LIMIT :limit"
                        ),
                        {"org": org_id, "pid": project_id, "limit": limit},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_sync(r) for r in rows]

    # --- quotas ----------------------------------------------------------------------
    async def get_quota(self, org_id: str) -> ProjectQuota:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(f"SELECT {_QUOTA_COLS} FROM project_quotas WHERE org_id = :org"),
                        {"org": org_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return ProjectQuota(org_id=org_id) if row is None else _to_quota(row)

    async def set_quota(self, quota: ProjectQuota) -> ProjectQuota:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": quota.org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "INSERT INTO project_quotas (org_id, max_projects, "
                            "max_active_worktrees, max_repository_bytes) VALUES "
                            "(:org, :mp, :mw, :mb) ON CONFLICT (org_id) DO UPDATE SET "
                            "max_projects = EXCLUDED.max_projects, "
                            "max_active_worktrees = EXCLUDED.max_active_worktrees, "
                            "max_repository_bytes = EXCLUDED.max_repository_bytes, "
                            f"updated_at = now() RETURNING {_QUOTA_COLS}"
                        ),
                        {
                            "org": quota.org_id,
                            "mp": quota.max_projects,
                            "mw": quota.max_active_worktrees,
                            "mb": quota.max_repository_bytes,
                        },
                    )
                )
                .mappings()
                .one()
            )
        return _to_quota(row)

    # --- github installations (global; no org GUC) -----------------------------------
    async def upsert_installation(
        self,
        *,
        org_id: str,
        installation_id: int,
        app_id: int,
        account_login: str,
        account_type: str,
    ) -> GitHubInstallation:
        record_id = new_installation_id()
        try:
            async with self._engine.begin() as conn:
                existing = (
                    (
                        await conn.execute(
                            text(
                                f"SELECT {_INSTALL_COLS} FROM github_installations "
                                "WHERE installation_id = :iid AND status <> 'deleted'"
                            ),
                            {"iid": installation_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if existing is not None and existing["org_id"] != org_id:
                    raise ProjectConflictError(
                        "installation is already bound to another organization"
                    )
                row = (
                    (
                        await conn.execute(
                            text(
                                "INSERT INTO github_installations (id, org_id, installation_id, "
                                "app_id, account_login, account_type) VALUES "
                                "(:id, :org, :iid, :app, :login, :atype) "
                                "ON CONFLICT (installation_id, org_id) DO UPDATE SET "
                                "app_id = EXCLUDED.app_id, account_login = EXCLUDED.account_login, "
                                "account_type = EXCLUDED.account_type, status = 'active', "
                                "suspended_at = NULL, deleted_at = NULL, updated_at = now() "
                                f"RETURNING {_INSTALL_COLS}"
                            ),
                            {
                                "id": record_id,
                                "org": org_id,
                                "iid": installation_id,
                                "app": app_id,
                                "login": account_login,
                                "atype": account_type,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
        except IntegrityError as exc:
            raise ProjectConflictError(
                "installation is already bound to another organization"
            ) from exc
        return _to_installation(row)

    async def get_installation(
        self, org_id: str, installation_id: int
    ) -> GitHubInstallation | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_INSTALL_COLS} FROM github_installations "
                            "WHERE org_id = :org AND installation_id = :iid "
                            "AND status <> 'deleted'"
                        ),
                        {"org": org_id, "iid": installation_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_installation(row)

    async def get_installation_binding(self, installation_id: int) -> GitHubInstallation | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_INSTALL_COLS} FROM github_installations "
                            "WHERE installation_id = :iid AND status <> 'deleted'"
                        ),
                        {"iid": installation_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_installation(row)

    async def set_installation_status(
        self, installation_id: int, status: InstallationStatus
    ) -> GitHubInstallation | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE github_installations SET status = :status, "
                            "suspended_at = CASE WHEN :status = 'suspended' THEN now() "
                            "ELSE suspended_at END, "
                            "deleted_at = CASE WHEN :status = 'deleted' THEN now() "
                            "ELSE deleted_at END, updated_at = now() "
                            "WHERE installation_id = :iid AND status <> 'deleted' "
                            f"RETURNING {_INSTALL_COLS}"
                        ),
                        {"iid": installation_id, "status": status.value},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_installation(row)

    async def list_installations(self, org_id: str) -> list[GitHubInstallation]:
        async with self._engine.begin() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_INSTALL_COLS} FROM github_installations "
                            "WHERE org_id = :org AND status <> 'deleted' ORDER BY created_at"
                        ),
                        {"org": org_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_installation(r) for r in rows]

    # --- github repositories ---------------------------------------------------------
    async def upsert_repository(
        self,
        *,
        org_id: str,
        installation_id: int,
        repo_id: int,
        full_name: str,
        default_branch: str,
        is_private: bool,
        clone_url: str,
    ) -> GitHubRepository:
        record_id = new_repository_id()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(_SET_ORG, {"org": org_id})
                row = (
                    (
                        await conn.execute(
                            text(
                                "INSERT INTO github_repositories (id, org_id, installation_id, "
                                "repo_id, full_name, default_branch, is_private, clone_url) "
                                "VALUES (:id, :org, :iid, :repo, :name, :branch, :priv, :url) "
                                "ON CONFLICT (org_id, repo_id) DO UPDATE SET "
                                "installation_id = EXCLUDED.installation_id, "
                                "full_name = EXCLUDED.full_name, "
                                "default_branch = EXCLUDED.default_branch, "
                                "is_private = EXCLUDED.is_private, "
                                "clone_url = EXCLUDED.clone_url, updated_at = now() "
                                f"RETURNING {_REPO_COLS}"
                            ),
                            {
                                "id": record_id,
                                "org": org_id,
                                "iid": installation_id,
                                "repo": repo_id,
                                "name": full_name,
                                "branch": default_branch,
                                "priv": is_private,
                                "url": clone_url,
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
        except IntegrityError as exc:
            raise ProjectConflictError("repository references an unknown installation") from exc
        return _to_repository(row)

    async def get_repository(self, org_id: str, repository_id: str) -> GitHubRepository | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_REPO_COLS} FROM github_repositories "
                            "WHERE id = :id AND org_id = :org"
                        ),
                        {"id": repository_id, "org": org_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_repository(row)

    async def get_repository_by_repo_id(self, org_id: str, repo_id: int) -> GitHubRepository | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_REPO_COLS} FROM github_repositories "
                            "WHERE org_id = :org AND repo_id = :repo"
                        ),
                        {"org": org_id, "repo": repo_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_repository(row)

    async def link_repository_project(
        self, org_id: str, repository_id: str, project_id: str | None
    ) -> GitHubRepository | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE github_repositories SET project_id = :pid, updated_at = now() "
                            "WHERE id = :id AND org_id = :org "
                            f"RETURNING {_REPO_COLS}"
                        ),
                        {"id": repository_id, "org": org_id, "pid": project_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_repository(row)

    async def list_repositories(
        self, org_id: str, *, installation_id: int | None = None
    ) -> list[GitHubRepository]:
        clause = "AND installation_id = :iid" if installation_id is not None else ""
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            rows = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_REPO_COLS} FROM github_repositories "
                            f"WHERE org_id = :org {clause} ORDER BY full_name"
                        ),
                        {"org": org_id, "iid": installation_id},
                    )
                )
                .mappings()
                .all()
            )
        return [_to_repository(r) for r in rows]

    # --- github sync state -----------------------------------------------------------
    async def upsert_sync_state(
        self,
        *,
        org_id: str,
        repository_id: str,
        last_delivery_id: str | None,
        last_synced_sha: str | None,
        last_synced_ref: str | None,
    ) -> GitHubSyncState:
        record_id = new_sync_state_id()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            "INSERT INTO github_sync_state (id, org_id, repository_id, "
                            "last_delivery_id, last_synced_sha, last_synced_ref, last_synced_at) "
                            "VALUES (:id, :org, :rid, :d, :sha, :ref, now()) "
                            "ON CONFLICT (org_id, repository_id) DO UPDATE SET "
                            "last_delivery_id = EXCLUDED.last_delivery_id, "
                            "last_synced_sha = EXCLUDED.last_synced_sha, "
                            "last_synced_ref = EXCLUDED.last_synced_ref, "
                            "last_synced_at = now(), updated_at = now() "
                            f"RETURNING {_SYNC_STATE_COLS}"
                        ),
                        {
                            "id": record_id,
                            "org": org_id,
                            "rid": repository_id,
                            "d": last_delivery_id,
                            "sha": last_synced_sha,
                            "ref": last_synced_ref,
                        },
                    )
                )
                .mappings()
                .one()
            )
        return _to_sync_state(row)

    async def get_sync_state(self, org_id: str, repository_id: str) -> GitHubSyncState | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": org_id})
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_SYNC_STATE_COLS} FROM github_sync_state "
                            "WHERE org_id = :org AND repository_id = :rid"
                        ),
                        {"org": org_id, "rid": repository_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_sync_state(row)

    # --- webhook deliveries (global) -------------------------------------------------
    async def record_delivery(
        self,
        *,
        delivery_id: str,
        event: str,
        installation_id: int | None,
        action: str | None,
    ) -> tuple[WebhookDelivery, bool]:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "INSERT INTO github_webhook_deliveries (delivery_id, event, "
                            "installation_id, action) VALUES (:id, :event, :iid, :action) "
                            "ON CONFLICT (delivery_id) DO NOTHING "
                            f"RETURNING {_DELIVERY_COLS}"
                        ),
                        {
                            "id": delivery_id,
                            "event": event,
                            "iid": installation_id,
                            "action": action,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is not None:
                return _to_delivery(row), True
            existing = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_DELIVERY_COLS} FROM github_webhook_deliveries "
                            "WHERE delivery_id = :id"
                        ),
                        {"id": delivery_id},
                    )
                )
                .mappings()
                .one()
            )
        return _to_delivery(existing), False

    async def mark_delivery(
        self, delivery_id: str, status: WebhookStatus
    ) -> WebhookDelivery | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE github_webhook_deliveries SET status = :status, "
                            "processed_at = CASE WHEN :status IN ('processed','skipped','failed') "
                            "THEN now() ELSE processed_at END WHERE delivery_id = :id "
                            f"RETURNING {_DELIVERY_COLS}"
                        ),
                        {"id": delivery_id, "status": status.value},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_delivery(row)

    async def get_delivery(self, delivery_id: str) -> WebhookDelivery | None:
        async with self._engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            f"SELECT {_DELIVERY_COLS} FROM github_webhook_deliveries "
                            "WHERE delivery_id = :id"
                        ),
                        {"id": delivery_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else _to_delivery(row)


__all__ = ["InMemoryProjectStore", "PostgresProjectStore", "ProjectStore"]
