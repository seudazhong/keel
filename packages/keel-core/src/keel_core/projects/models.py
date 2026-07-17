"""Managed-project + GitHub-binding domain models (M3.7, WS-P).

Pure, transport-free dataclasses + enums + validators + errors, mirroring the durable schema
in migration ``0015_projects_github``. Identifiers are stable, prefixed hex strings
(``prj_``/``pwt_``/``prn_``/``syn_``/``ghi_``/``ghr_``/``ghs_``) so a caller can tell a
resource's kind from its id and ids never collide across kinds.

A managed project is org-owned. Fine-grained Agent authority over a project reuses the
generic ``resource_grants`` model from :mod:`keel_core.identity` with the resource type
:data:`PROJECT_RESOURCE_TYPE` rather than duplicating a bespoke authority table.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from keel_core.errors import KeelError

# --- Identifiers ---------------------------------------------------------------------

_PROJECT_PREFIX = "prj_"
_WORKTREE_PREFIX = "pwt_"
_PROJECT_RUN_PREFIX = "prn_"
_SYNC_ENTRY_PREFIX = "syn_"
_INSTALLATION_PREFIX = "ghi_"
_REPOSITORY_PREFIX = "ghr_"
_SYNC_STATE_PREFIX = "ghs_"

type ProjectId = str
type WorktreeId = str
type ProjectRunId = str
type SyncEntryId = str
type InstallationId = str
type RepositoryId = str
type SyncStateId = str

# The generic ``resource_grants.resource_type`` used for Agent authority over a project.
PROJECT_RESOURCE_TYPE = "project"


def _new_id(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex}"


def new_project_id() -> ProjectId:
    return _new_id(_PROJECT_PREFIX)


def new_worktree_id() -> WorktreeId:
    return _new_id(_WORKTREE_PREFIX)


def new_project_run_id() -> ProjectRunId:
    return _new_id(_PROJECT_RUN_PREFIX)


def new_sync_entry_id() -> SyncEntryId:
    return _new_id(_SYNC_ENTRY_PREFIX)


def new_installation_id() -> InstallationId:
    return _new_id(_INSTALLATION_PREFIX)


def new_repository_id() -> RepositoryId:
    return _new_id(_REPOSITORY_PREFIX)


def new_sync_state_id() -> SyncStateId:
    return _new_id(_SYNC_STATE_PREFIX)


# --- Errors --------------------------------------------------------------------------


class ProjectError(KeelError):
    """Base class for managed-project-domain errors."""


class ProjectValidationError(ProjectError):
    """A supplied project value (slug/name/branch/ref) is invalid."""


class ProjectNotFoundError(ProjectError):
    """A referenced project resource does not exist (or is not visible)."""


class ProjectConflictError(ProjectError):
    """A uniqueness constraint (slug/repo/installation) was violated."""


class ProjectOptimisticConcurrencyError(ProjectError):
    """A durable update lost an optimistic-concurrency race (stale ``version``)."""


class ProjectCrossOrgError(ProjectError):
    """A resource from one org was referenced from another (confused-deputy defense)."""


class ProjectQuotaExceededError(ProjectError):
    """A configured project quota would be exceeded."""


# --- Enumerations --------------------------------------------------------------------


class ProjectSource(StrEnum):
    blank = "blank"
    github = "github"


class ProjectVisibility(StrEnum):
    private = "private"
    internal = "internal"


class ProjectStatus(StrEnum):
    active = "active"
    archived = "archived"
    deleted = "deleted"


class WorktreeStatus(StrEnum):
    active = "active"
    reclaimed = "reclaimed"


class SyncKind(StrEnum):
    import_ = "import"
    fetch = "fetch"
    webhook_push = "webhook_push"
    default_branch = "default_branch"


class SyncStatus(StrEnum):
    pending = "pending"
    succeeded = "succeeded"
    failed = "failed"


class InstallationStatus(StrEnum):
    active = "active"
    suspended = "suspended"
    deleted = "deleted"


class WebhookStatus(StrEnum):
    received = "received"
    processed = "processed"
    skipped = "skipped"
    failed = "failed"


class StorageBackend(StrEnum):
    local = "local"


# --- Validation ----------------------------------------------------------------------

_SLUG_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,38}[a-z0-9])$")
_BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,254}$")
_DISPLAY_NAME_MAX = 200


def validate_project_slug(value: str) -> str:
    """Normalize + validate a project slug (lower, 3-40 chars, ``a-z0-9-``, no edge dash)."""
    slug = value.strip().lower()
    if not _SLUG_RE.match(slug):
        raise ProjectValidationError(
            "project slug must be 3-40 chars of a-z, 0-9 or '-' (no leading/trailing '-')"
        )
    return slug


def validate_display_name(value: str) -> str:
    name = value.strip()
    if not name or len(name) > _DISPLAY_NAME_MAX:
        raise ProjectValidationError(f"display name must be 1-{_DISPLAY_NAME_MAX} characters")
    return name


def validate_branch(value: str) -> str:
    """Validate a Git branch/ref name (no traversal, no unsafe sequences)."""
    branch = value.strip()
    if (
        not _BRANCH_RE.match(branch)
        or ".." in branch
        or "@{" in branch
        or branch.endswith((".", "/", ".lock"))
        or "//" in branch
    ):
        raise ProjectValidationError("branch must be a safe Git ref name")
    return branch


# --- Records -------------------------------------------------------------------------


@dataclass(frozen=True)
class Project:
    """A durable, org-owned managed project."""

    id: ProjectId
    org_id: str
    slug: str
    display_name: str
    source: ProjectSource = ProjectSource.blank
    visibility: ProjectVisibility = ProjectVisibility.private
    status: ProjectStatus = ProjectStatus.active
    default_branch: str = "main"
    active_git_handle: str | None = None
    storage_backend: StorageBackend = StorageBackend.local
    github_repository_id: int | None = None
    version: int = 1
    created_at: datetime | None = None
    updated_at: datetime | None = None
    archived_at: datetime | None = None
    deleted_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.status is ProjectStatus.active


@dataclass(frozen=True)
class ProjectWorktree:
    """A run-scoped materialized worktree bound to a durable run id."""

    id: WorktreeId
    org_id: str
    project_id: ProjectId
    run_id: str
    coding_run_id: str
    git_ref: str = "HEAD"
    commit_sha: str | None = None
    handle_path: str = ""
    status: WorktreeStatus = WorktreeStatus.active
    created_at: datetime | None = None
    reclaimed_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.status is WorktreeStatus.active


@dataclass(frozen=True)
class ProjectRun:
    """The project<->durable-run association (a run belongs to exactly one project)."""

    id: ProjectRunId
    org_id: str
    project_id: ProjectId
    run_id: str
    created_at: datetime | None = None


@dataclass(frozen=True)
class RepoSyncEntry:
    """One append-only repo sync ledger entry."""

    id: SyncEntryId
    org_id: str
    project_id: ProjectId
    kind: SyncKind
    status: SyncStatus = SyncStatus.pending
    git_ref: str | None = None
    before_sha: str | None = None
    after_sha: str | None = None
    delivery_id: str | None = None
    detail: dict[str, str] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class ProjectQuota:
    """Per-org project quotas."""

    org_id: str
    max_projects: int = 100
    max_active_worktrees: int = 64
    max_repository_bytes: int = 2 * 1024 * 1024 * 1024
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class GitHubInstallation:
    """A GitHub App installation bound to an org."""

    id: InstallationId
    org_id: str
    installation_id: int
    app_id: int
    account_login: str
    account_type: str = "Organization"
    status: InstallationStatus = InstallationStatus.active
    created_at: datetime | None = None
    updated_at: datetime | None = None
    suspended_at: datetime | None = None
    deleted_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.status is InstallationStatus.active


@dataclass(frozen=True)
class GitHubRepository:
    """A repository visible through an installation, optionally linked to a project."""

    id: RepositoryId
    org_id: str
    installation_id: int
    repo_id: int
    full_name: str
    default_branch: str = "main"
    is_private: bool = True
    clone_url: str = ""
    project_id: ProjectId | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class GitHubSyncState:
    """Per-repository durable sync cursor."""

    id: SyncStateId
    org_id: str
    repository_id: RepositoryId
    last_delivery_id: str | None = None
    last_synced_sha: str | None = None
    last_synced_ref: str | None = None
    last_synced_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class WebhookDelivery:
    """One received GitHub webhook delivery (global; PK makes a replay a no-op)."""

    delivery_id: str
    event: str
    installation_id: int | None = None
    action: str | None = None
    status: WebhookStatus = WebhookStatus.received
    received_at: datetime | None = None
    processed_at: datetime | None = None


__all__ = [
    "PROJECT_RESOURCE_TYPE",
    "GitHubInstallation",
    "GitHubRepository",
    "GitHubSyncState",
    "InstallationId",
    "InstallationStatus",
    "Project",
    "ProjectConflictError",
    "ProjectCrossOrgError",
    "ProjectError",
    "ProjectId",
    "ProjectNotFoundError",
    "ProjectOptimisticConcurrencyError",
    "ProjectQuota",
    "ProjectQuotaExceededError",
    "ProjectRun",
    "ProjectRunId",
    "ProjectSource",
    "ProjectStatus",
    "ProjectValidationError",
    "ProjectVisibility",
    "ProjectWorktree",
    "RepoSyncEntry",
    "RepositoryId",
    "StorageBackend",
    "SyncEntryId",
    "SyncKind",
    "SyncStateId",
    "SyncStatus",
    "WebhookDelivery",
    "WebhookStatus",
    "WorktreeId",
    "WorktreeStatus",
    "new_installation_id",
    "new_project_id",
    "new_project_run_id",
    "new_repository_id",
    "new_sync_entry_id",
    "new_sync_state_id",
    "new_worktree_id",
    "validate_branch",
    "validate_display_name",
    "validate_project_slug",
]
