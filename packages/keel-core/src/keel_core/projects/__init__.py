"""Managed projects + GitHub synchronization (M3.7, WS-P).

Org-owned managed projects, their durable Git storage handles, run-scoped worktrees, a repo
sync ledger, per-org quotas, and the GitHub App binding (installations / repositories /
webhook deliveries / sync state). Authorization reuses the identity principal / resource /
capability model and the generic ``resource_grants`` for project Agent authority.
"""

from keel_core.projects.audit import (
    InMemoryProjectAuditSink,
    LoggingProjectAuditSink,
    ProjectAuditAction,
    ProjectAuditEvent,
    ProjectAuditSink,
)
from keel_core.projects.models import (
    PROJECT_RESOURCE_TYPE,
    GitHubInstallation,
    GitHubRepository,
    GitHubSyncState,
    InstallationStatus,
    Project,
    ProjectConflictError,
    ProjectCrossOrgError,
    ProjectError,
    ProjectNotFoundError,
    ProjectOptimisticConcurrencyError,
    ProjectQuota,
    ProjectQuotaExceededError,
    ProjectRun,
    ProjectSource,
    ProjectStatus,
    ProjectValidationError,
    ProjectVisibility,
    ProjectWorktree,
    RepoSyncEntry,
    StorageBackend,
    SyncKind,
    SyncStatus,
    WebhookDelivery,
    WebhookStatus,
    WorktreeStatus,
)
from keel_core.projects.service import GitHubIntegration, ProjectService, WebhookOutcome
from keel_core.projects.storage import (
    InMemoryProjectStorage,
    LocalProjectStorage,
    MaterializedWorktree,
    ProjectStorage,
    worktree_storage_id,
)
from keel_core.projects.store import (
    InMemoryProjectStore,
    PostgresProjectStore,
    ProjectStore,
)

__all__ = [
    "PROJECT_RESOURCE_TYPE",
    "GitHubInstallation",
    "GitHubIntegration",
    "GitHubRepository",
    "GitHubSyncState",
    "InMemoryProjectAuditSink",
    "InMemoryProjectStorage",
    "InMemoryProjectStore",
    "InstallationStatus",
    "LocalProjectStorage",
    "LoggingProjectAuditSink",
    "MaterializedWorktree",
    "PostgresProjectStore",
    "Project",
    "ProjectAuditAction",
    "ProjectAuditEvent",
    "ProjectAuditSink",
    "ProjectConflictError",
    "ProjectCrossOrgError",
    "ProjectError",
    "ProjectNotFoundError",
    "ProjectOptimisticConcurrencyError",
    "ProjectQuota",
    "ProjectQuotaExceededError",
    "ProjectRun",
    "ProjectService",
    "ProjectSource",
    "ProjectStatus",
    "ProjectStorage",
    "ProjectStore",
    "ProjectValidationError",
    "ProjectVisibility",
    "ProjectWorktree",
    "RepoSyncEntry",
    "StorageBackend",
    "SyncKind",
    "SyncStatus",
    "WebhookDelivery",
    "WebhookOutcome",
    "WebhookStatus",
    "WorktreeStatus",
    "worktree_storage_id",
]
