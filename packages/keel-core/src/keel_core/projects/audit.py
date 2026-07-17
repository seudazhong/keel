"""Audit records for managed-project mutations (M3.7, WS-P).

Project create / import / sync / grant / archive / delete emit a structured, non-sensitive
audit record. Records carry only *metadata* (ids, slugs, refs, shas) — never a token,
credential, or a full clone URL with embedded credentials — so the trail is safe to ship to
a log pipeline. Mirrors :mod:`keel_core.identity.audit`.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

logger = logging.getLogger("keel.projects.audit")

_FORBIDDEN_DETAIL_KEYS = frozenset(
    {"token", "jwt", "api_key", "secret", "password", "private_key", "clone_url", "url"}
)


class ProjectAuditAction(StrEnum):
    project_created = "project.created"
    project_imported = "project.imported"
    project_synced = "project.synced"
    project_archived = "project.archived"
    project_deleted = "project.deleted"
    project_purged = "project.purged"
    project_grant_created = "project.grant_created"
    project_grant_revoked = "project.grant_revoked"
    worktree_materialized = "project.worktree_materialized"
    worktree_reclaimed = "project.worktree_reclaimed"
    run_associated = "project.run_associated"
    installation_linked = "project.installation_linked"
    installation_removed = "project.installation_removed"
    webhook_processed = "project.webhook_processed"


@dataclass(frozen=True)
class ProjectAuditEvent:
    """One immutable, non-sensitive project audit record."""

    action: ProjectAuditAction
    actor_user_id: str | None
    org_id: str | None
    target_id: str | None
    details: Mapping[str, str] = field(default_factory=dict)
    at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        forbidden = _FORBIDDEN_DETAIL_KEYS & set(self.details)
        if forbidden:
            raise ValueError(f"audit details must not contain sensitive keys: {sorted(forbidden)}")


@runtime_checkable
class ProjectAuditSink(Protocol):
    """Where project audit records are delivered."""

    def record(self, event: ProjectAuditEvent) -> None: ...


class LoggingProjectAuditSink:
    """Emit project audit records to the structured logger (default sink)."""

    def record(self, event: ProjectAuditEvent) -> None:
        logger.info(
            "projects.audit action=%s actor=%s org=%s target=%s details=%s",
            event.action.value,
            event.actor_user_id,
            event.org_id,
            event.target_id,
            dict(event.details),
        )


class InMemoryProjectAuditSink:
    """Collect project audit records in a list (tests)."""

    def __init__(self) -> None:
        self.events: list[ProjectAuditEvent] = []

    def record(self, event: ProjectAuditEvent) -> None:
        self.events.append(event)


__all__ = [
    "InMemoryProjectAuditSink",
    "LoggingProjectAuditSink",
    "ProjectAuditAction",
    "ProjectAuditEvent",
    "ProjectAuditSink",
]
