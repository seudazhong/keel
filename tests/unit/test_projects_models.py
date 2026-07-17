"""Managed-project domain models: validation, ids, audit redaction (M3.7)."""

from __future__ import annotations

import pytest

from keel_core.projects.audit import (
    InMemoryProjectAuditSink,
    ProjectAuditAction,
    ProjectAuditEvent,
)
from keel_core.projects.models import (
    ProjectValidationError,
    new_installation_id,
    new_project_id,
    new_worktree_id,
    validate_branch,
    validate_display_name,
    validate_project_slug,
)


def test_id_prefixes_distinct() -> None:
    assert new_project_id().startswith("prj_")
    assert new_worktree_id().startswith("pwt_")
    assert new_installation_id().startswith("ghi_")


def test_slug_validation() -> None:
    assert validate_project_slug("  My-Proj ") == "my-proj"
    for bad in ["ab", "-lead", "trail-", "has space", "UPPER!"]:
        with pytest.raises(ProjectValidationError):
            validate_project_slug(bad)


def test_branch_validation() -> None:
    assert validate_branch("main") == "main"
    assert validate_branch("feature/x-1") == "feature/x-1"
    for bad in ["../etc", "a..b", "a//b", "end.lock", "trail/", "bad@{ref}"]:
        with pytest.raises(ProjectValidationError):
            validate_branch(bad)


def test_display_name_validation() -> None:
    assert validate_display_name(" Name ") == "Name"
    with pytest.raises(ProjectValidationError):
        validate_display_name("   ")
    with pytest.raises(ProjectValidationError):
        validate_display_name("x" * 201)


def test_audit_rejects_sensitive_keys() -> None:
    with pytest.raises(ValueError):
        ProjectAuditEvent(ProjectAuditAction.project_created, "u", "o", "p", {"token": "secret"})
    sink = InMemoryProjectAuditSink()
    sink.record(ProjectAuditEvent(ProjectAuditAction.project_created, "u", "o", "p", {"slug": "s"}))
    assert sink.events[0].details["slug"] == "s"
