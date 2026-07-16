"""Typed records and validation for coding-project storage."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import NewType

ProjectId = NewType("ProjectId", str)
CodingRunId = NewType("CodingRunId", str)
ObjectKey = NewType("ObjectKey", str)

_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")
_GIT_REF = re.compile(r"(?:HEAD|refs/(?:heads|tags)/)?[A-Za-z0-9][A-Za-z0-9._/-]{0,254}\Z")
_WINDOWS_RESERVED = frozenset(
    {"con", "prn", "aux", "nul"}
    | {f"com{number}" for number in range(1, 10)}
    | {f"lpt{number}" for number in range(1, 10)}
)


class CodingStorageError(RuntimeError):
    """Base error for coding-project storage."""


class InvalidStorageInput(CodingStorageError, ValueError):
    """An identifier, path, reference, or remote failed validation."""


class StorageNotFound(CodingStorageError):
    """The requested storage resource does not exist."""


class StorageConflict(CodingStorageError):
    """The requested resource already exists or is currently inconsistent."""


class StorageQuotaExceeded(CodingStorageError):
    """A configured storage quota would be exceeded."""


class ArtifactRetention(StrEnum):
    ephemeral = "ephemeral"
    retained = "retained"


@dataclass(frozen=True, slots=True)
class ProjectRecord:
    project_id: ProjectId
    repository_path: Path
    head: str | None


@dataclass(frozen=True, slots=True)
class SnapshotRecord:
    project_id: ProjectId
    snapshot_id: str
    commit: str
    size_bytes: int
    created_at: datetime
    bundle_path: Path


@dataclass(frozen=True, slots=True)
class WorktreeRecord:
    project_id: ProjectId
    run_id: CodingRunId
    commit: str
    path: Path
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ArtifactRecord:
    project_id: ProjectId
    run_id: CodingRunId
    content_hash: str
    size_bytes: int
    name: str
    media_type: str
    created_at: datetime
    retention: ArtifactRetention
    retained_until: datetime | None
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ObjectInfo:
    key: ObjectKey
    size_bytes: int
    sha256: str
    modified_at: datetime


@dataclass(frozen=True, slots=True)
class ReapResult:
    removed: int
    reclaimed_bytes: int


def validate_identifier(value: str, *, kind: str) -> str:
    reserved_stem = value.split(".", 1)[0]
    if (
        not _IDENTIFIER.fullmatch(value)
        or value in {".", ".."}
        or value.endswith((".", " "))
        or reserved_stem in _WINDOWS_RESERVED
    ):
        raise InvalidStorageInput(
            f"{kind} must be a portable lowercase identifier of 1-64 characters"
        )
    return value


def project_id(value: str) -> ProjectId:
    return ProjectId(validate_identifier(value, kind="project_id"))


def run_id(value: str) -> CodingRunId:
    return CodingRunId(validate_identifier(value, kind="run_id"))


def validate_git_ref(value: str) -> str:
    if (
        not _GIT_REF.fullmatch(value)
        or ".." in value
        or "@{" in value
        or value.endswith((".", "/"))
        or "//" in value
        or "/." in value
    ):
        raise InvalidStorageInput("unsupported or unsafe Git reference")
    return value


def validate_artifact_name(value: str) -> str:
    if not value or len(value) > 255 or "\x00" in value or Path(value).name != value:
        raise InvalidStorageInput("artifact name must be a single non-empty path component")
    return value
