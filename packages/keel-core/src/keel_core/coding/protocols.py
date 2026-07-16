"""Storage seams for coding projects."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

from .models import (
    ArtifactRecord,
    ArtifactRetention,
    CodingRunId,
    ObjectInfo,
    ObjectKey,
    ProjectId,
    ProjectRecord,
    ReapResult,
    SnapshotRecord,
    WorktreeRecord,
)


@runtime_checkable
class ActiveGitStore(Protocol):
    def create_project(
        self, project_id: ProjectId, *, default_branch: str = "main"
    ) -> ProjectRecord: ...

    def import_project(
        self, project_id: ProjectId, remote: str | Path, *, default_branch: str = "main"
    ) -> ProjectRecord: ...

    def fetch(self, project_id: ProjectId, remote: str | Path) -> ProjectRecord: ...

    def get_project(self, project_id: ProjectId) -> ProjectRecord: ...


@runtime_checkable
class SnapshotStore(Protocol):
    def create_snapshot(self, project_id: ProjectId) -> SnapshotRecord: ...

    def restore_snapshot(self, project_id: ProjectId, snapshot_id: str) -> ProjectRecord: ...

    def list_snapshots(self, project_id: ProjectId) -> list[SnapshotRecord]: ...


@runtime_checkable
class WorktreeStore(Protocol):
    def materialize(
        self, project_id: ProjectId, run_id: CodingRunId, *, ref: str = "HEAD"
    ) -> WorktreeRecord: ...

    def remove(self, project_id: ProjectId, run_id: CodingRunId) -> bool: ...

    def reap(self, *, older_than: datetime) -> ReapResult: ...


@runtime_checkable
class ArtifactStore(Protocol):
    def put(
        self,
        project_id: ProjectId,
        run_id: CodingRunId,
        data: bytes,
        *,
        name: str,
        media_type: str = "application/octet-stream",
        retention: ArtifactRetention = ArtifactRetention.ephemeral,
        retained_until: datetime | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> ArtifactRecord: ...

    def read(self, project_id: ProjectId, run_id: CodingRunId, content_hash: str) -> bytes: ...

    def retain(
        self,
        project_id: ProjectId,
        run_id: CodingRunId,
        content_hash: str,
        *,
        until: datetime | None = None,
    ) -> ArtifactRecord: ...

    def delete(self, project_id: ProjectId, run_id: CodingRunId, content_hash: str) -> bool: ...

    def reap(self, *, older_than: datetime) -> ReapResult: ...


@runtime_checkable
class ObjectStore(Protocol):
    """S3-shaped byte-object seam; this slice intentionally supplies local FS only."""

    def put(self, key: ObjectKey, data: bytes) -> ObjectInfo: ...

    def get(self, key: ObjectKey) -> bytes: ...

    def stat(self, key: ObjectKey) -> ObjectInfo | None: ...

    def delete(self, key: ObjectKey) -> bool: ...

    def list(self, prefix: ObjectKey) -> Iterable[ObjectInfo]: ...
