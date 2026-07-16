"""Confinement, recovery, lifecycle, and quota tests for coding storage."""

from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from keel_core.coding import (
    ActiveGitStore,
    ArtifactRetention,
    ArtifactStore,
    GitRunner,
    InvalidStorageInput,
    LocalActiveGitStore,
    LocalArtifactStore,
    LocalCodingStorage,
    LocalObjectStore,
    LocalSnapshotStore,
    LocalWorktreeStore,
    ObjectKey,
    ObjectStore,
    SnapshotStore,
    StorageConflict,
    StorageNotFound,
    StorageQuotaExceeded,
    StorageQuotas,
    WorktreeStore,
    project_id,
    run_id,
)

_OLD = datetime(2020, 1, 1, tzinfo=UTC)


def _run(git: GitRunner, cwd: Path, *args: str) -> str:
    return git.run(list(args), cwd=cwd).stdout.strip()


def _remote(tmp_path: Path) -> tuple[Path, GitRunner]:
    git = GitRunner()
    remote = tmp_path / "source"
    _run(git, tmp_path, "init", "--initial-branch=main", str(remote))
    (remote / "README.md").write_text("one\n", encoding="utf-8")
    _run(git, remote, "add", "README.md")
    _run(
        git,
        remote,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.test",
        "commit",
        "-m",
        "first",
    )
    return remote, git


def _storage(tmp_path: Path, **kwargs: object) -> LocalCodingStorage:
    return LocalCodingStorage(
        tmp_path / "coding",
        allow_local_remotes=True,
        **kwargs,  # type: ignore[arg-type]
    )


def test_protocol_views_and_project_import_are_typed(tmp_path: Path) -> None:
    remote, _git = _remote(tmp_path)
    storage = _storage(tmp_path)
    active = LocalActiveGitStore(storage)
    snapshots = LocalSnapshotStore(storage)
    worktrees = LocalWorktreeStore(storage)
    artifacts = LocalArtifactStore(storage)

    assert isinstance(active, ActiveGitStore)
    assert isinstance(snapshots, SnapshotStore)
    assert isinstance(worktrees, WorktreeStore)
    assert isinstance(artifacts, ArtifactStore)

    project = active.import_project(project_id("alpha"), remote)
    assert project.head is not None
    assert project.repository_path.name == "alpha.git"


@pytest.mark.parametrize(
    "bad",
    ["../other", "..", "/absolute", r"other\escape", "-leading", "a" * 65, "x\x00y"],
)
def test_t1_identifiers_cannot_escape_storage_roots(tmp_path: Path, bad: str) -> None:
    storage = _storage(tmp_path)
    with pytest.raises(InvalidStorageInput):
        storage.create_project(project_id(bad))
    assert list(storage.projects_root.iterdir()) == []


def test_t2_remote_policy_rejects_unsafe_protocols_and_hosts(tmp_path: Path) -> None:
    storage = LocalCodingStorage(tmp_path / "coding", allowed_https_hosts={"good.example"})
    storage.create_project(project_id("alpha"))

    for remote in (
        "ssh://evil.example/repo.git",
        "https://evil.example/repo.git",
        "https://user:secret@good.example/repo.git",
        "https://good.example/repo.git?token=secret",
        "file:///some/repo",
        "../repo",
    ):
        with pytest.raises(InvalidStorageInput):
            storage.fetch(project_id("alpha"), remote)


def test_t3_artifacts_are_scoped_and_cross_project_reads_fail(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    storage.create_project(project_id("alpha"))
    storage.create_project(project_id("beta"))
    artifacts = LocalArtifactStore(storage)
    record = artifacts.put(
        project_id("alpha"),
        run_id("run-1"),
        b"proposal",
        name="proposal.patch",
        metadata={"kind": "proposal"},
    )

    with pytest.raises(StorageNotFound):
        artifacts.read(project_id("beta"), run_id("run-1"), record.content_hash)
    with pytest.raises(StorageNotFound):
        artifacts.read(project_id("alpha"), run_id("run-2"), record.content_hash)
    assert artifacts.read(project_id("alpha"), run_id("run-1"), record.content_hash) == b"proposal"


def test_concurrent_project_locking_keeps_worktree_state_consistent(tmp_path: Path) -> None:
    remote, _git = _remote(tmp_path)
    storage_a = _storage(tmp_path)
    storage_b = LocalCodingStorage(storage_a.root, allow_local_remotes=True)
    storage_a.import_project(project_id("alpha"), remote)

    def materialize(index: int) -> Path:
        storage = storage_a if index == 1 else storage_b
        return storage.materialize(project_id("alpha"), run_id(f"run-{index}"), ref="main").path

    with ThreadPoolExecutor(max_workers=2) as pool:
        paths = list(pool.map(materialize, (1, 2)))

    assert all((path / "README.md").read_text(encoding="utf-8") == "one\n" for path in paths)
    assert storage_a.remove(project_id("alpha"), run_id("run-1"))
    assert storage_b.remove(project_id("alpha"), run_id("run-2"))


def test_snapshot_restores_authoritative_repository_after_new_fetch(tmp_path: Path) -> None:
    remote, git = _remote(tmp_path)
    storage = _storage(tmp_path)
    original = storage.import_project(project_id("alpha"), remote)
    snapshot = storage.create_snapshot(project_id("alpha"))
    assert snapshot.commit == original.head

    (remote / "README.md").write_text("two\n", encoding="utf-8")
    _run(git, remote, "add", "README.md")
    _run(
        git,
        remote,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.test",
        "commit",
        "-m",
        "second",
    )
    changed = storage.fetch(project_id("alpha"), remote)
    assert changed.head != original.head

    restored = storage.restore_snapshot(project_id("alpha"), snapshot.snapshot_id)
    assert restored.head == original.head
    worktree = storage.materialize(project_id("alpha"), run_id("recovered"))
    assert (worktree.path / "README.md").read_text(encoding="utf-8") == "one\n"


def test_snapshot_tampering_is_detected_without_replacing_active_repo(tmp_path: Path) -> None:
    remote, _git = _remote(tmp_path)
    storage = _storage(tmp_path)
    project = storage.import_project(project_id("alpha"), remote)
    snapshot = storage.create_snapshot(project_id("alpha"))
    snapshot.bundle_path.write_bytes(b"not a bundle")

    with pytest.raises(StorageConflict, match="hash"):
        storage.restore_snapshot(project_id("alpha"), snapshot.snapshot_id)
    assert storage.get_project(project_id("alpha")).head == project.head


def test_worktree_cleanup_removes_residue_and_is_idempotent(tmp_path: Path) -> None:
    remote, _git = _remote(tmp_path)
    storage = _storage(tmp_path)
    storage.import_project(project_id("alpha"), remote)
    worktree = storage.materialize(project_id("alpha"), run_id("run-1"))
    (worktree.path / "untracked.tmp").write_text("residue", encoding="utf-8")

    assert storage.remove(project_id("alpha"), run_id("run-1"))
    assert not worktree.path.exists()
    assert not storage.remove(project_id("alpha"), run_id("run-1"))


def test_worktree_reaper_uses_owned_markers_only(tmp_path: Path) -> None:
    remote, _git = _remote(tmp_path)
    storage = _storage(tmp_path)
    storage.import_project(project_id("alpha"), remote)
    worktree = storage.materialize(project_id("alpha"), run_id("old-run"))
    marker = worktree.path / ".keel-worktree.json"
    value = json.loads(marker.read_text(encoding="utf-8"))
    value["created_at"] = _OLD.isoformat()
    marker.write_text(json.dumps(value), encoding="utf-8")

    result = storage.reap_worktrees(older_than=_OLD + timedelta(days=1))
    assert result.removed == 1
    assert not worktree.path.exists()
    assert storage.reap_worktrees(older_than=_OLD + timedelta(days=1)).removed == 0


def test_artifact_hashes_retention_reaping_and_idempotent_delete(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    storage.create_project(project_id("alpha"))
    artifacts = LocalArtifactStore(storage)
    content = b"diff --git a/a b/a\n"
    first = artifacts.put(project_id("alpha"), run_id("run-1"), content, name="proposal.patch")
    duplicate = artifacts.put(
        project_id("alpha"), run_id("run-1"), content, name="ignored-name.patch"
    )
    assert first == duplicate
    assert first.content_hash == hashlib.sha256(content).hexdigest()

    retained = artifacts.retain(project_id("alpha"), run_id("run-1"), first.content_hash)
    assert retained.retention is ArtifactRetention.retained
    metadata = storage.artifacts_root / "alpha" / "run-1" / first.content_hash / "metadata.json"
    value = json.loads(metadata.read_text(encoding="utf-8"))
    value["created_at"] = _OLD.isoformat()
    metadata.write_text(json.dumps(value), encoding="utf-8")
    assert artifacts.reap(older_than=_OLD + timedelta(days=1)).removed == 0

    assert artifacts.delete(project_id("alpha"), run_id("run-1"), first.content_hash)
    assert not artifacts.delete(project_id("alpha"), run_id("run-1"), first.content_hash)


def test_artifact_size_and_project_quotas_are_enforced(tmp_path: Path) -> None:
    quotas = StorageQuotas(
        max_artifact_bytes=4,
        max_project_artifact_bytes=6,
        max_snapshots_per_project=2,
        max_worktrees_per_project=2,
    )
    storage = _storage(tmp_path, quotas=quotas)
    storage.create_project(project_id("alpha"))
    artifacts = LocalArtifactStore(storage)
    with pytest.raises(StorageQuotaExceeded, match="size"):
        artifacts.put(project_id("alpha"), run_id("one"), b"12345", name="large.bin")
    artifacts.put(project_id("alpha"), run_id("one"), b"1234", name="one.bin")
    with pytest.raises(StorageQuotaExceeded, match="project"):
        artifacts.put(project_id("alpha"), run_id("two"), b"5678", name="two.bin")


def test_local_object_store_has_s3_shaped_keys_but_confines_paths(tmp_path: Path) -> None:
    objects = LocalObjectStore(tmp_path / "objects", max_object_bytes=5)
    assert isinstance(objects, ObjectStore)
    info = objects.put(ObjectKey("alpha/run/blob"), b"data")
    assert info.sha256 == hashlib.sha256(b"data").hexdigest()
    assert objects.get(ObjectKey("alpha/run/blob")) == b"data"
    assert [item.key for item in objects.list(ObjectKey("alpha"))] == [ObjectKey("alpha/run/blob")]
    for unsafe in ("../escape", "/absolute", r"alpha\escape"):
        with pytest.raises(InvalidStorageInput):
            objects.put(ObjectKey(unsafe), b"x")
    with pytest.raises(StorageQuotaExceeded):
        objects.put(ObjectKey("alpha/large"), b"123456")
