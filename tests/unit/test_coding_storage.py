"""Confinement, recovery, lifecycle, and quota tests for coding storage."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import threading
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from keel_core.coding import (
    ActiveGitStore,
    ArtifactRetention,
    ArtifactStore,
    CodingRunId,
    GitCommandError,
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
    ProjectId,
    SnapshotStore,
    StorageConflict,
    StorageNotFound,
    StorageQuotaExceeded,
    StorageQuotas,
    WorktreeStore,
    project_id,
    run_id,
)
from keel_core.coding import local as coding_local

_OLD = datetime(2020, 1, 1, tzinfo=UTC)


class RecordingGitRunner(GitRunner):
    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, ...]] = []

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Path | None = None,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append(tuple(args))
        return super().run(args, cwd=cwd, check=check)

    def run_bounded(
        self,
        args: Sequence[str],
        *,
        monitored_path: Path,
        max_bytes: int,
        cwd: Path | None = None,
        check: bool = True,
        poll_interval_seconds: float = 0.01,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append(tuple(args))
        return super().run_bounded(
            args,
            monitored_path=monitored_path,
            max_bytes=max_bytes,
            cwd=cwd,
            check=check,
            poll_interval_seconds=poll_interval_seconds,
        )


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
    [
        "../other",
        "..",
        "/absolute",
        r"other\escape",
        "-leading",
        "Uppercase",
        "con",
        "con.txt",
        "com1",
        "lpt9.log",
        "trailing.",
        "a" * 65,
        "x\x00y",
    ],
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


def test_materialized_workspace_is_an_isolated_normal_git_clone(tmp_path: Path) -> None:
    remote, git = _remote(tmp_path)
    storage = _storage(tmp_path)
    project = storage.import_project(project_id("alpha"), remote)
    workspace = storage.materialize(project_id("alpha"), run_id("sandbox"))

    assert _run(git, workspace.path, "status", "--porcelain") == ""
    assert _run(git, workspace.path, "remote") == ""
    assert not (workspace.path / ".git" / "objects" / "info" / "alternates").exists()
    (workspace.path / "sandbox.txt").write_text("sandbox\n", encoding="utf-8")
    _run(git, workspace.path, "add", "sandbox.txt")
    _run(
        git,
        workspace.path,
        "-c",
        "user.name=Sandbox",
        "-c",
        "user.email=sandbox@example.test",
        "commit",
        "-m",
        "sandbox-only",
    )
    _run(git, workspace.path, "branch", "sandbox-only")

    assert (
        git.run(
            [
                "--git-dir",
                str(project.repository_path),
                "show-ref",
                "--verify",
                "refs/heads/sandbox-only",
            ],
            check=False,
        ).returncode
        != 0
    )
    assert storage.get_project(project_id("alpha")).head == project.head


def test_concurrent_failed_import_cannot_delete_successful_install(tmp_path: Path) -> None:
    remote, _git = _remote(tmp_path)
    invalid_remote = tmp_path / "not-a-repository"
    invalid_remote.mkdir()
    storage_a = _storage(tmp_path)
    storage_b = LocalCodingStorage(storage_a.root, allow_local_remotes=True)

    def attempt(storage: LocalCodingStorage, source: Path) -> object:
        try:
            return storage.import_project(project_id("alpha"), source)
        except Exception as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(
            pool.map(
                lambda pair: attempt(*pair),
                ((storage_a, invalid_remote), (storage_b, remote)),
            )
        )

    assert any(not isinstance(outcome, Exception) for outcome in outcomes)
    assert storage_a.get_project(project_id("alpha")).head is not None


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


def test_snapshot_restore_preserves_complete_refs_and_streams_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote, git = _remote(tmp_path)
    head = _run(git, remote, "rev-parse", "HEAD")
    _run(git, remote, "tag", "v1")
    _run(git, remote, "update-ref", "refs/notes/review", head)
    storage = _storage(tmp_path)
    project = storage.import_project(project_id("alpha"), remote)

    original_read_bytes = Path.read_bytes

    def guarded_read_bytes(path: Path) -> bytes:
        if path.suffix == ".bundle":
            raise AssertionError("bundle files must be streamed")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    snapshot = storage.create_snapshot(project_id("alpha"))
    _run(
        git,
        tmp_path,
        "--git-dir",
        str(project.repository_path),
        "update-ref",
        "-d",
        "refs/notes/review",
    )
    _run(
        git, tmp_path, "--git-dir", str(project.repository_path), "update-ref", "-d", "refs/tags/v1"
    )
    storage.restore_snapshot(project_id("alpha"), snapshot.snapshot_id)

    assert (
        _run(
            git,
            tmp_path,
            "--git-dir",
            str(project.repository_path),
            "rev-parse",
            "refs/notes/review",
        )
        == head
    )
    assert (
        _run(git, tmp_path, "--git-dir", str(project.repository_path), "rev-parse", "refs/tags/v1")
        == head
    )


def test_restore_refuses_to_invalidate_active_workspace(tmp_path: Path) -> None:
    remote, _git = _remote(tmp_path)
    storage = _storage(tmp_path)
    storage.import_project(project_id("alpha"), remote)
    snapshot = storage.create_snapshot(project_id("alpha"))
    workspace = storage.materialize(project_id("alpha"), run_id("active"))

    with pytest.raises(StorageConflict, match="active workspaces"):
        storage.restore_snapshot(project_id("alpha"), snapshot.snapshot_id)
    assert workspace.path.exists()
    storage.remove(project_id("alpha"), run_id("active"))
    storage.restore_snapshot(project_id("alpha"), snapshot.snapshot_id)


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
    marker = storage.worktrees_root / "alpha" / ".old-run.json"
    value = json.loads(marker.read_text(encoding="utf-8"))
    value["created_at"] = _OLD.isoformat()
    marker.write_text(json.dumps(value), encoding="utf-8")

    result = storage.reap_worktrees(older_than=_OLD + timedelta(days=1))
    assert result.removed == 1
    assert not worktree.path.exists()
    assert storage.reap_worktrees(older_than=_OLD + timedelta(days=1)).removed == 0


def test_worktree_reaper_does_not_delete_recreated_same_run_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    remote, _git = _remote(tmp_path)
    storage = _storage(tmp_path)
    project = project_id("alpha")
    run = run_id("same-run")
    storage.import_project(project, remote)
    workspace = storage.materialize(project, run)
    marker = storage.worktrees_root / "alpha" / ".same-run.json"
    old_marker = json.loads(marker.read_text(encoding="utf-8"))
    old_marker["created_at"] = _OLD.isoformat()
    marker.write_text(json.dumps(old_marker), encoding="utf-8")
    discovered = threading.Event()
    original_reap = storage._reap_workspace

    def observed_reap(
        project_value: ProjectId,
        run_value: CodingRunId,
        *,
        observed_identity: tuple[int, int],
        observed_workspace_id: str | None,
        observed_marker: bool,
        cutoff: datetime,
    ) -> tuple[bool, int]:
        discovered.set()
        return original_reap(
            project_value,
            run_value,
            observed_identity=observed_identity,
            observed_workspace_id=observed_workspace_id,
            observed_marker=observed_marker,
            cutoff=cutoff,
        )

    monkeypatch.setattr(storage, "_reap_workspace", observed_reap)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with storage._lock(project).acquire():
            future = pool.submit(storage.reap_worktrees, older_than=_OLD + timedelta(days=1))
            assert discovered.wait(timeout=5)
            coding_local._remove_tree(workspace.path)
            marker.unlink()
            workspace.path.mkdir()
            (workspace.path / "new-instance.txt").write_text("new", encoding="utf-8")
            coding_local._atomic_json(
                marker,
                {
                    "project_id": "alpha",
                    "run_id": "same-run",
                    "workspace_id": "new-instance",
                    "commit": workspace.commit,
                    "created_at": datetime.now(UTC).isoformat(),
                },
            )
        assert future.result(timeout=5).removed == 0
    assert (workspace.path / "new-instance.txt").read_text(encoding="utf-8") == "new"


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


def test_artifact_reaper_rechecks_retention_under_project_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = _storage(tmp_path)
    project = project_id("alpha")
    run = run_id("run-1")
    storage.create_project(project)
    artifact = storage.put_artifact(project, run, b"old", name="old.bin")
    metadata = storage.artifacts_root / "alpha" / "run-1" / artifact.content_hash / "metadata.json"
    value = json.loads(metadata.read_text(encoding="utf-8"))
    value["created_at"] = _OLD.isoformat()
    metadata.write_text(json.dumps(value), encoding="utf-8")
    discovered = threading.Event()
    original_read_json = coding_local._read_json

    def observed_read_json(path: Path) -> dict[str, object]:
        result = original_read_json(path)
        if threading.current_thread().name.startswith("artifact-reaper") and path == metadata:
            discovered.set()
        return result

    monkeypatch.setattr(coding_local, "_read_json", observed_read_json)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="artifact-reaper") as pool:
        with storage._lock(project).acquire():
            future = pool.submit(storage.reap_artifacts, older_than=_OLD + timedelta(days=1))
            assert discovered.wait(timeout=5)
            retained = original_read_json(metadata)
            retained["retention"] = ArtifactRetention.retained.value
            retained["retained_until"] = None
            coding_local._atomic_json(metadata, retained)
        assert future.result(timeout=5).removed == 0
    assert storage.read_artifact(project, run, artifact.content_hash) == b"old"


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


def test_repository_and_snapshot_byte_quotas_are_enforced(tmp_path: Path) -> None:
    tiny_repository = _storage(
        tmp_path / "repository",
        quotas=StorageQuotas(max_repository_bytes=1),
    )
    with pytest.raises(StorageQuotaExceeded, match="quota"):
        tiny_repository.create_project(project_id("alpha"))
    assert not (tiny_repository.projects_root / "alpha.git").exists()

    remote, _git = _remote(tmp_path)
    tiny_import = _storage(
        tmp_path / "import",
        quotas=StorageQuotas(max_repository_bytes=1),
    )
    with pytest.raises(StorageQuotaExceeded, match="quota"):
        tiny_import.import_project(project_id("alpha"), remote)
    assert not (tiny_import.projects_root / "alpha.git").exists()
    assert not list(tiny_import.projects_root.glob(".alpha.import.*"))

    tiny_snapshot = _storage(
        tmp_path / "snapshot",
        quotas=StorageQuotas(max_snapshot_bytes=1),
    )
    tiny_snapshot.import_project(project_id("alpha"), remote)
    with pytest.raises(StorageQuotaExceeded, match="quota"):
        tiny_snapshot.create_snapshot(project_id("alpha"))
    assert list((tiny_snapshot.snapshots_root / "alpha").glob("*.bundle")) == []
    assert list((tiny_snapshot.snapshots_root / "alpha").glob("*.tmp")) == []


def test_bounded_runner_terminates_writer_as_soon_as_quota_is_crossed(
    tmp_path: Path,
) -> None:
    script = tmp_path / "slow_writer.py"
    destination = tmp_path / "bounded-output.bin"
    finished = tmp_path / "finished"
    script.write_text(
        "import os, pathlib, sys, time\n"
        "destination = pathlib.Path(sys.argv[1])\n"
        "with destination.open('wb') as handle:\n"
        "    for _ in range(100):\n"
        "        handle.write(os.urandom(1024))\n"
        "        handle.flush()\n"
        "        os.fsync(handle.fileno())\n"
        "        time.sleep(0.02)\n"
        "pathlib.Path(sys.argv[2]).write_text('finished')\n",
        encoding="utf-8",
    )
    runner = GitRunner(sys.executable, timeout_seconds=10)

    with pytest.raises(StorageQuotaExceeded, match="quota"):
        runner.run_bounded(
            [str(script), str(destination), str(finished)],
            monitored_path=destination,
            max_bytes=4096,
            poll_interval_seconds=0.005,
        )
    assert destination.stat().st_size < 100 * 1024
    assert not finished.exists()


def test_fetch_quota_failure_keeps_authoritative_repository_untouched(
    tmp_path: Path,
) -> None:
    remote, git = _remote(tmp_path)
    storage = _storage(tmp_path)
    project = storage.import_project(project_id("alpha"), remote)
    before = project.head
    current_size = coding_local._tree_size(project.repository_path)
    storage.quotas = StorageQuotas(max_repository_bytes=current_size + 64 * 1024)
    (remote / "large.bin").write_bytes(os.urandom(512 * 1024))
    _run(git, remote, "add", "large.bin")
    _run(
        git,
        remote,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.test",
        "commit",
        "-m",
        "large",
    )

    with pytest.raises(StorageQuotaExceeded, match="quota"):
        storage.fetch(project_id("alpha"), remote)
    assert storage.get_project(project_id("alpha")).head == before
    assert not list(storage.projects_root.glob(".alpha.stage.*"))


def test_failed_fetch_leaves_authoritative_refs_unchanged(tmp_path: Path) -> None:
    remote, git = _remote(tmp_path)
    runner = RecordingGitRunner()
    storage = LocalCodingStorage(
        tmp_path / "coding",
        git=runner,
        allow_local_remotes=True,
    )
    project = storage.import_project(project_id("alpha"), remote)
    before = _run(git, tmp_path, "--git-dir", str(project.repository_path), "show-ref")
    invalid_remote = tmp_path / "invalid-fetch"
    invalid_remote.mkdir()

    with pytest.raises(GitCommandError):
        storage.fetch(project_id("alpha"), invalid_remote)
    after = _run(git, tmp_path, "--git-dir", str(project.repository_path), "show-ref")
    assert after == before
    fetch_calls = [call for call in runner.calls if "fetch" in call]
    assert fetch_calls
    assert all("--atomic" in call for call in fetch_calls)


def test_purge_project_erases_every_on_disk_trace_and_preserves_others(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    storage.create_project(project_id("alpha"))
    storage.create_project(project_id("beta"))
    artifacts = LocalArtifactStore(storage)
    artifacts.put(project_id("alpha"), run_id("run-1"), b"a", name="a.patch")
    artifacts.put(project_id("beta"), run_id("run-1"), b"b", name="b.patch")
    # Residual snapshot + worktree state for alpha (confined under the storage roots).
    for root in (storage.snapshots_root, storage.worktrees_root):
        (root / "alpha").mkdir(parents=True, exist_ok=True)
        (root / "alpha" / "residual").write_text("x", encoding="utf-8")

    alpha_paths = (
        storage.projects_root / "alpha.git",
        storage.snapshots_root / "alpha",
        storage.worktrees_root / "alpha",
        storage.artifacts_root / "alpha",
    )
    assert all(path.exists() for path in alpha_paths)

    assert storage.purge_project(project_id("alpha")) is True
    assert not any(path.exists() for path in alpha_paths)
    # A repeat purge is idempotent: nothing left to remove.
    assert storage.purge_project(project_id("alpha")) is False
    # A purge of an absent project is a no-op, never an error.
    assert storage.purge_project(project_id("never")) is False

    # Cross-project data is untouched.
    assert (storage.projects_root / "beta.git").exists()
    assert (storage.artifacts_root / "beta").exists()


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
