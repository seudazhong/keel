"""Secure local filesystem and system-Git coding storage drivers."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import shutil
import signal
import stat
import subprocess
import threading
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import unquote, urlsplit

from .models import (
    ArtifactRecord,
    ArtifactRetention,
    CodingRunId,
    CodingStorageError,
    InvalidStorageInput,
    ObjectInfo,
    ObjectKey,
    ProjectId,
    ProjectRecord,
    ReapResult,
    SnapshotRecord,
    StorageConflict,
    StorageNotFound,
    StorageQuotaExceeded,
    WorktreeRecord,
    validate_artifact_name,
    validate_git_ref,
)
from .models import (
    project_id as checked_project_id,
)
from .models import (
    run_id as checked_run_id,
)

_SHA256_LENGTH = 64


@dataclass(frozen=True, slots=True)
class StorageQuotas:
    max_artifact_bytes: int = 25 * 1024 * 1024
    max_project_artifact_bytes: int = 250 * 1024 * 1024
    max_repository_bytes: int = 2 * 1024 * 1024 * 1024
    max_snapshot_bytes: int = 2 * 1024 * 1024 * 1024
    max_project_snapshot_bytes: int = 10 * 1024 * 1024 * 1024
    max_snapshots_per_project: int = 100
    max_worktrees_per_project: int = 16

    def __post_init__(self) -> None:
        for value in asdict(self).values():
            if value <= 0:
                raise ValueError("storage quotas must be positive")


class GitCommandError(RuntimeError):
    """A bounded system-Git failure."""

    def __init__(self, args: Sequence[str], stderr: str) -> None:
        command = " ".join(args[:3])
        super().__init__(f"Git command failed ({command}): {stderr[-1000:]}")
        self.args_list = tuple(args)
        self.stderr = stderr


class GitRunner:
    """Small control-plane runner that never invokes a shell."""

    def __init__(self, executable: str = "git", *, timeout_seconds: float = 120.0) -> None:
        if not executable or "\x00" in executable:
            raise ValueError("invalid Git executable")
        self.executable = executable
        self.timeout_seconds = timeout_seconds

    def _command_and_env(
        self, args: Sequence[str], *, extra_env: Mapping[str, str] | None = None
    ) -> tuple[list[str], dict[str, str]]:
        validated: list[str] = []
        for arg in args:
            value = os.fspath(arg)
            if "\x00" in value or "\r" in value or "\n" in value:
                raise InvalidStorageInput("Git arguments may not contain control characters")
            validated.append(value)
        env = {
            "PATH": os.environ.get("PATH", ""),
            "SYSTEMROOT": os.environ.get("SYSTEMROOT", ""),
            "HOME": os.environ.get("HOME", ""),
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_SYSTEM": os.devnull,
            "GIT_ASKPASS": "",
            "SSH_ASKPASS": "",
            "LC_ALL": "C",
        }
        if extra_env:
            # Additional, caller-supplied environment (e.g. GIT_CONFIG_* carrying an auth header).
            # A credential is deliberately passed here — never as a command argument — so it stays
            # out of the process argument list, logs, and on-disk repository config.
            for key, value in extra_env.items():
                if "\x00" in key or "\x00" in value or "\r" in key or "\n" in key:
                    raise InvalidStorageInput("Git environment may not contain control characters")
                env[key] = value
        return [self.executable, *validated], env

    def run(
        self,
        args: Sequence[str],
        *,
        cwd: Path | None = None,
        check: bool = True,
        extra_env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command, env = self._command_and_env(args, extra_env=extra_env)
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=self.timeout_seconds,
            shell=False,
            check=False,
        )
        if check and result.returncode:
            raise GitCommandError(command[1:], result.stderr.strip())
        return result

    def run_bounded(
        self,
        args: Sequence[str],
        *,
        monitored_path: Path,
        max_bytes: int,
        cwd: Path | None = None,
        check: bool = True,
        poll_interval_seconds: float = 0.01,
        extra_env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run Git while continuously enforcing a destination byte ceiling."""

        if max_bytes <= 0:
            raise StorageQuotaExceeded("destination byte quota is exhausted")
        command, env = self._command_and_env(args, extra_env=extra_env)
        popen_kwargs: dict[str, Any] = {}
        if os.name == "nt":
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            popen_kwargs["start_new_session"] = True
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **popen_kwargs,
        )
        stdout = bytearray()
        stderr = bytearray()

        def drain(pipe: Any, target: bytearray) -> None:
            try:
                while chunk := pipe.read(64 * 1024):
                    target.extend(chunk)
                    if len(target) > 1024 * 1024:
                        del target[: len(target) - 1024 * 1024]
            finally:
                pipe.close()

        stdout_thread = threading.Thread(target=drain, args=(process.stdout, stdout), daemon=True)
        stderr_thread = threading.Thread(target=drain, args=(process.stderr, stderr), daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        exceeded = False
        timed_out = False
        monitor_error: Exception | None = None
        deadline = time.monotonic() + self.timeout_seconds
        while process.poll() is None:
            try:
                current_size = _monitored_size(monitored_path, max_bytes=max_bytes)
            except Exception as exc:
                monitor_error = exc
                self._terminate(process)
                break
            if current_size > max_bytes:
                exceeded = True
                self._terminate(process)
                break
            if time.monotonic() >= deadline:
                timed_out = True
                self._terminate(process)
                break
            time.sleep(poll_interval_seconds)
        process.wait()
        stdout_thread.join()
        stderr_thread.join()
        if _monitored_size(monitored_path, max_bytes=max_bytes) > max_bytes:
            exceeded = True
        stdout_text = bytes(stdout).decode("utf-8", errors="replace")
        stderr_text = bytes(stderr).decode("utf-8", errors="replace")
        result = subprocess.CompletedProcess(command, process.returncode, stdout_text, stderr_text)
        if exceeded:
            raise StorageQuotaExceeded("Git destination exceeded its byte quota")
        if timed_out:
            raise subprocess.TimeoutExpired(command, self.timeout_seconds)
        if monitor_error is not None:
            raise monitor_error
        if check and result.returncode:
            raise GitCommandError(command[1:], stderr_text.strip())
        return result

    @staticmethod
    def _terminate(process: subprocess.Popen[bytes]) -> None:
        if process.poll() is not None:
            return
        try:
            if os.name == "nt":
                control_break = getattr(signal, "CTRL_BREAK_EVENT", None)
                if control_break is not None:
                    process.send_signal(control_break)
                else:
                    process.kill()
            else:
                posix_os = importlib.import_module("os")
                posix_os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=2)
        except (OSError, subprocess.TimeoutExpired):
            if process.poll() is None:
                if os.name == "nt":
                    process.kill()
                else:
                    posix_os = importlib.import_module("os")
                    posix_os.killpg(process.pid, getattr(signal, "SIGKILL", signal.SIGTERM))
                process.wait()


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise InvalidStorageInput("retention timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hash_file(path: Path, *, max_bytes: int | None = None) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            size += len(chunk)
            if max_bytes is not None and size > max_bytes:
                raise StorageQuotaExceeded("file exceeds the configured size limit")
            digest.update(chunk)
    return digest.hexdigest(), size


def _monitored_size(path: Path, *, max_bytes: int) -> int:
    try:
        if path.is_file():
            return path.stat().st_size
        if not path.exists():
            return 0
    except OSError:
        return 0
    size = 0
    pending = [path]
    while pending:
        directory = pending.pop()
        try:
            entries = list(os.scandir(directory))
        except (FileNotFoundError, NotADirectoryError):
            continue
        for entry in entries:
            try:
                if entry.is_symlink():
                    raise InvalidStorageInput(
                        "symlinks are not allowed in monitored Git destinations"
                    )
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    size += entry.stat(follow_symlinks=False).st_size
                    if size > max_bytes:
                        return size
            except FileNotFoundError:
                continue
    return size


def _tree_size(path: Path, *, max_bytes: int | None = None) -> int:
    size = 0
    for item in path.rglob("*"):
        if item.is_symlink():
            raise InvalidStorageInput("symlinks are not allowed in managed repositories")
        if item.is_file():
            size += item.stat().st_size
            if max_bytes is not None and size > max_bytes:
                raise StorageQuotaExceeded("repository exceeds the configured size limit")
    return size


def _validate_hash(value: str) -> str:
    if len(value) != _SHA256_LENGTH or any(c not in "0123456789abcdef" for c in value):
        raise InvalidStorageInput("content hash must be a lowercase SHA-256 digest")
    return value


def _validate_commit_sha(value: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 40 or any(c not in "0123456789abcdef" for c in normalized):
        raise InvalidStorageInput("commit sha must be a 40-character lowercase hex object name")
    return normalized


def _safe_child(base: Path, *parts: str) -> Path:
    candidate = base.joinpath(*parts)
    resolved_base = base.resolve()
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(resolved_base)
    except ValueError as exc:
        raise InvalidStorageInput("path escapes its storage scope") from exc
    current = resolved_base
    for part in parts:
        current /= part
        if current.is_symlink():
            raise InvalidStorageInput("symlinks are not allowed in managed storage paths")
    return resolved


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_bytes(
        path,
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )


def _remove_tree(path: Path, *, ignore_errors: bool = False) -> None:
    if not path.exists():
        return
    if path.is_symlink():
        raise InvalidStorageInput("refusing to recursively remove a symlink")

    def make_writable_and_retry(function: Any, target: str, _error: BaseException) -> None:
        os.chmod(target, stat.S_IWRITE)
        function(target)

    try:
        shutil.rmtree(path, onexc=make_writable_and_retry)
    except OSError:
        if not ignore_errors:
            raise


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("metadata must be an object")
    return value


class _ProjectLock:
    def __init__(self, path: Path, timeout_seconds: float) -> None:
        self.path = path
        self.timeout_seconds = timeout_seconds

    @contextmanager
    def acquire(self) -> Iterator[None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        deadline = time.monotonic() + self.timeout_seconds
        try:
            while True:
                try:
                    if os.name == "nt":
                        import msvcrt

                        if self.path.stat().st_size == 0:
                            handle.write(b"\0")
                            handle.flush()
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        fcntl = importlib.import_module("fcntl")
                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError as exc:
                    if time.monotonic() >= deadline:
                        raise StorageConflict(
                            f"timed out acquiring project lock: {self.path.stem}"
                        ) from exc
                    time.sleep(0.01)
            yield
        finally:
            try:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl = importlib.import_module("fcntl")
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()


class LocalObjectStore:
    """Filesystem implementation of the object-store seam."""

    def __init__(self, root: Path, *, max_object_bytes: int = 100 * 1024 * 1024) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_object_bytes = max_object_bytes

    def _path(self, key: ObjectKey) -> Path:
        raw = str(key)
        pure = PurePosixPath(raw)
        if (
            not raw
            or pure.is_absolute()
            or any(part in {"", ".", ".."} for part in pure.parts)
            or "\\" in raw
        ):
            raise InvalidStorageInput("object key must be a confined relative POSIX path")
        return _safe_child(self.root, *pure.parts)

    def put(self, key: ObjectKey, data: bytes) -> ObjectInfo:
        if len(data) > self.max_object_bytes:
            raise StorageQuotaExceeded("object exceeds the configured size limit")
        path = self._path(key)
        _atomic_bytes(path, data)
        info = self.stat(key)
        if info is None:
            raise StorageConflict("object disappeared after atomic write")
        return info

    def get(self, key: ObjectKey) -> bytes:
        path = self._path(key)
        if not path.is_file() or path.is_symlink():
            raise StorageNotFound(str(key))
        return path.read_bytes()

    def stat(self, key: ObjectKey) -> ObjectInfo | None:
        path = self._path(key)
        if not path.is_file() or path.is_symlink():
            return None
        data = path.read_bytes()
        modified = datetime.fromtimestamp(path.stat().st_mtime, UTC)
        return ObjectInfo(key, len(data), _sha256(data), modified)

    def delete(self, key: ObjectKey) -> bool:
        path = self._path(key)
        if not path.exists():
            return False
        if not path.is_file() or path.is_symlink():
            raise InvalidStorageInput("refusing to delete a non-file object")
        path.unlink()
        return True

    def list(self, prefix: ObjectKey) -> Iterable[ObjectInfo]:
        prefix_path = self._path(prefix)
        if not prefix_path.exists():
            return ()
        found: list[ObjectInfo] = []
        for path in prefix_path.rglob("*"):
            if path.is_file() and not path.is_symlink():
                key = ObjectKey(path.relative_to(self.root).as_posix())
                info = self.stat(key)
                if info is not None:
                    found.append(info)
        return tuple(sorted(found, key=lambda item: str(item.key)))


class LocalCodingStorage:
    """Local/POSIX implementation of all coding storage protocols."""

    def __init__(
        self,
        root: Path,
        *,
        git: GitRunner | None = None,
        allow_local_remotes: bool = False,
        allowed_https_hosts: Iterable[str] = (),
        quotas: StorageQuotas | None = None,
        lock_timeout_seconds: float = 30.0,
    ) -> None:
        self.root = root.resolve()
        self.projects_root = self.root / "projects"
        self.snapshots_root = self.root / "snapshots"
        self.worktrees_root = self.root / "worktrees"
        self.artifacts_root = self.root / "artifacts"
        self.locks_root = self.root / "locks"
        for directory in (
            self.projects_root,
            self.snapshots_root,
            self.worktrees_root,
            self.artifacts_root,
            self.locks_root,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        self.git = git or GitRunner()
        self.allow_local_remotes = allow_local_remotes
        self.allowed_https_hosts = frozenset(host.lower() for host in allowed_https_hosts)
        self.quotas = quotas or StorageQuotas()
        self.lock_timeout_seconds = lock_timeout_seconds

    def _project(self, value: ProjectId) -> ProjectId:
        return checked_project_id(str(value))

    def _run(self, value: CodingRunId) -> CodingRunId:
        return checked_run_id(str(value))

    def _repo(self, value: ProjectId) -> Path:
        project = self._project(value)
        return _safe_child(self.projects_root, f"{project}.git")

    def _lock(self, value: ProjectId) -> _ProjectLock:
        project = self._project(value)
        return _ProjectLock(
            _safe_child(self.locks_root, f"{project}.lock"),
            self.lock_timeout_seconds,
        )

    def _require_repo(self, value: ProjectId) -> Path:
        repo = self._repo(value)
        if not repo.is_dir() or repo.is_symlink():
            raise StorageNotFound(f"project not found: {value}")
        return repo

    def purge_project(self, project_id: ProjectId) -> bool:
        """Erase every on-disk trace of one project (repo, snapshots, worktrees, artifacts).

        Path-confined via ``_safe_child`` (never escapes the storage root) and serialized
        under the project lock. Idempotent: returns True if anything was removed, False if
        the project already had no on-disk state. Used by scoped/project data erasure.
        """
        project = self._project(project_id)
        removed = False
        with self._lock(project).acquire():
            targets = (
                _safe_child(self.projects_root, f"{project}.git"),
                _safe_child(self.snapshots_root, str(project)),
                _safe_child(self.worktrees_root, str(project)),
                _safe_child(self.artifacts_root, str(project)),
            )
            for path in targets:
                if path.exists():
                    _remove_tree(path, ignore_errors=True)
                    removed = True
        return removed

    def _git_dir(
        self, repo: Path, *args: str, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return self.git.run(["--git-dir", str(repo), *args], check=check)

    def _head(self, repo: Path) -> str | None:
        result = self._git_dir(repo, "rev-parse", "--verify", "HEAD", check=False)
        return result.stdout.strip() if result.returncode == 0 else None

    def _head_target(self, repo: Path) -> str | None:
        result = self._git_dir(repo, "symbolic-ref", "-q", "HEAD", check=False)
        return result.stdout.strip() if result.returncode == 0 else None

    def _set_head(self, repo: Path, target: str | None, commit: str | None) -> None:
        if target is not None:
            validate_git_ref(target)
            self._git_dir(repo, "symbolic-ref", "HEAD", target)
        elif commit is not None:
            self._git_dir(repo, "update-ref", "--no-deref", "HEAD", commit)

    def _list_refs(self, repo: Path) -> dict[str, str]:
        result = self._git_dir(repo, "for-each-ref", "--format=%(refname)\t%(objectname)")
        refs: dict[str, str] = {}
        for line in result.stdout.splitlines():
            name, separator, object_id = line.partition("\t")
            if not separator or not name.startswith("refs/"):
                raise StorageConflict("Git returned an invalid reference listing")
            refs[name] = object_id
        return refs

    def _init_bare(self, path: Path, *, default_branch: str) -> None:
        branch = validate_git_ref(default_branch)
        if branch == "HEAD" or branch.startswith("refs/"):
            raise InvalidStorageInput("default_branch must be an unqualified branch name")
        self.git.run_bounded(
            ["init", "--bare", f"--initial-branch={branch}", str(path)],
            monitored_path=path,
            max_bytes=self.quotas.max_repository_bytes,
        )

    def _atomic_fetch(self, repo: Path, remote: str) -> None:
        if (
            _monitored_size(repo, max_bytes=self.quotas.max_repository_bytes)
            > self.quotas.max_repository_bytes
        ):
            raise StorageQuotaExceeded("repository exceeds the configured size limit")
        self.git.run_bounded(
            [
                "--git-dir",
                str(repo),
                "-c",
                "protocol.file.allow=always",
                "-c",
                "http.followRedirects=false",
                "fetch",
                "--atomic",
                "--prune",
                "--force",
                remote,
                "+refs/*:refs/*",
            ],
            monitored_path=repo,
            max_bytes=self.quotas.max_repository_bytes,
        )

    def _set_default_head(self, repo: Path, *candidates: str) -> None:
        if self._head(repo) is not None:
            return
        for branch in candidates:
            probe = self._git_dir(repo, "show-ref", "--verify", f"refs/heads/{branch}", check=False)
            if probe.returncode == 0:
                self._git_dir(repo, "symbolic-ref", "HEAD", f"refs/heads/{branch}")
                return

    def _verify_repository_quota(self, repo: Path) -> None:
        _tree_size(repo, max_bytes=self.quotas.max_repository_bytes)

    def _stage_repository(self, project: ProjectId, source: Path) -> Path:
        temporary = _safe_child(self.projects_root, f".{project}.stage.{uuid.uuid4().hex}.tmp")
        self._init_bare(temporary, default_branch="main")
        try:
            if self._list_refs(source):
                self._atomic_fetch(temporary, str(source))
            self._set_head(temporary, self._head_target(source), self._head(source))
            return temporary
        except Exception:
            _remove_tree(temporary, ignore_errors=True)
            raise

    def _replace_repository(self, project: ProjectId, repo: Path, staged: Path) -> None:
        backup = _safe_child(self.projects_root, f".{project}.backup.{uuid.uuid4().hex}")
        try:
            os.replace(repo, backup)
            try:
                os.replace(staged, repo)
            except Exception:
                os.replace(backup, repo)
                raise
            _fsync_directory(self.projects_root)
        finally:
            _remove_tree(staged, ignore_errors=True)
            if repo.exists():
                _remove_tree(backup, ignore_errors=True)

    def _active_workspaces(self, project: ProjectId) -> list[Path]:
        root = _safe_child(self.worktrees_root, str(project))
        if not root.exists():
            return []
        return [path for path in root.iterdir() if path.is_dir() and not path.is_symlink()]

    def _workspace_marker(self, project: ProjectId, run: CodingRunId) -> Path:
        return _safe_child(self.worktrees_root, str(project), f".{run}.json")

    def _record(self, project: ProjectId, repo: Path) -> ProjectRecord:
        return ProjectRecord(project, repo, self._head(repo))

    def _validated_remote(self, remote: str | Path) -> str:
        raw = os.fspath(remote)
        if not raw or "\x00" in raw or "\r" in raw or "\n" in raw:
            raise InvalidStorageInput("invalid Git remote")
        if isinstance(remote, Path) or Path(raw).is_absolute():
            if not self.allow_local_remotes:
                raise InvalidStorageInput("local remotes are disabled")
            if os.name == "nt" and raw.startswith(("\\\\", "//")):
                raise InvalidStorageInput("network-share remotes are not local test remotes")
            resolved_path = Path(raw).resolve(strict=True)
            if not resolved_path.is_dir() and not resolved_path.is_file():
                raise InvalidStorageInput("local remote does not exist")
            return str(resolved_path)
        parsed = urlsplit(raw)
        if parsed.scheme.lower() == "https":
            if parsed.username or parsed.password or not parsed.hostname:
                raise InvalidStorageInput("HTTPS remotes may not contain credentials")
            try:
                port = parsed.port
            except ValueError as exc:
                raise InvalidStorageInput("HTTPS remote has an invalid port") from exc
            if parsed.query or parsed.fragment or port not in {None, 443}:
                raise InvalidStorageInput(
                    "HTTPS remotes may not contain query credentials or ports"
                )
            if parsed.hostname.lower() not in self.allowed_https_hosts:
                raise InvalidStorageInput("HTTPS remote host is not allow-listed")
            return raw
        if parsed.scheme.lower() == "file":
            if not self.allow_local_remotes or parsed.netloc not in {"", "localhost"}:
                raise InvalidStorageInput("local/file remotes are disabled or non-local")
            local = Path(unquote(parsed.path))
            if os.name == "nt" and local.as_posix().startswith("/") and len(local.as_posix()) > 3:
                local = Path(local.as_posix()[1:])
            resolved = local.resolve(strict=True)
            if not resolved.is_dir() and not resolved.is_file():
                raise InvalidStorageInput("local remote does not exist")
            return resolved.as_uri()
        if parsed.scheme:
            raise InvalidStorageInput(
                "only allow-listed HTTPS and explicit local/file remotes are allowed"
            )
        if not self.allow_local_remotes:
            raise InvalidStorageInput("local remotes are disabled")
        resolved = Path(raw).resolve(strict=True)
        if not resolved.is_dir() and not resolved.is_file():
            raise InvalidStorageInput("local remote does not exist")
        return str(resolved)

    def create_project(
        self, project_id: ProjectId, *, default_branch: str = "main"
    ) -> ProjectRecord:
        project = self._project(project_id)
        repo = self._repo(project)
        with self._lock(project).acquire():
            if repo.exists():
                raise StorageConflict(f"project already exists: {project}")
            temporary = _safe_child(self.projects_root, f".{project}.{uuid.uuid4().hex}.tmp")
            try:
                self._init_bare(temporary, default_branch=default_branch)
                self._verify_repository_quota(temporary)
                os.replace(temporary, repo)
                _fsync_directory(self.projects_root)
            finally:
                _remove_tree(temporary, ignore_errors=True)
        return self._record(project, repo)

    def import_project(
        self,
        project_id: ProjectId,
        remote: str | Path,
        *,
        default_branch: str = "main",
    ) -> ProjectRecord:
        project = self._project(project_id)
        safe_remote = self._validated_remote(remote)
        repo = self._repo(project)
        with self._lock(project).acquire():
            if repo.exists():
                raise StorageConflict(f"project already exists: {project}")
            temporary = _safe_child(self.projects_root, f".{project}.import.{uuid.uuid4().hex}.tmp")
            try:
                self._init_bare(temporary, default_branch=default_branch)
                self._atomic_fetch(temporary, safe_remote)
                self._set_default_head(temporary, default_branch, "main", "master")
                self._verify_repository_quota(temporary)
                os.replace(temporary, repo)
                _fsync_directory(self.projects_root)
            finally:
                _remove_tree(temporary, ignore_errors=True)
            return self._record(project, repo)

    def fetch(self, project_id: ProjectId, remote: str | Path) -> ProjectRecord:
        project = self._project(project_id)
        safe_remote = self._validated_remote(remote)
        with self._lock(project).acquire():
            repo = self._require_repo(project)
            staged = self._stage_repository(project, repo)
            try:
                self._atomic_fetch(staged, safe_remote)
                self._set_default_head(staged, "main", "master")
                self._verify_repository_quota(staged)
                self._replace_repository(project, repo, staged)
            except Exception:
                _remove_tree(staged, ignore_errors=True)
                raise
            return self._record(project, repo)

    def get_project(self, project_id: ProjectId) -> ProjectRecord:
        project = self._project(project_id)
        return self._record(project, self._require_repo(project))

    def fetch_commits(
        self,
        project_id: ProjectId,
        remote: str | Path,
        commit_shas: Sequence[str],
        *,
        auth_header: str | None = None,
    ) -> None:
        """Fetch specific commit SHAs into the authoritative repo, pinned under review refs.

        The read-only review PR path uses this to make a PR's exact base/head commits
        materializable **without** ever treating a PR number as a Git ref. The optional
        ``auth_header`` (an ``Authorization: <scheme> <credential>`` value) is handed to Git only
        through the environment (``GIT_CONFIG_*`` → ``http.extraHeader``), so a JIT installation
        token never appears in a command argument, in on-disk repository config, or in a log
        line. Each requested object is confirmed present afterwards (fail closed if the server
        withheld it). The remote URL is validated/allow-listed (SSRF defense) and redirects are
        refused.
        """
        shas = [_validate_commit_sha(sha) for sha in commit_shas]
        if not shas:
            return
        safe_remote = self._validated_remote(remote)
        extra_env = self._auth_header_env(auth_header) if auth_header else None
        with self._lock(project_id).acquire():
            repo = self._require_repo(project_id)
            refspecs = [f"{sha}:refs/keel-review/{sha}" for sha in shas]
            self.git.run_bounded(
                [
                    "--git-dir",
                    str(repo),
                    "-c",
                    "protocol.file.allow=always",
                    "-c",
                    "protocol.version=2",
                    "-c",
                    "http.followRedirects=false",
                    "fetch",
                    "--no-tags",
                    "--no-write-fetch-head",
                    "--force",
                    safe_remote,
                    *refspecs,
                ],
                monitored_path=repo,
                max_bytes=self.quotas.max_repository_bytes,
                extra_env=extra_env,
            )
            for sha in shas:
                probe = self.git.run(
                    ["--git-dir", str(repo), "cat-file", "-e", f"{sha}^{{commit}}"],
                    check=False,
                )
                if probe.returncode != 0:
                    raise StorageConflict(f"requested commit was not fetched from remote: {sha}")

    @staticmethod
    def _auth_header_env(auth_header: str) -> dict[str, str]:
        """Env carrying an ``http.extraHeader`` so a credential never enters argv/config/logs."""
        header = auth_header.strip()
        if not header or "\r" in header or "\n" in header or "\x00" in header:
            raise InvalidStorageInput("invalid authorization header")
        name, separator, value = header.partition(":")
        if separator != ":" or name.strip().lower() != "authorization" or not value.strip():
            raise InvalidStorageInput("authorization header name and value are required")
        header = f"Authorization: {value.strip()}"
        return {
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "http.extraHeader",
            "GIT_CONFIG_VALUE_0": header,
        }

    def create_snapshot(self, project_id: ProjectId) -> SnapshotRecord:
        project = self._project(project_id)
        with self._lock(project).acquire():
            repo = self._require_repo(project)
            commit = self._head(repo)
            if commit is None:
                raise StorageConflict("cannot snapshot an empty repository")
            project_root = _safe_child(self.snapshots_root, str(project))
            project_root.mkdir(parents=True, exist_ok=True)
            existing = list(project_root.glob("*.json"))
            snapshot_usage = sum(
                path.stat().st_size
                for path in project_root.glob("*.bundle")
                if path.is_file() and not path.is_symlink()
            )
            remaining_project_bytes = self.quotas.max_project_snapshot_bytes - snapshot_usage
            bundle_limit = min(self.quotas.max_snapshot_bytes, remaining_project_bytes)
            if bundle_limit <= 0:
                raise StorageQuotaExceeded("project snapshot byte quota exceeded")
            temporary = project_root / f".{uuid.uuid4().hex}.bundle.tmp"
            try:
                self.git.run_bounded(
                    [
                        "--git-dir",
                        str(repo),
                        "bundle",
                        "create",
                        str(temporary),
                        "--all",
                    ],
                    monitored_path=temporary,
                    max_bytes=bundle_limit,
                )
                snapshot_id, bundle_size = _hash_file(temporary, max_bytes=bundle_limit)
                bundle = _safe_child(project_root, f"{snapshot_id}.bundle")
                metadata = _safe_child(project_root, f"{snapshot_id}.json")
                if metadata.exists():
                    existing_record = self._snapshot_record(project, metadata)
                    existing_digest, existing_size = _hash_file(
                        existing_record.bundle_path,
                        max_bytes=self.quotas.max_snapshot_bytes,
                    )
                    if existing_digest != snapshot_id or existing_size != bundle_size:
                        raise StorageConflict("existing snapshot bundle verification failed")
                    return existing_record
                if len(existing) >= self.quotas.max_snapshots_per_project:
                    raise StorageQuotaExceeded("project snapshot quota exceeded")
                if snapshot_usage + bundle_size > self.quotas.max_project_snapshot_bytes:
                    raise StorageQuotaExceeded("project snapshot byte quota exceeded")
                created = _utc_now()
                if not bundle.exists():
                    os.replace(temporary, bundle)
                    _fsync_directory(project_root)
                _atomic_json(
                    metadata,
                    {
                        "project_id": str(project),
                        "snapshot_id": snapshot_id,
                        "commit": commit,
                        "head_target": self._head_target(repo),
                        "refs": self._list_refs(repo),
                        "size_bytes": bundle_size,
                        "created_at": created.isoformat(),
                    },
                )
                return SnapshotRecord(project, snapshot_id, commit, bundle_size, created, bundle)
            finally:
                temporary.unlink(missing_ok=True)

    def _snapshot_record(self, project: ProjectId, metadata_path: Path) -> SnapshotRecord:
        value = _read_json(metadata_path)
        if value.get("project_id") != str(project):
            raise InvalidStorageInput("snapshot metadata crosses project scope")
        snapshot_id = _validate_hash(str(value["snapshot_id"]))
        bundle = _safe_child(metadata_path.parent, f"{snapshot_id}.bundle")
        if not bundle.is_file() or bundle.is_symlink():
            raise StorageNotFound(f"snapshot bundle missing: {snapshot_id}")
        return SnapshotRecord(
            project,
            snapshot_id,
            str(value["commit"]),
            int(value["size_bytes"]),
            _parse_datetime(str(value["created_at"])),
            bundle,
        )

    def list_snapshots(self, project_id: ProjectId) -> list[SnapshotRecord]:
        project = self._project(project_id)
        root = _safe_child(self.snapshots_root, str(project))
        if not root.exists():
            return []
        records = [self._snapshot_record(project, path) for path in root.glob("*.json")]
        return sorted(records, key=lambda item: item.created_at)

    def restore_snapshot(self, project_id: ProjectId, snapshot_id: str) -> ProjectRecord:
        project = self._project(project_id)
        digest = _validate_hash(snapshot_id)
        metadata = _safe_child(self.snapshots_root, str(project), f"{digest}.json")
        if not metadata.is_file() or metadata.is_symlink():
            raise StorageNotFound(f"snapshot not found: {digest}")
        with self._lock(project).acquire():
            repo = self._require_repo(project)
            if self._active_workspaces(project):
                raise StorageConflict("cannot restore while active workspaces exist")
            snapshot = self._snapshot_record(project, metadata)
            actual_digest, actual_size = _hash_file(
                snapshot.bundle_path, max_bytes=self.quotas.max_snapshot_bytes
            )
            if actual_digest != digest or actual_size != snapshot.size_bytes:
                raise StorageConflict("snapshot bundle hash verification failed")
            value = _read_json(metadata)
            refs_value = value.get("refs")
            if not isinstance(refs_value, dict) or any(
                not isinstance(name, str) or not isinstance(object_id, str)
                for name, object_id in refs_value.items()
            ):
                raise StorageConflict("snapshot reference metadata is invalid")
            expected_refs = dict(refs_value)
            head_target = value.get("head_target")
            if head_target is not None and not isinstance(head_target, str):
                raise StorageConflict("snapshot HEAD metadata is invalid")
            temporary = _safe_child(self.projects_root, f".{project}.restore.{uuid.uuid4().hex}")
            try:
                self._init_bare(temporary, default_branch="main")
                self._atomic_fetch(temporary, str(snapshot.bundle_path))
                self._set_head(temporary, head_target, snapshot.commit)
                restored = self._head(temporary)
                if restored != snapshot.commit:
                    raise StorageConflict("restored snapshot HEAD does not match metadata")
                if self._list_refs(temporary) != expected_refs:
                    raise StorageConflict("restored snapshot reference set does not match metadata")
                self._verify_repository_quota(temporary)
                self._replace_repository(project, repo, temporary)
            finally:
                _remove_tree(temporary, ignore_errors=True)
            return self._record(project, repo)

    def materialize(
        self,
        project_id: ProjectId,
        run_id: CodingRunId,
        *,
        ref: str = "HEAD",
    ) -> WorktreeRecord:
        project = self._project(project_id)
        run = self._run(run_id)
        safe_ref = validate_git_ref(ref)
        path = _safe_child(self.worktrees_root, str(project), str(run))
        with self._lock(project).acquire():
            repo = self._require_repo(project)
            parent = path.parent
            parent.mkdir(parents=True, exist_ok=True)
            active = [item for item in parent.iterdir() if item.is_dir()]
            if len(active) >= self.quotas.max_worktrees_per_project:
                raise StorageQuotaExceeded("project worktree quota exceeded")
            if path.exists():
                raise StorageConflict(f"worktree already exists: {run}")
            try:
                resolved = self._git_dir(repo, "rev-parse", "--verify", f"{safe_ref}^{{commit}}")
                commit = resolved.stdout.strip()
                self.git.run(["init", "--initial-branch=keel-workspace", str(path)])
                self.git.run(
                    [
                        "-c",
                        "protocol.file.allow=always",
                        "fetch",
                        "--atomic",
                        "--force",
                        str(repo),
                        "+refs/*:refs/*",
                    ],
                    cwd=path,
                )
                self.git.run(["checkout", "--detach", commit], cwd=path)
                created = _utc_now()
                _atomic_json(
                    self._workspace_marker(project, run),
                    {
                        "project_id": str(project),
                        "run_id": str(run),
                        "workspace_id": uuid.uuid4().hex,
                        "commit": commit,
                        "created_at": created.isoformat(),
                    },
                )
                return WorktreeRecord(project, run, commit, path, created)
            except Exception:
                _remove_tree(path, ignore_errors=True)
                raise

    def remove(self, project_id: ProjectId, run_id: CodingRunId) -> bool:
        project = self._project(project_id)
        run = self._run(run_id)
        path = _safe_child(self.worktrees_root, str(project), str(run))
        marker = self._workspace_marker(project, run)
        with self._lock(project).acquire():
            existed = path.exists()
            if path.exists():
                if path.is_symlink():
                    raise InvalidStorageInput("refusing to remove symlinked worktree")
                _remove_tree(path)
            marker.unlink(missing_ok=True)
            return existed

    # --- Control-plane patch-writeback plumbing (WS-PP) ------------------------------
    def resolve_commit(self, project_id: ProjectId, ref: str) -> str:
        """Resolve a ref/sha to an exact commit sha in the authoritative repo (control plane)."""
        safe_ref = validate_git_ref(ref)
        with self._lock(project_id).acquire():
            repo = self._require_repo(project_id)
            resolved = self._git_dir(repo, "rev-parse", "--verify", f"{safe_ref}^{{commit}}")
        return resolved.stdout.strip()

    def commit_tree(self, project_id: ProjectId, commit_sha: str) -> str:
        """The tree sha of a commit — used to verify a writeback reproduces the approved tree."""
        sha = _validate_commit_sha(commit_sha)
        with self._lock(project_id).acquire():
            repo = self._require_repo(project_id)
            resolved = self._git_dir(repo, "rev-parse", "--verify", f"{sha}^{{tree}}")
        return resolved.stdout.strip()

    def ingest_worktree_commit(
        self,
        project_id: ProjectId,
        run_id: CodingRunId,
        commit_sha: str,
        *,
        pin_ref: str,
    ) -> None:
        """Persist a commit produced in a disposable worktree into the authoritative repo.

        The generation worktree is anonymous and disposable; the exact proposed commit must
        survive its removal so the human-approved writeback can push *exactly* that commit. This
        pushes the commit object graph from the worktree into the authoritative bare repo under a
        pinned ``refs/keel-patch/<...>`` ref (so it is never garbage-collected before writeback).
        The pinned ref is control-plane only and never exposed to the sandbox or a remote.
        """
        sha = _validate_commit_sha(commit_sha)
        if not pin_ref.startswith("refs/keel-patch/"):
            raise InvalidStorageInput("pin_ref must be in the refs/keel-patch/ namespace")
        safe_pin = validate_git_ref(pin_ref)
        worktree = _safe_child(self.worktrees_root, str(project_id), str(run_id))
        with self._lock(project_id).acquire():
            repo = self._require_repo(project_id)
            if not worktree.is_dir() or worktree.is_symlink():
                raise StorageConflict("worktree is not available to ingest from")
            self.git.run(
                [
                    "-c",
                    "protocol.file.allow=always",
                    "push",
                    "--force",
                    str(repo),
                    f"{sha}:{safe_pin}",
                ],
                cwd=worktree,
            )
            probe = self._git_dir(repo, "cat-file", "-e", f"{sha}^{{commit}}", check=False)
            if probe.returncode != 0:
                raise StorageConflict("commit was not ingested into the authoritative repo")

    def unpin_commit(self, project_id: ProjectId, pin_ref: str) -> None:
        """Delete a control-plane pin ref (best-effort cleanup after a terminal proposal)."""
        if not pin_ref.startswith("refs/keel-patch/"):
            raise InvalidStorageInput("pin_ref must be in the refs/keel-patch/ namespace")
        safe_pin = validate_git_ref(pin_ref)
        with self._lock(project_id).acquire():
            repo = self._require_repo(project_id)
            self._git_dir(repo, "update-ref", "-d", safe_pin, check=False)

    def push_commit(
        self,
        project_id: ProjectId,
        remote: str | Path,
        commit_sha: str,
        *,
        target_branch: str,
        auth_header: str | None = None,
    ) -> None:
        """Push exactly one commit to a **dedicated** remote branch (no force, no tags).

        A single explicit refspec ``<sha>:refs/heads/<branch>`` is pushed — never a wildcard,
        never ``--force``, never a tag or ref deletion, and the caller must have validated
        ``target_branch`` is a dedicated run branch (never the default branch). The optional
        ``auth_header`` (a JIT installation token as ``Authorization: <scheme> <credential>``) is
        handed to Git only through the environment (``GIT_CONFIG_*`` -> ``http.extraHeader``) so the
        token never appears in a command argument, on-disk config, or a log line. The remote URL is
        validated/allow-listed (SSRF defense) and redirects are refused. GitHub rejects a
        non-fast-forward push to an existing branch, so this can never clobber another branch.
        """
        sha = _validate_commit_sha(commit_sha)
        safe_target = validate_git_ref(target_branch)
        if safe_target == "HEAD" or safe_target.startswith("refs/tags/"):
            raise InvalidStorageInput("push target must be a branch, never a tag or HEAD")
        safe_remote = self._validated_remote(remote)
        extra_env = self._auth_header_env(auth_header) if auth_header else None
        refspec = f"{sha}:refs/heads/{safe_target}"
        with self._lock(project_id).acquire():
            repo = self._require_repo(project_id)
            self.git.run(
                [
                    "--git-dir",
                    str(repo),
                    "-c",
                    "protocol.version=2",
                    "-c",
                    "http.followRedirects=false",
                    "push",
                    "--atomic",
                    "--no-tags",
                    safe_remote,
                    refspec,
                ],
                extra_env=extra_env,
            )

    def commit_worktree(
        self,
        project_id: ProjectId,
        run_id: CodingRunId,
        *,
        message: str,
        author_name: str = "Keel Patch Bot",
        author_email: str = "patch-bot@keel.invalid",
    ) -> str | None:
        """Stage all changes in a generation worktree and record one deterministic commit.

        Returns the new commit sha, or ``None`` when the working tree has no changes (an empty
        proposal is never committed). Author/committer identity and dates are pinned so an
        identical tree yields an identical commit sha (content-addressed, reproducible). Runs
        entirely on the control plane over the disposable worktree; the sandbox never commits.
        """
        worktree = _safe_child(self.worktrees_root, str(project_id), str(run_id))
        with self._lock(project_id).acquire():
            if not worktree.is_dir() or worktree.is_symlink():
                raise StorageConflict("generation worktree is not available")
            self.git.run(["add", "-A"], cwd=worktree)
            status = self.git.run(["status", "--porcelain"], cwd=worktree)
            if not status.stdout.strip():
                return None
            commit_env = {
                "GIT_AUTHOR_NAME": author_name,
                "GIT_AUTHOR_EMAIL": author_email,
                "GIT_COMMITTER_NAME": author_name,
                "GIT_COMMITTER_EMAIL": author_email,
                "GIT_AUTHOR_DATE": "2000-01-01T00:00:00+00:00",
                "GIT_COMMITTER_DATE": "2000-01-01T00:00:00+00:00",
            }
            self.git.run(
                ["commit", "--no-verify", "--no-gpg-sign", "-m", message[:4000]],
                cwd=worktree,
                extra_env=commit_env,
            )
            head = self.git.run(["rev-parse", "--verify", "HEAD^{commit}"], cwd=worktree)
        return head.stdout.strip()

    def worktree_head_tree(
        self, project_id: ProjectId, run_id: CodingRunId, commit_sha: str
    ) -> str:
        sha = _validate_commit_sha(commit_sha)
        worktree = _safe_child(self.worktrees_root, str(project_id), str(run_id))
        with self._lock(project_id).acquire():
            resolved = self.git.run(["rev-parse", "--verify", f"{sha}^{{tree}}"], cwd=worktree)
        return resolved.stdout.strip()

    def worktree_changed_files(
        self,
        project_id: ProjectId,
        run_id: CodingRunId,
        base_sha: str,
        head_sha: str,
    ) -> list[dict[str, Any]]:
        """The changed (path, status, blob sha, size) rows between two commits in a worktree.

        Uses ``git diff --raw`` so the exact new-blob sha of every changed path is captured
        without re-hashing, plus ``cat-file`` for the blob size. A rename carries its old path.
        """
        base = _validate_commit_sha(base_sha)
        head = _validate_commit_sha(head_sha)
        worktree = _safe_child(self.worktrees_root, str(project_id), str(run_id))
        rows: list[dict[str, Any]] = []
        with self._lock(project_id).acquire():
            raw = self.git.run(
                [
                    "diff",
                    "--raw",
                    "--abbrev=40",
                    "--no-color",
                    "-M",
                    "-z",
                    base,
                    head,
                ],
                cwd=worktree,
            ).stdout
            fields = raw.split("\x00")
            i = 0
            while i < len(fields):
                meta = fields[i]
                if not meta.startswith(":"):
                    i += 1
                    continue
                parts = meta.split(" ")
                # :old_mode new_mode old_sha new_sha status
                new_sha = parts[3]
                status = parts[4]
                code = status[0]
                if code in ("R", "C"):
                    old_path = fields[i + 1]
                    new_path = fields[i + 2]
                    i += 3
                else:
                    new_path = fields[i + 1]
                    old_path = ""
                    i += 2
                size = 0
                if code != "D" and new_sha and set(new_sha) != {"0"}:
                    probe = self.git.run(["cat-file", "-s", new_sha], cwd=worktree, check=False)
                    if probe.returncode == 0:
                        size = int(probe.stdout.strip() or "0")
                rows.append(
                    {
                        "status": code,
                        "path": new_path,
                        "old_path": old_path,
                        "blob_sha": "" if code == "D" else new_sha,
                        "size_bytes": size,
                    }
                )
        return rows

    def worktree_diff(
        self,
        project_id: ProjectId,
        run_id: CodingRunId,
        base_sha: str,
        head_sha: str,
        *,
        max_bytes: int,
    ) -> tuple[bytes, bool]:
        """Return the unified ``base..head`` diff, truncation-flagged at ``max_bytes``."""
        base = _validate_commit_sha(base_sha)
        head = _validate_commit_sha(head_sha)
        worktree = _safe_child(self.worktrees_root, str(project_id), str(run_id))
        with self._lock(project_id).acquire():
            result = self.git.run(["diff", "--no-color", "--no-ext-diff", base, head], cwd=worktree)
        data = result.stdout.encode("utf-8", errors="replace")
        if len(data) > max_bytes:
            return data[:max_bytes], True
        return data, False

    def worktree_blob_is_binary(
        self, project_id: ProjectId, run_id: CodingRunId, base_sha: str, head_sha: str, path: str
    ) -> bool:
        """Whether a changed path is a binary file (rejected by the binary policy)."""
        base = _validate_commit_sha(base_sha)
        head = _validate_commit_sha(head_sha)
        worktree = _safe_child(self.worktrees_root, str(project_id), str(run_id))
        with self._lock(project_id).acquire():
            # ``git diff --numstat`` reports ``-\t-`` for binary paths.
            check = self.git.run(
                ["diff", "--numstat", "--no-color", base, head, "--", path],
                cwd=worktree,
                check=False,
            )
        first = check.stdout.strip().split("\n")[0] if check.stdout.strip() else ""
        return first.startswith("-\t-")

    def _reap_workspace(
        self,
        project: ProjectId,
        run: CodingRunId,
        *,
        observed_identity: tuple[int, int],
        observed_workspace_id: str | None,
        observed_marker: bool,
        cutoff: datetime,
    ) -> tuple[bool, int]:
        path = _safe_child(self.worktrees_root, str(project), str(run))
        marker = self._workspace_marker(project, run)
        with self._lock(project).acquire():
            if not path.is_dir() or path.is_symlink():
                return False, 0
            current_stat = path.stat()
            if (current_stat.st_dev, current_stat.st_ino) != observed_identity:
                return False, 0
            marker_exists = marker.is_file() and not marker.is_symlink()
            if marker_exists != observed_marker:
                return False, 0
            created = datetime.fromtimestamp(current_stat.st_mtime, UTC)
            if marker_exists:
                value = _read_json(marker)
                if (
                    value.get("project_id") != str(project)
                    or value.get("run_id") != str(run)
                    or value.get("workspace_id") != observed_workspace_id
                ):
                    return False, 0
                created = _parse_datetime(str(value["created_at"]))
            if created >= cutoff:
                return False, 0
            reclaimed = sum(
                item.stat().st_size
                for item in path.rglob("*")
                if item.is_file() and not item.is_symlink()
            )
            _remove_tree(path)
            marker.unlink(missing_ok=True)
            return True, reclaimed

    def reap_worktrees(self, *, older_than: datetime) -> ReapResult:
        if older_than.tzinfo is None or older_than.utcoffset() is None:
            raise InvalidStorageInput("reaper cutoff must be timezone-aware")
        cutoff = older_than.astimezone(UTC)
        removed = 0
        reclaimed = 0
        for project_path in self.worktrees_root.iterdir():
            if not project_path.is_dir() or project_path.is_symlink():
                continue
            try:
                project = checked_project_id(project_path.name)
            except InvalidStorageInput:
                continue
            for path in project_path.iterdir():
                if not path.is_dir() or path.is_symlink():
                    continue
                try:
                    run = checked_run_id(path.name)
                    if path != _safe_child(self.worktrees_root, str(project), str(run)):
                        continue
                    marker = self._workspace_marker(project, run)
                    path_stat = path.stat()
                    observed_marker = marker.is_file() and not marker.is_symlink()
                    observed_workspace_id: str | None = None
                    if observed_marker:
                        value = _read_json(marker)
                        if value.get("project_id") != str(project) or value.get("run_id") != str(
                            run
                        ):
                            continue
                        workspace_id = value.get("workspace_id")
                        if not isinstance(workspace_id, str) or not workspace_id:
                            continue
                        observed_workspace_id = workspace_id
                    was_removed, bytes_removed = self._reap_workspace(
                        project,
                        run,
                        observed_identity=(path_stat.st_dev, path_stat.st_ino),
                        observed_workspace_id=observed_workspace_id,
                        observed_marker=observed_marker,
                        cutoff=cutoff,
                    )
                    if was_removed:
                        removed += 1
                        reclaimed += bytes_removed
                except (KeyError, OSError, ValueError, json.JSONDecodeError, CodingStorageError):
                    continue
        return ReapResult(removed, reclaimed)

    def _artifact_dir(
        self, project_id: ProjectId, run_id: CodingRunId, content_hash: str
    ) -> tuple[ProjectId, CodingRunId, Path]:
        project = self._project(project_id)
        run = self._run(run_id)
        digest = _validate_hash(content_hash)
        return project, run, _safe_child(self.artifacts_root, str(project), str(run), digest)

    def _artifact_usage(self, project: ProjectId) -> int:
        root = _safe_child(self.artifacts_root, str(project))
        if not root.exists():
            return 0
        return sum(path.stat().st_size for path in root.glob("*/*/blob") if path.is_file())

    def put_artifact(
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
    ) -> ArtifactRecord:
        project = self._project(project_id)
        run = self._run(run_id)
        safe_name = validate_artifact_name(name)
        if len(data) > self.quotas.max_artifact_bytes:
            raise StorageQuotaExceeded("artifact exceeds the configured size limit")
        if not media_type or len(media_type) > 255 or any(c in media_type for c in "\r\n\x00"):
            raise InvalidStorageInput("invalid artifact media type")
        custom = dict(metadata or {})
        if any(not isinstance(k, str) or not isinstance(v, str) for k, v in custom.items()):
            raise InvalidStorageInput("artifact metadata keys and values must be strings")
        encoded_metadata_size = len(json.dumps(custom).encode())
        if encoded_metadata_size > 64 * 1024:
            raise StorageQuotaExceeded("artifact metadata exceeds 64 KiB")
        retained = _timestamp(retained_until)
        digest = _sha256(data)
        _, _, directory = self._artifact_dir(project, run, digest)
        with self._lock(project).acquire():
            self._require_repo(project)
            blob = directory / "blob"
            metadata_path = directory / "metadata.json"
            if metadata_path.exists():
                existing = self._artifact_record(project, run, digest)
                if blob.read_bytes() != data:
                    raise StorageConflict("content hash collision")
                return existing
            if directory.exists():
                if directory.is_symlink() or not directory.is_dir():
                    raise InvalidStorageInput("unsafe artifact residue")
                _remove_tree(directory)
            if self._artifact_usage(project) + len(data) > self.quotas.max_project_artifact_bytes:
                raise StorageQuotaExceeded("project artifact quota exceeded")
            created = _utc_now()
            directory.mkdir(parents=True, exist_ok=False)
            try:
                _atomic_bytes(blob, data)
                _atomic_json(
                    metadata_path,
                    {
                        "project_id": str(project),
                        "run_id": str(run),
                        "content_hash": digest,
                        "size_bytes": len(data),
                        "name": safe_name,
                        "media_type": media_type,
                        "created_at": created.isoformat(),
                        "retention": retention.value,
                        "retained_until": retained,
                        "metadata": custom,
                    },
                )
            except Exception:
                _remove_tree(directory, ignore_errors=True)
                raise
            return self._artifact_record(project, run, digest)

    def _artifact_record(
        self, project: ProjectId, run: CodingRunId, content_hash: str
    ) -> ArtifactRecord:
        _, _, directory = self._artifact_dir(project, run, content_hash)
        metadata_path = directory / "metadata.json"
        blob = directory / "blob"
        if (
            not metadata_path.is_file()
            or metadata_path.is_symlink()
            or not blob.is_file()
            or blob.is_symlink()
        ):
            raise StorageNotFound(f"artifact not found: {content_hash}")
        value = _read_json(metadata_path)
        if value.get("project_id") != str(project) or value.get("run_id") != str(run):
            raise InvalidStorageInput("artifact metadata crosses its declared scope")
        digest = _validate_hash(str(value["content_hash"]))
        data = blob.read_bytes()
        if _sha256(data) != digest or len(data) != int(value["size_bytes"]):
            raise StorageConflict("artifact content verification failed")
        custom = value.get("metadata", {})
        if not isinstance(custom, dict) or any(
            not isinstance(k, str) or not isinstance(v, str) for k, v in custom.items()
        ):
            raise InvalidStorageInput("invalid artifact metadata")
        until_raw = value.get("retained_until")
        return ArtifactRecord(
            project,
            run,
            digest,
            len(data),
            str(value["name"]),
            str(value["media_type"]),
            _parse_datetime(str(value["created_at"])),
            ArtifactRetention(str(value["retention"])),
            _parse_datetime(str(until_raw)) if until_raw else None,
            custom,
        )

    def read_artifact(self, project_id: ProjectId, run_id: CodingRunId, content_hash: str) -> bytes:
        project, run, directory = self._artifact_dir(project_id, run_id, content_hash)
        self._artifact_record(project, run, content_hash)
        return (directory / "blob").read_bytes()

    def retain_artifact(
        self,
        project_id: ProjectId,
        run_id: CodingRunId,
        content_hash: str,
        *,
        until: datetime | None = None,
    ) -> ArtifactRecord:
        project, run, directory = self._artifact_dir(project_id, run_id, content_hash)
        with self._lock(project).acquire():
            record = self._artifact_record(project, run, content_hash)
            value = _read_json(directory / "metadata.json")
            value["retention"] = ArtifactRetention.retained.value
            value["retained_until"] = _timestamp(until)
            _atomic_json(directory / "metadata.json", value)
            return self._artifact_record(record.project_id, record.run_id, content_hash)

    def delete_artifact(
        self, project_id: ProjectId, run_id: CodingRunId, content_hash: str
    ) -> bool:
        project, _run, directory = self._artifact_dir(project_id, run_id, content_hash)
        with self._lock(project).acquire():
            if not directory.exists():
                return False
            if directory.is_symlink() or not directory.is_dir():
                raise InvalidStorageInput("refusing to delete an unsafe artifact path")
            reclaimed = directory.with_name(f".delete-{directory.name}-{uuid.uuid4().hex}")
            os.replace(directory, reclaimed)
            _remove_tree(reclaimed)
            return True

    def reap_artifacts(self, *, older_than: datetime) -> ReapResult:
        if older_than.tzinfo is None or older_than.utcoffset() is None:
            raise InvalidStorageInput("reaper cutoff must be timezone-aware")
        cutoff = older_than.astimezone(UTC)
        removed = 0
        reclaimed = 0
        for metadata in self.artifacts_root.glob("*/*/*/metadata.json"):
            try:
                value = _read_json(metadata)
                project = checked_project_id(str(value["project_id"]))
                run = checked_run_id(str(value["run_id"]))
                digest = _validate_hash(str(value["content_hash"]))
                _, _, directory = self._artifact_dir(project, run, digest)
                with self._lock(project).acquire():
                    record = self._artifact_record(project, run, digest)
                    if record.created_at >= cutoff:
                        continue
                    if record.retention is ArtifactRetention.retained and (
                        record.retained_until is None or record.retained_until > _utc_now()
                    ):
                        continue
                    reclaimed_path = directory.with_name(
                        f".delete-{directory.name}-{uuid.uuid4().hex}"
                    )
                    os.replace(directory, reclaimed_path)
                    _remove_tree(reclaimed_path)
                    reclaimed += record.size_bytes
                    removed += 1
            except (KeyError, OSError, ValueError, json.JSONDecodeError, CodingStorageError):
                continue
        return ReapResult(removed, reclaimed)


class LocalActiveGitStore:
    """Narrow ActiveGitStore view over a shared local coding storage instance."""

    def __init__(self, storage: LocalCodingStorage) -> None:
        self._storage = storage

    def create_project(
        self, project_id: ProjectId, *, default_branch: str = "main"
    ) -> ProjectRecord:
        return self._storage.create_project(project_id, default_branch=default_branch)

    def import_project(
        self,
        project_id: ProjectId,
        remote: str | Path,
        *,
        default_branch: str = "main",
    ) -> ProjectRecord:
        return self._storage.import_project(project_id, remote, default_branch=default_branch)

    def fetch(self, project_id: ProjectId, remote: str | Path) -> ProjectRecord:
        return self._storage.fetch(project_id, remote)

    def get_project(self, project_id: ProjectId) -> ProjectRecord:
        return self._storage.get_project(project_id)


class LocalSnapshotStore:
    def __init__(self, storage: LocalCodingStorage) -> None:
        self._storage = storage

    def create_snapshot(self, project_id: ProjectId) -> SnapshotRecord:
        return self._storage.create_snapshot(project_id)

    def restore_snapshot(self, project_id: ProjectId, snapshot_id: str) -> ProjectRecord:
        return self._storage.restore_snapshot(project_id, snapshot_id)

    def list_snapshots(self, project_id: ProjectId) -> list[SnapshotRecord]:
        return self._storage.list_snapshots(project_id)


class LocalWorktreeStore:
    def __init__(self, storage: LocalCodingStorage) -> None:
        self._storage = storage

    def materialize(
        self, project_id: ProjectId, run_id: CodingRunId, *, ref: str = "HEAD"
    ) -> WorktreeRecord:
        return self._storage.materialize(project_id, run_id, ref=ref)

    def remove(self, project_id: ProjectId, run_id: CodingRunId) -> bool:
        return self._storage.remove(project_id, run_id)

    def reap(self, *, older_than: datetime) -> ReapResult:
        return self._storage.reap_worktrees(older_than=older_than)


class LocalArtifactStore:
    def __init__(self, storage: LocalCodingStorage) -> None:
        self._storage = storage

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
    ) -> ArtifactRecord:
        return self._storage.put_artifact(
            project_id,
            run_id,
            data,
            name=name,
            media_type=media_type,
            retention=retention,
            retained_until=retained_until,
            metadata=metadata,
        )

    def read(self, project_id: ProjectId, run_id: CodingRunId, content_hash: str) -> bytes:
        return self._storage.read_artifact(project_id, run_id, content_hash)

    def retain(
        self,
        project_id: ProjectId,
        run_id: CodingRunId,
        content_hash: str,
        *,
        until: datetime | None = None,
    ) -> ArtifactRecord:
        return self._storage.retain_artifact(project_id, run_id, content_hash, until=until)

    def delete(self, project_id: ProjectId, run_id: CodingRunId, content_hash: str) -> bool:
        return self._storage.delete_artifact(project_id, run_id, content_hash)

    def reap(self, *, older_than: datetime) -> ReapResult:
        return self._storage.reap_artifacts(older_than=older_than)
