"""Deterministic snapshot transfer archive for controlled patch worktrees (WS-PP, M4 P3a).

A *snapshot* is the full working tree the control plane ships to the disposable sandbox
before a controlled generation run, and the tree the sandbox ships back afterwards. It is
materially different from a *changed-file* diff (see :mod:`keel_core.patch.models`): a
snapshot bounds the entire Keel checkout (>800 files), so it defines its own, larger bounds
and never reuses ``MAX_CHANGED_FILES`` / ``MAX_DIFF_BYTES``.

Design invariants (fail closed, P5):

* Only regular files travel. ``.git`` and forbidden/secret paths are skipped on *build*
  (recording only the included files); any symlink, hardlink, junction, reparse point,
  device, FIFO, or other special entry fails the build closed (never silently skipped).
* Every path is validated as canonical POSIX (relative, forward-slash, no drive, no
  traversal, no NUL, bounded length, no duplicate/casefold collision) on build and parse.
* The gzip+tar stream is byte-deterministic (fixed mtime/uid/gid/uname/gname, GNU format,
  no pax/sparse) so an identical tree always yields identical bytes.
* Parsing an untrusted archive validates the *entire* stream before a caller mutates any
  disposable worktree, and enforces compressed-body, total-uncompressed (gzip-bomb),
  per-file, and file-count bounds.
* No source bytes or error detail are ever logged from this module; failures raise typed
  :mod:`keel_core.patch.errors` exceptions whose messages never carry file contents.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import os
import stat
import tarfile
import zlib
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from .errors import PatchBoundsExceeded, PatchPolicyViolation, PatchValidationError
from .models import is_forbidden_path

# --- Snapshot bounds (fail closed; distinct from changed-file bounds) ----------------

# A full-tree snapshot must comfortably hold the whole Keel checkout (>800 files today),
# so these are deliberately larger than and independent of ``MAX_CHANGED_FILES`` (200) and
# the diff byte ceilings in :mod:`keel_core.patch.models`.
MAX_SNAPSHOT_FILES = 10_000
MAX_SNAPSHOT_FILE_BYTES = 25_000_000
MAX_SNAPSHOT_TOTAL_BYTES = 512_000_000
MAX_SNAPSHOT_ARCHIVE_BYTES = 256_000_000
MAX_SNAPSHOT_PATH_CHARS = 1_024

# Deterministic tar/gzip metadata. Fixed so an identical tree yields byte-identical output.
_TAR_MTIME = 0
_TAR_UID = 0
_TAR_GID = 0
_TAR_UNAME = ""
_TAR_GNAME = ""
_GZIP_MTIME = 0
_GZIP_COMPRESSLEVEL = 9
_MODE_FILE = 0o644
_MODE_EXEC = 0o755

# git's heuristic: a NUL byte in the leading window marks the content binary.
_BINARY_SNIFF_BYTES = 8000
_GUNZIP_CHUNK = 1 << 20  # 1 MiB bounded read window

_ALLOWED_TAR_TYPES = (tarfile.REGTYPE, tarfile.AREGTYPE)


@dataclass(frozen=True, slots=True)
class SnapshotBounds:
    """Bounded ceilings enforced while building and parsing a snapshot archive."""

    max_files: int = MAX_SNAPSHOT_FILES
    max_file_bytes: int = MAX_SNAPSHOT_FILE_BYTES
    max_total_bytes: int = MAX_SNAPSHOT_TOTAL_BYTES
    max_archive_bytes: int = MAX_SNAPSHOT_ARCHIVE_BYTES
    max_path_chars: int = MAX_SNAPSHOT_PATH_CHARS

    def __post_init__(self) -> None:
        for name in (
            "max_files",
            "max_file_bytes",
            "max_total_bytes",
            "max_archive_bytes",
            "max_path_chars",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value <= 0:
                raise PatchValidationError(f"snapshot bound {name} must be a positive integer")


DEFAULT_SNAPSHOT_BOUNDS = SnapshotBounds()


# --- Immutable content model ---------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SnapshotFile:
    """One validated regular file carried in a snapshot (path + exact bytes + flags)."""

    path: str
    data: bytes
    executable: bool
    binary: bool

    @property
    def size(self) -> int:
        return len(self.data)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()


@dataclass(frozen=True, slots=True)
class SnapshotEntry:
    """Immutable manifest row: content-addressed metadata for one snapshot file."""

    path: str
    size: int
    sha256: str
    executable: bool
    binary: bool


@dataclass(frozen=True, slots=True)
class SnapshotManifest:
    """Immutable, deterministically-ordered index of a snapshot's files."""

    entries: tuple[SnapshotEntry, ...]
    total_bytes: int

    @property
    def file_count(self) -> int:
        return len(self.entries)

    @property
    def paths(self) -> tuple[str, ...]:
        return tuple(entry.path for entry in self.entries)


@dataclass(frozen=True, slots=True)
class ParsedSnapshot:
    """The fully-validated, in-memory result of parsing a snapshot archive."""

    files: Mapping[str, SnapshotFile]
    manifest: SnapshotManifest


@dataclass(frozen=True, slots=True)
class AppliedExport:
    """Outcome of applying a sandbox export archive onto a disposable worktree."""

    written: tuple[str, ...]
    deleted: tuple[str, ...]
    unchanged_binaries: tuple[str, ...]
    manifest: SnapshotManifest


# --- Path + content helpers ----------------------------------------------------------


def validate_snapshot_path(
    value: object, *, bounds: SnapshotBounds = DEFAULT_SNAPSHOT_BOUNDS, field_name: str = "path"
) -> str:
    """Validate an untrusted path as canonical POSIX and repo-relative (fail closed).

    Unlike :func:`keel_core.patch.models.normalize_repo_path`, a backslash is *rejected*
    rather than rewritten: a canonical POSIX archive path never contains one, so its
    presence signals a hostile or malformed archive.
    """

    if not isinstance(value, str):
        raise PatchValidationError(f"{field_name} must be a string")
    if not value:
        raise PatchValidationError(f"{field_name} must not be empty")
    if "\x00" in value:
        raise PatchValidationError(f"{field_name} must not contain null bytes")
    if len(value) > bounds.max_path_chars:
        raise PatchBoundsExceeded(f"{field_name} exceeds {bounds.max_path_chars} characters")
    if "\\" in value:
        raise PatchValidationError(f"{field_name} must be canonical POSIX (no backslash)")
    if value.startswith("/") or value.startswith("~"):
        raise PatchValidationError(f"{field_name} must be repository-relative")
    if len(value) >= 2 and value[1] == ":":
        raise PatchValidationError(f"{field_name} must not be a drive-absolute path")
    parts = value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise PatchValidationError(f"{field_name} must not contain empty or traversal components")
    return value


def _is_binary(data: bytes) -> bool:
    return b"\x00" in data[:_BINARY_SNIFF_BYTES]


def _is_reparse_point(st: os.stat_result) -> bool:
    attributes = getattr(st, "st_file_attributes", 0)
    reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(attributes & reparse)


def _is_real_dir(path: Path) -> bool:
    try:
        st = os.lstat(path)
    except OSError:
        return False
    return stat.S_ISDIR(st.st_mode) and not _is_reparse_point(st)


# --- Build (directory / files -> deterministic archive) ------------------------------


def _iter_regular_files(root: Path) -> Iterator[tuple[str, Path, int]]:
    """Yield ``(posix_relpath, abs_path, mode)`` for every regular file under ``root``.

    ``.git`` and forbidden/secret paths are skipped by explicit policy. Any symlink,
    reparse point/junction, device, FIFO, socket, or other special entry fails closed.
    """

    stack: list[tuple[Path, str]] = [(root, "")]
    while stack:
        current, prefix = stack.pop()
        with os.scandir(current) as iterator:
            for entry in iterator:
                rel = entry.name if not prefix else f"{prefix}/{entry.name}"
                if is_forbidden_path(rel):
                    continue
                try:
                    st = entry.stat(follow_symlinks=False)
                except OSError as exc:
                    raise PatchValidationError("snapshot source could not be read") from exc
                if entry.is_symlink() or _is_reparse_point(st):
                    raise PatchPolicyViolation(
                        "snapshot source may not contain a symlink or reparse point"
                    )
                if stat.S_ISDIR(st.st_mode):
                    stack.append((Path(entry.path), rel))
                elif stat.S_ISREG(st.st_mode):
                    yield rel, Path(entry.path), st.st_mode
                else:
                    raise PatchPolicyViolation("snapshot source may not contain a special file")


def _read_regular_file(path: Path, *, max_bytes: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, "rb", closefd=True) as handle:
        st = os.fstat(handle.fileno())
        if not stat.S_ISREG(st.st_mode):
            raise PatchPolicyViolation("snapshot source may not contain a special file")
        data = handle.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise PatchBoundsExceeded("snapshot file exceeds per-file byte bound")
    return data


def scan_directory_for_snapshot(
    root: os.PathLike[str] | str, *, bounds: SnapshotBounds = DEFAULT_SNAPSHOT_BOUNDS
) -> list[SnapshotFile]:
    """Read a directory tree into validated, deterministically-ordered snapshot files."""

    root_path = Path(root)
    if not _is_real_dir(root_path):
        raise PatchValidationError("snapshot source must be an existing directory")
    files: list[SnapshotFile] = []
    total = 0
    seen: dict[str, str] = {}
    for rel, abs_path, mode in _iter_regular_files(root_path):
        path = validate_snapshot_path(rel, bounds=bounds)
        if is_forbidden_path(path):
            continue
        key = path.casefold()
        if key in seen:
            raise PatchValidationError("snapshot contains a duplicate or case-colliding path")
        if len(files) >= bounds.max_files:
            raise PatchBoundsExceeded("snapshot exceeds file-count bound")
        data = _read_regular_file(abs_path, max_bytes=bounds.max_file_bytes)
        total += len(data)
        if total > bounds.max_total_bytes:
            raise PatchBoundsExceeded("snapshot exceeds total byte bound")
        seen[key] = path
        files.append(
            SnapshotFile(
                path=path,
                data=data,
                executable=bool(mode & 0o111),
                binary=_is_binary(data),
            )
        )
    files.sort(key=lambda item: item.path)
    return files


def _manifest_from_files(files: Iterable[SnapshotFile]) -> SnapshotManifest:
    ordered = sorted(files, key=lambda item: item.path)
    entries = tuple(
        SnapshotEntry(
            path=item.path,
            size=item.size,
            sha256=item.sha256,
            executable=item.executable,
            binary=item.binary,
        )
        for item in ordered
    )
    return SnapshotManifest(entries=entries, total_bytes=sum(entry.size for entry in entries))


def build_snapshot_archive(
    files: Sequence[SnapshotFile], *, bounds: SnapshotBounds = DEFAULT_SNAPSHOT_BOUNDS
) -> tuple[bytes, SnapshotManifest]:
    """Serialize validated files into a byte-deterministic gzip+tar archive + manifest."""

    ordered = sorted(files, key=lambda item: item.path)
    seen: dict[str, str] = {}
    total = 0
    for item in ordered:
        validate_snapshot_path(item.path, bounds=bounds)
        key = item.path.casefold()
        if key in seen:
            raise PatchValidationError("snapshot contains a duplicate or case-colliding path")
        seen[key] = item.path
        if item.size > bounds.max_file_bytes:
            raise PatchBoundsExceeded("snapshot file exceeds per-file byte bound")
        total += item.size
    if len(ordered) > bounds.max_files:
        raise PatchBoundsExceeded("snapshot exceeds file-count bound")
    if total > bounds.max_total_bytes:
        raise PatchBoundsExceeded("snapshot exceeds total byte bound")

    raw = io.BytesIO()
    with gzip.GzipFile(
        fileobj=raw, mode="wb", compresslevel=_GZIP_COMPRESSLEVEL, mtime=_GZIP_MTIME
    ) as gz:
        with tarfile.open(fileobj=gz, mode="w", format=tarfile.GNU_FORMAT) as tar:
            for item in ordered:
                info = tarfile.TarInfo(name=item.path)
                info.size = item.size
                info.mtime = _TAR_MTIME
                info.mode = _MODE_EXEC if item.executable else _MODE_FILE
                info.type = tarfile.REGTYPE
                info.uid = _TAR_UID
                info.gid = _TAR_GID
                info.uname = _TAR_UNAME
                info.gname = _TAR_GNAME
                tar.addfile(info, io.BytesIO(item.data))
    archive = raw.getvalue()
    if len(archive) > bounds.max_archive_bytes:
        raise PatchBoundsExceeded("snapshot archive exceeds compressed byte bound")
    return archive, _manifest_from_files(ordered)


def build_snapshot_from_directory(
    root: os.PathLike[str] | str, *, bounds: SnapshotBounds = DEFAULT_SNAPSHOT_BOUNDS
) -> tuple[bytes, SnapshotManifest]:
    """Scan a directory and serialize it into a deterministic archive + manifest."""

    return build_snapshot_archive(scan_directory_for_snapshot(root, bounds=bounds), bounds=bounds)


# --- Parse (untrusted archive -> validated in-memory map) ----------------------------


def _bounded_gunzip(archive: bytes, *, max_total: int) -> bytes:
    out = bytearray()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(archive), mode="rb") as gz:
            while True:
                chunk = gz.read(_GUNZIP_CHUNK)
                if not chunk:
                    break
                out += chunk
                if len(out) > max_total:
                    raise PatchBoundsExceeded("snapshot archive exceeds total uncompressed bound")
    except PatchBoundsExceeded:
        raise
    except (OSError, EOFError, zlib.error) as exc:
        raise PatchValidationError("snapshot archive is not valid gzip") from exc
    return bytes(out)


def _iter_validated_members(archive: object, *, bounds: SnapshotBounds) -> Iterator[SnapshotFile]:
    """Validate every tar member of an untrusted archive, yielding safe snapshot files.

    This is the single validation chokepoint shared by :func:`parse_snapshot_archive`
    (which collects the whole map in memory) and the sandbox extractor (which streams
    each validated file to staging).
    """

    if not isinstance(archive, (bytes, bytearray)):
        raise PatchValidationError("snapshot archive must be bytes")
    payload = bytes(archive)
    if not payload:
        raise PatchValidationError("snapshot archive is empty")
    if len(payload) > bounds.max_archive_bytes:
        raise PatchBoundsExceeded("snapshot archive exceeds compressed byte bound")

    raw = _bounded_gunzip(payload, max_total=bounds.max_total_bytes)
    seen: dict[str, str] = {}
    count = 0
    total = 0
    try:
        tar = tarfile.open(fileobj=io.BytesIO(raw), mode="r:")
    except tarfile.TarError as exc:
        raise PatchValidationError("snapshot archive is not a valid tar") from exc
    with tar:
        while True:
            try:
                member = tar.next()
            except tarfile.TarError as exc:
                raise PatchValidationError("snapshot archive is malformed") from exc
            if member is None:
                break
            if member.type not in _ALLOWED_TAR_TYPES:
                raise PatchPolicyViolation("snapshot archive contains a non-regular entry")
            path = validate_snapshot_path(member.name, bounds=bounds)
            if is_forbidden_path(path):
                raise PatchPolicyViolation("snapshot archive contains a forbidden path")
            key = path.casefold()
            if key in seen:
                raise PatchValidationError(
                    "snapshot archive contains a duplicate or case-colliding path"
                )
            count += 1
            if count > bounds.max_files:
                raise PatchBoundsExceeded("snapshot archive exceeds file-count bound")
            if member.size < 0 or member.size > bounds.max_file_bytes:
                raise PatchBoundsExceeded("snapshot archive entry exceeds per-file byte bound")
            total += member.size
            if total > bounds.max_total_bytes:
                raise PatchBoundsExceeded("snapshot archive exceeds total byte bound")
            extracted = tar.extractfile(member)
            if extracted is None:
                raise PatchValidationError("snapshot archive entry has no content")
            with extracted:
                data = extracted.read(bounds.max_file_bytes + 1)
            if len(data) != member.size:
                raise PatchValidationError("snapshot archive entry size mismatch")
            seen[key] = path
            yield SnapshotFile(
                path=path,
                data=data,
                executable=bool(member.mode & 0o111),
                binary=_is_binary(data),
            )


def parse_snapshot_archive(
    archive: object, *, bounds: SnapshotBounds = DEFAULT_SNAPSHOT_BOUNDS
) -> ParsedSnapshot:
    """Fully validate an untrusted archive into an ordered in-memory map + manifest."""

    files: dict[str, SnapshotFile] = {}
    for snapshot_file in _iter_validated_members(archive, bounds=bounds):
        files[snapshot_file.path] = snapshot_file
    ordered = dict(sorted(files.items()))
    return ParsedSnapshot(files=ordered, manifest=_manifest_from_files(ordered.values()))


# --- Apply (validated export -> disposable worktree) ---------------------------------


def _safe_join(root: Path, relpath: str) -> Path:
    target = root / relpath
    root_norm = os.path.normpath(str(root))
    target_norm = os.path.normpath(str(target))
    prefix = root_norm if root_norm.endswith(os.sep) else root_norm + os.sep
    if target_norm != root_norm and not target_norm.startswith(prefix):
        raise PatchPolicyViolation("snapshot path escapes the worktree")
    return target


def _write_new_file(target: Path, data: bytes, *, mode: int) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_TRUNC
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_BINARY", 0)
    )
    fd = os.open(target, flags, mode)
    with os.fdopen(fd, "wb", closefd=True) as handle:
        handle.write(data)
    os.chmod(target, mode)


def _overwrite_file(target: Path, data: bytes) -> None:
    flags = os.O_WRONLY | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)
    fd = os.open(target, flags)
    with os.fdopen(fd, "wb", closefd=True) as handle:
        handle.write(data)


def apply_export_to_worktree(
    worktree: os.PathLike[str] | str,
    archive: object,
    *,
    bounds: SnapshotBounds = DEFAULT_SNAPSHOT_BOUNDS,
) -> AppliedExport:
    """Apply a sandbox export archive onto a disposable worktree, fail-closed.

    The *entire* archive is parsed and validated, and every policy rule is checked against
    the current worktree, *before* any file is mutated:

    * an existing binary must be byte-identical (the model may not modify binaries);
    * a new binary, or a text file becoming binary, is a :class:`PatchPolicyViolation`;
    * a binary omitted from the export is left untouched;
    * a text file present in the worktree but omitted from the export is deleted;
    * an existing file keeps its original mode; a new file is forced to ``0644`` so the
      model can never introduce an executable; exact bytes (including CRLF) are preserved.
    """

    worktree_path = Path(worktree)
    if not _is_real_dir(worktree_path):
        raise PatchValidationError("worktree must be an existing directory")

    parsed = parse_snapshot_archive(archive, bounds=bounds)
    before = {item.path: item for item in scan_directory_for_snapshot(worktree_path, bounds=bounds)}

    to_write: list[SnapshotFile] = []
    unchanged_binaries: list[str] = []
    for path, entry in parsed.files.items():
        existing = before.get(path)
        if entry.binary:
            if existing is None:
                raise PatchPolicyViolation("export may not introduce a binary file")
            if not existing.binary:
                raise PatchPolicyViolation("export may not convert a text file to binary")
            if existing.data != entry.data:
                raise PatchPolicyViolation("export may not modify an existing binary file")
            unchanged_binaries.append(path)
        else:
            to_write.append(entry)

    to_delete = [
        path
        for path, existing in before.items()
        if path not in parsed.files and not existing.binary
    ]

    written: list[str] = []
    for entry in to_write:
        target = _safe_join(worktree_path, entry.path)
        if entry.path in before:
            _overwrite_file(target, entry.data)
        else:
            _write_new_file(target, entry.data, mode=_MODE_FILE)
        written.append(entry.path)

    for path in to_delete:
        target = _safe_join(worktree_path, path)
        os.unlink(target)

    return AppliedExport(
        written=tuple(sorted(written)),
        deleted=tuple(sorted(to_delete)),
        unchanged_binaries=tuple(sorted(unchanged_binaries)),
        manifest=parsed.manifest,
    )


__all__ = [
    "AppliedExport",
    "DEFAULT_SNAPSHOT_BOUNDS",
    "MAX_SNAPSHOT_ARCHIVE_BYTES",
    "MAX_SNAPSHOT_FILES",
    "MAX_SNAPSHOT_FILE_BYTES",
    "MAX_SNAPSHOT_PATH_CHARS",
    "MAX_SNAPSHOT_TOTAL_BYTES",
    "ParsedSnapshot",
    "SnapshotBounds",
    "SnapshotEntry",
    "SnapshotFile",
    "SnapshotManifest",
    "apply_export_to_worktree",
    "build_snapshot_archive",
    "build_snapshot_from_directory",
    "parse_snapshot_archive",
    "scan_directory_for_snapshot",
    "validate_snapshot_path",
]
