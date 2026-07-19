"""Authenticated snapshot transfer service for the isolated sandbox (WS-PP, M4 P3a).

The transfer service moves a full working-tree *snapshot* between the control plane and a
per-scope ``ws_<hex>`` namespace directory owned by the sandbox:

* **upload** — validate an untrusted gzip+tar archive entirely into same-filesystem staging,
  then atomically swap it into the namespace root (``target -> backup``, ``staging -> target``,
  rollback on failure, then delete ``backup``);
* **export** — serialize a namespace root back into a deterministic archive, failing closed on
  any symlink/special entry;
* **delete** — idempotently remove a namespace root after validating it is our own real
  directory, surfacing I/O errors.

Concurrency contract
--------------------
Every operation on a namespace holds a per-namespace :class:`asyncio.Lock`, so uploads,
exports, and deletes for one namespace are serialized within the process. Independently, a
process-global :class:`asyncio.Semaphore` (see ``transfer_slot`` / ``max_concurrent_transfers``)
bounds how many transfers may be validating/materializing/exporting a snapshot at once *across
all namespaces*: it is held by the HTTP route across the bounded body read and the upload/export
so concurrent requests cannot multiply peak in-memory snapshot bytes without limit. The route
acquires the global slot before the per-namespace lock (a consistent order, so no deadlock).

The *commit point* of an upload is the single ``os.replace(staging, target)`` rename: because
extraction happens in a sibling staging directory, a concurrent reader (a file RPC or an export)
observes either the entire previous tree or the entire new tree — never a partially-extracted
tree. After the commit the new tree is authoritative and is never re-swapped; if the orphaned
backup cannot be removed, the upload still succeeds with ``UploadResult.cleanup_pending=True``
(a retry would redo a committed swap), a warning is logged, and the next same-namespace upload or
delete sweeps the leftover backup — so stale directories cannot accumulate unboundedly.

Sequencing transfers against file RPCs *across* requests (a lease must not run tools while a
transfer is in flight) is the control plane's responsibility under the lease protocol (P3a-2
seam); this service guarantees only intra-process atomicity and per-namespace serialization.

No source bytes or error detail are logged here; failures raise typed exceptions the route
layer maps to status codes, and deferred-cleanup warnings carry only the opaque temp-dir name.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from keel_core.patch.transfer import (
    DEFAULT_SNAPSHOT_BOUNDS,
    ParsedSnapshot,
    SnapshotBounds,
    SnapshotManifest,
    build_snapshot_from_directory,
    parse_snapshot_archive,
)

logger = logging.getLogger(__name__)

# The same opaque, validated namespace token the executor uses.
_WORKSPACE_NAMESPACE = re.compile(r"^ws_[0-9a-f]{1,64}$")

# Same-filesystem staging/backup siblings live under the trusted base with these prefixes.
_STAGING_PREFIX = ".keel-transfer-staging"
_BACKUP_PREFIX = ".keel-transfer-backup"

_STAGED_FILE_MODE = 0o644

# Global (cross-namespace) ceiling on transfers being validated/materialized/exported at once.
# Each in-flight transfer can hold up to the uncompressed snapshot bound in memory, so this
# bounds peak memory to ``DEFAULT_MAX_CONCURRENT_TRANSFERS * max_total_bytes`` rather than
# letting concurrent requests multiply it without limit. Deliberately small; tune per host.
DEFAULT_MAX_CONCURRENT_TRANSFERS = 2


class TransferError(Exception):
    """Base class for sandbox transfer failures."""


class TransferNamespaceError(TransferError):
    """The namespace is malformed, unknown, or not a managed directory (fail closed)."""


class TransferIOError(TransferError):
    """An underlying filesystem operation failed while transferring a snapshot."""


@dataclass(frozen=True, slots=True)
class UploadResult:
    namespace: str
    manifest: SnapshotManifest
    # True when the new tree was committed but the previous tree's backup could not be removed;
    # the upload still succeeded (the swap must not be retried) and a later same-namespace
    # operation sweeps the leftover backup. See ``_atomic_swap``.
    cleanup_pending: bool = False


@dataclass(frozen=True, slots=True)
class ExportResult:
    namespace: str
    archive: bytes
    manifest: SnapshotManifest


@dataclass(frozen=True, slots=True)
class DeleteResult:
    namespace: str
    deleted: bool


class NamespaceLocator(Protocol):
    """A directory-backed provider that can locate + validate ``ws_<hex>`` namespace roots."""

    @property
    def namespace_base(self) -> Path: ...

    def namespace_directory(self, namespace: str) -> Path | None: ...

    def is_own_namespace_directory(self, namespace: str) -> bool: ...


class SandboxTransferService:
    """Extract / export / delete per-namespace snapshots under a confined base directory."""

    def __init__(
        self,
        locator: NamespaceLocator,
        *,
        bounds: SnapshotBounds = DEFAULT_SNAPSHOT_BOUNDS,
        max_concurrent_transfers: int = DEFAULT_MAX_CONCURRENT_TRANSFERS,
    ) -> None:
        if max_concurrent_transfers <= 0:
            raise ValueError("max_concurrent_transfers must be a positive integer")
        self._locator = locator
        self._bounds = bounds
        self._locks: dict[str, asyncio.Lock] = {}
        self._max_concurrent_transfers = max_concurrent_transfers
        self._transfer_semaphore = asyncio.Semaphore(max_concurrent_transfers)

    @property
    def bounds(self) -> SnapshotBounds:
        return self._bounds

    @property
    def max_concurrent_transfers(self) -> int:
        return self._max_concurrent_transfers

    def transfer_slot(self) -> asyncio.Semaphore:
        """The process-global transfer gate the route holds across body-read + upload/export.

        Acquired at the HTTP boundary (not inside the per-namespace methods) so the bounded
        request-body buffer is counted too, and so a single slot is never taken twice for one
        request. It is namespace-agnostic: concurrent transfers of *different* namespaces are
        limited just as much as same-namespace ones, bounding total in-memory snapshot bytes.
        """

        return self._transfer_semaphore

    # -- public async API (serialized per namespace) ---------------------------------

    async def upload(self, namespace: str, archive: bytes) -> UploadResult:
        validated = self._validate_namespace(namespace)
        async with self._lock(validated):
            return await asyncio.to_thread(self._upload_sync, validated, archive)

    async def export(self, namespace: str) -> ExportResult:
        validated = self._validate_namespace(namespace)
        async with self._lock(validated):
            return await asyncio.to_thread(self._export_sync, validated)

    async def delete(self, namespace: str) -> DeleteResult:
        validated = self._validate_namespace(namespace)
        async with self._lock(validated):
            return await asyncio.to_thread(self._delete_sync, validated)

    # -- helpers ---------------------------------------------------------------------

    def _validate_namespace(self, namespace: object) -> str:
        if not isinstance(namespace, str) or not _WORKSPACE_NAMESPACE.match(namespace):
            raise TransferNamespaceError("invalid workspace namespace")
        return namespace

    def _lock(self, namespace: str) -> asyncio.Lock:
        lock = self._locks.get(namespace)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[namespace] = lock
        return lock

    # -- upload ----------------------------------------------------------------------

    def _upload_sync(self, namespace: str, archive: bytes) -> UploadResult:
        # Full validation into an in-memory map before touching disk (gzip decompression only
        # happens here, after the route has verified the request HMAC).
        parsed = parse_snapshot_archive(archive, bounds=self._bounds)
        target = self._locator.namespace_directory(namespace)
        if target is None:
            raise TransferNamespaceError("namespace could not be provisioned")
        base = self._locator.namespace_base
        # Retire any staging/backup left by a prior interrupted or cleanup-deferred transfer of
        # this namespace before starting a new one, bounding leftover accumulation. Best-effort
        # and namespace-scoped; runs under the per-namespace lock so it never races a concurrent
        # same-namespace transfer, and never touches another namespace's temp dirs.
        self._sweep_stale_transfer_dirs(base, namespace)
        token = uuid.uuid4().hex
        staging = base / f"{_STAGING_PREFIX}.{namespace}.{token}"
        backup = base / f"{_BACKUP_PREFIX}.{namespace}.{token}"
        try:
            self._materialize(parsed, staging)
            cleanup_pending = self._atomic_swap(target, staging, backup)
        except BaseException:
            # The swap was not committed (or its own rollback ran); remove any staging tree so
            # a failed upload leaves the previous namespace intact. Suppress cleanup errors so
            # the primary failure is the one surfaced to the caller.
            with contextlib.suppress(OSError):
                if staging.exists():
                    shutil.rmtree(staging)
            raise
        return UploadResult(
            namespace=namespace, manifest=parsed.manifest, cleanup_pending=cleanup_pending
        )

    def _sweep_stale_transfer_dirs(self, base: Path, namespace: str) -> None:
        """Best-effort removal of leftover staging/backup dirs from prior ``namespace`` transfers.

        Only this namespace's own prefixed temp dirs are touched, so a concurrent transfer of a
        different namespace is never disturbed. Never raises: a sweep failure is logged (no source
        bytes, just the opaque temp-dir name) and left for the next attempt, so deferred cleanup
        can never fail an otherwise valid request. ``rmtree`` refuses to follow a top-level
        symlink/junction, so a planted alias is not traversed.
        """

        prefixes = (f"{_STAGING_PREFIX}.{namespace}.", f"{_BACKUP_PREFIX}.{namespace}.")
        try:
            entries = list(os.scandir(base))
        except OSError:
            return
        for entry in entries:
            if not entry.name.startswith(prefixes):
                continue
            try:
                shutil.rmtree(entry.path)
            except OSError:
                logger.warning("sandbox transfer could not sweep stale temp dir %s", entry.name)

    def _materialize(self, parsed: ParsedSnapshot, staging: Path) -> None:
        try:
            os.mkdir(staging)
        except OSError as exc:
            raise TransferIOError("could not create staging directory") from exc
        for path, snapshot_file in parsed.files.items():
            destination = staging / path
            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
                self._write_staged_file(destination, snapshot_file.data)
            except OSError as exc:
                raise TransferIOError("could not write staged file") from exc

    @staticmethod
    def _write_staged_file(destination: Path, data: bytes) -> None:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_BINARY", 0)
        )
        fd = os.open(destination, flags, _STAGED_FILE_MODE)
        with os.fdopen(fd, "wb", closefd=True) as handle:
            handle.write(data)

    def _atomic_swap(self, target: Path, staging: Path, backup: Path) -> bool:
        """Swap ``staging`` into ``target``; return whether backup cleanup was deferred.

        Pre-commit failures (staging the backup, or the commit rename) raise ``TransferIOError``
        with the previous tree restored, so the caller may safely retry. Once the commit rename
        succeeds the new tree is authoritative and must never be re-swapped; if the now-orphaned
        backup cannot be removed the upload still SUCCEEDS (retrying would redo a committed swap
        and could accumulate backups). A warning is logged (the opaque temp-dir name only, never
        source bytes) and ``True`` is returned so the caller records ``cleanup_pending`` and a
        later same-namespace operation sweeps the leftover backup.
        """

        try:
            os.replace(target, backup)
        except OSError as exc:
            raise TransferIOError("could not stage backup of existing namespace") from exc
        try:
            os.replace(staging, target)
        except OSError as exc:
            # Restore the previous tree; the caller removes the leftover staging directory.
            with contextlib.suppress(OSError):
                os.replace(backup, target)
            raise TransferIOError("could not commit staged namespace") from exc
        # Committed. The previous tree now lives only in ``backup``; try to remove it but never
        # fail the committed upload on cleanup (never ``ignore_errors``; ``rmtree`` unlinks
        # symlinks without following them).
        try:
            shutil.rmtree(backup)
        except OSError:
            logger.warning(
                "sandbox transfer committed; deferred backup cleanup for %s", backup.name
            )
            return True
        return False

    # -- export ----------------------------------------------------------------------

    def _export_sync(self, namespace: str) -> ExportResult:
        if not self._locator.is_own_namespace_directory(namespace):
            raise TransferNamespaceError("namespace does not exist")
        target = self._locator.namespace_directory(namespace)
        if target is None:
            raise TransferNamespaceError("namespace is not a managed directory")
        archive, manifest = build_snapshot_from_directory(target, bounds=self._bounds)
        return ExportResult(namespace=namespace, archive=archive, manifest=manifest)

    # -- delete ----------------------------------------------------------------------

    def _delete_sync(self, namespace: str) -> DeleteResult:
        base = self._locator.namespace_base
        # Retire any leftover staging/backup for this namespace as well, so a deleted namespace
        # leaves nothing behind (bounds accumulation; best-effort and namespace-scoped).
        self._sweep_stale_transfer_dirs(base, namespace)
        child = base / namespace
        try:
            os.lstat(child)
        except FileNotFoundError:
            return DeleteResult(namespace=namespace, deleted=False)
        except OSError as exc:
            raise TransferIOError("could not stat namespace") from exc
        if not self._locator.is_own_namespace_directory(namespace):
            raise TransferNamespaceError("namespace is not a managed directory")
        try:
            shutil.rmtree(child)
        except OSError as exc:
            raise TransferIOError("could not delete namespace") from exc
        return DeleteResult(namespace=namespace, deleted=True)


__all__ = [
    "DEFAULT_MAX_CONCURRENT_TRANSFERS",
    "DeleteResult",
    "ExportResult",
    "NamespaceLocator",
    "SandboxTransferService",
    "TransferError",
    "TransferIOError",
    "TransferNamespaceError",
    "UploadResult",
]
