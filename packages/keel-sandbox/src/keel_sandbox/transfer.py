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
exports, and deletes for one namespace are serialized within the process. The *commit point*
of an upload is the single ``os.replace(staging, target)`` rename: because extraction happens
in a sibling staging directory, a concurrent reader (a file RPC or an export) observes either
the entire previous tree or the entire new tree — never a partially-extracted tree. Sequencing
transfers against file RPCs *across* requests (a lease must not run tools while a transfer is in
flight) is the control plane's responsibility under the lease protocol (P3a-2 seam); this
service guarantees only intra-process atomicity and per-namespace serialization.

No source bytes or error detail are logged here; failures raise typed exceptions the route
layer maps to status codes.
"""

from __future__ import annotations

import asyncio
import contextlib
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

# The same opaque, validated namespace token the executor uses.
_WORKSPACE_NAMESPACE = re.compile(r"^ws_[0-9a-f]{1,64}$")

# Same-filesystem staging/backup siblings live under the trusted base with these prefixes.
_STAGING_PREFIX = ".keel-transfer-staging"
_BACKUP_PREFIX = ".keel-transfer-backup"

_STAGED_FILE_MODE = 0o644


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
    ) -> None:
        self._locator = locator
        self._bounds = bounds
        self._locks: dict[str, asyncio.Lock] = {}

    @property
    def bounds(self) -> SnapshotBounds:
        return self._bounds

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
        token = uuid.uuid4().hex
        staging = base / f"{_STAGING_PREFIX}.{namespace}.{token}"
        backup = base / f"{_BACKUP_PREFIX}.{namespace}.{token}"
        try:
            self._materialize(parsed, staging)
            self._atomic_swap(target, staging, backup)
        except BaseException:
            # The swap was not committed (or its own rollback ran); remove any staging tree so
            # a failed upload leaves the previous namespace intact. Suppress cleanup errors so
            # the primary failure is the one surfaced to the caller.
            with contextlib.suppress(OSError):
                if staging.exists():
                    shutil.rmtree(staging)
            raise
        return UploadResult(namespace=namespace, manifest=parsed.manifest)

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

    def _atomic_swap(self, target: Path, staging: Path, backup: Path) -> None:
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
        # Committed. The previous tree now lives only in ``backup``; remove it and surface any
        # failure (never ``ignore_errors``; ``rmtree`` unlinks symlinks without following them).
        try:
            shutil.rmtree(backup)
        except OSError as exc:
            raise TransferIOError("could not remove replaced namespace backup") from exc

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
    "DeleteResult",
    "ExportResult",
    "NamespaceLocator",
    "SandboxTransferService",
    "TransferError",
    "TransferIOError",
    "TransferNamespaceError",
    "UploadResult",
]
