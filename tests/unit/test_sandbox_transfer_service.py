"""Unit coverage for the sandbox snapshot transfer service (WS-PP, M4 P3a).

Exercises the extract-to-staging + atomic-swap upload, deterministic export, idempotent delete,
per-namespace serialization, and fail-closed handling of hostile inputs directly against a real
:class:`DirectoryWorkspaceProvider`. No HTTP layer, Postgres, or Redis.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
from pathlib import Path

import pytest

from keel_core.patch import transfer as core_transfer
from keel_core.patch.errors import PatchPolicyViolation
from keel_core.patch.transfer import SnapshotBounds, build_snapshot_archive
from keel_core.tools import UnsafeLocalDevExecutionEnvironment
from keel_sandbox.service import DirectoryWorkspaceProvider
from keel_sandbox.transfer import (
    SandboxTransferService,
    TransferIOError,
    TransferNamespaceError,
)

_NS = "ws_" + "a" * 32
_NS_B = "ws_" + "b" * 32


def _provider(tmp_path: Path) -> DirectoryWorkspaceProvider:
    return DirectoryWorkspaceProvider(
        tmp_path / "namespaces",
        lambda root: UnsafeLocalDevExecutionEnvironment(root),
    )


def _archive(files: dict[str, bytes]) -> bytes:
    snapshot = [
        core_transfer.SnapshotFile(path, data, False, b"\x00" in data[:8000])
        for path, data in files.items()
    ]
    archive, _ = build_snapshot_archive(snapshot)
    return archive


def _can_symlink(tmp_path: Path) -> bool:
    probe = tmp_path / "_symlink_probe"
    probe.mkdir()
    try:
        os.symlink(probe / "t", probe / "l")
        return True
    except (OSError, NotImplementedError):
        return False
    finally:
        shutil.rmtree(probe, ignore_errors=True)


async def test_upload_materializes_then_export_roundtrips(tmp_path: Path) -> None:
    service = SandboxTransferService(_provider(tmp_path))
    archive = _archive({"a.txt": b"hello\n", "pkg/b.py": b"print(1)\n"})
    result = await service.upload(_NS, archive)
    assert result.manifest.file_count == 2
    root = tmp_path / "namespaces" / _NS
    assert (root / "a.txt").read_bytes() == b"hello\n"
    assert (root / "pkg" / "b.py").read_bytes() == b"print(1)\n"
    export = await service.export(_NS)
    parsed = core_transfer.parse_snapshot_archive(export.archive)
    assert parsed.files["a.txt"].data == b"hello\n"
    assert sorted(parsed.files) == ["a.txt", "pkg/b.py"]


async def test_reupload_replaces_atomically(tmp_path: Path) -> None:
    service = SandboxTransferService(_provider(tmp_path))
    await service.upload(_NS, _archive({"old.txt": b"1", "keep.txt": b"a"}))
    await service.upload(_NS, _archive({"new.txt": b"2"}))
    root = tmp_path / "namespaces" / _NS
    assert (root / "new.txt").read_bytes() == b"2"
    # The previous tree is fully replaced (no leftover old files, no staging/backup residue).
    assert not (root / "old.txt").exists()
    assert not (root / "keep.txt").exists()
    siblings = [p.name for p in (tmp_path / "namespaces").iterdir()]
    assert siblings == [_NS]


async def test_upload_rollback_retains_old_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = SandboxTransferService(_provider(tmp_path))
    await service.upload(_NS, _archive({"original.txt": b"ORIGINAL"}))

    real_replace = os.replace

    def flaky_replace(src: object, dst: object, *args: object, **kwargs: object) -> None:
        # Fail only the commit step (staging -> target); the backup move succeeds first.
        if "staging" in str(src):
            raise OSError("injected swap failure")
        return real_replace(src, dst, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr("keel_sandbox.transfer.os.replace", flaky_replace)
    with pytest.raises(TransferIOError):
        await service.upload(_NS, _archive({"replacement.txt": b"NEW"}))

    root = tmp_path / "namespaces" / _NS
    assert (root / "original.txt").read_bytes() == b"ORIGINAL"
    assert not (root / "replacement.txt").exists()
    # No staging/backup residue is left behind under the base.
    residue = [p.name for p in (tmp_path / "namespaces").iterdir() if p.name != _NS]
    assert residue == []


async def test_upload_rejects_forbidden_archive(tmp_path: Path) -> None:
    service = SandboxTransferService(_provider(tmp_path))
    # Craft an archive naming a forbidden path; the core parser rejects it fail-closed.
    hostile = _archive_with_path(".git/config", b"[core]")
    with pytest.raises(PatchPolicyViolation):
        await service.upload(_NS, hostile)
    assert not (tmp_path / "namespaces" / _NS / ".git").exists()


async def test_export_unknown_namespace_fails_closed(tmp_path: Path) -> None:
    service = SandboxTransferService(_provider(tmp_path))
    with pytest.raises(TransferNamespaceError):
        await service.export(_NS)


async def test_export_fails_closed_on_symlink(tmp_path: Path) -> None:
    if not _can_symlink(tmp_path):
        pytest.skip("symlinks not permitted on this platform")
    service = SandboxTransferService(_provider(tmp_path))
    await service.upload(_NS, _archive({"a.txt": b"x"}))
    root = tmp_path / "namespaces" / _NS
    os.symlink(root / "a.txt", root / "link.txt")
    with pytest.raises(PatchPolicyViolation):
        await service.export(_NS)


async def test_delete_is_idempotent_and_validates(tmp_path: Path) -> None:
    service = SandboxTransferService(_provider(tmp_path))
    await service.upload(_NS, _archive({"a.txt": b"x"}))
    first = await service.delete(_NS)
    assert first.deleted is True
    assert not (tmp_path / "namespaces" / _NS).exists()
    second = await service.delete(_NS)
    assert second.deleted is False


async def test_delete_fails_closed_on_symlinked_namespace(tmp_path: Path) -> None:
    if not _can_symlink(tmp_path):
        pytest.skip("symlinks not permitted on this platform")
    provider = _provider(tmp_path)
    service = SandboxTransferService(provider)
    base = tmp_path / "namespaces"
    base.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_bytes(b"do not delete")
    os.symlink(outside, base / _NS, target_is_directory=True)
    with pytest.raises(TransferNamespaceError):
        await service.delete(_NS)
    # The symlink target's contents were never touched.
    assert (outside / "keep.txt").read_bytes() == b"do not delete"


@pytest.mark.parametrize("bad", ["not-a-ns", "ws_UPPER", "../escape", "ws_", ""])
async def test_invalid_namespace_rejected(tmp_path: Path, bad: str) -> None:
    service = SandboxTransferService(_provider(tmp_path))
    with pytest.raises(TransferNamespaceError):
        await service.upload(bad, _archive({"a.txt": b"x"}))


async def test_concurrent_uploads_are_atomic(tmp_path: Path) -> None:
    service = SandboxTransferService(_provider(tmp_path))
    archive_a = _archive({f"a{i}.txt": b"A" for i in range(20)})
    archive_b = _archive({f"b{i}.txt": b"B" for i in range(20)})
    await asyncio.gather(
        service.upload(_NS, archive_a),
        service.upload(_NS, archive_b),
    )
    root = tmp_path / "namespaces" / _NS
    names = sorted(p.name for p in root.iterdir())
    # Per-namespace serialization + atomic swap => the final tree is exactly one full snapshot,
    # never an interleaved mix of both.
    only_a = all(name.startswith("a") for name in names)
    only_b = all(name.startswith("b") for name in names)
    assert (only_a or only_b) and len(names) == 20


def test_per_namespace_lock_identity(tmp_path: Path) -> None:
    service = SandboxTransferService(_provider(tmp_path))
    assert service._lock(_NS) is service._lock(_NS)
    assert service._lock(_NS) is not service._lock(_NS_B)


async def test_bounds_are_enforced(tmp_path: Path) -> None:
    from keel_core.patch.errors import PatchBoundsExceeded

    service = SandboxTransferService(_provider(tmp_path), bounds=SnapshotBounds(max_file_bytes=4))
    with pytest.raises(PatchBoundsExceeded):
        await service.upload(_NS, _archive({"big.txt": b"way too large"}))
    assert not (tmp_path / "namespaces" / _NS / "big.txt").exists()


def _archive_with_path(path: str, data: bytes) -> bytes:
    import gzip
    import io
    import tarfile

    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.GNU_FORMAT) as tar:
        info = tarfile.TarInfo(path)
        info.type = tarfile.REGTYPE
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    gz = io.BytesIO()
    with gzip.GzipFile(fileobj=gz, mode="wb", mtime=0) as handle:
        handle.write(raw.getvalue())
    return gz.getvalue()


if sys.platform == "win32":  # pragma: no cover - guard, symlink tests self-skip
    pass
