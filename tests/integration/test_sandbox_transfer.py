"""In-process integration proof for the authenticated sandbox snapshot transfer boundary.

Drives the real :func:`create_app` transfer routes through :class:`SandboxTransferClient` over an
in-process ASGI transport (the integration event loop is a Windows selector loop that cannot spawn
subprocesses), exercising the full build -> upload -> export -> apply round trip, per-namespace
isolation, and idempotent delete. No Postgres, Redis, or public network is touched.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import httpx
import pytest

from keel_core.patch import transfer as core_transfer
from keel_core.patch.transfer import apply_export_to_worktree, build_snapshot_from_directory
from keel_core.patch.transfer_client import (
    SandboxTransferClient,
    SandboxTransferNotFound,
)
from keel_core.tools import UnsafeLocalDevExecutionEnvironment
from keel_sandbox.service import DirectoryWorkspaceProvider, create_app
from keel_sandbox.transfer import SandboxTransferService

pytestmark = pytest.mark.integration

_SECRET = "test-sandbox-rpc-secret-" + ("x" * 32)


def _make_client(tmp_path: Path) -> tuple[SandboxTransferClient, Path]:
    namespaces_root = tmp_path / "namespaces"
    provider = DirectoryWorkspaceProvider(
        namespaces_root,
        lambda root: UnsafeLocalDevExecutionEnvironment(root),
    )
    app = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path / "default"),
        isolation_verified=True,
        shared_secret=_SECRET,
        workspace_provider=provider,
        transfer_service=SandboxTransferService(provider),
    )
    inner = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://sandbox")
    client = SandboxTransferClient("http://sandbox", shared_secret=_SECRET, client=inner)
    return client, namespaces_root


async def test_build_upload_export_apply_round_trip(tmp_path: Path) -> None:
    source = tmp_path / "source"
    (source / "pkg").mkdir(parents=True)
    (source / "README.md").write_bytes(b"# keel\n")
    (source / "pkg" / "mod.py").write_bytes(b"print('hi')\n")
    (source / "data.bin").write_bytes(bytes(range(256)))
    archive, manifest = build_snapshot_from_directory(source)

    client, _ = _make_client(tmp_path)
    ns = "ws_" + ("a" * 32)
    ack = await client.upload_snapshot(ns, archive)
    assert ack.files == manifest.file_count

    exported = await client.export_snapshot(ns)
    # Apply the export back into the *original* worktree (the real worker flow): the worktree
    # already holds the binary byte-for-byte, so it is left untouched, text is rewritten, and a
    # stale file absent from the export is deleted. Applying a new binary into an empty tree is
    # correctly forbidden and is covered by the unit suite.
    worktree = tmp_path / "worktree"
    shutil.copytree(source, worktree)
    (worktree / "stale.txt").write_bytes(b"remove me\n")
    applied = apply_export_to_worktree(worktree, exported)
    assert (worktree / "README.md").read_bytes() == b"# keel\n"
    assert (worktree / "pkg" / "mod.py").read_bytes() == b"print('hi')\n"
    assert (worktree / "data.bin").read_bytes() == bytes(range(256))
    assert not (worktree / "stale.txt").exists()
    assert "data.bin" in applied.unchanged_binaries
    assert applied.deleted == ("stale.txt",)

    await client.aclose()


async def test_namespace_isolation_between_scopes(tmp_path: Path) -> None:
    client, namespaces_root = _make_client(tmp_path)
    ns_a = "ws_" + ("a" * 32)
    ns_b = "ws_" + ("b" * 32)

    def _archive(payload: bytes) -> bytes:
        snapshot = [core_transfer.SnapshotFile("shared.txt", payload, False, False)]
        archive, _ = core_transfer.build_snapshot_archive(snapshot)
        return archive

    await client.upload_snapshot(ns_a, _archive(b"from-a"))
    await client.upload_snapshot(ns_b, _archive(b"from-b"))

    export_a = core_transfer.parse_snapshot_archive(await client.export_snapshot(ns_a))
    export_b = core_transfer.parse_snapshot_archive(await client.export_snapshot(ns_b))
    assert export_a.files["shared.txt"].data == b"from-a"
    assert export_b.files["shared.txt"].data == b"from-b"
    # The bytes really live under two distinct namespace roots (no shared workspace).
    assert (namespaces_root / ns_a / "shared.txt").read_bytes() == b"from-a"
    assert (namespaces_root / ns_b / "shared.txt").read_bytes() == b"from-b"

    await client.aclose()


async def test_delete_is_idempotent(tmp_path: Path) -> None:
    client, namespaces_root = _make_client(tmp_path)
    ns = "ws_" + ("c" * 32)
    snapshot = [core_transfer.SnapshotFile("f.txt", b"x", False, False)]
    archive, _ = core_transfer.build_snapshot_archive(snapshot)
    await client.upload_snapshot(ns, archive)
    assert (namespaces_root / ns).exists()

    first = await client.delete_namespace(ns)
    assert first.deleted is True
    assert not (namespaces_root / ns).exists()
    second = await client.delete_namespace(ns)
    assert second.deleted is False
    with pytest.raises(SandboxTransferNotFound):
        await client.export_snapshot(ns)

    await client.aclose()
