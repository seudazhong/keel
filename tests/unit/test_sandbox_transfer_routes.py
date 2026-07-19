"""Unit coverage for the authenticated sandbox transfer HTTP routes (M4 P3a).

Drives ``/v1/transfer/{upload,export,delete}/{namespace}`` in-process via ``ASGITransport`` with
real request HMAC signing, asserting fail-closed status codes, bounded-body rejection before
decompression, replay protection, isolation gating, and keyed response signatures bound to the
caller's nonce.
"""

from __future__ import annotations

import asyncio
import gzip
import io
import tarfile
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from keel_core.patch import transfer as core_transfer
from keel_core.patch.transfer import SnapshotBounds, build_snapshot_archive
from keel_core.tools import UnsafeLocalDevExecutionEnvironment
from keel_core.tools.rpc_auth import RpcRequestSigner, RpcResponseVerifier
from keel_sandbox.service import DirectoryWorkspaceProvider, create_app
from keel_sandbox.transfer import SandboxTransferService

_SECRET = "test-sandbox-rpc-secret-" + "x" * 32
_NS = "ws_" + "a" * 32


def _archive(files: dict[str, bytes]) -> bytes:
    snapshot = [
        core_transfer.SnapshotFile(path, data, False, b"\x00" in data[:8000])
        for path, data in files.items()
    ]
    archive, _ = build_snapshot_archive(snapshot)
    return archive


def _forbidden_archive() -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.GNU_FORMAT) as tar:
        info = tarfile.TarInfo(".git/config")
        info.type = tarfile.REGTYPE
        info.size = 3
        tar.addfile(info, io.BytesIO(b"abc"))
    gz = io.BytesIO()
    with gzip.GzipFile(fileobj=gz, mode="wb", mtime=0) as handle:
        handle.write(raw.getvalue())
    return gz.getvalue()


def _build_app(
    tmp_path: Path,
    *,
    isolation_verified: bool = True,
    bounds: SnapshotBounds | None = None,
) -> tuple[httpx.ASGITransport, DirectoryWorkspaceProvider]:
    provider = DirectoryWorkspaceProvider(
        tmp_path / "namespaces",
        lambda root: UnsafeLocalDevExecutionEnvironment(root),
    )
    service = (
        SandboxTransferService(provider)
        if bounds is None
        else SandboxTransferService(provider, bounds=bounds)
    )
    app = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path / "default"),
        isolation_verified=isolation_verified,
        shared_secret=_SECRET,
        workspace_provider=provider,
        transfer_service=service,
    )
    return httpx.ASGITransport(app=app), provider


def _client(transport: httpx.ASGITransport) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=transport, base_url="http://sandbox")


def _nonce(tag: str) -> str:
    # The verifier requires 32-128 chars from [A-Za-z0-9_-]; pad the readable tag.
    return (tag + "_" + "0" * 64)[:48]


async def test_upload_happy_path_returns_signed_response(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path)
    signer = RpcRequestSigner(_SECRET)
    verifier = RpcResponseVerifier(_SECRET)
    body = _archive({"a.txt": b"hello\n"})
    path = f"/v1/transfer/upload/{_NS}"
    headers = signer.headers(body, method="POST", path=path, nonce=_nonce("upload"))
    async with _client(transport) as client:
        resp = await client.post(path, content=body, headers=headers)
    assert resp.status_code == 200
    assert resp.json() == {
        "namespace": _NS,
        "files": 1,
        "total_bytes": 6,
        "cleanup_pending": False,
    }
    assert verifier.verify(
        resp.headers,
        resp.content,
        request_nonce=_nonce("upload"),
        request_body=body,
        path=path,
    )
    assert (tmp_path / "namespaces" / _NS / "a.txt").read_bytes() == b"hello\n"


async def test_missing_authentication_rejected(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path)
    body = _archive({"a.txt": b"x"})
    async with _client(transport) as client:
        resp = await client.post(f"/v1/transfer/upload/{_NS}", content=body)
    assert resp.status_code == 401


async def test_tampered_signature_rejected(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path)
    signer = RpcRequestSigner(_SECRET)
    body = _archive({"a.txt": b"x"})
    path = f"/v1/transfer/upload/{_NS}"
    headers = signer.headers(body, method="POST", path=path)
    headers["X-Keel-Rpc-Signature"] = "0" * 64
    async with _client(transport) as client:
        resp = await client.post(path, content=body, headers=headers)
    assert resp.status_code == 401


async def test_body_tamper_breaks_signature(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path)
    signer = RpcRequestSigner(_SECRET)
    body = _archive({"a.txt": b"x"})
    path = f"/v1/transfer/upload/{_NS}"
    headers = signer.headers(body, method="POST", path=path)
    async with _client(transport) as client:
        resp = await client.post(path, content=body + b"tampered", headers=headers)
    assert resp.status_code == 401


async def test_replayed_nonce_rejected(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path)
    signer = RpcRequestSigner(_SECRET)
    body = _archive({"a.txt": b"x"})
    path = f"/v1/transfer/upload/{_NS}"
    headers = signer.headers(body, method="POST", path=path, nonce=_nonce("replay"))
    async with _client(transport) as client:
        first = await client.post(path, content=body, headers=headers)
        second = await client.post(path, content=body, headers=dict(headers))
    assert first.status_code == 200
    assert second.status_code == 401


async def test_oversized_content_length_rejected_before_decompress(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path, bounds=SnapshotBounds(max_archive_bytes=64))
    signer = RpcRequestSigner(_SECRET)
    body = b"\x00" * 512  # larger than the 64-byte archive ceiling; content-length is set
    path = f"/v1/transfer/upload/{_NS}"
    headers = signer.headers(body, method="POST", path=path)
    async with _client(transport) as client:
        resp = await client.post(path, content=body, headers=headers)
    assert resp.status_code == 413


async def test_oversized_chunked_body_rejected(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path, bounds=SnapshotBounds(max_archive_bytes=64))
    signer = RpcRequestSigner(_SECRET)
    path = f"/v1/transfer/upload/{_NS}"
    # A chunked stream omits Content-Length; the route must cap it as bytes arrive.
    headers = signer.headers(b"", method="POST", path=path)

    async def _chunks() -> AsyncIterator[bytes]:
        for _ in range(10):
            yield b"\x00" * 32

    async with _client(transport) as client:
        resp = await client.post(path, content=_chunks(), headers=headers)
    assert resp.status_code == 413


async def test_isolation_not_verified_returns_503(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path, isolation_verified=False)
    signer = RpcRequestSigner(_SECRET)
    body = _archive({"a.txt": b"x"})
    path = f"/v1/transfer/upload/{_NS}"
    headers = signer.headers(body, method="POST", path=path)
    async with _client(transport) as client:
        resp = await client.post(path, content=body, headers=headers)
    assert resp.status_code == 503


async def test_forbidden_path_archive_rejected_422(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path)
    signer = RpcRequestSigner(_SECRET)
    body = _forbidden_archive()
    path = f"/v1/transfer/upload/{_NS}"
    headers = signer.headers(body, method="POST", path=path)
    async with _client(transport) as client:
        resp = await client.post(path, content=body, headers=headers)
    assert resp.status_code == 422
    assert not (tmp_path / "namespaces" / _NS).exists()


async def test_gzip_bomb_rejected_by_bounds(tmp_path: Path) -> None:
    transport, _ = _build_app(
        tmp_path, bounds=SnapshotBounds(max_total_bytes=1024, max_file_bytes=1024)
    )
    signer = RpcRequestSigner(_SECRET)
    # A tiny archive that inflates far beyond the uncompressed ceiling.
    body = _archive({"bomb.bin": b"\x00" * 1_000_000})
    path = f"/v1/transfer/upload/{_NS}"
    headers = signer.headers(body, method="POST", path=path)
    async with _client(transport) as client:
        resp = await client.post(path, content=body, headers=headers)
    assert resp.status_code == 413


async def test_export_roundtrips_and_signs(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path)
    signer = RpcRequestSigner(_SECRET)
    verifier = RpcResponseVerifier(_SECRET)
    upload_path = f"/v1/transfer/upload/{_NS}"
    body = _archive({"a.txt": b"data\n"})
    async with _client(transport) as client:
        await client.post(
            upload_path,
            content=body,
            headers=signer.headers(body, method="POST", path=upload_path),
        )
        export_path = f"/v1/transfer/export/{_NS}"
        headers = signer.headers(b"", method="POST", path=export_path, nonce=_nonce("export"))
        resp = await client.post(export_path, content=b"", headers=headers)
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/gzip"
    assert verifier.verify(
        resp.headers,
        resp.content,
        request_nonce=_nonce("export"),
        request_body=b"",
        path=export_path,
    )
    parsed = core_transfer.parse_snapshot_archive(resp.content)
    assert parsed.files["a.txt"].data == b"data\n"


async def test_export_missing_namespace_404(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path)
    signer = RpcRequestSigner(_SECRET)
    path = f"/v1/transfer/export/{_NS}"
    headers = signer.headers(b"", method="POST", path=path)
    async with _client(transport) as client:
        resp = await client.post(path, content=b"", headers=headers)
    assert resp.status_code == 404


async def test_delete_idempotent_and_signed(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path)
    signer = RpcRequestSigner(_SECRET)
    upload_path = f"/v1/transfer/upload/{_NS}"
    body = _archive({"a.txt": b"x"})
    async with _client(transport) as client:
        await client.post(
            upload_path,
            content=body,
            headers=signer.headers(body, method="POST", path=upload_path),
        )
        del_path = f"/v1/transfer/delete/{_NS}"
        first = await client.post(
            del_path, content=b"", headers=signer.headers(b"", method="POST", path=del_path)
        )
        second = await client.post(
            del_path, content=b"", headers=signer.headers(b"", method="POST", path=del_path)
        )
    assert first.status_code == 200 and first.json()["deleted"] is True
    assert second.status_code == 200 and second.json()["deleted"] is False


async def test_namespace_isolation_between_scopes(tmp_path: Path) -> None:
    transport, _ = _build_app(tmp_path)
    signer = RpcRequestSigner(_SECRET)
    ns_a = "ws_" + "a" * 32
    ns_b = "ws_" + "b" * 32
    async with _client(transport) as client:
        for ns, payload in ((ns_a, b"AAA"), (ns_b, b"BBB")):
            path = f"/v1/transfer/upload/{ns}"
            body = _archive({"f.txt": payload})
            await client.post(
                path, content=body, headers=signer.headers(body, method="POST", path=path)
            )
        exp_a = f"/v1/transfer/export/{ns_a}"
        resp = await client.post(
            exp_a, content=b"", headers=signer.headers(b"", method="POST", path=exp_a)
        )
    parsed = core_transfer.parse_snapshot_archive(resp.content)
    assert parsed.files["f.txt"].data == b"AAA"
    assert (tmp_path / "namespaces" / ns_a / "f.txt").read_bytes() == b"AAA"
    assert (tmp_path / "namespaces" / ns_b / "f.txt").read_bytes() == b"BBB"


async def test_upload_route_gates_concurrency_across_namespaces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The route holds a process-global transfer slot around the whole upload, so even concurrent
    # uploads to *different* namespaces cannot run their (memory-heavy) body-read + materialize
    # steps at the same time when the limit is 1.
    provider = DirectoryWorkspaceProvider(
        tmp_path / "namespaces",
        lambda root: UnsafeLocalDevExecutionEnvironment(root),
    )
    service = SandboxTransferService(provider, max_concurrent_transfers=1)
    state = {"current": 0, "max": 0}
    real_upload = service.upload

    async def recording_upload(namespace: str, archive: bytes) -> object:
        state["current"] += 1
        state["max"] = max(state["max"], state["current"])
        try:
            await asyncio.sleep(0.02)
            return await real_upload(namespace, archive)
        finally:
            state["current"] -= 1

    monkeypatch.setattr(service, "upload", recording_upload)
    app = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path / "default"),
        isolation_verified=True,
        shared_secret=_SECRET,
        workspace_provider=provider,
        transfer_service=service,
    )
    transport = httpx.ASGITransport(app=app)
    signer = RpcRequestSigner(_SECRET)
    body = _archive({"a.txt": b"x"})
    namespaces = [f"ws_{c * 32}" for c in "abcd"]

    async def do_upload(namespace: str) -> int:
        path = f"/v1/transfer/upload/{namespace}"
        headers = signer.headers(body, method="POST", path=path, nonce=_nonce(namespace[:12]))
        async with _client(transport) as client:
            resp = await client.post(path, content=body, headers=headers)
        return resp.status_code

    codes = await asyncio.gather(*(do_upload(ns) for ns in namespaces))
    assert all(code == 200 for code in codes)
    assert state["max"] == 1
