"""Unit coverage for :class:`SandboxTransferClient` (M4 P3a).

Uses ``httpx.MockTransport`` to assert response-signature verification (tamper/replay fail closed),
strict HTTP status -> typed error mapping, response body bounds, and malformed-ack handling, plus a
real in-process ASGI round trip against the sandbox app for genuine interop.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from keel_core.patch import transfer as core_transfer
from keel_core.patch.transfer import SnapshotBounds, build_snapshot_archive
from keel_core.patch.transfer_client import (
    SandboxTransferAuthError,
    SandboxTransferClient,
    SandboxTransferNotFound,
    SandboxTransferProtocolError,
    SandboxTransferRejected,
    SandboxTransferUnavailable,
)
from keel_core.tools import UnsafeLocalDevExecutionEnvironment
from keel_core.tools.rpc_auth import RPC_NONCE_HEADER, RpcResponseSigner
from keel_sandbox.service import DirectoryWorkspaceProvider, create_app
from keel_sandbox.transfer import SandboxTransferService

_SECRET = "test-sandbox-rpc-secret-" + "x" * 32
_NS = "ws_" + "a" * 32
_RESP_SIGNER = RpcResponseSigner(_SECRET)


def _archive(files: dict[str, bytes]) -> bytes:
    snapshot = [
        core_transfer.SnapshotFile(path, data, False, b"\x00" in data[:8000])
        for path, data in files.items()
    ]
    archive, _ = build_snapshot_archive(snapshot)
    return archive


def _signed(
    request: httpx.Request,
    status: int,
    body: bytes,
    *,
    content_type: str = "application/json",
    sign_body: bytes | None = None,
    sign_nonce: str | None = None,
) -> httpx.Response:
    """Build a response signed like the real sandbox would sign it.

    ``sign_body``/``sign_nonce`` override what the signature is computed over, to simulate a
    tampered body (signature over different bytes) or a replayed response (signature over a
    different request nonce).
    """

    nonce = request.headers.get(RPC_NONCE_HEADER, "")
    sig_headers = _RESP_SIGNER.headers(
        body if sign_body is None else sign_body,
        request_nonce=nonce if sign_nonce is None else sign_nonce,
        request_body=request.content,
        method="POST",
        path=request.url.path,
    )
    return httpx.Response(
        status, content=body, headers={"content-type": content_type, **sig_headers}
    )


def _client(handler: object) -> SandboxTransferClient:
    transport = httpx.MockTransport(handler)  # type: ignore[arg-type]
    inner = httpx.AsyncClient(transport=transport, base_url="http://sandbox")
    return SandboxTransferClient("http://sandbox", shared_secret=_SECRET, client=inner)


async def test_upload_happy_path_verifies_and_parses(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = b'{"namespace":"%s","files":2,"total_bytes":9}' % _NS.encode()
        return _signed(request, 200, body)

    client = _client(handler)
    ack = await client.upload_snapshot(_NS, _archive({"a.txt": b"x"}))
    assert (ack.namespace, ack.files, ack.total_bytes) == (_NS, 2, 9)
    await client.aclose()


async def test_export_happy_path_returns_bytes() -> None:
    archive = _archive({"a.txt": b"data\n"})

    def handler(request: httpx.Request) -> httpx.Response:
        return _signed(request, 200, archive, content_type="application/gzip")

    client = _client(handler)
    result = await client.export_snapshot(_NS)
    assert core_transfer.parse_snapshot_archive(result).files["a.txt"].data == b"data\n"
    await client.aclose()


async def test_delete_happy_path() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return _signed(request, 200, b'{"namespace":"%s","deleted":true}' % _NS.encode())

    client = _client(handler)
    ack = await client.delete_namespace(_NS)
    assert ack.deleted is True and ack.namespace == _NS
    await client.aclose()


async def test_response_tamper_fails_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        # Signature computed over the honest body, but a tampered body is returned.
        honest = b'{"namespace":"%s","files":1,"total_bytes":1}' % _NS.encode()
        return _signed(request, 200, honest + b"TAMPER", sign_body=honest)

    client = _client(handler)
    with pytest.raises(SandboxTransferAuthError):
        await client.upload_snapshot(_NS, _archive({"a.txt": b"x"}))
    await client.aclose()


async def test_response_replay_fails_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = b'{"namespace":"%s","files":1,"total_bytes":1}' % _NS.encode()
        # Signature bound to a different nonce than our request -> replay from another call.
        return _signed(request, 200, body, sign_nonce="z" * 44)

    client = _client(handler)
    with pytest.raises(SandboxTransferAuthError):
        await client.upload_snapshot(_NS, _archive({"a.txt": b"x"}))
    await client.aclose()


async def test_missing_response_signature_fails_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = b'{"namespace":"%s","files":1,"total_bytes":1}' % _NS.encode()
        return httpx.Response(200, content=body, headers={"content-type": "application/json"})

    client = _client(handler)
    with pytest.raises(SandboxTransferAuthError):
        await client.upload_snapshot(_NS, _archive({"a.txt": b"x"}))
    await client.aclose()


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (401, SandboxTransferAuthError),
        (404, SandboxTransferNotFound),
        (400, SandboxTransferRejected),
        (413, SandboxTransferRejected),
        (422, SandboxTransferRejected),
        (503, SandboxTransferUnavailable),
        (500, SandboxTransferUnavailable),
        (502, SandboxTransferUnavailable),
        (418, SandboxTransferProtocolError),
    ],
)
async def test_status_maps_to_typed_error(status: int, error: type[Exception]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=b'{"detail":"x"}')

    client = _client(handler)
    with pytest.raises(error):
        await client.export_snapshot(_NS)
    await client.aclose()


async def test_rejected_carries_status_code() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, content=b"nope")

    client = _client(handler)
    with pytest.raises(SandboxTransferRejected) as excinfo:
        await client.export_snapshot(_NS)
    assert excinfo.value.status_code == 422
    await client.aclose()


async def test_oversized_response_body_rejected() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return _signed(request, 200, b"x" * 5000)  # ack ceiling is 4096 bytes

    client = _client(handler)
    with pytest.raises(SandboxTransferProtocolError):
        await client.delete_namespace(_NS)
    await client.aclose()


async def test_malformed_ack_rejected() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return _signed(request, 200, b"this is not json")

    client = _client(handler)
    with pytest.raises(SandboxTransferProtocolError):
        await client.upload_snapshot(_NS, _archive({"a.txt": b"x"}))
    await client.aclose()


async def test_connection_error_is_unavailable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    client = _client(handler)
    with pytest.raises(SandboxTransferUnavailable):
        await client.export_snapshot(_NS)
    await client.aclose()


async def test_timeout_is_unavailable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow")

    client = _client(handler)
    with pytest.raises(SandboxTransferUnavailable):
        await client.export_snapshot(_NS)
    await client.aclose()


@pytest.mark.parametrize("bad", ["nope", "ws_UPPER", "../x", "ws_", "WS_abc"])
async def test_client_side_namespace_validation(bad: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - never called
        raise AssertionError("must not send request for invalid namespace")

    client = _client(handler)
    with pytest.raises(SandboxTransferProtocolError):
        await client.upload_snapshot(bad, _archive({"a.txt": b"x"}))
    await client.aclose()


async def test_client_side_archive_bound() -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - never called
        raise AssertionError("must not send oversized archive")

    transport = httpx.MockTransport(handler)
    inner = httpx.AsyncClient(transport=transport, base_url="http://sandbox")
    client = SandboxTransferClient(
        "http://sandbox",
        shared_secret=_SECRET,
        client=inner,
        bounds=SnapshotBounds(max_archive_bytes=8),
    )
    with pytest.raises(SandboxTransferRejected):
        await client.upload_snapshot(_NS, b"x" * 64)
    await client.aclose()


async def test_real_asgi_round_trip(tmp_path: Path) -> None:
    provider = DirectoryWorkspaceProvider(
        tmp_path / "namespaces",
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
    archive = _archive({"a.txt": b"hello\n", "pkg/b.py": b"print(1)\n"})
    ack = await client.upload_snapshot(_NS, archive)
    assert ack.files == 2
    exported = await client.export_snapshot(_NS)
    parsed = core_transfer.parse_snapshot_archive(exported)
    assert parsed.files["a.txt"].data == b"hello\n"
    deleted = await client.delete_namespace(_NS)
    assert deleted.deleted is True
    with pytest.raises(SandboxTransferNotFound):
        await client.export_snapshot(_NS)
    await client.aclose()
