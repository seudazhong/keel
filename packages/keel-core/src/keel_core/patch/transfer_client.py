"""Authenticated client for the sandbox snapshot transfer service (WS-PP, M4 P3a).

:class:`SandboxTransferClient` is the control-plane/worker counterpart of
:mod:`keel_sandbox.transfer`. It signs each upload/export/delete request with the shared RPC
secret, **verifies the keyed response signature** bound to its own request nonce before trusting
any bytes (fail closed), bounds the response body it will buffer, and maps HTTP status codes to
strict typed errors. It never uses a broad ``except`` and surfaces cleanup failures rather than
swallowing them.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TypeGuard

import httpx

from keel_core.patch.transfer import DEFAULT_SNAPSHOT_BOUNDS, SnapshotBounds
from keel_core.tools.rpc_auth import (
    RPC_NONCE_HEADER,
    RpcRequestSigner,
    RpcResponseVerifier,
)

# The opaque, validated per-scope workspace namespace token.
_WORKSPACE_NAMESPACE = re.compile(r"^ws_[0-9a-f]{1,64}$")

# Upload/delete acknowledgements are tiny JSON documents; never buffer more than this.
_MAX_ACK_BYTES = 4096

_DEFAULT_TIMEOUT_SECONDS = 120.0


def _is_int(value: object) -> TypeGuard[int]:
    # JSON booleans decode to ``bool`` (an ``int`` subclass); reject them for numeric fields.
    return isinstance(value, int) and not isinstance(value, bool)


class SandboxTransferClientError(Exception):
    """Base class for sandbox transfer client failures."""


class SandboxTransferAuthError(SandboxTransferClientError):
    """The sandbox rejected our authentication, or its response failed verification.

    Raised fail-closed: an unverifiable response is treated as hostile, never trusted.
    """


class SandboxTransferRejected(SandboxTransferClientError):
    """The sandbox permanently rejected the request (bad namespace/bounds/policy)."""

    def __init__(self, message: str, *, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class SandboxTransferNotFound(SandboxTransferClientError):
    """The requested namespace does not exist (export of an unknown namespace)."""


class SandboxTransferUnavailable(SandboxTransferClientError):
    """A transient failure (timeout, connection error, 5xx, isolation not ready) — retryable."""


class SandboxTransferProtocolError(SandboxTransferClientError):
    """The sandbox response was malformed, oversized, or otherwise unusable."""


@dataclass(frozen=True, slots=True)
class UploadAck:
    namespace: str
    files: int
    total_bytes: int
    # The sandbox committed the new tree but deferred removing the previous tree's backup; the
    # upload succeeded and must not be retried. Optional for backward compatibility (older
    # sandboxes omit it), defaulting to False.
    cleanup_pending: bool = False


@dataclass(frozen=True, slots=True)
class DeleteAck:
    namespace: str
    deleted: bool


class SandboxTransferClient:
    """Signed, response-verified HTTP client for sandbox snapshot upload/export/delete."""

    def __init__(
        self,
        base_url: str,
        *,
        shared_secret: str | None = None,
        allow_unauthenticated_local_test: bool = False,
        client: httpx.AsyncClient | None = None,
        bounds: SnapshotBounds = DEFAULT_SNAPSHOT_BOUNDS,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        if not base_url:
            raise ValueError("sandbox base URL is required")
        self._signer = RpcRequestSigner(
            shared_secret,
            allow_unauthenticated_local_test=allow_unauthenticated_local_test,
        )
        self._response_verifier = RpcResponseVerifier(
            shared_secret,
            allow_unauthenticated_local_test=allow_unauthenticated_local_test,
        )
        self._bounds = bounds
        self._timeout = timeout_seconds
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"), timeout=timeout_seconds
        )

    async def upload_snapshot(self, namespace: str, archive: bytes) -> UploadAck:
        ns = self._validate_namespace(namespace)
        if not isinstance(archive, (bytes, bytearray)):
            raise SandboxTransferProtocolError("archive must be bytes")
        payload = bytes(archive)
        if len(payload) > self._bounds.max_archive_bytes:
            raise SandboxTransferRejected(
                "snapshot archive exceeds compressed bound", status_code=413
            )
        content = await self._send(
            f"/v1/transfer/upload/{ns}",
            body=payload,
            content_type="application/gzip",
            max_response_bytes=_MAX_ACK_BYTES,
        )
        return self._parse_upload_ack(content)

    async def export_snapshot(self, namespace: str) -> bytes:
        ns = self._validate_namespace(namespace)
        return await self._send(
            f"/v1/transfer/export/{ns}",
            body=b"",
            content_type="application/octet-stream",
            max_response_bytes=self._bounds.max_archive_bytes,
        )

    async def delete_namespace(self, namespace: str) -> DeleteAck:
        ns = self._validate_namespace(namespace)
        content = await self._send(
            f"/v1/transfer/delete/{ns}",
            body=b"",
            content_type="application/octet-stream",
            max_response_bytes=_MAX_ACK_BYTES,
        )
        return self._parse_delete_ack(ns, content)

    async def aclose(self) -> None:
        # Surface cleanup failures explicitly (a caller may log/aggregate) rather than swallow.
        if self._owns_client:
            await self._client.aclose()

    # -- internals -------------------------------------------------------------------

    @staticmethod
    def _validate_namespace(namespace: object) -> str:
        if not isinstance(namespace, str) or not _WORKSPACE_NAMESPACE.match(namespace):
            raise SandboxTransferProtocolError("invalid workspace namespace")
        return namespace

    async def _send(
        self,
        path: str,
        *,
        body: bytes,
        content_type: str,
        max_response_bytes: int,
    ) -> bytes:
        headers = self._signer.headers(body, method="POST", path=path)
        headers["Content-Type"] = content_type
        nonce = headers.get(RPC_NONCE_HEADER, "")
        request = self._client.build_request(
            "POST", path, content=body, headers=headers, timeout=self._timeout
        )
        try:
            response = await self._client.send(request, stream=True)
        except httpx.TimeoutException as exc:
            raise SandboxTransferUnavailable("sandbox transfer timed out") from exc
        except httpx.HTTPError as exc:
            raise SandboxTransferUnavailable("sandbox transfer connection failed") from exc
        try:
            content = await self._read_bounded_response(response, max_response_bytes)
        finally:
            await response.aclose()
        self._raise_for_status(response.status_code)
        if not self._response_verifier.verify(
            response.headers,
            content,
            request_nonce=nonce,
            request_body=body,
            method="POST",
            path=path,
        ):
            raise SandboxTransferAuthError("sandbox response authentication failed")
        return content

    @staticmethod
    async def _read_bounded_response(response: httpx.Response, max_bytes: int) -> bytes:
        declared = response.headers.get("content-length")
        if declared is not None:
            try:
                length = int(declared)
            except ValueError as exc:
                raise SandboxTransferProtocolError("invalid response content-length") from exc
            if length < 0 or length > max_bytes:
                raise SandboxTransferProtocolError("sandbox response exceeds bound")
        buffer = bytearray()
        async for chunk in response.aiter_bytes():
            buffer.extend(chunk)
            if len(buffer) > max_bytes:
                raise SandboxTransferProtocolError("sandbox response exceeds bound")
        return bytes(buffer)

    @staticmethod
    def _raise_for_status(status_code: int) -> None:
        if status_code == 200:
            return
        if status_code == 401:
            raise SandboxTransferAuthError("sandbox rejected request authentication")
        if status_code == 404:
            raise SandboxTransferNotFound("sandbox namespace not found")
        if status_code in (400, 413, 422):
            raise SandboxTransferRejected(
                f"sandbox rejected transfer ({status_code})", status_code=status_code
            )
        if status_code == 503 or status_code >= 500:
            raise SandboxTransferUnavailable(f"sandbox transfer unavailable ({status_code})")
        raise SandboxTransferProtocolError(f"unexpected sandbox status {status_code}")

    @staticmethod
    def _parse_upload_ack(content: bytes) -> UploadAck:
        data = SandboxTransferClient._decode_json_object(content)
        try:
            namespace = data["namespace"]
            files = data["files"]
            total_bytes = data["total_bytes"]
        except KeyError as exc:
            raise SandboxTransferProtocolError("malformed upload acknowledgement") from exc
        if not isinstance(namespace, str) or not _is_int(files) or not _is_int(total_bytes):
            raise SandboxTransferProtocolError("malformed upload acknowledgement")
        cleanup_pending = data.get("cleanup_pending", False)
        if not isinstance(cleanup_pending, bool):
            raise SandboxTransferProtocolError("malformed upload acknowledgement")
        return UploadAck(
            namespace=namespace,
            files=files,
            total_bytes=total_bytes,
            cleanup_pending=cleanup_pending,
        )

    @staticmethod
    def _parse_delete_ack(namespace: str, content: bytes) -> DeleteAck:
        data = SandboxTransferClient._decode_json_object(content)
        try:
            deleted = data["deleted"]
        except KeyError as exc:
            raise SandboxTransferProtocolError("malformed delete acknowledgement") from exc
        if not isinstance(deleted, bool):
            raise SandboxTransferProtocolError("malformed delete acknowledgement")
        return DeleteAck(namespace=namespace, deleted=deleted)

    @staticmethod
    def _decode_json_object(content: bytes) -> dict[str, object]:
        try:
            data = json.loads(content)
        except json.JSONDecodeError as exc:
            raise SandboxTransferProtocolError("sandbox returned invalid JSON") from exc
        if not isinstance(data, dict):
            raise SandboxTransferProtocolError("sandbox returned an unexpected payload")
        return data


__all__ = [
    "DeleteAck",
    "SandboxTransferAuthError",
    "SandboxTransferClient",
    "SandboxTransferClientError",
    "SandboxTransferNotFound",
    "SandboxTransferProtocolError",
    "SandboxTransferRejected",
    "SandboxTransferUnavailable",
    "UploadAck",
]
