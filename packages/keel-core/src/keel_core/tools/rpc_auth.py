"""HMAC request authentication for the sandbox execution RPC."""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import time
from collections.abc import Callable, Mapping

RPC_TIMESTAMP_HEADER = "X-Keel-Rpc-Timestamp"
RPC_NONCE_HEADER = "X-Keel-Rpc-Nonce"
RPC_SIGNATURE_HEADER = "X-Keel-Rpc-Signature"
RPC_RESPONSE_SIGNATURE_HEADER = "X-Keel-Rpc-Response-Signature"
MIN_RPC_SECRET_BYTES = 32
DEFAULT_REPLAY_WINDOW_SECONDS = 60
DEFAULT_NONCE_CACHE_SIZE = 10_000

_NONCE = re.compile(r"^[A-Za-z0-9_-]{32,128}$")


def _secret_bytes(secret: str | None, *, allow_unauthenticated_local_test: bool) -> bytes | None:
    if not secret:
        if allow_unauthenticated_local_test:
            return None
        raise RuntimeError("sandbox RPC shared secret is required")
    encoded = secret.encode("utf-8")
    if len(encoded) < MIN_RPC_SECRET_BYTES:
        raise RuntimeError(
            f"sandbox RPC shared secret must be at least {MIN_RPC_SECRET_BYTES} bytes"
        )
    return encoded


def _signature_payload(
    method: str,
    path: str,
    timestamp: str,
    nonce: str,
    body: bytes,
) -> bytes:
    body_digest = hashlib.sha256(body).hexdigest()
    return "\n".join((method.upper(), path, timestamp, nonce, body_digest)).encode("utf-8")


def _response_signature_payload(
    method: str,
    path: str,
    request_nonce: str,
    request_body: bytes,
    response_body: bytes,
) -> bytes:
    """Bind the response to the exact request it answers.

    The ``response`` domain prefix separates this payload from request
    signatures, and including the request nonce plus the request body digest
    ties a signature to a single in-flight request so a valid response cannot
    be replayed against a different request.
    """

    request_digest = hashlib.sha256(request_body).hexdigest()
    response_digest = hashlib.sha256(response_body).hexdigest()
    return "\n".join(
        (
            "response",
            method.upper(),
            path,
            request_nonce,
            request_digest,
            response_digest,
        )
    ).encode("utf-8")


class RpcRequestSigner:
    """Sign requests while keeping the shared secret out of headers and payloads."""

    def __init__(
        self,
        secret: str | None,
        *,
        allow_unauthenticated_local_test: bool = False,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._secret = _secret_bytes(
            secret,
            allow_unauthenticated_local_test=allow_unauthenticated_local_test,
        )
        self._clock = clock

    def headers(
        self,
        body: bytes,
        *,
        method: str = "POST",
        path: str = "/v1/execute",
        timestamp: int | None = None,
        nonce: str | None = None,
    ) -> dict[str, str]:
        if self._secret is None:
            return {}
        timestamp_text = str(int(self._clock()) if timestamp is None else timestamp)
        nonce_text = nonce or secrets.token_urlsafe(32)
        signature = hmac.new(
            self._secret,
            _signature_payload(method, path, timestamp_text, nonce_text, body),
            hashlib.sha256,
        ).hexdigest()
        return {
            RPC_TIMESTAMP_HEADER: timestamp_text,
            RPC_NONCE_HEADER: nonce_text,
            RPC_SIGNATURE_HEADER: signature,
        }


class RpcRequestVerifier:
    """Verify request binding and reject stale or replayed nonces."""

    def __init__(
        self,
        secret: str | None,
        *,
        allow_unauthenticated_local_test: bool = False,
        replay_window_seconds: int = DEFAULT_REPLAY_WINDOW_SECONDS,
        nonce_cache_size: int = DEFAULT_NONCE_CACHE_SIZE,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if replay_window_seconds <= 0:
            raise ValueError("replay_window_seconds must be positive")
        if nonce_cache_size <= 0:
            raise ValueError("nonce_cache_size must be positive")
        self._secret = _secret_bytes(
            secret,
            allow_unauthenticated_local_test=allow_unauthenticated_local_test,
        )
        self._window = replay_window_seconds
        self._cache_size = nonce_cache_size
        self._clock = clock
        self._nonces: dict[str, int] = {}

    def verify(
        self,
        headers: Mapping[str, str],
        body: bytes,
        *,
        method: str,
        path: str,
    ) -> bool:
        if self._secret is None:
            return True
        timestamp_text = headers.get(RPC_TIMESTAMP_HEADER, "")
        nonce = headers.get(RPC_NONCE_HEADER, "")
        provided = headers.get(RPC_SIGNATURE_HEADER, "")
        try:
            timestamp = int(timestamp_text)
        except ValueError:
            return False
        now = int(self._clock())
        if abs(now - timestamp) > self._window or not _NONCE.fullmatch(nonce):
            return False
        expected = hmac.new(
            self._secret,
            _signature_payload(method, path, timestamp_text, nonce, body),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, provided):
            return False

        cutoff = now - self._window
        self._nonces = {
            cached_nonce: seen_at
            for cached_nonce, seen_at in self._nonces.items()
            if seen_at >= cutoff
        }
        if nonce in self._nonces:
            return False
        if len(self._nonces) >= self._cache_size:
            return False
        self._nonces[nonce] = timestamp
        return True


class RpcResponseSigner:
    """Sign RPC responses so the caller can authenticate their integrity.

    The signature is bound to the request nonce, method, path, and request body
    digest as well as the response body digest. This proves the response was
    produced by a holder of the shared secret for this specific request, and it
    prevents a captured response from being replayed against a different request
    (whose nonce differs).
    """

    def __init__(
        self,
        secret: str | None,
        *,
        allow_unauthenticated_local_test: bool = False,
    ) -> None:
        self._secret = _secret_bytes(
            secret,
            allow_unauthenticated_local_test=allow_unauthenticated_local_test,
        )

    def headers(
        self,
        response_body: bytes,
        *,
        request_nonce: str,
        request_body: bytes,
        method: str = "POST",
        path: str,
    ) -> dict[str, str]:
        if self._secret is None:
            return {}
        signature = hmac.new(
            self._secret,
            _response_signature_payload(method, path, request_nonce, request_body, response_body),
            hashlib.sha256,
        ).hexdigest()
        return {RPC_RESPONSE_SIGNATURE_HEADER: signature}


class RpcResponseVerifier:
    """Verify that an RPC response was signed for the caller's own request."""

    def __init__(
        self,
        secret: str | None,
        *,
        allow_unauthenticated_local_test: bool = False,
    ) -> None:
        self._secret = _secret_bytes(
            secret,
            allow_unauthenticated_local_test=allow_unauthenticated_local_test,
        )

    def verify(
        self,
        headers: Mapping[str, str],
        response_body: bytes,
        *,
        request_nonce: str,
        request_body: bytes,
        method: str = "POST",
        path: str,
    ) -> bool:
        if self._secret is None:
            return True
        provided = headers.get(RPC_RESPONSE_SIGNATURE_HEADER, "")
        if not provided:
            return False
        expected = hmac.new(
            self._secret,
            _response_signature_payload(method, path, request_nonce, request_body, response_body),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, provided)
