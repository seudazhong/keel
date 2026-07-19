"""Unit coverage for the RPC response HMAC signer/verifier (WS-PP, M4 P3a).

A response signature authenticates that a holder of the shared secret produced *this*
response for *this* request. It binds to the request nonce, method, path, and request-body
digest so a captured response cannot be replayed against a different request.
"""

from __future__ import annotations

import pytest

from keel_core.tools.rpc_auth import (
    RPC_NONCE_HEADER,
    RPC_RESPONSE_SIGNATURE_HEADER,
    RpcRequestSigner,
    RpcResponseSigner,
    RpcResponseVerifier,
)

_SECRET = "response-secret-" + "y" * 32
_PATH = "/v1/transfer/export"


def _signed_request(secret: str = _SECRET) -> tuple[bytes, str]:
    body = b'{"namespace":"ws_abc"}'
    headers = RpcRequestSigner(secret).headers(body, method="POST", path=_PATH)
    return body, headers[RPC_NONCE_HEADER]


def test_sign_then_verify_roundtrip() -> None:
    request_body, nonce = _signed_request()
    response_body = b"archive-bytes"
    signer = RpcResponseSigner(_SECRET)
    verifier = RpcResponseVerifier(_SECRET)
    headers = signer.headers(
        response_body, request_nonce=nonce, request_body=request_body, path=_PATH
    )
    assert RPC_RESPONSE_SIGNATURE_HEADER in headers
    assert verifier.verify(
        headers, response_body, request_nonce=nonce, request_body=request_body, path=_PATH
    )


def test_verify_rejects_tampered_response_body() -> None:
    request_body, nonce = _signed_request()
    headers = RpcResponseSigner(_SECRET).headers(
        b"original", request_nonce=nonce, request_body=request_body, path=_PATH
    )
    assert not RpcResponseVerifier(_SECRET).verify(
        headers, b"tampered", request_nonce=nonce, request_body=request_body, path=_PATH
    )


def test_verify_rejects_replay_against_different_request() -> None:
    request_body, nonce = _signed_request()
    response_body = b"archive-bytes"
    headers = RpcResponseSigner(_SECRET).headers(
        response_body, request_nonce=nonce, request_body=request_body, path=_PATH
    )
    # A different in-flight request has a different nonce; the captured signature must not verify.
    _, other_nonce = _signed_request()
    assert other_nonce != nonce
    assert not RpcResponseVerifier(_SECRET).verify(
        headers,
        response_body,
        request_nonce=other_nonce,
        request_body=request_body,
        path=_PATH,
    )


def test_verify_rejects_wrong_path_or_body_binding() -> None:
    request_body, nonce = _signed_request()
    response_body = b"archive-bytes"
    headers = RpcResponseSigner(_SECRET).headers(
        response_body, request_nonce=nonce, request_body=request_body, path=_PATH
    )
    verifier = RpcResponseVerifier(_SECRET)
    assert not verifier.verify(
        headers, response_body, request_nonce=nonce, request_body=request_body, path="/v1/other"
    )
    assert not verifier.verify(
        headers,
        response_body,
        request_nonce=nonce,
        request_body=b"different-request",
        path=_PATH,
    )


def test_verify_rejects_missing_header() -> None:
    request_body, nonce = _signed_request()
    assert not RpcResponseVerifier(_SECRET).verify(
        {}, b"body", request_nonce=nonce, request_body=request_body, path=_PATH
    )


def test_verify_rejects_wrong_secret() -> None:
    request_body, nonce = _signed_request()
    response_body = b"archive-bytes"
    headers = RpcResponseSigner(_SECRET).headers(
        response_body, request_nonce=nonce, request_body=request_body, path=_PATH
    )
    other_secret = "different-secret-" + "z" * 32
    assert not RpcResponseVerifier(other_secret).verify(
        headers, response_body, request_nonce=nonce, request_body=request_body, path=_PATH
    )


def test_unauthenticated_local_test_disables_signing() -> None:
    signer = RpcResponseSigner("", allow_unauthenticated_local_test=True)
    verifier = RpcResponseVerifier("", allow_unauthenticated_local_test=True)
    headers = signer.headers(b"body", request_nonce="n" * 32, request_body=b"req", path=_PATH)
    assert headers == {}
    assert verifier.verify(
        headers, b"body", request_nonce="n" * 32, request_body=b"req", path=_PATH
    )


def test_missing_secret_without_local_test_raises() -> None:
    with pytest.raises(RuntimeError):
        RpcResponseSigner("")
    with pytest.raises(RuntimeError):
        RpcResponseVerifier(None)
