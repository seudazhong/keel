"""Credential classification for the request actor (M3.6 security review, #6/#8).

Exercises :func:`resolve_actor` directly: a valid dotted API key presented as a bearer must
not be mistaken for (and rejected as) a JWT; a malformed JWT must never be laundered into an
API-key/open-mode bypass; a real OIDC token resolves to a user; and an OIDC *provider*
outage fails closed with 503 rather than an uncontrolled 500 or a 401.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from oidc_helpers import make_rsa_key, sign_token

from keel_core.identity import (
    IdentityService,
    InMemoryIdentityStore,
    OIDCConfig,
    OIDCVerifier,
    StaticJWKSProvider,
)
from keel_core.identity.oidc import OIDCAvailabilityError
from keel_server.auth import parse_api_keys
from keel_server.identity_context import ActorKind, resolve_actor

_ISSUER = "https://issuer.example"
_AUD = "keel"
_KEY = make_rsa_key()


class _Headers:
    def __init__(self, data: dict[str, str]) -> None:
        self._data = {k.lower(): v for k, v in data.items()}

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._data.get(key.lower(), default)


def _request(
    *,
    headers: dict[str, str],
    verifier: OIDCVerifier | None = None,
    identity: IdentityService | None = None,
    api_keys: dict | None = None,
    auth_required: bool = False,
) -> SimpleNamespace:
    state = SimpleNamespace(
        oidc_verifier=verifier,
        identity=identity,
        api_keys=api_keys or {},
        auth_required=auth_required,
    )
    return SimpleNamespace(headers=_Headers(headers), app=SimpleNamespace(state=state))


def _verifier(provider=None) -> OIDCVerifier:
    return OIDCVerifier(
        OIDCConfig(issuer=_ISSUER, audiences=frozenset({_AUD})),
        provider or StaticJWKSProvider([_KEY.jwk]),
    )


def _token(subject: str = "alice") -> str:
    return sign_token(_KEY, issuer=_ISSUER, audience=_AUD, subject=subject)


async def test_dotted_api_key_bearer_is_not_rejected_as_jwt() -> None:
    # A perfectly valid API key that happens to have three dotted segments.
    api_keys = parse_api_keys("abc.def.ghi:operator")
    service = IdentityService(InMemoryIdentityStore())
    req = _request(
        headers={"Authorization": "Bearer abc.def.ghi"},
        verifier=_verifier(),
        identity=service,
        api_keys=api_keys,
    )
    actor = await resolve_actor(req)
    assert actor.kind is ActorKind.machine
    assert actor.user_id is None


async def test_malformed_jwt_is_not_an_api_key_bypass() -> None:
    # Looks like a JWT, fails verification, and is NOT a configured key -> 401 (never a
    # fall-through to the open-mode implicit admin, even with no keys configured).
    service = IdentityService(InMemoryIdentityStore())
    req = _request(
        headers={"Authorization": "Bearer aaa.bbb.ccc"},
        verifier=_verifier(),
        identity=service,
        api_keys={},
    )
    with pytest.raises(HTTPException) as exc:
        await resolve_actor(req)
    assert exc.value.status_code == 401


async def test_real_oidc_token_resolves_to_user() -> None:
    service = IdentityService(InMemoryIdentityStore(), allow_jit_provisioning=True)
    req = _request(
        headers={"Authorization": f"Bearer {_token('alice')}"},
        verifier=_verifier(),
        identity=service,
    )
    actor = await resolve_actor(req)
    assert actor.kind is ActorKind.user
    assert actor.user_id is not None
    assert actor.oidc_subject == "alice"


async def test_provider_outage_fails_closed_503() -> None:
    class _Down:
        async def get_keys(self, *, force_refresh: bool = False):
            raise OIDCAvailabilityError("jwks down")

    service = IdentityService(InMemoryIdentityStore(), allow_jit_provisioning=True)
    req = _request(
        headers={"Authorization": f"Bearer {_token('bob')}"},
        verifier=_verifier(_Down()),
        identity=service,
    )
    with pytest.raises(HTTPException) as exc:
        await resolve_actor(req)
    assert exc.value.status_code == 503


async def test_explicit_x_api_key_header_takes_api_key_path() -> None:
    api_keys = parse_api_keys("plainkey123:admin")
    service = IdentityService(InMemoryIdentityStore())
    req = _request(
        headers={"X-API-Key": "plainkey123"},
        verifier=_verifier(),
        identity=service,
        api_keys=api_keys,
    )
    actor = await resolve_actor(req)
    assert actor.kind is ActorKind.machine


async def test_open_mode_no_bearer_is_local_operator() -> None:
    service = IdentityService(InMemoryIdentityStore())
    req = _request(headers={}, verifier=_verifier(), identity=service, api_keys={})
    actor = await resolve_actor(req)
    assert actor.kind is ActorKind.local
