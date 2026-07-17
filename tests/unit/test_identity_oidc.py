"""OIDC JWT verification: valid, expired, wrong issuer/aud, unknown kid, rotation (M3.6)."""

from __future__ import annotations

import time

import pytest
from oidc_helpers import make_ec_key, make_rsa_key, sign_token

from keel_core.identity import (
    OIDCConfig,
    OIDCVerificationError,
    OIDCVerifier,
    StaticJWKSProvider,
)
from keel_core.identity.oidc import JWKSProvider


def _verifier(*keys, audiences=("keel",), issuer="https://issuer.example") -> OIDCVerifier:
    provider = StaticJWKSProvider([k.jwk for k in keys])
    config = OIDCConfig(issuer=issuer, audiences=frozenset(audiences))
    return OIDCVerifier(config, provider)


async def test_valid_token_returns_claims() -> None:
    key = make_rsa_key()
    verifier = _verifier(key)
    claims = await verifier.verify(sign_token(key, subject="abc", email="a@b.com"))
    assert claims.subject == "abc"
    assert claims.email == "a@b.com"
    assert claims.email_verified is True
    assert claims.issuer == "https://issuer.example"


async def test_ec_token_supported() -> None:
    key = make_ec_key()
    verifier = _verifier(key)
    claims = await verifier.verify(sign_token(key))
    assert claims.subject == "subject-123"


async def test_expired_token_rejected() -> None:
    key = make_rsa_key()
    verifier = _verifier(key)
    token = sign_token(key, now=int(time.time()) - 10_000, lifetime=100)
    with pytest.raises(OIDCVerificationError):
        await verifier.verify(token)


async def test_not_yet_valid_token_rejected() -> None:
    key = make_rsa_key()
    verifier = _verifier(key)
    token = sign_token(key, nbf=int(time.time()) + 10_000)
    with pytest.raises(OIDCVerificationError):
        await verifier.verify(token)


async def test_wrong_issuer_rejected() -> None:
    key = make_rsa_key()
    verifier = _verifier(key)
    with pytest.raises(OIDCVerificationError):
        await verifier.verify(sign_token(key, issuer="https://evil.example"))


async def test_wrong_audience_rejected() -> None:
    key = make_rsa_key()
    verifier = _verifier(key)
    with pytest.raises(OIDCVerificationError):
        await verifier.verify(sign_token(key, audience="some-other-app"))


async def test_unknown_kid_rejected() -> None:
    signing = make_rsa_key(kid="k1")
    other = make_rsa_key(kid="k2")
    verifier = _verifier(other)  # JWKS only has k2
    with pytest.raises(OIDCVerificationError):
        await verifier.verify(sign_token(signing))


async def test_hs256_alg_confusion_rejected() -> None:
    import jwt as pyjwt

    key = make_rsa_key()
    verifier = _verifier(key)
    forged = pyjwt.encode(
        {
            "iss": "https://issuer.example",
            "sub": "x",
            "aud": "keel",
            "iat": int(time.time()),
            "exp": int(time.time()) + 300,
        },
        "public-knowledge",
        algorithm="HS256",
        headers={"kid": key.kid},
    )
    with pytest.raises(OIDCVerificationError):
        await verifier.verify(forged)


async def test_missing_exp_rejected() -> None:
    from oidc_helpers import OMIT

    key = make_rsa_key()
    verifier = _verifier(key)
    with pytest.raises(OIDCVerificationError):
        await verifier.verify(sign_token(key, exp=OMIT))


async def test_key_rotation_refreshes_jwks_once() -> None:
    old = make_rsa_key(kid="old")
    new = make_rsa_key(kid="new")

    class RotatingProvider:
        """Serves the old key until a forced refresh, then the rotated key."""

        def __init__(self) -> None:
            self.calls = 0
            self.rotated = False

        async def get_keys(self, *, force_refresh: bool = False):
            self.calls += 1
            if force_refresh:
                self.rotated = True
            return [new.jwk] if self.rotated else [old.jwk]

    provider: JWKSProvider = RotatingProvider()
    verifier = OIDCVerifier(
        OIDCConfig(issuer="https://issuer.example", audiences=frozenset({"keel"})),
        provider,
    )
    # Token signed with the rotated key: first JWKS lookup misses, a single forced
    # refresh brings in the new key and verification then succeeds.
    claims = await verifier.verify(sign_token(new, subject="rotated"))
    assert claims.subject == "rotated"
    assert isinstance(provider, RotatingProvider) and provider.rotated
