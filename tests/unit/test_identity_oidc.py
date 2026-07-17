"""OIDC JWT verification: valid, expired, wrong issuer/aud, unknown kid, rotation (M3.6).

Also covers the hardened controls: a hard-coded asymmetric algorithm allowlist, multi-audience
``azp`` binding, unknown-kid amplification bounds (coalescing + min interval + negative cache),
and typed provider-availability vs invalid-token errors.
"""

from __future__ import annotations

import asyncio
import time

import pytest
from oidc_helpers import make_ec_key, make_rsa_key, sign_token

from keel_core.identity import (
    OIDCAvailabilityError,
    OIDCConfig,
    OIDCVerificationError,
    OIDCVerifier,
    StaticJWKSProvider,
)
from keel_core.identity.oidc import HTTPJWKSProvider, JWKSProvider


def _verifier(
    *keys, audiences=("keel",), issuer="https://issuer.example", **kwargs
) -> OIDCVerifier:
    provider = StaticJWKSProvider([k.jwk for k in keys])
    config = OIDCConfig(issuer=issuer, audiences=frozenset(audiences), **kwargs)
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


# --- hard-coded asymmetric allowlist (#4) --------------------------------------------


def test_config_rejects_symmetric_and_none_algorithms() -> None:
    aud = frozenset({"keel"})
    for bad in (("HS256",), ("none",), ("RS256", "HS512"), ("dir",)):
        with pytest.raises(ValueError):
            OIDCConfig(issuer="https://issuer.example", audiences=aud, algorithms=bad)
    # The settings factory is guarded too (an operator cannot enable HS* via env config).
    with pytest.raises(ValueError):
        OIDCConfig.from_settings(
            issuer="https://issuer.example", audience="keel", algorithms=["HS256"]
        )


def test_verifier_rejects_non_asymmetric_config() -> None:
    # Even if a config object were built bypassing __post_init__, the verifier refuses it.
    cfg = OIDCConfig(issuer="https://issuer.example", audiences=frozenset({"keel"}))
    object.__setattr__(cfg, "algorithms", ("HS256",))
    with pytest.raises(ValueError):
        OIDCVerifier(cfg, StaticJWKSProvider([]))


async def test_ps256_supported() -> None:
    key = make_rsa_key(kid="ps", alg="PS256")
    verifier = _verifier(key)
    claims = await verifier.verify(sign_token(key))
    assert claims.subject == "subject-123"


async def test_attacker_hs256_rejected_by_default_verifier() -> None:
    import jwt as pyjwt

    key = make_rsa_key()
    verifier = _verifier(key)  # safe asymmetric defaults
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


# --- multi-audience azp binding (#5) -------------------------------------------------


async def test_multi_audience_requires_matching_azp() -> None:
    key = make_rsa_key()
    verifier = _verifier(key, audiences=("keel", "other"), client_id="keel")
    claims = await verifier.verify(sign_token(key, aud=["keel", "other"], azp="keel"))
    assert claims.subject == "subject-123"
    # Missing azp on a multi-audience token is rejected.
    with pytest.raises(OIDCVerificationError):
        await verifier.verify(sign_token(key, aud=["keel", "other"]))
    # A mismatched azp (token minted for a different relying party) is rejected.
    with pytest.raises(OIDCVerificationError):
        await verifier.verify(sign_token(key, aud=["keel", "other"], azp="attacker"))


async def test_multi_audience_without_client_id_rejected() -> None:
    key = make_rsa_key()
    verifier = _verifier(key, audiences=("keel", "other"))  # no client_id configured
    with pytest.raises(OIDCVerificationError):
        await verifier.verify(sign_token(key, aud=["keel", "other"], azp="keel"))


async def test_single_audience_does_not_require_azp() -> None:
    key = make_rsa_key()
    verifier = _verifier(key, audiences=("keel",), client_id="keel")
    claims = await verifier.verify(sign_token(key, audience="keel"))
    assert claims.subject == "subject-123"


def test_from_settings_derives_client_id_from_single_audience() -> None:
    cfg = OIDCConfig.from_settings(issuer="https://issuer.example", audience="keel")
    assert cfg.client_id == "keel"
    multi = OIDCConfig.from_settings(issuer="https://issuer.example", audience="a,b")
    assert multi.client_id is None  # ambiguous -> must be configured explicitly


# --- unknown-kid amplification bounds (#7) -------------------------------------------


class _CountingProvider:
    """A static JWKS that counts (forced) refresh calls."""

    def __init__(self, keys: list[dict]) -> None:
        self.keys = keys
        self.calls = 0
        self.force_calls = 0

    async def get_keys(self, *, force_refresh: bool = False) -> list[dict]:
        self.calls += 1
        if force_refresh:
            self.force_calls += 1
        return list(self.keys)


class _FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, delta: float) -> None:
        self.t += delta


async def test_repeated_unknown_kid_hits_negative_cache() -> None:
    known = make_rsa_key(kid="known")
    unknown = make_rsa_key(kid="ghost")
    provider = _CountingProvider([known.jwk])
    clock = _FakeClock()
    verifier = OIDCVerifier(
        OIDCConfig(issuer="https://issuer.example", audiences=frozenset({"keel"})),
        provider,
        clock=clock,
        negative_cache_ttl_seconds=1000,
    )
    for _ in range(6):
        with pytest.raises(OIDCVerificationError):
            await verifier.verify(sign_token(unknown))
    # Only the FIRST miss triggered a forced refresh; the negative cache absorbed the rest.
    assert provider.force_calls == 1


async def test_concurrent_unknown_kid_misses_do_not_fan_out(monkeypatch) -> None:
    known = make_rsa_key(kid="known")
    unknown = make_rsa_key(kid="ghost")
    clock = _FakeClock()
    provider = _HTTPProviderStub([known.jwk], clock=clock, min_refresh_interval_seconds=1000)
    verifier = OIDCVerifier(
        OIDCConfig(issuer="https://issuer.example", audiences=frozenset({"keel"})),
        provider,
        clock=clock,
        negative_cache_ttl_seconds=1000,
    )

    async def attempt() -> None:
        with pytest.raises(OIDCVerificationError):
            await verifier.verify(sign_token(unknown))

    await asyncio.gather(*(attempt() for _ in range(12)))
    # Coalescing + the min refresh interval bound the outbound fetches to a single call.
    assert provider.fetches == 1


async def test_key_rotation_recovers_after_min_interval() -> None:
    old = make_rsa_key(kid="old")
    new = make_rsa_key(kid="new")
    clock = _FakeClock()
    provider = _HTTPProviderStub([old.jwk], clock=clock, min_refresh_interval_seconds=60)
    verifier = OIDCVerifier(
        OIDCConfig(issuer="https://issuer.example", audiences=frozenset({"keel"})),
        provider,
        clock=clock,
        negative_cache_ttl_seconds=30,
    )
    # First contact with the (future) rotated key: unknown -> one forced fetch -> still miss.
    with pytest.raises(OIDCVerificationError):
        await verifier.verify(sign_token(new, subject="rotated"))
    # Provider rotates its published keys; advance past both the negative-cache TTL and the
    # provider's min refresh interval so the next miss re-fetches and recovers.
    provider.current = [new.jwk]
    clock.advance(120)
    claims = await verifier.verify(sign_token(new, subject="rotated"))
    assert claims.subject == "rotated"


# --- typed provider availability vs invalid token (#8) -------------------------------


async def test_provider_availability_error_propagates() -> None:
    class _Down:
        async def get_keys(self, *, force_refresh: bool = False):
            raise OIDCAvailabilityError("jwks endpoint down")

    verifier = OIDCVerifier(
        OIDCConfig(issuer="https://issuer.example", audiences=frozenset({"keel"})),
        _Down(),
    )
    with pytest.raises(OIDCAvailabilityError):
        await verifier.verify(sign_token(make_rsa_key()))


async def test_http_provider_wraps_transport_errors(monkeypatch) -> None:
    import httpx

    provider = HTTPJWKSProvider("https://issuer.example/jwks")

    class _BoomClient:
        def __init__(self, *a, **k) -> None: ...

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url):
            raise httpx.ConnectError("boom")

    monkeypatch.setattr(httpx, "AsyncClient", _BoomClient)
    with pytest.raises(OIDCAvailabilityError):
        await provider.get_keys(force_refresh=True)


async def test_http_provider_wraps_bad_json(monkeypatch) -> None:
    import httpx

    provider = HTTPJWKSProvider("https://issuer.example/jwks")

    class _Resp:
        def raise_for_status(self) -> None: ...

        def json(self):
            raise ValueError("not json")

    class _Client:
        def __init__(self, *a, **k) -> None: ...

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url):
            return _Resp()

    monkeypatch.setattr(httpx, "AsyncClient", _Client)
    with pytest.raises(OIDCAvailabilityError):
        await provider.get_keys(force_refresh=True)


class _HTTPProviderStub(HTTPJWKSProvider):
    """An :class:`HTTPJWKSProvider` whose network ``_fetch`` is replaced by an in-memory,
    call-counting stub (keeps the real coalescing / min-interval logic under test)."""

    def __init__(self, keys: list[dict], **kwargs) -> None:
        super().__init__("https://issuer.example/jwks", **kwargs)
        self.current = list(keys)
        self.fetches = 0

    async def _fetch(self) -> list[dict]:
        self.fetches += 1
        await asyncio.sleep(0.01)  # widen the window so concurrent callers coalesce
        return list(self.current)
