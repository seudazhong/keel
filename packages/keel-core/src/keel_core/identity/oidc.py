"""Production-capable OIDC ID-token / JWT verification (M3.6, WS-L).

A fail-closed verifier that validates an external OIDC provider's signed JWT before any
subject is trusted. It checks, in order: a supported signature algorithm, the signature
against the issuer's JWKS (with a rotation-aware, cached key set), the ``iss`` and ``aud``
claims, and the ``exp``/``nbf``/``iat`` time claims (with bounded clock ``leeway``). Any
failure raises :class:`OIDCVerificationError` — the verifier never returns partially
validated claims.

The JWKS source is an injected :class:`JWKSProvider` so tests drive it with a local, static
key set while production fetches (and caches / rotates) the provider's ``jwks_uri`` over
HTTPS. On an unknown ``kid`` the verifier refreshes the key set exactly once (handling a
provider key rotation) before failing closed.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import jwt
from jwt import PyJWK

from keel_core.errors import KeelError

# A monotonic clock seam (seconds), injectable so the JWKS cache TTL is testable.
type Clock = Callable[[], float]

# Asymmetric algorithms only — a symmetric ``HS*`` token would let anyone holding the
# (public) JWKS forge a token, so they are rejected outright (alg-confusion defense).
_DEFAULT_ALGORITHMS: tuple[str, ...] = ("RS256", "RS384", "RS512", "ES256", "ES384", "ES512")


class OIDCVerificationError(KeelError):
    """An OIDC token failed verification (fail closed; details are not user-facing)."""


@dataclass(frozen=True)
class OIDCConfig:
    """Issuer/audience policy for one trusted OIDC provider."""

    issuer: str
    audiences: frozenset[str]
    algorithms: tuple[str, ...] = _DEFAULT_ALGORITHMS
    leeway_seconds: int = 60
    require_iat: bool = True

    @classmethod
    def from_settings(
        cls,
        *,
        issuer: str,
        audience: str,
        algorithms: Sequence[str] | None = None,
        leeway_seconds: int = 60,
    ) -> OIDCConfig:
        auds = frozenset(a.strip() for a in audience.split(",") if a.strip())
        algs = tuple(algorithms) if algorithms else _DEFAULT_ALGORITHMS
        return cls(
            issuer=issuer.strip(),
            audiences=auds,
            algorithms=algs,
            leeway_seconds=leeway_seconds,
        )


@dataclass(frozen=True)
class OIDCClaims:
    """The validated subset of claims a caller may trust after verification."""

    issuer: str
    subject: str
    audience: tuple[str, ...]
    email: str | None
    email_verified: bool
    expires_at: int
    issued_at: int | None
    raw: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class JWKSProvider(Protocol):
    """Supplies the issuer's JSON Web Key Set, refreshable on rotation."""

    async def get_keys(self, *, force_refresh: bool = False) -> list[dict[str, Any]]: ...


class StaticJWKSProvider:
    """A fixed JWKS (tests / an air-gapped, pre-provisioned key set)."""

    def __init__(self, keys: Sequence[dict[str, Any]]) -> None:
        self._keys = [dict(key) for key in keys]

    async def get_keys(self, *, force_refresh: bool = False) -> list[dict[str, Any]]:
        return [dict(key) for key in self._keys]


class HTTPJWKSProvider:
    """Fetches + caches an issuer's JWKS over HTTPS with a TTL and rotation refresh.

    ``httpx`` is imported lazily so the pure verifier can be unit-tested (with a static
    provider) without a live network stack.
    """

    def __init__(
        self,
        jwks_uri: str,
        *,
        cache_ttl_seconds: int = 3600,
        request_timeout_seconds: float = 5.0,
        clock: Clock | None = None,
    ) -> None:
        if not jwks_uri.lower().startswith("https://"):
            raise ValueError("jwks_uri must be an https:// URL")
        self._jwks_uri = jwks_uri
        self._cache_ttl = cache_ttl_seconds
        self._timeout = request_timeout_seconds
        self._clock = clock or _system_clock
        self._cached: list[dict[str, Any]] = []
        self._fetched_at: float = 0.0

    async def get_keys(self, *, force_refresh: bool = False) -> list[dict[str, Any]]:
        now = self._clock()
        fresh = self._cached and (now - self._fetched_at) < self._cache_ttl
        if fresh and not force_refresh:
            return list(self._cached)
        keys = await self._fetch()
        self._cached = keys
        self._fetched_at = now
        return list(self._cached)

    async def _fetch(self) -> list[dict[str, Any]]:
        import httpx

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(self._jwks_uri)
            response.raise_for_status()
            document = response.json()
        keys = document.get("keys") if isinstance(document, dict) else None
        if not isinstance(keys, list):
            raise OIDCVerificationError("issuer JWKS document has no 'keys' array")
        return [key for key in keys if isinstance(key, dict)]


def _system_clock() -> float:
    return time.monotonic()


class OIDCVerifier:
    """Verifies an OIDC-issued JWT against a configured issuer/audience + JWKS."""

    def __init__(self, config: OIDCConfig, jwks_provider: JWKSProvider) -> None:
        if not config.issuer:
            raise ValueError("OIDCConfig.issuer is required")
        if not config.audiences:
            raise ValueError("OIDCConfig.audiences is required")
        self._config = config
        self._jwks = jwks_provider

    @property
    def config(self) -> OIDCConfig:
        return self._config

    async def verify(self, token: str) -> OIDCClaims:
        """Validate ``token`` end-to-end and return its trusted claims (fail closed)."""
        header = self._read_header(token)
        alg = header.get("alg")
        if alg not in self._config.algorithms:
            raise OIDCVerificationError(f"unsupported or disallowed algorithm: {alg!r}")
        kid = header.get("kid")

        key = await self._resolve_key(kid, alg)
        return self._decode(token, key)

    def _read_header(self, token: str) -> dict[str, Any]:
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError as exc:
            raise OIDCVerificationError("malformed token header") from exc
        if not isinstance(header, dict):
            raise OIDCVerificationError("malformed token header")
        return header

    async def _resolve_key(self, kid: str | None, alg: str) -> PyJWK:
        key = await self._find_key(kid, alg, force_refresh=False)
        if key is None:
            # A missing kid may indicate a rotation: refresh the JWKS exactly once.
            key = await self._find_key(kid, alg, force_refresh=True)
        if key is None:
            raise OIDCVerificationError("no matching signing key for token kid")
        return key

    async def _find_key(self, kid: str | None, alg: str, *, force_refresh: bool) -> PyJWK | None:
        keys = await self._jwks.get_keys(force_refresh=force_refresh)
        candidates: list[PyJWK] = []
        for raw in keys:
            if kid is not None and raw.get("kid") != kid:
                continue
            try:
                candidates.append(PyJWK.from_dict(raw))
            except (jwt.PyJWTError, KeyError, ValueError, TypeError):
                continue
        if not candidates:
            return None
        # When the JWK advertises an alg it must match the token header's alg.
        for candidate in candidates:
            advertised = getattr(candidate, "algorithm_name", None)
            if advertised is None or advertised == alg:
                return candidate
        return None

    def _decode(self, token: str, key: PyJWK) -> OIDCClaims:
        require = ["exp"]
        if self._config.require_iat:
            require.append("iat")
        try:
            payload = jwt.decode(
                token,
                key.key,
                algorithms=list(self._config.algorithms),
                issuer=self._config.issuer,
                audience=list(self._config.audiences),
                leeway=self._config.leeway_seconds,
                options={
                    "require": require,
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_nbf": True,
                    "verify_iat": True,
                    "verify_aud": True,
                    "verify_iss": True,
                },
            )
        except jwt.PyJWTError as exc:
            raise OIDCVerificationError(f"token verification failed: {type(exc).__name__}") from exc

        subject = payload.get("sub")
        if not isinstance(subject, str) or not subject:
            raise OIDCVerificationError("token has no subject (sub) claim")
        aud = payload.get("aud")
        audience = (aud,) if isinstance(aud, str) else tuple(aud) if isinstance(aud, list) else ()
        email = payload.get("email")
        return OIDCClaims(
            issuer=str(payload.get("iss", self._config.issuer)),
            subject=subject,
            audience=tuple(str(a) for a in audience),
            email=email if isinstance(email, str) else None,
            email_verified=bool(payload.get("email_verified", False)),
            expires_at=int(payload["exp"]),
            issued_at=int(payload["iat"]) if "iat" in payload else None,
            raw=dict(payload),
        )


__all__ = [
    "HTTPJWKSProvider",
    "JWKSProvider",
    "OIDCClaims",
    "OIDCConfig",
    "OIDCVerificationError",
    "OIDCVerifier",
    "StaticJWKSProvider",
]
