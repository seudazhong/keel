"""Production-capable OIDC ID-token / JWT verification (M3.6, WS-L).

A fail-closed verifier that validates an external OIDC provider's signed JWT before any
subject is trusted. It checks, in order: a hard-coded asymmetric signature algorithm, the
signature against the issuer's JWKS (with a rotation-aware, cached key set), the ``iss`` and
``aud`` claims (with multi-audience ``azp`` binding), and the ``exp``/``nbf``/``iat`` time
claims (with bounded clock ``leeway``). An invalid token raises
:class:`OIDCVerificationError` (→ 401); a provider outage raises
:class:`OIDCAvailabilityError` (→ 503). The verifier never returns partially validated
claims and never surfaces an uncontrolled ``500``.

The JWKS source is an injected :class:`JWKSProvider` so tests drive it with a local, static
key set while production fetches (and caches / rotates) the provider's ``jwks_uri`` over
HTTPS. An unknown ``kid`` triggers at most one coalesced, rate-limited JWKS refresh and is
then negatively cached, so an attacker cannot amplify a flood of unknown-kid tokens into a
flood of outbound JWKS requests; a genuinely rotated key still recovers on the next allowed
refresh.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

import jwt
from jwt import PyJWK

from keel_core.errors import KeelError

# A monotonic clock seam (seconds), injectable so cache TTLs are testable.
type Clock = Callable[[], float]

# Asymmetric algorithms only — a symmetric ``HS*`` token would let anyone holding the
# (public) JWKS forge a token, so they are rejected outright (alg-confusion defense). This
# is a HARD, hard-coded allowlist of the asymmetric families this implementation supports;
# ``none`` and every symmetric/octet algorithm are structurally excluded and cannot be
# re-enabled through configuration (see :meth:`OIDCConfig.__post_init__`).
_SUPPORTED_ALGORITHMS: frozenset[str] = frozenset(
    {
        "RS256",
        "RS384",
        "RS512",
        "ES256",
        "ES384",
        "ES512",
        "PS256",
        "PS384",
        "PS512",
    }
)
_DEFAULT_ALGORITHMS: tuple[str, ...] = (
    "RS256",
    "RS384",
    "RS512",
    "ES256",
    "ES384",
    "ES512",
    "PS256",
    "PS384",
    "PS512",
)


class OIDCVerificationError(KeelError):
    """An OIDC token failed verification (fail closed; details are not user-facing).

    Maps to a controlled ``401 Unauthorized`` — the token itself is invalid.
    """


class OIDCAvailabilityError(KeelError):
    """The OIDC provider (JWKS endpoint) could not be reached or returned garbage.

    Distinct from :class:`OIDCVerificationError`: the token may be perfectly valid but we
    cannot verify it right now. Maps to a controlled ``503 Service Unavailable`` (fail
    closed) — never an uncontrolled ``500`` and never an implicit auth bypass.
    """


@dataclass(frozen=True)
class OIDCConfig:
    """Issuer/audience policy for one trusted OIDC provider.

    ``algorithms`` is validated against the hard-coded asymmetric allowlist at construction
    so a misconfiguration (e.g. ``HS256`` / ``none``) fails loudly at startup rather than
    silently opening an alg-confusion hole. ``client_id`` is the expected authorized party
    (``azp``): when a token carries multiple audiences its ``azp`` MUST equal ``client_id``.
    """

    issuer: str
    audiences: frozenset[str]
    algorithms: tuple[str, ...] = _DEFAULT_ALGORITHMS
    leeway_seconds: int = 60
    require_iat: bool = True
    client_id: str | None = None

    def __post_init__(self) -> None:
        if not self.algorithms:
            raise ValueError("OIDCConfig.algorithms must list at least one algorithm")
        disallowed = tuple(a for a in self.algorithms if a not in _SUPPORTED_ALGORITHMS)
        if disallowed:
            raise ValueError(
                "OIDCConfig.algorithms must be asymmetric only "
                f"({'/'.join(sorted(_SUPPORTED_ALGORITHMS))}); rejected: {disallowed!r}"
            )

    @classmethod
    def from_settings(
        cls,
        *,
        issuer: str,
        audience: str,
        algorithms: Sequence[str] | None = None,
        leeway_seconds: int = 60,
        client_id: str | None = None,
    ) -> OIDCConfig:
        auds = frozenset(a.strip() for a in audience.split(",") if a.strip())
        algs = tuple(algorithms) if algorithms else _DEFAULT_ALGORITHMS
        derived_client = client_id.strip() if client_id and client_id.strip() else None
        # Derive a sensible default authorized party when exactly one audience is
        # configured: that single audience is, by definition, this relying party's id.
        if derived_client is None and len(auds) == 1:
            derived_client = next(iter(auds))
        return cls(
            issuer=issuer.strip(),
            audiences=auds,
            algorithms=algs,
            leeway_seconds=leeway_seconds,
            client_id=derived_client,
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

    Anti-amplification (an attacker must not be able to convert a flood of tokens carrying
    unknown ``kid``s into a matching flood of outbound JWKS requests):

    * **Coalescing** — concurrent refreshes share a single in-flight fetch (an
      ``asyncio.Lock``), so N simultaneous misses cause at most one network call.
    * **Minimum refresh interval** — a forced refresh (unknown ``kid``) is rate-limited: if
      a fetch happened within ``min_refresh_interval_seconds`` the cached set is reused
      instead of hitting the network again.

    ``httpx`` is imported lazily so the pure verifier can be unit-tested (with a static
    provider) without a live network stack. All fetch/transport/parse failures are wrapped
    in :class:`OIDCAvailabilityError` so the caller can fail closed with a controlled status
    rather than surfacing an uncontrolled ``500``.
    """

    def __init__(
        self,
        jwks_uri: str,
        *,
        cache_ttl_seconds: int = 3600,
        request_timeout_seconds: float = 5.0,
        min_refresh_interval_seconds: float = 60.0,
        clock: Clock | None = None,
    ) -> None:
        if not jwks_uri.lower().startswith("https://"):
            raise ValueError("jwks_uri must be an https:// URL")
        self._jwks_uri = jwks_uri
        self._cache_ttl = cache_ttl_seconds
        self._timeout = request_timeout_seconds
        self._min_refresh_interval = max(0.0, min_refresh_interval_seconds)
        self._clock = clock or _system_clock
        self._cached: list[dict[str, Any]] = []
        self._fetched_at: float = 0.0
        self._last_fetch_attempt: float = 0.0
        self._lock = asyncio.Lock()

    async def get_keys(self, *, force_refresh: bool = False) -> list[dict[str, Any]]:
        now = self._clock()
        fresh = self._cached and (now - self._fetched_at) < self._cache_ttl
        if fresh and not force_refresh:
            return list(self._cached)
        # A fetch is (maybe) needed. Serialize so concurrent misses coalesce into one call.
        async with self._lock:
            now = self._clock()
            fresh = self._cached and (now - self._fetched_at) < self._cache_ttl
            if fresh and not force_refresh:
                # Another coroutine refreshed while we waited for the lock.
                return list(self._cached)
            if force_refresh and self._cached:
                # Rate-limit attacker-driven forced refreshes: reuse the cache if we hit
                # the network very recently. Bounds outbound JWKS calls to ~1 per interval
                # regardless of how many unknown-kid tokens arrive.
                if (now - self._last_fetch_attempt) < self._min_refresh_interval:
                    return list(self._cached)
            self._last_fetch_attempt = now
            keys = await self._fetch()
            self._cached = keys
            self._fetched_at = self._clock()
            return list(self._cached)

    async def _fetch(self) -> list[dict[str, Any]]:
        import httpx

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(self._jwks_uri)
                response.raise_for_status()
                document = response.json()
        except httpx.HTTPError as exc:
            # Timeouts, connection failures, non-2xx responses.
            raise OIDCAvailabilityError(
                f"could not fetch issuer JWKS: {type(exc).__name__}"
            ) from exc
        except ValueError as exc:
            # response.json() on a non-JSON body raises ValueError (json.JSONDecodeError).
            raise OIDCAvailabilityError("issuer JWKS response was not valid JSON") from exc
        keys = document.get("keys") if isinstance(document, dict) else None
        if not isinstance(keys, list):
            raise OIDCAvailabilityError("issuer JWKS document has no 'keys' array")
        return [key for key in keys if isinstance(key, dict)]


def _system_clock() -> float:
    return time.monotonic()


class OIDCVerifier:
    """Verifies an OIDC-issued JWT against a configured issuer/audience + JWKS.

    Beyond signature/claims validation the verifier bounds JWKS *unknown-kid amplification*
    with a small negative cache: a ``kid`` that is absent even after a forced refresh is
    remembered (TTL-bounded, size-bounded) so repeated tokens carrying that same unknown
    ``kid`` fail fast without triggering another refresh. A genuinely rotated key is still
    picked up once the provider's minimum refresh interval elapses.
    """

    def __init__(
        self,
        config: OIDCConfig,
        jwks_provider: JWKSProvider,
        *,
        clock: Clock | None = None,
        negative_cache_ttl_seconds: float = 60.0,
        negative_cache_max: int = 1024,
    ) -> None:
        if not config.issuer:
            raise ValueError("OIDCConfig.issuer is required")
        if not config.audiences:
            raise ValueError("OIDCConfig.audiences is required")
        # Defense in depth: reject a config that somehow smuggled a non-asymmetric alg
        # (``OIDCConfig.__post_init__`` already enforces this at construction).
        disallowed = tuple(a for a in config.algorithms if a not in _SUPPORTED_ALGORITHMS)
        if disallowed:
            raise ValueError(f"OIDCVerifier refuses non-asymmetric algorithms: {disallowed!r}")
        self._config = config
        self._jwks = jwks_provider
        self._clock = clock or _system_clock
        self._neg_ttl = max(0.0, negative_cache_ttl_seconds)
        self._neg_max = max(1, negative_cache_max)
        self._unknown_kids: OrderedDict[str, float] = OrderedDict()
        self._neg_lock = asyncio.Lock()

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
        if key is not None:
            await self._forget_unknown_kid(kid)
            return key
        # Not in the cached key set. If this exact kid was recently confirmed absent (even
        # after a forced refresh), fail fast — do NOT force another refresh. This denies an
        # attacker the ability to drive one outbound JWKS request per unknown-kid token.
        if await self._is_recently_unknown(kid):
            raise OIDCVerificationError("no matching signing key for token kid")
        # A first-seen miss may indicate a rotation: refresh the JWKS exactly once (the
        # provider additionally coalesces + rate-limits the actual network call).
        key = await self._find_key(kid, alg, force_refresh=True)
        if key is None:
            await self._remember_unknown_kid(kid)
            raise OIDCVerificationError("no matching signing key for token kid")
        await self._forget_unknown_kid(kid)
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

    async def _is_recently_unknown(self, kid: str | None) -> bool:
        if kid is None:
            return False
        async with self._neg_lock:
            seen_at = self._unknown_kids.get(kid)
            if seen_at is None:
                return False
            if (self._clock() - seen_at) >= self._neg_ttl:
                self._unknown_kids.pop(kid, None)
                return False
            return True

    async def _remember_unknown_kid(self, kid: str | None) -> None:
        if kid is None:
            return
        async with self._neg_lock:
            self._unknown_kids[kid] = self._clock()
            self._unknown_kids.move_to_end(kid)
            while len(self._unknown_kids) > self._neg_max:
                self._unknown_kids.popitem(last=False)

    async def _forget_unknown_kid(self, kid: str | None) -> None:
        if kid is None:
            return
        async with self._neg_lock:
            self._unknown_kids.pop(kid, None)

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
        # Multi-audience hardening: a token minted for several audiences MUST name its
        # authorized party (``azp``), and it must equal our configured client id — otherwise
        # a token issued for a *different* relying party that merely lists us as one of many
        # audiences would be accepted here (confused-deputy across relying parties).
        if len(audience) > 1:
            expected = self._config.client_id
            if not expected:
                raise OIDCVerificationError(
                    "multi-audience token but no client_id is configured to check azp"
                )
            azp = payload.get("azp")
            if not isinstance(azp, str) or azp != expected:
                raise OIDCVerificationError("multi-audience token azp missing or mismatched")
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
    "OIDCAvailabilityError",
    "OIDCClaims",
    "OIDCConfig",
    "OIDCVerificationError",
    "OIDCVerifier",
    "StaticJWKSProvider",
]
