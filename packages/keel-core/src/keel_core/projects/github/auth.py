"""GitHub App authentication: app JWT + just-in-time installation tokens (M3.7, WS-P).

The control plane authenticates to GitHub as the App using a **short-lived** RS256 JWT signed
with the App's private key, then mints **least-scope installation access tokens just in time**
for a specific installation. Installation tokens are:

* cached only briefly in-process (well under GitHub's ~1h lifetime, bounded by
  ``cache_seconds``) to avoid minting on every call, and
* **never persisted** to any datastore and **never logged**.

The private key is resolved from a *reference* (``env:NAME``, ``file:PATH``, or a bare path)
so the raw PEM is not held inline in broadly-shared config, and is loaded lazily.
"""

from __future__ import annotations

import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import jwt

# GitHub rejects an app JWT whose ``exp`` is more than 10 minutes out or whose ``iat`` is in
# the future; use a conservative window with clock-skew tolerance.
_JWT_TTL_SECONDS = 540
_JWT_BACKDATE_SECONDS = 60
# Refresh an installation token this many seconds before its stated expiry (skew guard).
_TOKEN_REFRESH_SKEW_SECONDS = 60


class GitHubAppConfigError(RuntimeError):
    """The GitHub App is misconfigured (missing key/app id); fail closed, non-sensitive."""


def resolve_private_key(ref: str) -> str:
    """Resolve a private-key reference to PEM text (``env:NAME`` / ``file:PATH`` / path).

    Never logs the resolved key material. Raises :class:`GitHubAppConfigError` when the
    reference is empty or cannot be resolved.
    """
    reference = (ref or "").strip()
    if not reference:
        raise GitHubAppConfigError("no GitHub App private key reference configured")
    if reference.startswith("env:"):
        name = reference[len("env:") :]
        value = os.environ.get(name)
        if not value:
            raise GitHubAppConfigError("GitHub App private key env var is not set")
        return value
    path = reference[len("file:") :] if reference.startswith("file:") else reference
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise GitHubAppConfigError("GitHub App private key file could not be read") from exc


@dataclass(frozen=True)
class InstallationToken:
    """A minted installation access token and its absolute expiry (never persisted)."""

    token: str
    expires_at: datetime


def _parse_expiry(raw: object) -> datetime:
    if isinstance(raw, str) and raw:
        value = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            parsed = datetime.now(UTC)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed
    return datetime.now(UTC)


class AppJwtMinter:
    """Signs short-lived RS256 App JWTs from a lazily-loaded private key."""

    def __init__(
        self,
        *,
        app_id: int,
        private_key_loader: Callable[[], str],
        clock: Callable[[], float] | None = None,
    ) -> None:
        if app_id <= 0:
            raise GitHubAppConfigError("a positive GitHub App id is required")
        self._app_id = app_id
        self._loader = private_key_loader
        self._clock = clock or time.time
        self._key: str | None = None

    def _private_key(self) -> str:
        if self._key is None:
            self._key = self._loader()
        return self._key

    def generate(self) -> str:
        now = int(self._clock())
        payload = {
            "iat": now - _JWT_BACKDATE_SECONDS,
            "exp": now + _JWT_TTL_SECONDS,
            "iss": str(self._app_id),
        }
        return jwt.encode(payload, self._private_key(), algorithm="RS256")


class InstallationTokenService:
    """Mints + briefly caches least-scope installation tokens (never persisted/logged)."""

    def __init__(
        self,
        minter: AppJwtMinter,
        token_minter: Callable[[int, str], Awaitable[Any]],
        *,
        cache_seconds: int = 300,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        # ``token_minter`` is an async callable ``(installation_id, app_jwt) -> GitHubResponse``
        # (typically ``GitHubClient.mint_installation_token`` adapted); kept abstract so this
        # service has no hard import cycle with the HTTP client.
        self._minter = minter
        self._token_minter = token_minter
        self._cache_seconds = max(cache_seconds, 0)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._cache: dict[int, InstallationToken] = {}

    def _cached(self, installation_id: int) -> InstallationToken | None:
        token = self._cache.get(installation_id)
        if token is None:
            return None
        now = self._clock()
        remaining = (token.expires_at - now).total_seconds()
        if remaining <= _TOKEN_REFRESH_SKEW_SECONDS:
            self._cache.pop(installation_id, None)
            return None
        return token

    async def get_token(self, installation_id: int) -> InstallationToken:
        cached = self._cached(installation_id)
        if cached is not None:
            return cached
        app_jwt = self._minter.generate()
        response = await self._token_minter(installation_id, app_jwt)
        body = getattr(response, "json_body", None)
        if not isinstance(body, dict) or not isinstance(body.get("token"), str):
            raise GitHubAppConfigError("GitHub did not return an installation token")
        expires_at = _parse_expiry(body.get("expires_at"))
        if self._cache_seconds:
            cap = self._clock().timestamp() + self._cache_seconds
            if expires_at.timestamp() > cap:
                expires_at = datetime.fromtimestamp(cap, tz=UTC)
        token = InstallationToken(token=body["token"], expires_at=expires_at)
        self._cache[installation_id] = token
        return token

    def invalidate(self, installation_id: int) -> None:
        self._cache.pop(installation_id, None)


__all__ = [
    "AppJwtMinter",
    "GitHubAppConfigError",
    "InstallationToken",
    "InstallationTokenService",
    "resolve_private_key",
]
