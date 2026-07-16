"""RBAC: API-key principals + role tiers (B3), hashed + constant-time (M3.3).

Open by default: with no ``KEEL_API_KEYS`` configured **and** cloud mode off the server
runs single-user — every request is an implicit ``admin`` — preserving Keel's
local/self-hosted posture. Configuring keys switches on enforcement: each request must
present a valid key (``X-API-Key`` or an ``Authorization`` bearer token) whose role meets
the endpoint's minimum, else 401 (missing/unknown key) or 403 (insufficient role).

Keys are never held or compared in plaintext: each configured key is stored as a SHA-256
digest and a presented key is hashed and compared in **constant time**
(``hmac.compare_digest``), so a timing side-channel can't recover a key. In **cloud mode**
(``KEEL_CLOUD_MODE=1``) the open path is disabled — an empty key set fails closed and
every request is rejected.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import IntEnum

from fastapi import HTTPException, Request, status


class Role(IntEnum):
    """Ordered access tiers; higher includes the powers of lower."""

    viewer = 1
    operator = 2
    admin = 3


_ROLE_BY_NAME = {role.name: role for role in Role}


@dataclass(frozen=True)
class Principal:
    """The authenticated caller: a display name and its granted role."""

    name: str
    role: Role


def hash_api_key(key: str) -> str:
    """Return the SHA-256 hex digest used to store/compare an API key."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def parse_api_keys(raw: str) -> dict[str, Principal]:
    """Parse ``key:role[,key:role...]`` into a ``{key_hash: Principal}`` map.

    Blank entries, entries without a role, and unknown role names are skipped so a
    typo can't silently grant access. The plaintext key is hashed immediately and never
    retained; the map is keyed by the digest.
    """
    keys: dict[str, Principal] = {}
    for entry in raw.split(","):
        key, sep, role_name = entry.strip().partition(":")
        if not sep:
            continue
        key = key.strip()
        role = _ROLE_BY_NAME.get(role_name.strip().lower())
        if key and role is not None:
            keys[hash_api_key(key)] = Principal(name=f"{role.name}:{key[:4]}…", role=role)
    return keys


def _presented_key(request: Request) -> str | None:
    header = request.headers.get("x-api-key")
    if header and header.strip():
        return header.strip()
    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip() or None
    return None


def _match_principal(keys: dict[str, Principal], presented: str) -> Principal | None:
    """Constant-time lookup: hash the presented key, compare against every stored digest.

    Iterating with ``compare_digest`` over all entries keeps the comparison time
    independent of which (if any) key matched, avoiding a timing oracle.
    """
    presented_hash = hash_api_key(presented)
    matched: Principal | None = None
    for stored_hash, principal in keys.items():
        if hmac.compare_digest(stored_hash, presented_hash):
            matched = principal
    return matched


def authenticate(request: Request) -> Principal:
    """Resolve the request's principal.

    Open mode (no configured keys, cloud mode off) -> implicit admin. In cloud mode an
    empty key set fails closed (503): running a cloud deployment with no auth is a
    misconfiguration, not an invitation.
    """
    keys: dict[str, Principal] = getattr(request.app.state, "api_keys", {}) or {}
    if not keys:
        if getattr(request.app.state, "auth_required", False):
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "authentication is required in cloud mode but no API keys are configured",
            )
        return Principal(name="local", role=Role.admin)
    presented = _presented_key(request)
    if presented is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing API key")
    principal = _match_principal(keys, presented)
    if principal is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid API key")
    return principal


def require_role(minimum: Role) -> Callable[[Request], Awaitable[Principal]]:
    """A FastAPI dependency that authenticates then enforces ``principal.role >= minimum``."""

    async def dependency(request: Request) -> Principal:
        principal = authenticate(request)
        if principal.role < minimum:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "insufficient role")
        return principal

    return dependency
