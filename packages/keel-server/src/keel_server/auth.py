"""RBAC: API-key principals + role tiers (B3).

Open by default: with no ``KEEL_API_KEYS`` configured the server runs single-user —
every request is an implicit ``admin`` — preserving Keel's local/self-hosted posture.
Configuring keys switches on enforcement: each request must present a valid key
(``X-API-Key`` or ``Authorization: Bearer <key>``) whose role meets the endpoint's
minimum, else 401 (missing/unknown key) or 403 (insufficient role).
"""

from __future__ import annotations

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


def parse_api_keys(raw: str) -> dict[str, Principal]:
    """Parse ``key:role[,key:role...]`` into a ``{key: Principal}`` map.

    Blank entries, entries without a role, and unknown role names are skipped so a
    typo can't silently grant access.
    """
    keys: dict[str, Principal] = {}
    for entry in raw.split(","):
        key, sep, role_name = entry.strip().partition(":")
        if not sep:
            continue
        key = key.strip()
        role = _ROLE_BY_NAME.get(role_name.strip().lower())
        if key and role is not None:
            keys[key] = Principal(name=f"{role.name}:{key[:4]}…", role=role)
    return keys


def _presented_key(request: Request) -> str | None:
    header = request.headers.get("x-api-key")
    if header and header.strip():
        return header.strip()
    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip() or None
    return None


def authenticate(request: Request) -> Principal:
    """Resolve the request's principal. Open mode (no configured keys) -> implicit admin."""
    keys: dict[str, Principal] = getattr(request.app.state, "api_keys", {}) or {}
    if not keys:
        return Principal(name="local", role=Role.admin)
    presented = _presented_key(request)
    if presented is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing API key")
    principal = keys.get(presented)
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
