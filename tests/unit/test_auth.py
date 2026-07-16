"""RBAC auth tests: api-key parsing + role-gated dependencies (open + keyed modes)."""

from __future__ import annotations

from typing import Annotated, Any

import httpx
import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport

from keel_server.auth import Principal, Role, hash_api_key, parse_api_keys, require_role


def test_parse_api_keys_maps_roles_and_skips_junk() -> None:
    keys = parse_api_keys("adm:admin, op:operator ,vw:VIEWER,bad:notarole,, norole")
    # Keys are hashed at rest; the map is keyed by the SHA-256 digest, never plaintext.
    assert {k: p.role for k, p in keys.items()} == {
        hash_api_key("adm"): Role.admin,
        hash_api_key("op"): Role.operator,
        hash_api_key("vw"): Role.viewer,  # role name is case-insensitive
    }
    assert "adm" not in keys  # plaintext key never stored


def _app() -> FastAPI:
    app = FastAPI()

    @app.get("/read")
    async def read(
        p: Annotated[Principal, Depends(require_role(Role.viewer))],
    ) -> dict[str, str]:
        return {"who": p.name, "role": p.role.name}

    @app.post("/mutate", dependencies=[Depends(require_role(Role.operator))])
    async def mutate() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/admin", dependencies=[Depends(require_role(Role.admin))])
    async def admin() -> dict[str, bool]:
        return {"ok": True}

    return app


async def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_open_mode_allows_everything_as_admin() -> None:
    app = _app()  # no app.state.api_keys -> open mode
    async with await _client(app) as client:
        assert (await client.get("/read")).json() == {"who": "local", "role": "admin"}
        assert (await client.post("/mutate")).status_code == 200
        assert (await client.get("/admin")).status_code == 200


@pytest.mark.parametrize(
    ("key", "read", "mutate", "admin"),
    [
        ("vw", 200, 403, 403),  # viewer: read only
        ("op", 200, 200, 403),  # operator: read + mutate
        ("adm", 200, 200, 200),  # admin: everything
    ],
)
async def test_keyed_mode_enforces_role_tiers(key: str, read: int, mutate: int, admin: int) -> None:
    app = _app()
    app.state.api_keys = parse_api_keys("adm:admin,op:operator,vw:viewer")
    headers: dict[str, Any] = {"X-API-Key": key}
    async with await _client(app) as client:
        assert (await client.get("/read", headers=headers)).status_code == read
        assert (await client.post("/mutate", headers=headers)).status_code == mutate
        assert (await client.get("/admin", headers=headers)).status_code == admin


async def test_keyed_mode_rejects_missing_and_invalid_keys() -> None:
    app = _app()
    app.state.api_keys = parse_api_keys("adm:admin")
    async with await _client(app) as client:
        assert (await client.get("/read")).status_code == 401  # missing
        assert (await client.get("/read", headers={"X-API-Key": "nope"})).status_code == 401
        # Bearer scheme is accepted too.
        assert (
            await client.get("/read", headers={"Authorization": "Bearer adm"})
        ).status_code == 200


async def test_cloud_mode_fails_closed_without_keys() -> None:
    """Cloud mode + no keys is a misconfiguration: every request is rejected (503)."""
    app = _app()
    app.state.auth_required = True  # cloud mode
    async with await _client(app) as client:
        assert (await client.get("/read")).status_code == 503
        assert (await client.post("/mutate")).status_code == 503


async def test_open_mode_still_allows_when_auth_not_required() -> None:
    """Without cloud mode and no keys, the local open path stays (implicit admin)."""
    app = _app()
    app.state.auth_required = False
    async with await _client(app) as client:
        assert (await client.get("/read")).json()["role"] == "admin"
