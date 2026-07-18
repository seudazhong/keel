"""Contract tests for the runtime-login wiring in ``docker-compose.yml`` (M3A, WS-DB).

Deterministic, Docker-free checks that the standard Compose stack runs keel-server/keel-worker as
the **least-privilege, non-owner** runtime DB login while migrate/provision use the privileged
owner/migrator URL — and that the runtime password is never inlined (server/worker connect
password-less via a generated libpq PGPASSFILE). PyYAML resolves the anchors + merge keys exactly
as Compose would. Also guards the M3B sandbox contracts this branch must preserve.

No Docker daemon, image build, or network is required; this runs in the non-integration suite.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml
from sqlalchemy.engine import make_url

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"

_CONTROL_PLANE = ("keel-server", "keel-worker")
_OWNER_JOBS = ("keel-migrate", "keel-provision")

_RUNTIME_URL = "postgresql+psycopg://keel_runtime_login@postgres:5432/keel"
_PGPASS_PATH = "/keel-secrets-db/runtime_pgpass"
_PASSWORD_FILE = "/keel-secrets-db/runtime_db_password"


@pytest.fixture(scope="module")
def compose() -> dict:
    assert COMPOSE_PATH.exists(), "docker-compose.yml is missing"
    return yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def compose_text() -> str:
    return COMPOSE_PATH.read_text(encoding="utf-8")


def _parse_volume(entry: str) -> tuple[str, str, str]:
    parts = entry.split(":")
    if len(parts) == 2:
        return parts[0], parts[1], ""
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise AssertionError(f"unexpected volume form: {entry!r}")


def test_new_services_present(compose: dict) -> None:
    services = compose["services"]
    for name in ("keel-runtime-secret-init", "keel-provision", *_OWNER_JOBS, *_CONTROL_PLANE):
        assert name in services, f"service {name} missing from compose"


def test_control_plane_uses_password_less_runtime_login(compose: dict) -> None:
    """server/worker connect as keel_runtime_login with NO password in the URL (PGPASSFILE)."""
    services = compose["services"]
    for name in _CONTROL_PLANE:
        env = services[name]["environment"]
        assert env.get("KEEL_DATABASE_URL") == _RUNTIME_URL, name
        url = make_url(env["KEEL_DATABASE_URL"])
        assert url.username == "keel_runtime_login", name
        assert url.password is None, f"{name} runtime URL must carry NO password"
        # Password comes from the generated libpq pgpass file.
        assert env.get("PGPASSFILE") == _PGPASS_PATH, name
        # Fail-closed non-owner enforcement is on even though this trusted stack is not cloud mode.
        assert env.get("KEEL_REQUIRE_RUNTIME_DB_PRINCIPAL") == "true", name
        # The control plane must NOT receive the privileged owner/migrator URL.
        assert "KEEL_MIGRATION_DATABASE_URL" not in env, f"{name} must not hold the owner URL"


def test_owner_jobs_use_migration_url_not_runtime(compose: dict) -> None:
    services = compose["services"]
    for name in _OWNER_JOBS:
        env = services[name].get("environment", {})
        assert env.get("KEEL_MIGRATION_DATABASE_URL"), f"{name} must use the owner/migrator URL"
        # The owner jobs must not be pinned at the non-owner runtime login.
        assert "KEEL_DATABASE_URL" not in env, f"{name} must not use the runtime login URL"
        assert "PGPASSFILE" not in env, name


def test_migrate_and_provision_are_one_shot(compose: dict) -> None:
    services = compose["services"]
    for name in ("keel-runtime-secret-init", *_OWNER_JOBS):
        assert services[name].get("restart") == "no", f"{name} must be a one-shot bootstrap"


def test_runtime_secret_init_generates_via_module(compose: dict) -> None:
    init = compose["services"]["keel-runtime-secret-init"]
    cmd = init["command"]
    # Uses the tested module rather than an inline script.
    assert cmd == ["python", "-m", "keel_core.provision_runtime_secret"], cmd
    env = init["environment"]
    assert env["KEEL_RUNTIME_DB_PASSWORD_FILE"] == _PASSWORD_FILE
    assert env["KEEL_RUNTIME_PGPASS_FILE"] == _PGPASS_PATH
    assert env["KEEL_RUNTIME_DB_USER"] == "keel_runtime_login"
    assert env["KEEL_RUNTIME_DB_HOST"] == "postgres"
    # Writes into the dedicated secret volume (read-write, since it generates the files).
    mounts = [_parse_volume(v) for v in init["volumes"]]
    assert [(s, t, m) for s, t, m in mounts if s == "runtimesecret"] == [
        ("runtimesecret", "/keel-secrets-db", "")
    ], mounts


def test_provision_uses_password_file_and_verifies(compose: dict) -> None:
    provision = compose["services"]["keel-provision"]
    cmd = provision["command"]
    assert cmd[:3] == ["python", "-m", "keel_core.provision_runtime_cli"], cmd
    # Secret comes from a file (never argv), and the login is verified least-privilege.
    assert "--password-file" in cmd and _PASSWORD_FILE in cmd, cmd
    assert "--verify" in cmd, cmd
    # Reads the secret volume read-only.
    ro = [_parse_volume(v) for v in provision["volumes"] if _parse_volume(v)[0] == "runtimesecret"]
    assert ro and all(mode == "ro" for _, _, mode in ro), provision["volumes"]


def test_provision_waits_for_migrate_and_secret_init(compose: dict) -> None:
    deps = compose["services"]["keel-provision"]["depends_on"]
    assert deps["postgres"]["condition"] == "service_healthy"
    assert deps["keel-migrate"]["condition"] == "service_completed_successfully"
    assert deps["keel-runtime-secret-init"]["condition"] == "service_completed_successfully"


def test_control_plane_waits_for_provision(compose: dict) -> None:
    services = compose["services"]
    for name in _CONTROL_PLANE:
        deps = services[name]["depends_on"]
        assert (
            deps.get("keel-provision", {}).get("condition") == "service_completed_successfully"
        ), f"{name} must wait for the runtime login to be provisioned+verified"


def test_control_plane_mounts_runtime_secret_read_only(compose: dict) -> None:
    services = compose["services"]
    for name in _CONTROL_PLANE:
        mounts = [
            _parse_volume(v)
            for v in services[name]["volumes"]
            if _parse_volume(v)[0] == "runtimesecret"
        ]
        assert mounts, f"{name} must mount the runtime secret volume"
        assert all(mode == "ro" for _, _, mode in mounts), f"{name} runtime secret must be ro"


def test_runtimesecret_volume_declared(compose: dict) -> None:
    assert "runtimesecret" in compose.get("volumes", {})


def test_no_inline_runtime_password_anywhere(compose_text: str) -> None:
    # A password-bearing runtime URL would look like `keel_runtime_login:<something>@`.
    leak = re.search(r"keel_runtime_login:[^@\s/]+@", compose_text)
    assert leak is None, "the runtime login password must never be inlined in a URL"


def test_m3b_sandbox_contract_preserved(compose: dict) -> None:
    """Adding the runtime-login anchor must not drop the M3B authenticated-sandbox wiring."""
    services = compose["services"]
    for name in _CONTROL_PLANE:
        env = services[name]["environment"]
        assert env.get("KEEL_EXECUTION_BACKEND") == "sandbox", name
        assert env.get("KEEL_SANDBOX_URL") == "http://keel-sandbox:8090", name
        assert env.get("KEEL_SANDBOX_RPC_SECRET_FILE"), name
        assert "KEEL_SANDBOX_RPC_SECRET" not in env, name


def test_m3b_web_healthcheck_loopback_preserved(compose: dict) -> None:
    """The M3B 127.0.0.1 web healthcheck contract must survive this branch's edits."""
    test_cmd = " ".join(compose["services"]["keel-web"]["healthcheck"]["test"])
    assert "http://127.0.0.1/web-health" in test_cmd
