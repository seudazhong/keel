"""Unit tests for the runtime DB secret bootstrap (M3A, WS-DB).

Covers :mod:`keel_core.provision_runtime_secret` — the Compose/K8s helper that generates (or
idempotently reuses) the runtime login password and writes a libpq ``pgpass`` file for
password-less server/worker connects. No database or Docker is required.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from keel_core.provision_runtime_secret import ensure_runtime_secret

_KW = {"host": "postgres", "port": "5432", "dbname": "keel", "user": "keel_runtime_login"}


def _pgpass_line(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def test_generate_writes_password_and_pgpass(tmp_path: Path) -> None:
    pw = tmp_path / "runtime_db_password"
    pg = tmp_path / "runtime_pgpass"

    status = ensure_runtime_secret(password_path=pw, pgpass_path=pg, **_KW)

    assert status == "generated"
    password = pw.read_text(encoding="utf-8").strip()
    assert len(password) >= 40
    # pgpass is host:port:db:user:password with exactly 5 fields (password has no ':'/'\').
    assert _pgpass_line(pg) == f"postgres:5432:keel:keel_runtime_login:{password}"
    assert ":" not in password and "\\" not in password


def test_reuse_is_idempotent(tmp_path: Path) -> None:
    pw = tmp_path / "runtime_db_password"
    pg = tmp_path / "runtime_pgpass"

    first = ensure_runtime_secret(password_path=pw, pgpass_path=pg, **_KW)
    generated = pw.read_text(encoding="utf-8").strip()
    second = ensure_runtime_secret(password_path=pw, pgpass_path=pg, **_KW)

    assert first == "generated"
    assert second == "reused"
    # The already-provisioned password must NOT be rotated on re-run (would desync the login).
    assert pw.read_text(encoding="utf-8").strip() == generated
    assert _pgpass_line(pg).endswith(f":{generated}")


def test_reuse_refreshes_pgpass_from_existing_password(tmp_path: Path) -> None:
    pw = tmp_path / "runtime_db_password"
    pg = tmp_path / "runtime_pgpass"
    pw.write_text("preset-password-value\n", encoding="utf-8")

    status = ensure_runtime_secret(password_path=pw, pgpass_path=pg, **_KW)

    assert status == "reused"
    assert pw.read_text(encoding="utf-8").strip() == "preset-password-value"
    assert _pgpass_line(pg) == "postgres:5432:keel:keel_runtime_login:preset-password-value"


@pytest.mark.skipif(os.name == "nt", reason="POSIX file mode bits are not enforced on Windows")
def test_files_are_0600(tmp_path: Path) -> None:
    pw = tmp_path / "runtime_db_password"
    pg = tmp_path / "runtime_pgpass"

    ensure_runtime_secret(password_path=pw, pgpass_path=pg, **_KW)

    assert stat.S_IMODE(pw.stat().st_mode) == 0o600
    assert stat.S_IMODE(pg.stat().st_mode) == 0o600


def test_generate_leaves_no_stray_tempfiles(tmp_path: Path) -> None:
    pw = tmp_path / "runtime_db_password"
    pg = tmp_path / "runtime_pgpass"

    ensure_runtime_secret(password_path=pw, pgpass_path=pg, **_KW)

    # The atomic sibling-tempfile write must clean up after itself.
    names = sorted(p.name for p in tmp_path.iterdir())
    assert names == ["runtime_db_password", "runtime_pgpass"]


def test_main_reads_env_and_never_prints_secret(tmp_path: Path) -> None:
    pw = tmp_path / "runtime_db_password"
    pg = tmp_path / "runtime_pgpass"
    env = {
        **os.environ,
        "KEEL_RUNTIME_DB_PASSWORD_FILE": str(pw),
        "KEEL_RUNTIME_PGPASS_FILE": str(pg),
        "KEEL_RUNTIME_DB_HOST": "postgres",
        "KEEL_RUNTIME_DB_PORT": "5432",
        "KEEL_RUNTIME_DB_NAME": "keel",
        "KEEL_RUNTIME_DB_USER": "keel_runtime_login",
    }
    result = subprocess.run(
        [sys.executable, "-m", "keel_core.provision_runtime_secret"],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    password = pw.read_text(encoding="utf-8").strip()
    assert password, "password file must be written"
    assert password not in result.stdout and password not in result.stderr
    assert _pgpass_line(pg).endswith(f":{password}")
