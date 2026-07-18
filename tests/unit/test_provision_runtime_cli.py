"""Unit tests for the runtime-login provisioning CLI password resolution (M3A, WS-DB).

Covers :func:`keel_core.provision_runtime_cli._resolve_password` — the secret-hygiene seam that
reads the login password from a mounted file, stdin, or the environment, but NEVER a CLI argument.
No database is required.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest

from keel_core.provision_runtime_cli import _resolve_password, build_parser


def test_password_file_reads_first_line_stripped(tmp_path: Path) -> None:
    secret = tmp_path / "runtime_db_password"
    secret.write_text("s3cr3t-runtime-pw\n", encoding="utf-8")

    args = build_parser().parse_args(["--password-file", str(secret)])
    assert _resolve_password(args) == "s3cr3t-runtime-pw"


def test_password_file_uses_only_first_line(tmp_path: Path) -> None:
    secret = tmp_path / "runtime_db_password"
    secret.write_text("first-line-only\nignored-second-line\n", encoding="utf-8")

    args = build_parser().parse_args(["--password-file", str(secret)])
    assert _resolve_password(args) == "first-line-only"


def test_password_file_empty_fails_closed(tmp_path: Path) -> None:
    secret = tmp_path / "runtime_db_password"
    secret.write_text("\n", encoding="utf-8")

    args = build_parser().parse_args(["--password-file", str(secret)])
    with pytest.raises(ValueError, match="empty"):
        _resolve_password(args)


def test_password_file_missing_path_fails_closed_without_leaking(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"

    args = build_parser().parse_args(["--password-file", str(missing)])
    with pytest.raises(ValueError, match="could not read runtime login password") as exc:
        _resolve_password(args)
    # The sanitized message carries the path + exception class, never file contents.
    assert "FileNotFoundError" in str(exc.value)


def test_password_env_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEEL_RUNTIME_DB_PASSWORD", "env-runtime-pw")

    args = build_parser().parse_args([])  # default: --password-env KEEL_RUNTIME_DB_PASSWORD
    assert _resolve_password(args) == "env-runtime-pw"


def test_password_env_empty_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KEEL_RUNTIME_DB_PASSWORD", raising=False)

    args = build_parser().parse_args([])
    with pytest.raises(ValueError, match="empty"):
        _resolve_password(args)


def test_password_stdin_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO("stdin-runtime-pw\ntrailing\n"))

    args = build_parser().parse_args(["--password-stdin"])
    assert _resolve_password(args) == "stdin-runtime-pw"


def test_file_and_stdin_are_mutually_exclusive(tmp_path: Path) -> None:
    secret = tmp_path / "runtime_db_password"
    secret.write_text("x\n", encoding="utf-8")

    # argparse rejects combining two secret sources (they share a mutually-exclusive group).
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--password-file", str(secret), "--password-stdin"])
