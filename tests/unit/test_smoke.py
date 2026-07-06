"""Smoke tests: every package imports and the CLI runs (no external services)."""

from __future__ import annotations

import importlib

import pytest

MODULES = [
    "keel_core",
    "keel_core.config",
    "keel_core.db",
    "keel_server",
    "keel_server.app",
    "keel_worker",
    "keel_worker.main",
    "keel_scheduler",
    "keel_scheduler.main",
    "keel_sandbox",
    "keel_cli",
    "keel_cli.main",
    "keel_sdk",
]


@pytest.mark.parametrize("module_name", MODULES)
def test_import(module_name: str) -> None:
    importlib.import_module(module_name)


def test_cli_version() -> None:
    from typer.testing import CliRunner

    from keel_cli.main import app

    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert "keel" in result.stdout
