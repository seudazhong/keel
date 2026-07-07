"""Smoke tests: every package imports and the CLI runs (no external services)."""

from __future__ import annotations

import importlib

import pytest

MODULES = [
    "keel_core",
    "keel_core.config",
    "keel_core.db",
    "keel_core.types",
    "keel_core.errors",
    "keel_core.events",
    "keel_core.agents",
    "keel_core.protocols",
    "keel_core.api",
    "keel_core.context",
    "keel_core.scope",
    "keel_core.state",
    "keel_core.memory",
    "keel_core.tokens",
    "keel_core.secrets",
    "keel_core.connectors",
    "keel_core.approvals",
    "keel_core.digest",
    "keel_core.embeddings",
    "keel_core.search",
    "keel_core.extensibility",
    "keel_core.projections",
    "keel_core.loop",
    "keel_core.providers",
    "keel_core.permissions",
    "keel_core.tools",
    "keel_core.tools.bounding",
    "keel_core.tools.executor",
    "keel_core.tools.files",
    "keel_core.tools.shell",
    "keel_core.eventbus",
    "keel_core.observability",
    "keel_core.tracing",
    "keel_core.testing",
    "keel_core.testing.record_replay",
    "keel_server",
    "keel_server.app",
    "keel_server.api.v1",
    "keel_server.api.gateway",
    "keel_server.gateway",
    "keel_server.gateway.onebot",
    "keel_server.runtime",
    "keel_server.webui",
    "keel_worker",
    "keel_worker.main",
    "keel_scheduler",
    "keel_scheduler.main",
    "keel_scheduler.atmostonce",
    "keel_scheduler.store",
    "keel_sandbox",
    "keel_sandbox.policy",
    "keel_cli",
    "keel_cli.main",
    "keel_cli.runner",
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
