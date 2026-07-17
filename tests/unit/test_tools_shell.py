"""Shell tool tests (subprocess, timeout, bounded output)."""

from __future__ import annotations

import sys
from pathlib import Path

from keel_core.protocols import ToolContext
from keel_core.tools import UnsafeLocalDevExecutionEnvironment
from keel_core.tools.shell import ShellTool


def _ctx() -> ToolContext:
    return ToolContext(scope_id="s", session_id="sess")


async def test_shell_echo(tmp_path: Path) -> None:
    result = await ShellTool(
        UnsafeLocalDevExecutionEnvironment(tmp_path, shell_workspace_provisioned=True)
    ).run({"command": "echo keeltest"}, _ctx())
    assert result.ok
    assert "keeltest" in result.output


async def test_shell_timeout(tmp_path: Path) -> None:
    slow = "ping -n 5 127.0.0.1" if sys.platform == "win32" else "sleep 5"
    result = await ShellTool(
        UnsafeLocalDevExecutionEnvironment(tmp_path, shell_workspace_provisioned=True),
        timeout=0.2,
    ).run({"command": slow}, _ctx())
    assert not result.ok
    assert "timed out" in result.output


async def test_shell_nonzero_exit(tmp_path: Path) -> None:
    result = await ShellTool(
        UnsafeLocalDevExecutionEnvironment(tmp_path, shell_workspace_provisioned=True)
    ).run({"command": "exit 3"}, _ctx())
    assert not result.ok
