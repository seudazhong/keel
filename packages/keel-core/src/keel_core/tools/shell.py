"""Shell tool (FR-T2) — run a command in the workspace, timeout- and output-bounded.

M0/M1-α runs in-process via the platform shell; the two-level container sandbox
(ADR-0005) and per-command policy wrap this later in M1. Untrusted callers should
keep this off the safe toolset and behind the permission gate (default ask).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from keel_core.protocols import ToolContext, ToolResult
from keel_core.tools.bounding import bound_output


class ShellTool:
    name = "shell"
    description = "Run a shell command in the workspace (timeout-bounded)."

    def __init__(
        self, workspace: Path | str, *, timeout: float = 30.0, spill_dir: Path | None = None
    ) -> None:
        self._workspace = Path(workspace)
        self._timeout = timeout
        self._spill_dir = spill_dir

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"command": {"type": "string"}},
            "required": ["command"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        command = str(args.get("command", ""))
        if not command:
            return ToolResult(ok=False, output="empty command")
        proc = await asyncio.create_subprocess_shell(
            command,
            cwd=str(self._workspace),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=self._timeout)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            return ToolResult(ok=False, output=f"command timed out after {self._timeout}s")

        bounded = bound_output(stdout.decode("utf-8", errors="replace"), spill_dir=self._spill_dir)
        output = bounded.text or f"(exit {proc.returncode})"
        return ToolResult(ok=proc.returncode == 0, output=output, spill_path=bounded.spill_path)
