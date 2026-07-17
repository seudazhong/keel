"""Shell tool delegated through the configured execution environment."""

from __future__ import annotations

from typing import Any

from keel_core.protocols import ToolContext, ToolResult
from keel_core.tools.environment import (
    CommandRequest,
    ExecutionEnvironment,
    ExecutionLimits,
    OperationOptions,
)


class ShellTool:
    name = "shell"
    description = "Run a shell command in the workspace (timeout-bounded)."
    writes = True

    def __init__(self, environment: ExecutionEnvironment, *, timeout: float = 30.0) -> None:
        self._environment = environment
        self._options = OperationOptions(limits=ExecutionLimits(timeout_seconds=timeout))

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
        result = await self._environment.execute(CommandRequest(command, self._options))
        return ToolResult(ok=result.ok, output=result.output, spill_path=result.spill_path)
