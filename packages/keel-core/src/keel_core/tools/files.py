"""Built-in file tools delegated entirely through an execution environment."""

from __future__ import annotations

from typing import Any

from keel_core.protocols import ToolContext, ToolResult
from keel_core.tools.environment import (
    DeleteRequest,
    EditRequest,
    ExecutionEnvironment,
    ExecutionResult,
    GlobRequest,
    GrepRequest,
    ListRequest,
    ReadRequest,
    WriteRequest,
)


def _tool_result(result: ExecutionResult) -> ToolResult:
    return ToolResult(ok=result.ok, output=result.output, spill_path=result.spill_path)


class _EnvironmentTool:
    def __init__(self, environment: ExecutionEnvironment) -> None:
        self._environment = environment


class ReadTool(_EnvironmentTool):
    name = "read"
    description = "Read a UTF-8 text file within the workspace."
    writes = False

    def input_schema(self) -> dict[str, Any]:
        return {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        return _tool_result(await self._environment.read(ReadRequest(str(args.get("path", "")))))


class WriteTool(_EnvironmentTool):
    name = "write"
    description = "Write UTF-8 text to a file within the workspace (creates parents)."
    writes = True

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"path": {"type": "string"}, "content": {"type": "string"}},
            "required": ["path", "content"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        request = WriteRequest(
            path=str(args.get("path", "")),
            content=str(args.get("content", "")),
        )
        return _tool_result(await self._environment.write(request))


class EditTool(_EnvironmentTool):
    name = "edit"
    description = "Replace the single exact occurrence of `old` with `new` in a file."
    writes = True

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "old": {"type": "string"},
                "new": {"type": "string"},
            },
            "required": ["path", "old", "new"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        request = EditRequest(
            path=str(args.get("path", "")),
            old=str(args.get("old", "")),
            new=str(args.get("new", "")),
        )
        return _tool_result(await self._environment.edit(request))


class DeleteTool(_EnvironmentTool):
    name = "delete"
    description = "Delete a single regular file within the workspace (no directories or symlinks)."
    writes = True

    def input_schema(self) -> dict[str, Any]:
        return {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        return _tool_result(
            await self._environment.delete(DeleteRequest(str(args.get("path", ""))))
        )


class LsTool(_EnvironmentTool):
    name = "ls"
    description = "List entries of a directory within the workspace."
    writes = False

    def input_schema(self) -> dict[str, Any]:
        return {"type": "object", "properties": {"path": {"type": "string"}}}

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        request = ListRequest(str(args.get("path", ".")) or ".")
        return _tool_result(await self._environment.list(request))


class GlobTool(_EnvironmentTool):
    name = "glob"
    description = "Find files in the workspace matching a glob pattern."
    writes = False

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"pattern": {"type": "string"}},
            "required": ["pattern"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        request = GlobRequest(str(args.get("pattern", "*")))
        return _tool_result(await self._environment.glob(request))


class GrepTool(_EnvironmentTool):
    name = "grep"
    description = "Search workspace files for a regex, returning path:line:text matches."
    writes = False

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"pattern": {"type": "string"}, "glob": {"type": "string"}},
            "required": ["pattern"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        request = GrepRequest(
            pattern=str(args.get("pattern", "")),
            glob=str(args.get("glob", "**/*")),
        )
        return _tool_result(await self._environment.grep(request))
