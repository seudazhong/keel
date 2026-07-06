"""Built-in file tools (FR-T1), confined to a workspace root.

read / write / edit / ls / glob / grep. Every path is resolved and checked to stay
within the workspace and to avoid sensitive names (``.git``/``.env``). Output is
bounded (FR-T5).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from keel_core.protocols import ToolContext, ToolResult
from keel_core.tools.bounding import bound_output

_DENY_PARTS = frozenset({".git", ".env"})


def _safe_path(workspace: Path, rel: str) -> Path | None:
    """Resolve ``rel`` under ``workspace``; return None if it escapes or is denied."""
    if not rel:
        return None
    root = workspace.resolve()
    candidate = (root / rel).resolve()
    if candidate != root and not candidate.is_relative_to(root):
        return None
    if any(part in _DENY_PARTS for part in candidate.relative_to(root).parts):
        return None
    return candidate


def _confine(match: Path, root: Path) -> Path | None:
    """Return ``match`` as a workspace-relative path if confined, else None."""
    resolved = match.resolve()
    if not resolved.is_relative_to(root):
        return None
    rel = resolved.relative_to(root)
    if any(part in _DENY_PARTS for part in rel.parts):
        return None
    return rel


class _WorkspaceTool:
    def __init__(self, workspace: Path | str, *, spill_dir: Path | None = None) -> None:
        self._workspace = Path(workspace)
        self._spill_dir = spill_dir


class ReadTool(_WorkspaceTool):
    name = "read"
    description = "Read a UTF-8 text file within the workspace."
    writes = False

    def input_schema(self) -> dict[str, Any]:
        return {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        path = _safe_path(self._workspace, str(args.get("path", "")))
        if path is None:
            return ToolResult(ok=False, output="path denied or outside workspace")
        if not path.is_file():
            return ToolResult(ok=False, output="file not found")
        bounded = bound_output(
            path.read_text(encoding="utf-8", errors="replace"), spill_dir=self._spill_dir
        )
        return ToolResult(ok=True, output=bounded.text, spill_path=bounded.spill_path)


class WriteTool(_WorkspaceTool):
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
        path = _safe_path(self._workspace, str(args.get("path", "")))
        if path is None:
            return ToolResult(ok=False, output="path denied or outside workspace")
        content = str(args.get("content", ""))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return ToolResult(ok=True, output=f"wrote {len(content)} bytes")


class EditTool(_WorkspaceTool):
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
        path = _safe_path(self._workspace, str(args.get("path", "")))
        if path is None or not path.is_file():
            return ToolResult(ok=False, output="path denied or file not found")
        old = str(args.get("old", ""))
        new = str(args.get("new", ""))
        text = path.read_text(encoding="utf-8")
        occurrences = text.count(old)
        if occurrences == 0:
            return ToolResult(ok=False, output="`old` string not found")
        if occurrences > 1:
            return ToolResult(ok=False, output="`old` string is not unique")
        path.write_text(text.replace(old, new, 1), encoding="utf-8")
        return ToolResult(ok=True, output="edited 1 occurrence")


class LsTool(_WorkspaceTool):
    name = "ls"
    description = "List entries of a directory within the workspace."
    writes = False

    def input_schema(self) -> dict[str, Any]:
        return {"type": "object", "properties": {"path": {"type": "string"}}}

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        path = _safe_path(self._workspace, str(args.get("path", ".")) or ".")
        if path is None or not path.is_dir():
            return ToolResult(ok=False, output="path denied or not a directory")
        entries = sorted(f"{p.name}/" if p.is_dir() else p.name for p in path.iterdir())
        bounded = bound_output("\n".join(entries), spill_dir=self._spill_dir)
        return ToolResult(ok=True, output=bounded.text, spill_path=bounded.spill_path)


class GlobTool(_WorkspaceTool):
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
        root = self._workspace.resolve()
        try:
            candidates = list(root.glob(str(args.get("pattern", "*"))))
        except (ValueError, NotImplementedError) as exc:
            return ToolResult(ok=False, output=f"invalid glob pattern: {exc}")
        matches: list[str] = []
        for match in candidates:
            rel = _confine(match, root)  # drop anything that escapes the workspace
            if rel is not None:
                matches.append(str(rel))
        matches.sort()
        bounded = bound_output("\n".join(matches), spill_dir=self._spill_dir)
        return ToolResult(ok=True, output=bounded.text, spill_path=bounded.spill_path)


class GrepTool(_WorkspaceTool):
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
        try:
            regex = re.compile(str(args.get("pattern", "")))
        except re.error as exc:
            return ToolResult(ok=False, output=f"invalid regex: {exc}")
        root = self._workspace.resolve()
        try:
            candidates = sorted(root.glob(str(args.get("glob", "**/*"))))
        except (ValueError, NotImplementedError) as exc:
            return ToolResult(ok=False, output=f"invalid glob pattern: {exc}")
        hits: list[str] = []
        for match in candidates:
            rel = _confine(match, root)  # skip anything that escapes the workspace
            if rel is None or not match.is_file():
                continue
            for lineno, line in enumerate(
                match.read_text(encoding="utf-8", errors="replace").splitlines(), start=1
            ):
                if regex.search(line):
                    hits.append(f"{rel}:{lineno}:{line}")
        bounded = bound_output("\n".join(hits), spill_dir=self._spill_dir)
        return ToolResult(ok=True, output=bounded.text, spill_path=bounded.spill_path)
