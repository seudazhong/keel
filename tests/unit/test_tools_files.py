"""Built-in file tool tests (workspace-confined)."""

from __future__ import annotations

from pathlib import Path

from keel_core.protocols import ToolContext
from keel_core.tools.files import EditTool, GlobTool, GrepTool, LsTool, ReadTool, WriteTool


def _ctx() -> ToolContext:
    return ToolContext(scope_id="s", session_id="sess")


async def test_write_then_read(tmp_path: Path) -> None:
    ctx = _ctx()
    written = await WriteTool(tmp_path).run({"path": "a/b.txt", "content": "hello"}, ctx)
    assert written.ok
    read = await ReadTool(tmp_path).run({"path": "a/b.txt"}, ctx)
    assert read.ok
    assert read.output == "hello"


async def test_read_missing_file(tmp_path: Path) -> None:
    result = await ReadTool(tmp_path).run({"path": "nope.txt"}, _ctx())
    assert not result.ok


async def test_path_escape_and_sensitive_denied(tmp_path: Path) -> None:
    assert not (await ReadTool(tmp_path).run({"path": "../secret"}, _ctx())).ok
    assert not (await ReadTool(tmp_path).run({"path": ".git/config"}, _ctx())).ok
    assert not (await WriteTool(tmp_path).run({"path": ".env", "content": "x"}, _ctx())).ok


async def test_edit_unique_occurrence(tmp_path: Path) -> None:
    ctx = _ctx()
    await WriteTool(tmp_path).run({"path": "f.txt", "content": "foo bar foo"}, ctx)
    edited = await EditTool(tmp_path).run({"path": "f.txt", "old": "bar", "new": "baz"}, ctx)
    assert edited.ok
    assert (await ReadTool(tmp_path).run({"path": "f.txt"}, ctx)).output == "foo baz foo"


async def test_edit_rejects_non_unique(tmp_path: Path) -> None:
    ctx = _ctx()
    await WriteTool(tmp_path).run({"path": "f.txt", "content": "aa"}, ctx)
    edited = await EditTool(tmp_path).run({"path": "f.txt", "old": "a", "new": "b"}, ctx)
    assert not edited.ok


async def test_ls_glob_grep(tmp_path: Path) -> None:
    ctx = _ctx()
    await WriteTool(tmp_path).run({"path": "src/x.py", "content": "import os\nprint('hi')"}, ctx)
    await WriteTool(tmp_path).run({"path": "src/y.txt", "content": "nothing"}, ctx)

    listing = await LsTool(tmp_path).run({"path": "."}, ctx)
    assert "src/" in listing.output

    globbed = await GlobTool(tmp_path).run({"pattern": "src/*.py"}, ctx)
    assert globbed.output == str(Path("src/x.py"))

    grep = await GrepTool(tmp_path).run({"pattern": "import", "glob": "**/*.py"}, ctx)
    assert "x.py" in grep.output
    assert "import os" in grep.output
