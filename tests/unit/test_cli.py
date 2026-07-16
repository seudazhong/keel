"""CLI tests: session assembly, streaming render, approvals, one-shot run."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from keel_cli import runner as runner_mod
from keel_cli.main import app
from keel_cli.runner import ChatSession, build_session, build_tools, default_permissions
from keel_core.protocols import ProviderChunk, ToolCall, ToolContext
from keel_core.testing import ScriptedProviderGateway
from keel_core.types import FinishReason, PermissionDecision, StopReason


class _Capture:
    def __init__(self) -> None:
        self.parts: list[str] = []

    def __call__(self, text: str) -> None:
        self.parts.append(text)

    @property
    def text(self) -> str:
        return "".join(self.parts)


def test_cli_wires_shell_for_sanitized_workspace(tmp_path: Path) -> None:
    notices: list[str] = []
    tools = build_tools(tmp_path, write_notice=notices.append)
    assert [tool.name for tool in tools] == [
        "read",
        "write",
        "edit",
        "ls",
        "glob",
        "grep",
        "shell",
    ]
    assert notices == []


@pytest.mark.parametrize("git_kind", ["directory", "file"])
async def test_cli_git_workspace_disables_shell_but_keeps_file_tools(
    tmp_path: Path,
    git_kind: str,
) -> None:
    git_path = tmp_path / ".git"
    if git_kind == "directory":
        git_path.mkdir()
    else:
        git_path.write_text("gitdir: elsewhere", encoding="utf-8")
    out, meta = _Capture(), _Capture()
    session = build_session(
        model="test/model",
        workspace=tmp_path,
        provider=ScriptedProviderGateway(
            [[ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)]]
        ),
        write_out=out,
        write_meta=meta,
    )

    assert session.registry.get("shell") is None
    assert "shell" not in session.agent.toolset
    assert "shell disabled" in meta.text
    assert "file tools remain available" in meta.text

    write = session.registry.get("write")
    read = session.registry.get("read")
    assert write is not None and read is not None
    ctx = ToolContext(scope_id="cli:local", session_id="test")
    assert (await write.run({"path": "note.txt", "content": "hello"}, ctx)).ok
    result = await read.run({"path": "note.txt"}, ctx)
    assert result.ok and result.output == "hello"


def _session(
    tmp_path: Path, turns: list[list[ProviderChunk]], **kwargs: object
) -> tuple[ChatSession, _Capture, _Capture]:
    out, meta = _Capture(), _Capture()
    session = build_session(
        model="test/model",
        workspace=tmp_path,
        provider=ScriptedProviderGateway(turns),
        write_out=out,
        write_meta=meta,
        **kwargs,  # type: ignore[arg-type]
    )
    return session, out, meta


def test_default_permissions_profile() -> None:
    engine = default_permissions()
    ctx = None  # evaluate ignores ctx for these rules
    assert engine.evaluate("read", {}, ctx) is PermissionDecision.allow  # type: ignore[arg-type]
    assert engine.evaluate("write", {}, ctx) is PermissionDecision.ask  # type: ignore[arg-type]
    assert engine.evaluate("shell", {}, ctx) is PermissionDecision.ask  # type: ignore[arg-type]
    # An unknown tool falls through to the fail-closed default (ask).
    assert engine.evaluate("mystery", {}, ctx) is PermissionDecision.ask  # type: ignore[arg-type]


async def test_chat_session_streams_and_completes(tmp_path: Path) -> None:
    session, out, meta = _session(
        tmp_path,
        [
            [
                ProviderChunk(delta="Hel"),
                ProviderChunk(delta="lo", finish_reason=FinishReason.end_turn),
            ]
        ],
    )
    result = await session.send("hi")
    assert result.reason is StopReason.completed
    assert out.text == "Hello\n"  # streamed deltas + newline close
    assert session.renderer.output == "Hello"


async def test_chat_session_runs_allowed_tool(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_text("file body", encoding="utf-8")
    session, out, meta = _session(
        tmp_path,
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(id="c1", name="read", arguments={"path": "a.txt"}),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="done", finish_reason=FinishReason.end_turn)],
        ],
    )
    result = await session.send("read a.txt")
    assert result.reason is StopReason.completed
    assert "read(" in meta.text  # tool call rendered
    assert "ok:" in meta.text  # tool result rendered ok (read is allowed)
    assert out.text.endswith("done\n")


async def test_approval_denies_mutating_tool(tmp_path: Path) -> None:
    def deny(call: object, ctx: object) -> bool:
        return False

    session, out, meta = _session(
        tmp_path,
        [
            [
                ProviderChunk(
                    tool_call=ToolCall(
                        id="c1", name="write", arguments={"path": "b.txt", "content": "x"}
                    ),
                    finish_reason=FinishReason.tool_use,
                )
            ],
            [ProviderChunk(delta="ok", finish_reason=FinishReason.end_turn)],
        ],
        approve=deny,
    )
    result = await session.send("write b.txt")
    assert result.reason is StopReason.completed
    assert not (tmp_path / "b.txt").exists()  # ask -> denied -> nothing written
    assert "err:" in meta.text


def test_run_command_json(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def fake_provider() -> ScriptedProviderGateway:
        return ScriptedProviderGateway(
            [[ProviderChunk(delta="hello world", finish_reason=FinishReason.end_turn)]]
        )

    monkeypatch.setattr(runner_mod, "build_provider", fake_provider)
    result = CliRunner().invoke(app, ["run", "hi", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["reason"] == "completed"
    assert data["output"] == "hello world"


def test_run_command_nonzero_on_noncompletion(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def fake_provider() -> ScriptedProviderGateway:
        return ScriptedProviderGateway([[ProviderChunk()]])  # no text/tool/finish -> halted

    monkeypatch.setattr(runner_mod, "build_provider", fake_provider)
    result = CliRunner().invoke(app, ["run", "hi", "--json"])
    assert result.exit_code == 1
    assert json.loads(result.stdout)["reason"] == "halted"


async def test_chat_session_renders_provider_error(tmp_path: Path) -> None:
    class _Gateway:
        def stream(self, request: object):  # type: ignore[no-untyped-def]
            raise type("BadRequestError", (Exception,), {"status_code": 400})(
                'model "gpt-5.5" is not accessible via the /chat/completions endpoint'
            )

    out, meta = _Capture(), _Capture()
    session = build_session(
        model="github_copilot/gpt-5.5",
        workspace=tmp_path,
        provider=_Gateway(),
        write_out=out,
        write_meta=meta,
    )
    result = await session.send("hi")
    assert result.reason is StopReason.error
    assert result.error is not None
    assert "error:" in meta.text and "not accessible" in meta.text  # real message shown


def test_run_command_surfaces_provider_error(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class _Gateway:
        def stream(self, request: object):  # type: ignore[no-untyped-def]
            raise type("BadRequestError", (Exception,), {"status_code": 400})(
                'model "nope" is not accessible'
            )

    monkeypatch.setattr(runner_mod, "build_provider", lambda: _Gateway())
    result = CliRunner().invoke(app, ["run", "hi", "--json"])
    assert result.exit_code == 1
    data = json.loads(result.stdout)
    assert data["reason"] == "error"
    assert data["error"] is not None and "not accessible" in data["error"]
