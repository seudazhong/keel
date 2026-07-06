"""CLI tests: session assembly, streaming render, approvals, one-shot run."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from keel_cli import runner as runner_mod
from keel_cli.main import app
from keel_cli.runner import ChatSession, build_session, default_permissions
from keel_core.protocols import ProviderChunk, ToolCall
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
