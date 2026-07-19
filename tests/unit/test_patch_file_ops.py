"""File-operation safety: byte-preserving text edits + the file-only delete operation (P3a-2)."""

from __future__ import annotations

import os
from pathlib import Path

import httpx
import pytest

from keel_core.protocols import ToolContext
from keel_core.tools import (
    DeleteRequest,
    EditRequest,
    ExecutionErrorCode,
    SandboxExecutionEnvironment,
    UnsafeLocalDevExecutionEnvironment,
    WriteRequest,
)
from keel_core.tools.files import DeleteTool
from keel_core.tools.rpc import ExecutionOperation, ExecutionRpcRequest
from keel_core.tools.textio import (
    TextPolicyError,
    decode_text,
    detect_newline,
    encode_text,
    to_logical_newlines,
)
from keel_sandbox.service import ExecutorAdmissionPolicy, create_app

_RPC_SECRET = "test-sandbox-rpc-secret-" + ("x" * 32)


def _ctx() -> ToolContext:
    return ToolContext(scope_id="s", session_id="sess")


# --- textio helper ------------------------------------------------------------------


def test_decode_text_strict_rejects_binary_and_invalid_utf8() -> None:
    assert decode_text(b"hello") == "hello"
    assert decode_text("café".encode()) == "café"
    with pytest.raises(TextPolicyError):
        decode_text(b"a\x00b")  # NUL -> binary
    with pytest.raises(TextPolicyError):
        decode_text(b"\xff\xfe")  # not valid UTF-8, no replacement character


def test_detect_newline_lf_crlf_and_fail_closed() -> None:
    assert detect_newline(b"a\nb\n") == "\n"
    assert detect_newline(b"a\r\nb\r\n") == "\r\n"
    assert detect_newline(b"no-newline") == "\n"
    with pytest.raises(TextPolicyError):
        detect_newline(b"a\r\nb\nc\n")  # mixed CRLF + LF
    with pytest.raises(TextPolicyError):
        detect_newline(b"a\rb")  # bare CR


def test_encode_text_reemits_style_and_rejects_nul() -> None:
    assert encode_text("a\nb\n", "\n") == b"a\nb\n"
    assert encode_text("a\nb\n", "\r\n") == b"a\r\nb\r\n"
    # A logical view that already has CRLF is normalized, never doubled to \r\r\n.
    assert encode_text("a\r\nb", "\r\n") == b"a\r\nb"
    with pytest.raises(TextPolicyError):
        encode_text("a\x00b", "\n")


def test_to_logical_newlines() -> None:
    assert to_logical_newlines("a\r\nb\r\n") == "a\nb\n"


# --- byte-preserving write / edit ----------------------------------------------------


async def test_write_preserves_existing_crlf_style(tmp_path: Path) -> None:
    target = tmp_path / "f.txt"
    target.write_bytes(b"line1\r\nline2\r\n")
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await env.write(WriteRequest("f.txt", "alpha\nbeta\n"))
    assert result.ok
    assert target.read_bytes() == b"alpha\r\nbeta\r\n"


async def test_write_new_file_uses_lf(tmp_path: Path) -> None:
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    assert (await env.write(WriteRequest("new.txt", "a\nb\n"))).ok
    assert (tmp_path / "new.txt").read_bytes() == b"a\nb\n"


async def test_edit_preserves_crlf_and_untouched_bytes(tmp_path: Path) -> None:
    # The exact live P3a regression: editing a CRLF file must not collapse it to LF.
    target = tmp_path / "g.txt"
    target.write_bytes(b"line1\r\nline2\r\n")
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await env.edit(EditRequest("g.txt", old="line2", new="line2-edited"))
    assert result.ok
    assert target.read_bytes() == b"line1\r\nline2-edited\r\n"


async def test_edit_preserves_lf(tmp_path: Path) -> None:
    target = tmp_path / "h.txt"
    target.write_bytes(b"a\nb\n")
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    assert (await env.edit(EditRequest("h.txt", old="b", new="c"))).ok
    assert target.read_bytes() == b"a\nc\n"


async def test_edit_mixed_newlines_fails_closed(tmp_path: Path) -> None:
    target = tmp_path / "mixed.txt"
    target.write_bytes(b"a\r\nb\nc\n")
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await env.edit(EditRequest("mixed.txt", old="b", new="B"))
    assert not result.ok
    assert result.error is not None and result.error.code is ExecutionErrorCode.invalid
    assert target.read_bytes() == b"a\r\nb\nc\n"  # unchanged


async def test_write_and_edit_refuse_binary(tmp_path: Path) -> None:
    target = tmp_path / "bin"
    target.write_bytes(b"\x00\x01\x02BINARY")
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    write = await env.write(WriteRequest("bin", "text"))
    assert not write.ok and write.error is not None
    assert write.error.code is ExecutionErrorCode.invalid
    edit = await env.edit(EditRequest("bin", old="B", new="C"))
    assert not edit.ok and edit.error is not None
    assert edit.error.code is ExecutionErrorCode.invalid
    assert target.read_bytes() == b"\x00\x01\x02BINARY"  # untouched


async def test_write_rejects_nul_content(tmp_path: Path) -> None:
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await env.write(WriteRequest("x.txt", "a\x00b"))
    assert not result.ok and result.error is not None
    assert result.error.code is ExecutionErrorCode.invalid
    assert not (tmp_path / "x.txt").exists()


async def test_edit_invalid_utf8_fails_closed(tmp_path: Path) -> None:
    target = tmp_path / "latin.txt"
    target.write_bytes(b"\xff\xfe caf\xe9")  # not valid UTF-8, no NUL
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await env.edit(EditRequest("latin.txt", old="caf", new="CAF"))
    assert not result.ok and result.error is not None
    assert result.error.code is ExecutionErrorCode.invalid


# --- delete operation ----------------------------------------------------------------


async def test_delete_regular_file(tmp_path: Path) -> None:
    target = tmp_path / "gone.txt"
    target.write_text("bye", encoding="utf-8")
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await env.delete(DeleteRequest("gone.txt"))
    assert result.ok
    assert "1 file" in result.output
    assert not target.exists()


async def test_delete_missing_is_idempotent(tmp_path: Path) -> None:
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await env.delete(DeleteRequest("never.txt"))
    assert result.ok
    assert "did not exist" in result.output


async def test_delete_refuses_directory(tmp_path: Path) -> None:
    (tmp_path / "sub").mkdir()
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await env.delete(DeleteRequest("sub"))
    assert not result.ok and result.error is not None
    assert result.error.code is ExecutionErrorCode.denied
    assert (tmp_path / "sub").is_dir()


async def test_delete_refuses_binary_file(tmp_path: Path) -> None:
    target = tmp_path / "image.bin"
    target.write_bytes(b"\x00\x01\x02\x03")
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await env.delete(DeleteRequest("image.bin"))
    assert not result.ok and result.error is not None
    assert result.error.code is ExecutionErrorCode.denied
    assert target.exists()


async def test_delete_denies_forbidden_and_traversal(tmp_path: Path) -> None:
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    for path in ("../secret.txt", ".git/config", ".env"):
        result = await env.delete(DeleteRequest(path))
        assert not result.ok and result.error is not None
        assert result.error.code is ExecutionErrorCode.denied


async def test_delete_refuses_symlink(tmp_path: Path) -> None:
    real = tmp_path / "real.txt"
    real.write_text("data", encoding="utf-8")
    link = tmp_path / "link.txt"
    try:
        os.symlink(real, link)
    except (OSError, NotImplementedError):  # pragma: no cover - unprivileged Windows
        pytest.skip("symlink creation not permitted in this environment")
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await env.delete(DeleteRequest("link.txt"))
    assert not result.ok and result.error is not None
    assert result.error.code is ExecutionErrorCode.denied
    assert link.exists() and real.exists()  # neither the link nor its target is removed


async def test_delete_tool_delegates(tmp_path: Path) -> None:
    (tmp_path / "d.txt").write_text("x", encoding="utf-8")
    env = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await DeleteTool(env).run({"path": "d.txt"}, _ctx())
    assert result.ok
    assert not (tmp_path / "d.txt").exists()


# --- delete over the sandbox RPC -----------------------------------------------------


def test_admission_denies_delete_traversal_and_forbidden() -> None:
    admission = ExecutorAdmissionPolicy()
    for path in ("../secret", r"..\secret", ".git/config", ".env"):
        denial = admission.admit(
            ExecutionRpcRequest(operation=ExecutionOperation.delete, path=path)
        )
        assert denial is not None and denial.error is not None
        assert denial.error.code is ExecutionErrorCode.denied


async def test_delete_rpc_roundtrip(tmp_path: Path) -> None:
    (tmp_path / "f.txt").write_text("bye", encoding="utf-8")
    app = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path),
        isolation_verified=True,
        allow_unauthenticated_local_test=True,
    )
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://sandbox")
    environment = SandboxExecutionEnvironment(
        "http://sandbox", allow_unauthenticated_local_test=True, client=client
    )
    result = await environment.delete(DeleteRequest("f.txt"))
    assert result.ok
    assert not (tmp_path / "f.txt").exists()
    # Traversal is refused by admission over the wire, too.
    denied = await environment.delete(DeleteRequest("../secret"))
    assert not denied.ok and denied.error is not None
    assert denied.error.code is ExecutionErrorCode.denied
    await client.aclose()


async def test_delete_rpc_requires_authentication(tmp_path: Path) -> None:
    (tmp_path / "f.txt").write_text("bye", encoding="utf-8")
    app = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path),
        isolation_verified=True,
        shared_secret=_RPC_SECRET,
    )
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://sandbox")
    # A client signing with the wrong secret cannot delete: the request fails closed.
    environment = SandboxExecutionEnvironment(
        "http://sandbox", shared_secret="wrong-secret-" + ("y" * 32), client=client
    )
    result = await environment.delete(DeleteRequest("f.txt"))
    assert not result.ok
    assert (tmp_path / "f.txt").exists()  # nothing was deleted
    await client.aclose()
