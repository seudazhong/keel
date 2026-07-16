"""In-process transport proof for the sandbox RPC boundary."""

from __future__ import annotations

import sys
from pathlib import Path

import httpx

from keel_core.tools import (
    CommandRequest,
    ReadRequest,
    SandboxExecutionEnvironment,
    UnsafeLocalDevExecutionEnvironment,
    WriteRequest,
)
from keel_sandbox.service import create_app

_RPC_SECRET = "test-sandbox-rpc-secret-" + ("x" * 32)


async def test_rpc_round_trip_and_policy_denial(tmp_path: Path) -> None:
    service = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path),
        isolation_verified=True,
        shared_secret=_RPC_SECRET,
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service),
        base_url="http://sandbox",
    )
    environment = SandboxExecutionEnvironment(
        "http://sandbox",
        shared_secret=_RPC_SECRET,
        client=client,
    )

    written = await environment.write(WriteRequest("nested/file.txt", "hello"))
    assert written.ok
    read = await environment.read(ReadRequest("nested/file.txt"))
    assert read.ok and read.output == "hello"

    denied = await environment.read(ReadRequest("../outside.txt"))
    assert not denied.ok
    assert denied.error is not None
    assert denied.error.code.value == "denied"
    await client.aclose()


async def test_rpc_obfuscated_shell_is_blocked_by_workspace_validation(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("RPC-TOP-SECRET", encoding="utf-8")
    service = create_app(
        UnsafeLocalDevExecutionEnvironment(
            tmp_path,
            shell_workspace_provisioned=True,
        ),
        isolation_verified=True,
        shared_secret=_RPC_SECRET,
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service),
        base_url="http://sandbox",
    )
    environment = SandboxExecutionEnvironment(
        "http://sandbox",
        shared_secret=_RPC_SECRET,
        client=client,
    )
    command = (
        f'"{sys.executable}" -c "from pathlib import Path; '
        'print(Path(chr(46)+chr(101)+chr(110)+chr(118)).read_text())"'
    )
    result = await environment.execute(CommandRequest(command))
    assert not result.ok
    assert result.error is not None
    assert result.error.code.value == "denied"
    assert "RPC-TOP-SECRET" not in result.output
    await client.aclose()
