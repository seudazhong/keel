"""In-process transport proof for the sandbox RPC boundary."""

from __future__ import annotations

from pathlib import Path

import httpx

from keel_core.tools import (
    ReadRequest,
    SandboxExecutionEnvironment,
    UnsafeLocalDevExecutionEnvironment,
    WriteRequest,
)
from keel_sandbox.service import create_app


async def test_rpc_round_trip_and_policy_denial(tmp_path: Path) -> None:
    service = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path),
        isolation_verified=True,
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service),
        base_url="http://sandbox",
    )
    environment = SandboxExecutionEnvironment("http://sandbox", client=client)

    written = await environment.write(WriteRequest("nested/file.txt", "hello"))
    assert written.ok
    read = await environment.read(ReadRequest("nested/file.txt"))
    assert read.ok and read.output == "hello"

    denied = await environment.read(ReadRequest("../outside.txt"))
    assert not denied.ok
    assert denied.error is not None
    assert denied.error.code.value == "denied"
    await client.aclose()
