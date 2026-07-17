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


async def test_rpc_namespaces_are_confined_to_distinct_workspaces(tmp_path: Path) -> None:
    # The executor must never ignore the namespace: two scopes writing the *same* relative
    # path must resolve to two isolated workspaces (M3.6, item 6).
    from keel_sandbox.service import DirectoryWorkspaceProvider

    namespaces_root = tmp_path / "namespaces"

    def _build(root: Path) -> UnsafeLocalDevExecutionEnvironment:
        return UnsafeLocalDevExecutionEnvironment(root)

    provider = DirectoryWorkspaceProvider(
        namespaces_root,
        _build,
        default_environment=UnsafeLocalDevExecutionEnvironment(tmp_path / "default"),
    )
    service = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path / "default"),
        isolation_verified=True,
        shared_secret=_RPC_SECRET,
        workspace_provider=provider,
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service),
        base_url="http://sandbox",
    )
    ns_a = "ws_" + ("a" * 32)
    ns_b = "ws_" + ("b" * 32)
    env_a = SandboxExecutionEnvironment(
        "http://sandbox", shared_secret=_RPC_SECRET, client=client, workspace=ns_a
    )
    env_b = SandboxExecutionEnvironment(
        "http://sandbox", shared_secret=_RPC_SECRET, client=client, workspace=ns_b
    )

    assert (await env_a.write(WriteRequest("shared.txt", "from-a"))).ok
    assert (await env_b.write(WriteRequest("shared.txt", "from-b"))).ok

    read_a = await env_a.read(ReadRequest("shared.txt"))
    read_b = await env_b.read(ReadRequest("shared.txt"))
    assert read_a.ok and read_a.output == "from-a"
    assert read_b.ok and read_b.output == "from-b"  # not clobbered by A — isolated data

    # The bytes really live under two distinct namespace roots (no shared workspace).
    assert (namespaces_root / ns_a / "shared.txt").read_text() == "from-a"
    assert (namespaces_root / ns_b / "shared.txt").read_text() == "from-b"
    await client.aclose()


async def test_rpc_unprovisionable_scoped_workspace_fails_closed(tmp_path: Path) -> None:
    # A provider that cannot provision a scoped workspace fails the operation closed rather
    # than falling back to a shared/default workspace.
    from keel_sandbox.service import DirectoryWorkspaceProvider

    class _NoProvision(DirectoryWorkspaceProvider):
        def resolve(self, namespace: str | None):  # type: ignore[override]
            return None if namespace is not None else self._default

    provider = _NoProvision(
        tmp_path / "namespaces",
        lambda root: UnsafeLocalDevExecutionEnvironment(root),
        default_environment=UnsafeLocalDevExecutionEnvironment(tmp_path / "default"),
    )
    service = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path / "default"),
        isolation_verified=True,
        shared_secret=_RPC_SECRET,
        workspace_provider=provider,
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service),
        base_url="http://sandbox",
    )
    env = SandboxExecutionEnvironment(
        "http://sandbox",
        shared_secret=_RPC_SECRET,
        client=client,
        workspace="ws_" + ("c" * 32),
    )
    result = await env.write(WriteRequest("x.txt", "data"))
    assert not result.ok
    assert result.error is not None and result.error.code.value == "unavailable"
    await client.aclose()
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
