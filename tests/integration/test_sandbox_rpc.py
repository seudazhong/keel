"""In-process transport proof for the sandbox RPC boundary."""

from __future__ import annotations

import sys
from pathlib import Path

import httpx

from keel_core.tools import (
    CommandRequest,
    ExecutionResult,
    GlobRequest,
    GrepRequest,
    ListRequest,
    ReadRequest,
    RpcRequestSigner,
    SandboxExecutionEnvironment,
    UnsafeLocalDevExecutionEnvironment,
    WriteRequest,
)
from keel_core.tools.rpc_auth import (
    RPC_NONCE_HEADER,
    RPC_SIGNATURE_HEADER,
)
from keel_sandbox.service import create_app

_RPC_SECRET = "test-sandbox-rpc-secret-" + ("x" * 32)


class _RecordingShellEnvironment(UnsafeLocalDevExecutionEnvironment):
    """A per-namespace environment that records every command that reaches it.

    ``execute`` returns a canned success **without spawning a subprocess** — the integration
    event loop is a Windows selector loop that cannot create subprocesses, and, more to the
    point, reaching this method at all is what we assert (or refute): it proves whether the
    service gate admitted a namespaced command. File operations delegate to the real
    path-policy-confined implementation.
    """

    def __init__(self, root: Path) -> None:
        super().__init__(root, shell_workspace_provisioned=True)
        self.commands: list[str] = []

    async def execute(self, request: CommandRequest) -> ExecutionResult:
        self.commands.append(request.command)
        return ExecutionResult(ok=True, output="reached-environment", exit_code=0)


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


async def test_rpc_namespaced_shell_is_denied_without_os_isolation(tmp_path: Path) -> None:
    # A namespaced workspace is a child directory under a shared parent — that confines *file*
    # operations (path policy) but is NOT a shell sandbox: a subprocess can reference an
    # absolute path, expand a glob, or derive a sibling namespace path and escape its root.
    # Every namespaced ``command`` must be denied *before* it reaches the environment unless the
    # provider proves a real OS isolation boundary (``shell_isolated``).
    from keel_sandbox.service import DirectoryWorkspaceProvider

    built: dict[str, _RecordingShellEnvironment] = {}

    def _build(root: Path) -> _RecordingShellEnvironment:
        env = _RecordingShellEnvironment(root)
        built[str(root)] = env
        return env

    provider = DirectoryWorkspaceProvider(
        tmp_path / "namespaces",
        _build,
        default_environment=UnsafeLocalDevExecutionEnvironment(
            tmp_path / "default", shell_workspace_provisioned=True
        ),
        # Default: no proven OS boundary for a namespace root.
    )
    assert provider.namespaced_shell_isolated() is False
    service = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path / "default", shell_workspace_provisioned=True),
        isolation_verified=True,
        shared_secret=_RPC_SECRET,
        workspace_provider=provider,
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service),
        base_url="http://sandbox",
    )
    ns = "ws_" + ("a" * 32)
    sibling = "ws_" + ("b" * 32)
    env = SandboxExecutionEnvironment(
        "http://sandbox", shared_secret=_RPC_SECRET, client=client, workspace=ns
    )

    escapes = [
        "cat /etc/passwd",  # absolute path
        "cat $HOME/.ssh/id_rsa",  # shell expansion
        f"cat ../{sibling}/shared.txt",  # sibling namespace path derivation
    ]
    for command in escapes:
        result = await env.execute(CommandRequest(command))
        assert not result.ok
        assert result.error is not None
        assert result.error.code.value == "denied", command

    # The command NEVER reached any per-namespace environment — the gate ran first.
    assert all(spy.commands == [] for spy in built.values())

    # A file operation in the same namespace stays available (confined by path policy).
    assert (await env.write(WriteRequest("note.txt", "ok"))).ok
    await client.aclose()


async def test_rpc_namespaced_shell_allowed_only_with_proven_isolation(tmp_path: Path) -> None:
    # With an explicit ``shell_isolated`` assertion (a proven OS/container/microVM mount
    # boundary), the namespaced command is admitted and reaches the environment.
    from keel_sandbox.service import DirectoryWorkspaceProvider

    built: dict[str, _RecordingShellEnvironment] = {}

    def _build(root: Path) -> _RecordingShellEnvironment:
        env = _RecordingShellEnvironment(root)
        built[str(root)] = env
        return env

    provider = DirectoryWorkspaceProvider(
        tmp_path / "namespaces",
        _build,
        default_environment=UnsafeLocalDevExecutionEnvironment(
            tmp_path / "default", shell_workspace_provisioned=True
        ),
        shell_isolated=True,
    )
    assert provider.namespaced_shell_isolated() is True
    service = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path / "default", shell_workspace_provisioned=True),
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
        workspace="ws_" + ("a" * 32),
    )
    result = await env.execute(CommandRequest("echo hello"))
    assert result.ok
    assert result.output == "reached-environment"
    # The command was admitted through the gate and reached exactly the namespace environment.
    assert [cmd for spy in built.values() for cmd in spy.commands] == ["echo hello"]
    await client.aclose()

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


def _service_client(
    root: Path, *, isolation_verified: bool = True
) -> tuple[object, httpx.AsyncClient]:
    """Build the sandbox ASGI app + an in-process httpx client bound to it."""
    service = create_app(
        UnsafeLocalDevExecutionEnvironment(root),
        isolation_verified=isolation_verified,
        shared_secret=_RPC_SECRET,
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service),
        base_url="http://sandbox",
    )
    return service, client


async def test_rpc_glob_grep_list_round_trip(tmp_path: Path) -> None:
    # Read-side discovery tools (glob/grep/list) must round-trip through the authenticated RPC
    # exactly like write/read, confined to the sandbox workspace.
    _service, client = _service_client(tmp_path)
    environment = SandboxExecutionEnvironment(
        "http://sandbox", shared_secret=_RPC_SECRET, client=client
    )

    assert (await environment.write(WriteRequest("src/app.py", "import os\nTOKEN = 1\n"))).ok
    assert (await environment.write(WriteRequest("src/util.py", "TOKEN = 2\n"))).ok
    assert (await environment.write(WriteRequest("README.md", "docs\n"))).ok

    listed = await environment.list(ListRequest("."))
    assert listed.ok
    assert "README.md" in listed.output and "src/" in listed.output

    globbed = await environment.glob(GlobRequest("src/*.py"))
    assert globbed.ok
    glob_out = globbed.output.replace("\\", "/")  # normalize Windows path separators
    assert "src/app.py" in glob_out and "src/util.py" in glob_out
    assert "README.md" not in glob_out

    grepped = await environment.grep(GrepRequest(pattern="TOKEN", glob="src/*.py"))
    assert grepped.ok
    grep_out = grepped.output.replace("\\", "/")
    assert "src/app.py:2:TOKEN = 1" in grep_out
    assert "src/util.py:1:TOKEN = 2" in grep_out
    await client.aclose()


async def test_rpc_unsigned_request_is_rejected(tmp_path: Path) -> None:
    # A request with NO HMAC headers must be rejected at the service boundary (401) before any
    # tool runs — the shared secret is mandatory (the executor was built with one).
    _service, client = _service_client(tmp_path)
    # /v1/execute with a well-formed body but no signature headers.
    execute = await client.post(
        "/v1/execute",
        json={"op": "read", "path": "README.md"},
    )
    assert execute.status_code == 401
    # /v1/ping likewise requires authentication.
    ping = await client.post("/v1/ping", content=b"")
    assert ping.status_code == 401
    await client.aclose()


async def test_rpc_replayed_nonce_is_rejected(tmp_path: Path) -> None:
    # The HMAC binds a per-request nonce; replaying the identical signed headers a second time
    # must be rejected (401) even though the signature itself is valid.
    _service, client = _service_client(tmp_path)
    signer = RpcRequestSigner(_RPC_SECRET)
    # Sign an empty /v1/ping body with a pinned nonce so the replay is byte-identical.
    fixed_nonce = "n" * 40
    signed = signer.headers(b"", path="/v1/ping", nonce=fixed_nonce)
    assert signed[RPC_SIGNATURE_HEADER] and signed[RPC_NONCE_HEADER] == fixed_nonce

    first = await client.post("/v1/ping", content=b"", headers=signed)
    assert first.status_code == 200
    # Identical headers replayed: the nonce cache rejects it.
    second = await client.post("/v1/ping", content=b"", headers=signed)
    assert second.status_code == 401
    await client.aclose()


async def test_health_endpoint_is_unauthenticated_and_leaks_nothing(tmp_path: Path) -> None:
    # Container liveness must be callable without credentials and reveal no security state.
    (tmp_path / "secret.txt").write_text("WORKSPACE-CONTENT", encoding="utf-8")
    _service, client = _service_client(tmp_path)
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    # No secret, workspace content, or isolation posture leaks through liveness.
    text = response.text
    assert _RPC_SECRET not in text
    assert "WORKSPACE-CONTENT" not in text
    assert "isolation" not in text.lower()
    await client.aclose()


async def test_ping_readiness_and_probe_ready_contract(tmp_path: Path) -> None:
    # probe_ready proves reachability AND a valid auth contract, not merely a constructed client.
    _service, client = _service_client(tmp_path, isolation_verified=True)
    good = SandboxExecutionEnvironment("http://sandbox", shared_secret=_RPC_SECRET, client=client)
    ready = await good.probe_ready()
    assert ready.ok and ready.output == "ready"

    # Wrong shared secret -> the signed ping fails verification -> mapped to ``denied``.
    bad = SandboxExecutionEnvironment(
        "http://sandbox",
        shared_secret="wrong-secret-" + ("y" * 32),
        client=client,
    )
    denied = await bad.probe_ready()
    assert not denied.ok
    assert denied.error is not None and denied.error.code.value == "denied"
    await client.aclose()


async def test_probe_ready_reports_unavailable_when_isolation_unverified(tmp_path: Path) -> None:
    # A sandbox that has NOT asserted an isolation boundary must fail readiness closed (503),
    # which the client maps to ``unavailable`` so the control plane refuses to depend on it.
    _service, client = _service_client(tmp_path, isolation_verified=False)
    environment = SandboxExecutionEnvironment(
        "http://sandbox", shared_secret=_RPC_SECRET, client=client
    )
    result = await environment.probe_ready()
    assert not result.ok
    assert result.error is not None and result.error.code.value == "unavailable"
    await client.aclose()
