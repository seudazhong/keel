"""Execution environment safety, cancellation, and service wiring tests."""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import httpx
import pytest
import yaml

from keel_core.config import Settings
from keel_core.tools import (
    CancellationToken,
    CommandRequest,
    ExecutionErrorCode,
    ExecutionResult,
    OperationOptions,
    ReadRequest,
    RpcRequestSigner,
    RpcRequestVerifier,
    SandboxExecutionEnvironment,
    UnsafeLocalDevExecutionEnvironment,
    build_service_execution_environment,
)
from keel_core.tools.environment import ExecutionLimits
from keel_core.tools.rpc import ExecutionOperation, ExecutionRpcRequest
from keel_sandbox.__main__ import DEFAULT_SANDBOX_HOST
from keel_sandbox.service import ExecutorAdmissionPolicy, create_app

_RPC_SECRET = "test-sandbox-rpc-secret-" + ("x" * 32)


def test_compose_web_healthcheck_uses_ipv4_loopback() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose = yaml.safe_load((repo_root / "docker-compose.yml").read_text(encoding="utf-8"))
    probe = compose["services"]["keel-web"]["healthcheck"]["test"]
    assert "http://127.0.0.1/web-health" in probe


@pytest.mark.parametrize("service", ["server", "worker"])
def test_services_reject_unsafe_execution_without_trusted_preview(
    service: str,
    tmp_path: Path,
) -> None:
    settings = Settings(
        execution_backend="unsafe-local-dev",
        trusted_preview_allow_unsafe_execution=False,
    )
    with pytest.raises(RuntimeError, match="requires"):
        build_service_execution_environment(settings, tmp_path, service=service)  # type: ignore[arg-type]


@pytest.mark.parametrize("service", ["server", "worker"])
def test_services_use_sandbox_by_default(service: str, tmp_path: Path) -> None:
    environment = build_service_execution_environment(
        Settings(sandbox_rpc_secret=_RPC_SECRET),
        tmp_path,
        service=service,  # type: ignore[arg-type]
    )
    assert isinstance(environment, SandboxExecutionEnvironment)


@pytest.mark.parametrize("service", ["server", "worker"])
def test_services_fail_closed_when_sandbox_secret_is_missing(
    service: str,
    tmp_path: Path,
) -> None:
    with pytest.raises(RuntimeError, match="shared secret"):
        build_service_execution_environment(
            Settings(),
            tmp_path,
            service=service,  # type: ignore[arg-type]
        )


def test_unauthenticated_rpc_test_mode_is_rejected_in_production(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="local-test only"):
        build_service_execution_environment(
            Settings(app_env="prod", sandbox_rpc_local_test_mode=True),
            tmp_path,
            service="server",
        )


async def test_local_command_cooperative_cancellation(tmp_path: Path) -> None:
    environment = UnsafeLocalDevExecutionEnvironment(
        tmp_path,
        shell_workspace_provisioned=True,
    )
    token = CancellationToken()
    slow = "ping -n 6 127.0.0.1" if sys.platform == "win32" else "sleep 5"
    task = asyncio.create_task(
        environment.execute(
            CommandRequest(
                slow,
                OperationOptions(
                    limits=ExecutionLimits(timeout_seconds=5),
                    cancellation=token,
                ),
            )
        )
    )
    await asyncio.sleep(0.1)
    token.cancel()
    result = await task
    assert not result.ok
    assert result.error is not None
    assert result.error.code is ExecutionErrorCode.cancelled


async def test_shell_requires_explicit_sanitized_workspace_provisioning(tmp_path: Path) -> None:
    result = await UnsafeLocalDevExecutionEnvironment(tmp_path).execute(
        CommandRequest("echo should-not-run")
    )
    assert not result.ok
    assert result.error is not None
    assert result.error.code is ExecutionErrorCode.denied


async def test_typed_results_are_bounded(tmp_path: Path) -> None:
    (tmp_path / "large.txt").write_text("x" * 100, encoding="utf-8")
    environment = UnsafeLocalDevExecutionEnvironment(tmp_path)
    result = await environment.read(
        ReadRequest(
            "large.txt",
            OperationOptions(limits=ExecutionLimits(max_bytes=10)),
        )
    )
    assert result.ok
    assert result.truncated
    assert result.output.startswith("x" * 10)


async def test_rpc_transport_errors_fail_closed() -> None:
    async def unavailable(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"detail": "down"})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(unavailable),
        base_url="http://sandbox",
    )
    environment = SandboxExecutionEnvironment(
        "http://sandbox",
        shared_secret=_RPC_SECRET,
        client=client,
    )
    result = await environment.execute(CommandRequest("echo nope"))
    assert not result.ok
    assert result.error is not None
    assert result.error.code is ExecutionErrorCode.unavailable
    await client.aclose()


def test_trusted_preview_can_explicitly_select_unsafe_local(tmp_path: Path) -> None:
    environment = build_service_execution_environment(
        Settings(
            execution_backend="unsafe-local-dev",
            trusted_preview_allow_unsafe_execution=True,
        ),
        tmp_path,
        service="server",
    )
    assert isinstance(environment, UnsafeLocalDevExecutionEnvironment)


async def test_compose_trusted_preview_default_disables_shell_but_allows_files(
    tmp_path: Path,
) -> None:
    """The Compose `dev` contract: opt-in unsafe-local-dev with shell still fail-closed.

    Compose sets KEEL_EXECUTION_BACKEND=unsafe-local-dev and the allow-unsafe flag but
    deliberately leaves KEEL_TRUSTED_PREVIEW_SHELL_WORKSPACE_SANITIZED unset, because it
    cannot prove OS isolation. Shell/command execution must therefore be denied while
    file tools still work in the local preview.
    """
    settings = Settings(
        execution_backend="unsafe-local-dev",
        trusted_preview_allow_unsafe_execution=True,
    )
    assert settings.trusted_preview_shell_workspace_sanitized is False
    for service in ("server", "worker"):
        environment = build_service_execution_environment(
            settings,
            tmp_path,
            service=service,  # type: ignore[arg-type]
        )
        assert isinstance(environment, UnsafeLocalDevExecutionEnvironment)

        shell = await environment.execute(CommandRequest("echo hi"))
        assert not shell.ok
        assert shell.error is not None
        assert shell.error.code is ExecutionErrorCode.denied

        read = await environment.read(ReadRequest("missing.txt"))
        assert not read.ok
        assert read.error is not None
        assert read.error.code is ExecutionErrorCode.not_found


def test_rpc_authentication_fails_closed_without_strong_secret() -> None:
    with pytest.raises(RuntimeError, match="shared secret"):
        SandboxExecutionEnvironment("http://sandbox")
    with pytest.raises(RuntimeError, match="at least"):
        SandboxExecutionEnvironment("http://sandbox", shared_secret="weak")
    with pytest.raises(RuntimeError, match="shared secret"):
        create_app(UnsafeLocalDevExecutionEnvironment("."))


def test_sandbox_service_defaults_to_loopback_binding() -> None:
    assert DEFAULT_SANDBOX_HOST == "127.0.0.1"


def test_rpc_secret_is_redacted_from_settings_repr() -> None:
    settings = Settings(sandbox_rpc_secret=_RPC_SECRET)
    assert _RPC_SECRET not in repr(settings)


def test_replay_cache_fails_closed_when_full() -> None:
    body = b'{"operation":"read","path":"file.txt"}'
    signer = RpcRequestSigner(_RPC_SECRET, clock=lambda: 1000.0)
    verifier = RpcRequestVerifier(
        _RPC_SECRET,
        clock=lambda: 1000.0,
        nonce_cache_size=1,
    )
    first = signer.headers(body, nonce="a" * 32)
    second = signer.headers(body, nonce="b" * 32)
    assert verifier.verify(first, body, method="POST", path="/v1/execute")
    assert not verifier.verify(second, body, method="POST", path="/v1/execute")
    assert not verifier.verify(first, body, method="POST", path="/v1/execute")


async def test_explicit_unauthenticated_local_rpc_mode(tmp_path: Path) -> None:
    (tmp_path / "file.txt").write_text("local", encoding="utf-8")
    app = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path),
        isolation_verified=True,
        allow_unauthenticated_local_test=True,
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://sandbox",
    )
    environment = SandboxExecutionEnvironment(
        "http://sandbox",
        allow_unauthenticated_local_test=True,
        client=client,
    )
    result = await environment.read(ReadRequest("file.txt"))
    assert result.ok and result.output == "local"
    await client.aclose()


def test_admission_denies_traversal_sensitive_paths_and_egress() -> None:
    admission = ExecutorAdmissionPolicy()
    for path in ("../secret", r"..\secret", ".git/config", ".env"):
        denial = admission.admit(ExecutionRpcRequest(operation=ExecutionOperation.read, path=path))
        assert denial is not None
        assert denial.error is not None
        assert denial.error.code is ExecutionErrorCode.denied
    egress = admission.admit(
        ExecutionRpcRequest(
            operation=ExecutionOperation.command,
            command="curl https://example.com",
            requested_egress_hosts=["example.com"],
        )
    )
    assert egress is not None
    assert egress.error is not None
    assert egress.error.code is ExecutionErrorCode.denied
    command_path = admission.admit(
        ExecutionRpcRequest(
            operation=ExecutionOperation.command,
            command="cat .env",
        )
    )
    assert command_path is not None
    assert command_path.error is not None
    assert command_path.error.code is ExecutionErrorCode.denied


def test_unverified_sandbox_service_fails_closed(tmp_path: Path) -> None:
    app = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path),
        shared_secret=_RPC_SECRET,
    )
    transport = httpx.ASGITransport(app=app)

    async def invoke() -> ExecutionResult:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://sandbox",
        ) as client:
            environment = SandboxExecutionEnvironment(
                "http://sandbox",
                shared_secret=_RPC_SECRET,
                client=client,
            )
            return await environment.read(ReadRequest("file.txt"))

    result = asyncio.run(invoke())
    assert result.ok is False
    assert result.error is not None
    assert result.error.code is ExecutionErrorCode.unavailable


async def test_replay_tamper_and_binding_are_rejected(tmp_path: Path) -> None:
    (tmp_path / "file.txt").write_text("hello", encoding="utf-8")
    app = create_app(
        UnsafeLocalDevExecutionEnvironment(tmp_path),
        isolation_verified=True,
        shared_secret=_RPC_SECRET,
    )
    body = (
        ExecutionRpcRequest(
            operation=ExecutionOperation.read,
            path="file.txt",
        )
        .model_dump_json()
        .encode()
    )
    signer = RpcRequestSigner(_RPC_SECRET)
    nonce = "n" * 32
    headers = signer.headers(body, nonce=nonce)
    headers["Content-Type"] = "application/json"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://sandbox",
    ) as client:
        first = await client.post("/v1/execute", content=body, headers=headers)
        replay = await client.post("/v1/execute", content=body, headers=headers)
        tampered = await client.post(
            "/v1/execute",
            content=body.replace(b"file.txt", b"nope.txt"),
            headers=signer.headers(body, nonce="t" * 32),
        )
        wrong_binding = await client.post(
            "/v1/execute",
            content=body,
            headers=signer.headers(body, path="/v1/other", nonce="b" * 32),
        )
        stale = await client.post(
            "/v1/execute",
            content=body,
            headers=signer.headers(
                body,
                timestamp=int(time.time()) - 61,
                nonce="s" * 32,
            ),
        )
        unsigned = await client.post("/v1/execute", content=body)

    assert first.status_code == 200
    assert {replay.status_code, tampered.status_code, wrong_binding.status_code} == {401}
    assert stale.status_code == 401
    assert unsigned.status_code == 401


@pytest.mark.parametrize(
    ("denied_path", "expression"),
    [
        (".env", "Path(chr(46)+chr(101)+chr(110)+chr(118))"),
        (".git/config", "Path(chr(46)+'git')/'config'"),
    ],
)
async def test_obfuscated_shell_cannot_read_denied_workspace_paths(
    tmp_path: Path,
    denied_path: str,
    expression: str,
) -> None:
    target = tmp_path / denied_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("TOP-SECRET", encoding="utf-8")
    environment = UnsafeLocalDevExecutionEnvironment(
        tmp_path,
        shell_workspace_provisioned=True,
    )
    command = f'"{sys.executable}" -c "from pathlib import Path; print(({expression}).read_text())"'
    result = await environment.execute(CommandRequest(command))
    assert not result.ok
    assert result.error is not None
    assert result.error.code is ExecutionErrorCode.denied
    assert "TOP-SECRET" not in result.output


async def test_shell_sanitization_rejects_workspace_symlink_escape(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    secret = tmp_path / ".env"
    secret.write_text("SYMLINK-TOP-SECRET", encoding="utf-8")
    link = workspace / "innocent-name"
    try:
        link.symlink_to(secret)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    environment = UnsafeLocalDevExecutionEnvironment(
        workspace,
        shell_workspace_provisioned=True,
    )
    command = (
        f'"{sys.executable}" -c "from pathlib import Path; '
        "print(Path('innocent-name').read_text())\""
    )
    result = await environment.execute(CommandRequest(command))
    assert not result.ok
    assert result.error is not None
    assert result.error.code is ExecutionErrorCode.denied
    assert "SYMLINK-TOP-SECRET" not in result.output
