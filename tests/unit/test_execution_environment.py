"""Execution environment safety, cancellation, and service wiring tests."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx
import pytest

from keel_core.config import Settings
from keel_core.tools import (
    CancellationToken,
    CommandRequest,
    ExecutionErrorCode,
    OperationOptions,
    ReadRequest,
    SandboxExecutionEnvironment,
    UnsafeLocalDevExecutionEnvironment,
    build_service_execution_environment,
)
from keel_core.tools.environment import ExecutionLimits
from keel_core.tools.rpc import ExecutionOperation, ExecutionRpcRequest
from keel_sandbox.service import ExecutorAdmissionPolicy, create_app


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
        Settings(),
        tmp_path,
        service=service,  # type: ignore[arg-type]
    )
    assert isinstance(environment, SandboxExecutionEnvironment)


async def test_local_command_cooperative_cancellation(tmp_path: Path) -> None:
    environment = UnsafeLocalDevExecutionEnvironment(tmp_path)
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
    environment = SandboxExecutionEnvironment("http://sandbox", client=client)
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
    app = create_app(UnsafeLocalDevExecutionEnvironment(tmp_path))
    transport = httpx.ASGITransport(app=app)

    async def invoke() -> httpx.Response:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://sandbox",
        ) as client:
            return await client.post(
                "/v1/execute",
                json={"operation": "read", "path": "file.txt"},
            )

    response = asyncio.run(invoke())
    assert response.status_code == 503
