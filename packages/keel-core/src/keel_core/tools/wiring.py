"""Execution-environment selection with production-safe defaults."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Literal

from keel_core.config import Settings
from keel_core.tools.environment import (
    ExecutionEnvironment,
    UnsafeLocalDevExecutionEnvironment,
)
from keel_core.tools.rpc import SandboxExecutionEnvironment


class ExecutionBackend(StrEnum):
    sandbox = "sandbox"
    unsafe_local_dev = "unsafe-local-dev"


def build_service_execution_environment(
    settings: Settings,
    workspace: Path,
    *,
    service: Literal["server", "worker"],
) -> ExecutionEnvironment:
    """Build execution for a service, rejecting unsafe in-process use by default."""

    try:
        backend = ExecutionBackend(settings.execution_backend)
    except ValueError as exc:
        raise RuntimeError(f"{service}: unknown execution backend") from exc
    if backend is ExecutionBackend.sandbox:
        if not settings.sandbox_url:
            raise RuntimeError(f"{service}: sandbox URL is required")
        if settings.sandbox_rpc_local_test_mode and settings.app_env not in {"dev", "test"}:
            raise RuntimeError(f"{service}: unauthenticated sandbox RPC is local-test only")
        return SandboxExecutionEnvironment(
            settings.sandbox_url,
            shared_secret=settings.sandbox_rpc_secret.get_secret_value(),
            allow_unauthenticated_local_test=settings.sandbox_rpc_local_test_mode,
        )
    if not settings.trusted_preview_allow_unsafe_execution:
        raise RuntimeError(
            f"{service}: unsafe-local-dev execution requires "
            "KEEL_TRUSTED_PREVIEW_ALLOW_UNSAFE_EXECUTION=true"
        )
    return UnsafeLocalDevExecutionEnvironment(
        workspace,
        shell_workspace_provisioned=settings.trusted_preview_shell_workspace_sanitized,
    )
