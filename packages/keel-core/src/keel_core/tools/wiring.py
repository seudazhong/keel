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
        return SandboxExecutionEnvironment(settings.sandbox_url)
    if not settings.trusted_preview_allow_unsafe_execution:
        raise RuntimeError(
            f"{service}: unsafe-local-dev execution requires "
            "KEEL_TRUSTED_PREVIEW_ALLOW_UNSAFE_EXECUTION=true"
        )
    return UnsafeLocalDevExecutionEnvironment(workspace)
