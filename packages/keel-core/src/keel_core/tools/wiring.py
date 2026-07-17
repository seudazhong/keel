"""Execution-environment selection with production-safe defaults."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Literal

from keel_core.config import Settings
from keel_core.scoping import LOCAL_PREVIEW_SCOPE, workspace_namespace
from keel_core.tools.environment import (
    ExecutionEnvironment,
    UnavailableExecutionEnvironment,
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


def build_scoped_execution_environment(
    settings: Settings,
    workspace_root: Path,
    *,
    service: Literal["server", "worker"],
    scope_id: str,
) -> ExecutionEnvironment:
    """Build a per-scope, isolated execution environment (M3.6, item 3).

    Each data-plane scope gets its own workspace so one org/Agent's file/shell tools can never
    read or write another's. The scope id is mapped to a validated, opaque, traversal-free
    ``ws_<hash>`` namespace:

    * **sandbox** — the namespace is sent on every RPC so the executor confines the scope to a
      per-namespace workspace under its configured root. A sandbox that cannot provision a
      scoped workspace fails the file/shell operation closed (it never shares one).
    * **unsafe-local-dev** — a real per-scope subdirectory ``<root>/<namespace>`` is created
      and the workspace path policy confines all operations beneath it. The local-preview scope
      uses the root workspace directly (single-operator, non-cloud).

    If the environment cannot be built safely (unknown backend, missing sandbox url) the caller
    receives a fail-closed :class:`UnavailableExecutionEnvironment` rather than a shared one.
    """
    try:
        backend = ExecutionBackend(settings.execution_backend)
    except ValueError:
        return UnavailableExecutionEnvironment()
    namespace = workspace_namespace(scope_id)
    if backend is ExecutionBackend.sandbox:
        if not settings.sandbox_url:
            return UnavailableExecutionEnvironment()
        if settings.sandbox_rpc_local_test_mode and settings.app_env not in {"dev", "test"}:
            return UnavailableExecutionEnvironment()
        # The local-preview single-operator scope keeps the sandbox's default workspace; every
        # derived per-Agent scope is confined to its own namespace.
        scoped = None if scope_id == LOCAL_PREVIEW_SCOPE else namespace
        return SandboxExecutionEnvironment(
            settings.sandbox_url,
            shared_secret=settings.sandbox_rpc_secret.get_secret_value(),
            allow_unauthenticated_local_test=settings.sandbox_rpc_local_test_mode,
            workspace=scoped,
        )
    if not settings.trusted_preview_allow_unsafe_execution:
        return UnavailableExecutionEnvironment()
    if scope_id == LOCAL_PREVIEW_SCOPE:
        scoped_root = workspace_root
    else:
        scoped_root = (workspace_root / namespace).resolve()
        if not scoped_root.is_relative_to(workspace_root.resolve()):
            return UnavailableExecutionEnvironment()
        scoped_root.mkdir(parents=True, exist_ok=True)
    return UnsafeLocalDevExecutionEnvironment(
        scoped_root,
        shell_workspace_provisioned=settings.trusted_preview_shell_workspace_sanitized,
    )
