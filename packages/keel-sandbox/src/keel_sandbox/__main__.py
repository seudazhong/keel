"""Run the sandbox executor service inside a verified isolated runtime.

``KEEL_SANDBOX_ISOLATION_VERIFIED=true`` is an operator assertion that the container
mounts only the sanitized workspace (never its host parent, ``.git``, or ``.env``)
and enforces the admitted egress policy. ``KEEL_SANDBOX_WORKSPACE_SANITIZED=true``
separately enables shell only after the executor revalidates denied names are absent.
Neither assertion defaults on.
"""

from __future__ import annotations

import os
from pathlib import Path

import uvicorn

from keel_core.tools import UnsafeLocalDevExecutionEnvironment
from keel_sandbox.service import DirectoryWorkspaceProvider, create_app

DEFAULT_SANDBOX_HOST = "127.0.0.1"


def _enabled(name: str) -> bool:
    return os.getenv(name, "").lower() in {"1", "true", "yes"}


def main() -> None:
    workspace = os.getenv("KEEL_SANDBOX_WORKSPACE", "/workspace")
    sanitized = _enabled("KEEL_SANDBOX_WORKSPACE_SANITIZED")

    def _build(root: Path | str) -> UnsafeLocalDevExecutionEnvironment:
        return UnsafeLocalDevExecutionEnvironment(
            root,
            shell_workspace_provisioned=sanitized,
        )

    default_environment = _build(workspace)
    # Every validated ``ws_<hash>`` namespace is confined to its own child directory beneath
    # the namespaces root; ``None`` (unscoped) keeps the default workspace explicitly. Without
    # a namespaces root the executor stays single-workspace and denies scoped namespaces.
    namespaces_root = os.getenv("KEEL_SANDBOX_NAMESPACES_ROOT")
    provider = None
    if namespaces_root:
        provider = DirectoryWorkspaceProvider(
            namespaces_root,
            _build,
            default_environment=default_environment,
        )
    app = create_app(
        default_environment,
        isolation_verified=_enabled("KEEL_SANDBOX_ISOLATION_VERIFIED"),
        shared_secret=os.getenv("KEEL_SANDBOX_RPC_SECRET"),
        allow_unauthenticated_local_test=_enabled("KEEL_SANDBOX_RPC_LOCAL_TEST_MODE"),
        workspace_provider=provider,
    )
    uvicorn.run(
        app,
        host=os.getenv("KEEL_SANDBOX_HOST", DEFAULT_SANDBOX_HOST),
        port=int(os.getenv("KEEL_SANDBOX_PORT", "8090")),
    )


if __name__ == "__main__":
    main()
