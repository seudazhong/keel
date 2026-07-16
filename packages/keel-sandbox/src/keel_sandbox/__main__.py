"""Run the sandbox executor service inside a verified isolated runtime.

``KEEL_SANDBOX_ISOLATION_VERIFIED=true`` is an operator assertion that the container
mounts only the sanitized workspace (never its host parent, ``.git``, or ``.env``)
and enforces the admitted egress policy. ``KEEL_SANDBOX_WORKSPACE_SANITIZED=true``
separately enables shell only after the executor revalidates denied names are absent.
Neither assertion defaults on.
"""

from __future__ import annotations

import os

import uvicorn

from keel_core.tools import UnsafeLocalDevExecutionEnvironment
from keel_sandbox.service import create_app

DEFAULT_SANDBOX_HOST = "127.0.0.1"


def _enabled(name: str) -> bool:
    return os.getenv(name, "").lower() in {"1", "true", "yes"}


def main() -> None:
    workspace = os.getenv("KEEL_SANDBOX_WORKSPACE", "/workspace")
    environment = UnsafeLocalDevExecutionEnvironment(
        workspace,
        shell_workspace_provisioned=_enabled("KEEL_SANDBOX_WORKSPACE_SANITIZED"),
    )
    app = create_app(
        environment,
        isolation_verified=_enabled("KEEL_SANDBOX_ISOLATION_VERIFIED"),
        shared_secret=os.getenv("KEEL_SANDBOX_RPC_SECRET"),
        allow_unauthenticated_local_test=_enabled("KEEL_SANDBOX_RPC_LOCAL_TEST_MODE"),
    )
    uvicorn.run(
        app,
        host=os.getenv("KEEL_SANDBOX_HOST", DEFAULT_SANDBOX_HOST),
        port=int(os.getenv("KEEL_SANDBOX_PORT", "8090")),
    )


if __name__ == "__main__":
    main()
