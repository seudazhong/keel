"""Run the sandbox executor service inside a verified isolated runtime."""

from __future__ import annotations

import os

import uvicorn

from keel_core.tools import UnsafeLocalDevExecutionEnvironment
from keel_sandbox.service import create_app


def _enabled(name: str) -> bool:
    return os.getenv(name, "").lower() in {"1", "true", "yes"}


def main() -> None:
    workspace = os.getenv("KEEL_SANDBOX_WORKSPACE", "/workspace")
    environment = UnsafeLocalDevExecutionEnvironment(workspace)
    app = create_app(
        environment,
        isolation_verified=_enabled("KEEL_SANDBOX_ISOLATION_VERIFIED"),
    )
    uvicorn.run(
        app,
        host=os.getenv("KEEL_SANDBOX_HOST", "0.0.0.0"),
        port=int(os.getenv("KEEL_SANDBOX_PORT", "8090")),
    )


if __name__ == "__main__":
    main()
