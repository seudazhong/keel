"""Entry point: run the server with uvicorn."""

from __future__ import annotations

import uvicorn

from keel_core.config import get_settings


def main() -> None:
    """Run the Keel server."""
    settings = get_settings()
    uvicorn.run(
        "keel_server.app:app",
        host=settings.server_host,
        port=settings.server_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
