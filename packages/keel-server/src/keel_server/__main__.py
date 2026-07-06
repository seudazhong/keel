"""Entry point: run the server with uvicorn."""

from __future__ import annotations

import uvicorn

from keel_core.config import get_settings, load_env_file
from keel_core.observability import configure_logging, configure_tracing


def main() -> None:
    """Run the Keel server."""
    load_env_file()  # provider keys + KEEL_* from one .env
    settings = get_settings()
    configure_logging(settings.log_level)
    configure_tracing("keel-server")
    uvicorn.run(
        "keel_server.app:app",
        host=settings.server_host,
        port=settings.server_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
