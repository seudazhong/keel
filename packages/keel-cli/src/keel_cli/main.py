"""Typer CLI — a thin client of the server API (M0: version + health ping)."""

from __future__ import annotations

from typing import Annotated

import httpx
import typer

from keel_core import __version__
from keel_core.config import get_settings

app = typer.Typer(
    help="Keel CLI — admin/power-user client.",
    no_args_is_help=True,
    add_completion=False,
)


@app.command()
def version() -> None:
    """Print the Keel CLI version."""
    typer.echo(f"keel {__version__}")


@app.command()
def ping(
    base_url: Annotated[
        str, typer.Option(help="Server base URL; defaults to config host:port.")
    ] = "",
) -> None:
    """Check the server's /health endpoint."""
    settings = get_settings()
    host = "127.0.0.1" if settings.server_host in ("0.0.0.0", "") else settings.server_host
    url = base_url or f"http://{host}:{settings.server_port}"
    try:
        resp = httpx.get(f"{url}/health", timeout=5.0)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(resp.text)


def main() -> None:
    """CLI entry point."""
    app()


if __name__ == "__main__":
    main()
