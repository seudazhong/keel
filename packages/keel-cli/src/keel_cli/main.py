"""Keel CLI — the admin/power-user client (ADR-0009).

Commands:

- ``version`` / ``ping`` — M0 diagnostics.
- ``chat``  — interactive streaming REPL with tool approvals and ``/slash`` commands.
- ``run``   — one-shot / headless: send a single prompt, stream (or emit ``--json``),
  exit non-zero unless the run completed.

The heavy lifting lives in :mod:`keel_cli.runner`, which wires the agent loop,
provider gateway, built-in tools, store and permission profile into a session.
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path
from typing import Annotated, Any

import httpx
import typer

from keel_cli.runner import ChatSession, _preview, build_session
from keel_core import InMemoryEventStore, __version__
from keel_core.config import get_settings
from keel_core.events import EventType
from keel_core.tools.executor import ApproveFn
from keel_core.types import StopReason

app = typer.Typer(
    help="Keel CLI — admin/power-user client.",
    no_args_is_help=True,
    add_completion=False,
)


# --- M0 diagnostics ------------------------------------------------------------


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


# --- shared helpers ------------------------------------------------------------


def _err(text: str) -> None:
    sys.stderr.write(text)
    sys.stderr.flush()


def _configure_console() -> None:
    # Stream tokens may be any language (e.g. CJK); force UTF-8 so a legacy console
    # codepage (cp1252) can't raise UnicodeEncodeError mid-run and abort the loop.
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except (ValueError, OSError):  # pragma: no cover - stream can't be reconfigured
                pass


def _out(text: str) -> None:
    sys.stdout.write(text)
    sys.stdout.flush()


def _noop(text: str) -> None:
    return None


def _auto_approver(value: bool) -> ApproveFn:
    def _approve(call: Any, ctx: Any) -> bool:
        return value

    return _approve


def _interactive_approver() -> ApproveFn:
    def _approve(call: Any, ctx: Any) -> bool:
        return bool(
            typer.confirm(f"approve {call.name}({_preview(call.arguments)})?", default=False)
        )

    return _approve


def _prepare_loop(durable: bool) -> None:
    # psycopg's async driver requires the selector loop on Windows (the shell tool's
    # subprocess needs the proactor loop, so durable + shell don't mix on win32).
    if durable and sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def _make_store(durable: bool, scope_id: str) -> tuple[Any, Any]:
    """Return ``(store, engine)``; ``engine`` is None for the in-memory store."""
    if not durable:
        return InMemoryEventStore(), None
    from keel_core import PostgresEventStore
    from keel_core.db import make_async_engine

    engine = make_async_engine(get_settings())
    return PostgresEventStore(engine, scope_id), engine


# --- interactive chat ----------------------------------------------------------

_HELP = """commands:
  /help            show this help
  /new             start a fresh session (clears history)
  /clear           clear the screen
  /tools           list available tools
  /model [name]    show or set the model
  /history         replay the session's event log
  /exit, /quit     leave
"""


def _read_line() -> str:
    return input("you> ")


async def _print_history(session: ChatSession) -> None:
    async for event in session.store.read(session.session_id):
        role = event.payload.get("role")
        if role in ("user", "assistant"):
            _err(f"{role}: {_preview(event.payload.get('text', ''), 100)}\n")
        elif event.type is EventType.tool_call:
            _err(f"tool: {event.payload.get('tool')}({_preview(event.payload.get('args', {}))})\n")


async def _handle_slash(line: str, session: ChatSession) -> bool:
    """Run a ``/command``. Returns True when the REPL should exit."""
    parts = line[1:].split(maxsplit=1)
    cmd = parts[0].lower() if parts else ""
    arg = parts[1].strip() if len(parts) > 1 else ""

    if cmd in ("exit", "quit", "q"):
        return True
    if cmd in ("help", "h", "?"):
        _err(_HELP)
    elif cmd == "new":
        session.session_id = uuid.uuid4().hex
        if isinstance(session.store, InMemoryEventStore):
            session.store = InMemoryEventStore()
        _err("started a new session\n")
    elif cmd == "clear":
        sys.stdout.write("\x1b[2J\x1b[H")
        sys.stdout.flush()
    elif cmd == "tools":
        _err(", ".join(session.agent.toolset) + "\n")
    elif cmd == "model":
        if arg:
            session.agent.model = arg
            _err(f"model set to {arg}\n")
        else:
            _err(f"model: {session.agent.model}\n")
    elif cmd == "history":
        await _print_history(session)
    else:
        _err(f"unknown command: /{cmd} (try /help)\n")
    return False


async def _interactive(
    *, model: str, workspace: Path, allow_all: bool, durable: bool, scope_id: str
) -> None:
    store, engine = await _make_store(durable, scope_id)
    try:
        session = build_session(
            model=model,
            workspace=workspace,
            allow_all=allow_all,
            approve=_interactive_approver(),
            store=store,
            scope_id=scope_id,
        )
        _err(f"keel chat — model={model}, workspace={workspace}. /help for commands.\n")
        while True:
            try:
                line = (await asyncio.to_thread(_read_line)).strip()
            except (EOFError, KeyboardInterrupt):
                _err("\n")
                break
            if not line:
                continue
            if line.startswith("/"):
                if await _handle_slash(line, session):
                    break
                continue
            result = await session.send(line)
            if result.reason is not StopReason.completed:
                _err(f"[{result.reason}]\n")
    finally:
        if engine is not None:
            await engine.dispose()


@app.command()
def chat(
    model: Annotated[str, typer.Option("--model", "-m", help="LiteLLM model id.")] = "",
    workspace: Annotated[
        Path, typer.Option("--workspace", "-w", help="Tool workspace root.")
    ] = Path("."),
    allow_all: Annotated[
        bool, typer.Option("--allow-all", help="Allow every tool without prompting.")
    ] = False,
    durable: Annotated[
        bool, typer.Option("--durable", help="Persist the session to Postgres.")
    ] = False,
) -> None:
    """Start an interactive streaming chat session."""
    resolved = model or get_settings().default_model
    _prepare_loop(durable)
    asyncio.run(
        _interactive(
            model=resolved,
            workspace=workspace,
            allow_all=allow_all,
            durable=durable,
            scope_id="cli:local",
        )
    )


# --- one-shot / headless -------------------------------------------------------


@app.command(name="run")
def run_cmd(
    prompt: Annotated[list[str] | None, typer.Argument(help="Prompt (reads stdin if omitted).")] = (
        None
    ),
    model: Annotated[str, typer.Option("--model", "-m", help="LiteLLM model id.")] = "",
    workspace: Annotated[
        Path, typer.Option("--workspace", "-w", help="Tool workspace root.")
    ] = Path("."),
    allow_all: Annotated[
        bool, typer.Option("--allow-all", help="Allow every tool without prompting.")
    ] = False,
    yes: Annotated[
        bool, typer.Option("--yes", "-y", help="Auto-approve tools that would ask.")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit a single JSON result object.")
    ] = False,
    durable: Annotated[
        bool, typer.Option("--durable", help="Persist the session to Postgres.")
    ] = False,
) -> None:
    """Send a single prompt headlessly; exit non-zero unless the run completes."""
    text = " ".join(prompt) if prompt else sys.stdin.read()
    text = text.strip()
    if not text:
        typer.echo("error: no prompt provided", err=True)
        raise typer.Exit(code=2)

    resolved = model or get_settings().default_model
    _prepare_loop(durable)
    holder: dict[str, Any] = {}

    async def _main() -> None:
        store, engine = await _make_store(durable, "cli:local")
        try:
            session = build_session(
                model=resolved,
                workspace=workspace,
                allow_all=allow_all,
                approve=_auto_approver(yes or allow_all),
                store=store,
                write_out=_noop if json_output else _out,
                write_meta=_noop if json_output else _err,
            )
            result = await session.send(text)
            holder["result"] = result
            holder["output"] = session.renderer.output
        finally:
            if engine is not None:
                await engine.dispose()

    asyncio.run(_main())
    result = holder["result"]

    if json_output:
        typer.echo(
            json.dumps(
                {
                    "reason": str(result.reason),
                    "output": holder["output"],
                    "iterations": result.iterations,
                    "tokens": result.tokens,
                }
            )
        )
    elif result.reason is not StopReason.completed:
        _err(f"[{result.reason}]\n")

    raise typer.Exit(code=0 if result.reason is StopReason.completed else 1)


def main() -> None:
    """CLI entry point."""
    _configure_console()
    app()


if __name__ == "__main__":
    main()
