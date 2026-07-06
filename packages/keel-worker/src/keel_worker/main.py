"""arq worker settings and a no-op task.

Run with: ``arq keel_worker.main.WorkerSettings``
Health:   ``arq keel_worker.main.WorkerSettings --check``

M0 proves the worker connects to Redis and executes a job; real agent-run
tasks land in M1 (WS-A/F).
"""

from __future__ import annotations

import logging
from typing import Any

from arq.connections import RedisSettings

from keel_core import __version__
from keel_core.config import get_settings, load_env_file
from keel_core.observability import configure_logging, configure_tracing

logger = logging.getLogger("keel.worker")


async def noop(ctx: dict[str, Any]) -> str:
    """Placeholder task proving the worker executes enqueued jobs."""
    logger.info("noop task executed")
    return "ok"


async def startup(ctx: dict[str, Any]) -> None:
    load_env_file()  # provider keys visible to LiteLLM before any agent task runs
    configure_logging(get_settings().log_level)
    configure_tracing("keel-worker")
    logger.info("keel-worker %s starting", __version__)


async def shutdown(ctx: dict[str, Any]) -> None:
    logger.info("keel-worker shutting down")


def _redis_settings() -> RedisSettings:
    return RedisSettings.from_dsn(get_settings().redis_url)


class WorkerSettings:
    """arq worker configuration (referenced by the ``arq`` CLI)."""

    functions = [noop]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = _redis_settings()
