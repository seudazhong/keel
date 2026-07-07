"""FastAPI application factory: probes, the agent runtime, and the web UI.

Liveness/readiness let ``compose --profile dev`` report a healthy stack. A
``lifespan`` builds the :class:`~keel_server.runtime.AgentRuntime` (Redis fan-out +
durable store + provider) and stores it on ``app.state`` for the ``/v1`` routes.
Datastore clients connect lazily, so importing this module never needs a live
datastore; the runtime is only built when the app actually starts.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import text

from keel_core import __version__
from keel_core.api import HealthResponse, ReadinessResponse
from keel_core.config import get_settings, load_env_file
from keel_core.db import make_async_engine, make_redis
from keel_core.providers import LiteLLMGateway
from keel_server.api import gateway as gateway_api
from keel_server.api import v1
from keel_server.gateway import OneBotGateway, RateLimiter
from keel_server.runtime import AgentRuntime
from keel_server.webui import INDEX_HTML

logger = logging.getLogger("keel.server")


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build datastore clients + the agent runtime on startup; dispose on shutdown."""
    settings = get_settings()
    load_env_file()  # provider keys (OPENAI/ANTHROPIC/...) for LiteLLM
    redis_client = make_redis(settings)
    engine = make_async_engine(settings) if settings.event_store == "postgres" else None
    app.state.redis = redis_client
    app.state.engine = engine
    app.state.runtime = AgentRuntime(
        redis_client=redis_client,
        engine=engine,
        model=settings.default_model,
        workspace=Path.cwd(),
    )
    # OneBot IM gateway (optional): only wired when an API base is configured.
    if settings.onebot_api_base:
        app.state.onebot_gateway = OneBotGateway(
            provider=LiteLLMGateway(),
            send=gateway_api.make_onebot_sender(
                settings.onebot_api_base, settings.onebot_access_token
            ),
            workspace=Path.cwd(),
            self_id=settings.onebot_self_id or None,
            model=settings.default_model,
            rate_limiter=RateLimiter(limit=settings.im_rate_limit),
        )
    try:
        yield
    finally:
        await redis_client.aclose()
        if engine is not None:
            await engine.dispose()


def create_app() -> FastAPI:
    """Build the Keel FastAPI application."""
    app = FastAPI(title="Keel", version=__version__, lifespan=_lifespan)

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def index() -> str:
        """Serve the minimal web chat UI."""
        return INDEX_HTML

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        """Liveness: the process is up and serving."""
        return HealthResponse(service="keel-server", version=__version__)

    @app.get("/readiness", response_model=ReadinessResponse)
    async def readiness() -> JSONResponse:
        """Readiness: dependencies (Postgres, Redis) are reachable."""
        settings = get_settings()
        checks: dict[str, str] = {}
        ready = True
        engine = getattr(app.state, "engine", None)
        redis_client = getattr(app.state, "redis", None)

        if redis_client is None:
            body = ReadinessResponse(ready=False, checks={"runtime": "not initialized"})
            return JSONResponse(body.model_dump(), status_code=503)

        if settings.event_store == "postgres":
            try:
                async with engine.connect() as conn:  # type: ignore[union-attr]
                    await conn.execute(text("SELECT 1"))
                checks["postgres"] = "ok"
            except Exception as exc:  # noqa: BLE001 - report, never crash the probe
                checks["postgres"] = f"error: {exc.__class__.__name__}"
                ready = False
        else:
            checks["event_store"] = "memory"

        try:
            await redis_client.ping()
            checks["redis"] = "ok"
        except Exception as exc:  # noqa: BLE001
            checks["redis"] = f"error: {exc.__class__.__name__}"
            ready = False

        body = ReadinessResponse(ready=ready, checks=checks)
        return JSONResponse(body.model_dump(), status_code=200 if ready else 503)

    app.include_router(v1.router)
    app.include_router(gateway_api.router)

    return app


app = create_app()
