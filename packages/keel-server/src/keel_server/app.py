"""FastAPI application factory with liveness and readiness probes.

M0 exposes only health/readiness so ``compose --profile dev`` can report a
healthy stack. Engine/Redis clients are created lazily (no connection at
import), so importing this module never requires a live datastore.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text

from keel_core import __version__
from keel_core.api import HealthResponse, ReadinessResponse
from keel_core.config import get_settings
from keel_core.db import make_async_engine, make_redis
from keel_server.api import v1

logger = logging.getLogger("keel.server")


def create_app() -> FastAPI:
    """Build the Keel FastAPI application."""
    settings = get_settings()
    app = FastAPI(title="Keel", version=__version__)
    engine = make_async_engine(settings)
    redis_client = make_redis(settings)

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        """Liveness: the process is up and serving."""
        return HealthResponse(service="keel-server", version=__version__)

    @app.get("/readiness", response_model=ReadinessResponse)
    async def readiness() -> JSONResponse:
        """Readiness: dependencies (Postgres, Redis) are reachable."""
        checks: dict[str, str] = {}
        ready = True

        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            checks["postgres"] = "ok"
        except Exception as exc:  # noqa: BLE001 - report, never crash the probe
            checks["postgres"] = f"error: {exc.__class__.__name__}"
            ready = False

        try:
            await redis_client.ping()
            checks["redis"] = "ok"
        except Exception as exc:  # noqa: BLE001
            checks["redis"] = f"error: {exc.__class__.__name__}"
            ready = False

        body = ReadinessResponse(ready=ready, checks=checks)
        return JSONResponse(body.model_dump(), status_code=200 if ready else 503)

    app.include_router(v1.router)

    return app


app = create_app()
