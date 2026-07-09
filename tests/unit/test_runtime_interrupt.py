"""AgentRuntime interrupt wiring + the /v1/runs/{id}/interrupt endpoint."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport

from keel_server.api.v1 import router
from keel_server.runtime import AgentRuntime


def test_interrupt_run_marks_known_runs() -> None:
    runtime = AgentRuntime(redis_client=MagicMock(), model="m", workspace=Path("."))
    assert runtime.interrupt_run("unknown") is False  # no such run

    runtime._runs["r1"] = MagicMock()  # simulate an in-flight run
    assert runtime.interrupt_run("r1") is True
    assert "r1" in runtime._interrupted

    runtime._forget("r1")  # done-callback cleans both maps
    assert "r1" not in runtime._runs and "r1" not in runtime._interrupted


@pytest_asyncio.fixture
async def interrupt_client() -> AsyncIterator[httpx.AsyncClient]:
    class FakeRuntime:
        def interrupt_run(self, run_id: str) -> bool:
            return run_id == "known"

    app = FastAPI()
    app.include_router(router)
    app.state.runtime = FakeRuntime()
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_interrupt_endpoint(interrupt_client: httpx.AsyncClient) -> None:
    assert (await interrupt_client.post("/v1/runs/known/interrupt")).json() == {"ok": True}
    assert (await interrupt_client.post("/v1/runs/other/interrupt")).json() == {"ok": False}
