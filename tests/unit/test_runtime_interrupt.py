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


def test_set_model_switches_the_model() -> None:
    runtime = AgentRuntime(redis_client=MagicMock(), model="m1", workspace=Path("."))
    assert runtime.model == "m1"
    runtime.set_model("m2")
    assert runtime.model == "m2"


@pytest_asyncio.fixture
async def interrupt_client() -> AsyncIterator[httpx.AsyncClient]:
    class FakeRuntime:
        def __init__(self) -> None:
            self._m = "github_copilot/claude-sonnet-4.5"

        def interrupt_run(self, run_id: str) -> bool:
            return run_id == "known"

        @property
        def model(self) -> str:
            return self._m

        def set_model(self, model: str) -> None:
            self._m = model

    app = FastAPI()
    app.include_router(router)
    app.state.runtime = FakeRuntime()
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_interrupt_endpoint(interrupt_client: httpx.AsyncClient) -> None:
    assert (await interrupt_client.post("/v1/runs/known/interrupt")).json() == {"ok": True}
    assert (await interrupt_client.post("/v1/runs/other/interrupt")).json() == {"ok": False}


async def test_model_endpoints(interrupt_client: httpx.AsyncClient) -> None:
    got = (await interrupt_client.get("/v1/settings/model")).json()
    assert got["current"] == "github_copilot/claude-sonnet-4.5"
    assert "github_copilot/gpt-5.3-codex" in got["available"]

    put = await interrupt_client.put("/v1/settings/model", json={"model": "github_copilot/gpt-4o"})
    assert put.json() == {"ok": True, "current": "github_copilot/gpt-4o"}
    assert (await interrupt_client.get("/v1/settings/model")).json()["current"] == (
        "github_copilot/gpt-4o"
    )
