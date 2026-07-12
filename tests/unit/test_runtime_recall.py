"""AgentRuntime background session-indexing lifecycle."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from keel_core.embeddings import FakeEmbedder
from keel_server.runtime import AgentRuntime


class _RecordingIndexer:
    def __init__(self) -> None:
        self.called = asyncio.Event()
        self.release = asyncio.Event()
        self.sessions: list[str] = []

    async def index_session(self, session_id: str) -> int:
        self.sessions.append(session_id)
        self.called.set()
        await self.release.wait()
        return 1


class _FailingIndexer:
    async def index_session(self, session_id: str) -> int:
        raise RuntimeError(f"cannot index {session_id}")


class _BlockingIndexer:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def index_session(self, session_id: str) -> int:
        self.started.set()
        await asyncio.Event().wait()
        return 0


async def test_run_completion_schedules_separate_index_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    indexer = _RecordingIndexer()
    runtime = AgentRuntime(
        redis_client=MagicMock(),
        model="m",
        workspace=Path("."),
        embedder=FakeEmbedder(dim=8),
        session_indexer=indexer,  # type: ignore[arg-type]
    )
    monkeypatch.setattr("keel_server.runtime.run", AsyncMock())

    await runtime._run(MagicMock(), "session-1", "run-1")
    await asyncio.wait_for(indexer.called.wait(), timeout=1)

    assert indexer.sessions == ["session-1"]
    assert len(runtime._index_tasks) == 1
    assert runtime._runs == {}
    indexer.release.set()
    await runtime.aclose()


async def test_index_failure_is_logged_not_raised(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = AgentRuntime(
        redis_client=MagicMock(),
        model="m",
        workspace=Path("."),
        embedder=FakeEmbedder(dim=8),
        session_indexer=_FailingIndexer(),  # type: ignore[arg-type]
    )
    monkeypatch.setattr("keel_server.runtime.run", AsyncMock())
    caplog.set_level(logging.ERROR, logger="keel.server.runtime")

    await runtime._run(MagicMock(), "broken-session", "run-1")
    await asyncio.sleep(0)

    assert "session embedding index failed" in caplog.text
    assert "broken-session" in caplog.text
    await runtime.aclose()


async def test_aclose_cancels_pending_index_tasks() -> None:
    indexer = _BlockingIndexer()
    runtime = AgentRuntime(
        redis_client=MagicMock(),
        model="m",
        workspace=Path("."),
        embedder=FakeEmbedder(dim=8),
        session_indexer=indexer,  # type: ignore[arg-type]
    )

    runtime._schedule_session_index("session-1")
    await asyncio.wait_for(indexer.started.wait(), timeout=1)
    assert runtime._index_tasks

    await runtime.aclose()
    assert not runtime._index_tasks
