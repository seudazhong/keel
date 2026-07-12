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
    def __init__(self) -> None:
        self.finished = asyncio.Event()

    async def index_session(self, session_id: str) -> int:
        self.finished.set()
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

    # Sentinel entry so we can prove it remains in _runs while the index task is live.
    sentinel: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(0))
    runtime._runs["run-1"] = sentinel

    await runtime._run(MagicMock(), "session-1", "run-1")
    await asyncio.wait_for(indexer.called.wait(), timeout=1)

    assert indexer.sessions == ["session-1"]
    assert len(runtime._index_tasks) == 1
    assert "run-1" in runtime._runs  # _run itself never removes the entry
    indexer.release.set()
    runtime._forget("run-1")
    await runtime.aclose()


async def test_index_failure_is_logged_not_raised(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    indexer = _FailingIndexer()
    runtime = AgentRuntime(
        redis_client=MagicMock(),
        model="m",
        workspace=Path("."),
        embedder=FakeEmbedder(dim=8),
        session_indexer=indexer,  # type: ignore[arg-type]
    )
    monkeypatch.setattr("keel_server.runtime.run", AsyncMock())
    caplog.set_level(logging.ERROR, logger="keel.server.runtime")

    await runtime._run(MagicMock(), "broken-session", "run-1")
    await asyncio.wait_for(indexer.finished.wait(), timeout=1)

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


async def test_run_completing_after_aclose_does_not_schedule_indexer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run that finishes after aclose() must not enqueue a new index task."""
    indexer = _RecordingIndexer()
    runtime = AgentRuntime(
        redis_client=MagicMock(),
        model="m",
        workspace=Path("."),
        embedder=FakeEmbedder(dim=8),
        session_indexer=indexer,  # type: ignore[arg-type]
    )

    started: asyncio.Event = asyncio.Event()
    released: asyncio.Event = asyncio.Event()

    async def _blocking_run(**kwargs: object) -> None:
        started.set()
        await released.wait()

    monkeypatch.setattr("keel_server.runtime.run", _blocking_run)

    run_task = asyncio.create_task(runtime._run(MagicMock(), "session-4", "run-4"))
    await asyncio.wait_for(started.wait(), timeout=1)

    await runtime.aclose()  # close while _run is blocked inside run()

    released.set()  # unblock run() so _run proceeds to finally
    await run_task  # _schedule_session_index runs but must return early

    assert not indexer.sessions
    assert not runtime._index_tasks
