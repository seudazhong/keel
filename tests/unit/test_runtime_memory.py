"""Runtime wiring: memory/archival tool registration + permissions."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from sqlalchemy.ext.asyncio import create_async_engine

from keel_core.embeddings import FakeEmbedder, LiteLLMEmbedder
from keel_core.permissions import RuleBasedPermissionEngine
from keel_core.protocols import ToolContext
from keel_core.types import PermissionDecision
from keel_server.runtime import AgentRuntime, _build_memory_tools, _web_permissions

_ENGINE = create_async_engine("postgresql+psycopg://keel:keel@localhost:5432/keel")  # not connected


def test_build_memory_tools_includes_archival_with_embedder() -> None:
    names = {
        t.name
        for t in _build_memory_tools(
            _ENGINE,
            FakeEmbedder(dim=16),
            cap=2000,
            batch_size=64,
            catchup_limit=500,
        )
    }
    assert names == {
        "memory_append",
        "memory_replace",
        "memory_rethink",
        "session_search",
        "archival_insert",
        "archival_search",
    }


def test_build_memory_tools_omits_archival_without_embedder() -> None:
    names = {
        t.name
        for t in _build_memory_tools(
            _ENGINE,
            None,
            cap=2000,
            batch_size=64,
            catchup_limit=500,
        )
    }
    assert "archival_insert" not in names and "archival_search" not in names
    assert {"memory_append", "session_search"} <= names  # memory + lexical recall still there


def test_web_permissions_allow_memory_tools() -> None:
    perms: RuleBasedPermissionEngine = _web_permissions(("memory_append", "archival_insert"))
    ctx = ToolContext(scope_id="u", session_id="s")
    assert perms.evaluate("memory_append", {}, ctx) is PermissionDecision.allow
    assert perms.evaluate("archival_insert", {}, ctx) is PermissionDecision.allow
    assert perms.evaluate("write", {}, ctx) is PermissionDecision.ask  # mutating still gated


def test_runtime_passes_embedding_timeout_to_default_embedder() -> None:
    runtime = AgentRuntime(
        redis_client=MagicMock(),
        model="m",
        workspace=Path("."),
        engine=_ENGINE,
        embedding_timeout_seconds=2.5,
    )

    assert isinstance(runtime.embedder, LiteLLMEmbedder)
    assert runtime.embedder.timeout_seconds == 2.5
