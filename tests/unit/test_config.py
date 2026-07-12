"""Config layer tests (no external services)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


def test_defaults() -> None:
    from keel_core.config import Settings

    settings = Settings()
    assert settings.app_env == "dev"
    assert settings.database_url.startswith("postgresql+psycopg://")
    assert settings.redis_url.startswith("redis://")
    assert settings.server_port == 8000
    assert settings.sync_database_url == settings.database_url


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEEL_SERVER_PORT", "9000")
    monkeypatch.setenv("KEEL_APP_ENV", "prod")

    from keel_core.config import Settings

    settings = Settings()
    assert settings.server_port == 9000
    assert settings.app_env == "prod"


def test_load_env_file_injects_without_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from keel_core.config import load_env_file

    env = tmp_path / ".env"
    env.write_text("KEEL_TEST_NEW=fromfile\nKEEL_TEST_EXISTING=fromfile\n", encoding="utf-8")
    monkeypatch.setenv("KEEL_TEST_EXISTING", "real")  # a real env var must win
    monkeypatch.delenv("KEEL_TEST_NEW", raising=False)
    try:
        loaded = load_env_file(env)
        assert loaded == str(env)
        assert os.environ["KEEL_TEST_NEW"] == "fromfile"  # injected for LiteLLM et al.
        assert os.environ["KEEL_TEST_EXISTING"] == "real"  # not overridden
    finally:
        os.environ.pop("KEEL_TEST_NEW", None)


def test_load_env_file_missing_returns_none(tmp_path: Path) -> None:
    from keel_core.config import load_env_file

    assert load_env_file(tmp_path / "nope.env") is None


def test_scheduler_and_approval_defaults() -> None:
    from keel_core.config import Settings

    settings = Settings()
    assert settings.approval_timeout_hours == 24
    assert settings.scheduler_tick_seconds == 30


def test_fallback_model_list_parses_and_trims(monkeypatch: pytest.MonkeyPatch) -> None:
    from keel_core.config import Settings

    assert Settings().fallback_model_list == []  # empty by default -> no failover
    monkeypatch.setenv("KEEL_FALLBACK_MODELS", "openai/gpt-4o , github_copilot/claude-sonnet-4.5,")
    assert Settings().fallback_model_list == ["openai/gpt-4o", "github_copilot/claude-sonnet-4.5"]


def test_memory_embedding_defaults() -> None:
    from keel_core.config import Settings

    settings = Settings()
    assert settings.embedding_model == "ollama/bge-m3"
    assert settings.embedding_dim == 1024
    assert settings.embedding_send_dimensions is False
    assert settings.embedding_timeout_seconds == 10.0
    assert settings.memory_block_max_chars == 2000
    assert settings.session_embedding_batch_size == 64
    assert settings.session_embedding_catchup_limit == 500


def test_consolidation_defaults() -> None:
    from keel_core.config import Settings

    settings = Settings()
    assert settings.consolidation_min_messages == 10
    assert settings.consolidation_batch_messages == 50
    assert settings.consolidation_input_max_chars == 20_000
    assert settings.consolidation_message_max_chars == 4_000
    assert settings.consolidation_archival_min_confidence == 0.8
    assert settings.consolidation_lease_seconds == 600
    assert settings.consolidation_token_budget == 4_000
    assert settings.consolidation_semantic_dedupe_distance == 0.05


def test_consolidation_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEEL_CONSOLIDATION_MIN_MESSAGES", "3")
    monkeypatch.setenv("KEEL_CONSOLIDATION_ARCHIVAL_MIN_CONFIDENCE", "0.5")
    monkeypatch.setenv("KEEL_CONSOLIDATION_SEMANTIC_DEDUPE_DISTANCE", "0.2")

    from keel_core.config import Settings

    settings = Settings()
    assert settings.consolidation_min_messages == 3
    assert settings.consolidation_archival_min_confidence == 0.5
    assert settings.consolidation_semantic_dedupe_distance == 0.2
