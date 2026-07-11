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
    assert settings.memory_block_max_chars == 2000
