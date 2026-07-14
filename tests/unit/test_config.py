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


def test_job_defaults() -> None:
    from keel_core.config import Settings

    settings = Settings()
    assert settings.job_lease_seconds == 300
    assert settings.job_execution_timeout_seconds == 3600
    assert settings.job_dispatch_limit == 100
    assert settings.job_retry_base_seconds == 5
    assert settings.job_retry_max_seconds == 300
    assert settings.job_payload_max_bytes == 65_536
    assert settings.job_result_max_bytes == 65_536
    assert settings.job_progress_message_max_chars == 1_000
    assert settings.job_result_message_max_chars == 8_000
    assert settings.job_error_message_max_chars == 2_000


def test_job_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEEL_JOB_LEASE_SECONDS", "45")
    monkeypatch.setenv("KEEL_JOB_EXECUTION_TIMEOUT_SECONDS", "7200")
    monkeypatch.setenv("KEEL_JOB_DISPATCH_LIMIT", "17")
    monkeypatch.setenv("KEEL_JOB_RETRY_BASE_SECONDS", "2")
    monkeypatch.setenv("KEEL_JOB_PROGRESS_MESSAGE_MAX_CHARS", "123")

    from keel_core.config import Settings

    settings = Settings()
    assert settings.job_lease_seconds == 45
    assert settings.job_execution_timeout_seconds == 7200
    assert settings.job_dispatch_limit == 17
    assert settings.job_retry_base_seconds == 2
    assert settings.job_progress_message_max_chars == 123


def test_job_settings_reject_non_positive_values() -> None:
    from pydantic import ValidationError

    from keel_core.config import Settings

    with pytest.raises(ValidationError):
        Settings(job_lease_seconds=0)
    with pytest.raises(ValidationError):
        Settings(job_execution_timeout_seconds=0)
    with pytest.raises(ValidationError):
        Settings(job_result_max_bytes=-1)


def test_job_dispatch_limit_rejects_values_outside_store_bounds() -> None:
    from pydantic import ValidationError

    from keel_core.config import Settings

    with pytest.raises(ValidationError):
        Settings(job_dispatch_limit=0)
    with pytest.raises(ValidationError):
        Settings(job_dispatch_limit=101)
