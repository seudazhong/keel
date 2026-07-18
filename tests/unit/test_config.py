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
    assert settings.execution_backend == "sandbox"
    assert settings.sandbox_rpc_secret.get_secret_value() == ""
    assert settings.sandbox_rpc_local_test_mode is False
    assert settings.trusted_preview_allow_unsafe_execution is False


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


def test_knowledge_defaults() -> None:
    from keel_core.config import Settings

    settings = Settings()
    assert settings.knowledge_document_max_bytes == 1_048_576
    assert settings.knowledge_title_max_chars == 300
    assert settings.knowledge_description_max_chars == 2_000
    assert settings.knowledge_search_query_max_chars == 2_000
    assert settings.knowledge_search_k_max == 10
    assert settings.knowledge_chunk_target_chars == 1_600
    assert settings.knowledge_chunk_overlap_chars == 200
    assert settings.knowledge_embedding_batch_size == 32
    assert settings.knowledge_tool_output_max_chars == 8_000


@pytest.mark.parametrize(
    "field",
    [
        "knowledge_document_max_bytes",
        "knowledge_title_max_chars",
        "knowledge_description_max_chars",
        "knowledge_search_query_max_chars",
        "knowledge_search_k_max",
        "knowledge_chunk_target_chars",
        "knowledge_chunk_overlap_chars",
        "knowledge_embedding_batch_size",
        "knowledge_tool_output_max_chars",
    ],
)
def test_knowledge_settings_reject_non_positive_values(field: str) -> None:
    from pydantic import ValidationError

    from keel_core.config import Settings

    with pytest.raises(ValidationError):
        Settings(**{field: 0})


def test_knowledge_settings_reject_k_above_public_maximum() -> None:
    from pydantic import ValidationError

    from keel_core.config import Settings

    with pytest.raises(ValidationError):
        Settings(knowledge_search_k_max=11)


def test_maintenance_database_url_defaults_empty() -> None:
    from keel_core.config import Settings

    assert Settings().maintenance_database_url == ""


def test_require_maintenance_database_url_fails_closed_when_unset() -> None:
    from keel_core.config import Settings
    from keel_core.errors import MaintenanceDatabaseNotConfigured

    with pytest.raises(MaintenanceDatabaseNotConfigured):
        Settings(maintenance_database_url="").require_maintenance_database_url()


def test_require_maintenance_database_url_rejects_runtime_copy_in_cloud_mode() -> None:
    from keel_core.config import Settings
    from keel_core.errors import MaintenanceDatabaseNotConfigured

    url = "postgresql+psycopg://runtime@db/keel"
    settings = Settings(cloud_mode=True, database_url=url, maintenance_database_url=url)
    with pytest.raises(MaintenanceDatabaseNotConfigured):
        settings.require_maintenance_database_url()


def test_require_maintenance_database_url_returns_dedicated_url() -> None:
    from keel_core.config import Settings

    maint = "postgresql+psycopg://maint@db/keel"
    settings = Settings(
        cloud_mode=True,
        database_url="postgresql+psycopg://runtime@db/keel",
        maintenance_database_url=maint,
    )
    assert settings.require_maintenance_database_url() == maint


def test_require_maintenance_database_url_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEEL_MAINTENANCE_DATABASE_URL", "postgresql+psycopg://maint@db/keel")

    from keel_core.config import Settings

    assert Settings().require_maintenance_database_url() == "postgresql+psycopg://maint@db/keel"


@pytest.mark.parametrize("overlap", [100, 101])
def test_knowledge_settings_reject_overlap_at_or_above_target(overlap: int) -> None:
    from pydantic import ValidationError

    from keel_core.config import Settings

    with pytest.raises(ValidationError):
        Settings(knowledge_chunk_target_chars=100, knowledge_chunk_overlap_chars=overlap)


def test_legacy_machine_binding_defaults_none() -> None:
    from keel_core.config import Settings

    settings = Settings()
    assert settings.legacy_machine_org_id == ""
    assert settings.legacy_machine_agent_id == ""
    assert settings.legacy_machine_binding is None


def test_legacy_machine_binding_pair_resolves() -> None:
    from keel_core.config import Settings

    settings = Settings(legacy_machine_org_id="acme", legacy_machine_agent_id="agent-1")
    assert settings.legacy_machine_binding == ("acme", "agent-1")


@pytest.mark.parametrize(
    ("org", "agent"),
    [("acme", ""), ("", "agent-1")],
)
def test_legacy_machine_binding_half_pair_rejected(org: str, agent: str) -> None:
    from pydantic import ValidationError

    from keel_core.config import Settings

    with pytest.raises(ValidationError):
        Settings(legacy_machine_org_id=org, legacy_machine_agent_id=agent)


def test_legacy_machine_binding_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEEL_LEGACY_MACHINE_ORG_ID", "org-xyz")
    monkeypatch.setenv("KEEL_LEGACY_MACHINE_AGENT_ID", "agent-xyz")

    from keel_core.config import Settings

    assert Settings().legacy_machine_binding == ("org-xyz", "agent-xyz")
