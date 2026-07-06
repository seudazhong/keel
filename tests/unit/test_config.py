"""Config layer tests (no external services)."""

from __future__ import annotations

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
