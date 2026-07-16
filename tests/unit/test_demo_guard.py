"""The demo bootstrap guard refuses anything but an unmistakable dev/demo target.

Pure unit tests: no database or Redis connection is made, only URL/string
inspection, mirroring the eval harness's DB-name guard tests.
"""

from __future__ import annotations

import pytest

from keel_core.config import Settings
from keel_core.demo_guard import DemoGuardError, assert_demo_environment


def _settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "app_env": "dev",
        "database_url": "postgresql+psycopg://keel:keel@localhost:5432/keel_test",
        "redis_url": "redis://localhost:6379/0",
    }
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


@pytest.mark.parametrize("app_env", ["dev", "development", "local", "demo", "test", "DEV", "Test"])
def test_allows_recognized_dev_demo_environments(app_env: str) -> None:
    assert_demo_environment(_settings(app_env=app_env))


@pytest.mark.parametrize("app_env", ["prod", "production", "staging", "", "PRODUCTION"])
def test_refuses_unrecognized_app_env(app_env: str) -> None:
    with pytest.raises(DemoGuardError, match="KEEL_APP_ENV"):
        assert_demo_environment(_settings(app_env=app_env))


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql+psycopg://keel:keel@db.internal.example.com:5432/keel",
        "postgresql+psycopg://keel:keel@10.0.0.5:5432/keel_test",
        "postgresql+psycopg://keel:keel@prod-db.example.com:5432/keel_test",
    ],
)
def test_refuses_non_loopback_database_host(database_url: str) -> None:
    with pytest.raises(DemoGuardError, match="KEEL_DATABASE_URL"):
        assert_demo_environment(_settings(database_url=database_url))


def test_refuses_non_loopback_redis_host() -> None:
    with pytest.raises(DemoGuardError, match="KEEL_REDIS_URL"):
        assert_demo_environment(_settings(redis_url="redis://redis.example.com:6379/0"))


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql+psycopg://keel:keel@localhost:5432/keel_prod",
        "postgresql+psycopg://keel:keel@127.0.0.1:5432/PROD",
    ],
)
def test_refuses_production_like_database_name(database_url: str) -> None:
    with pytest.raises(DemoGuardError, match="database name"):
        assert_demo_environment(_settings(database_url=database_url))


def test_allows_the_default_local_dev_stack_database_name() -> None:
    """The guard's intended target *is* the shared local dev stack's ``keel`` db.

    Only names that actually look production-like (containing "prod") are
    refused by name; the plain default dev database name must be allowed as
    long as ``app_env`` and the host are also unmistakably dev/local.
    """
    assert_demo_environment(
        _settings(database_url="postgresql+psycopg://keel:keel@localhost:5432/keel")
    )


def test_refuses_invalid_database_url() -> None:
    with pytest.raises(DemoGuardError, match="KEEL_DATABASE_URL"):
        assert_demo_environment(_settings(database_url="not a url"))


def test_allows_loopback_variants() -> None:
    for host_in_url in ("localhost", "127.0.0.1", "[::1]"):
        assert_demo_environment(
            _settings(database_url=f"postgresql+psycopg://keel:keel@{host_in_url}:5432/keel_test")
        )


def test_never_silently_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing/blank app_env must refuse, never assume a safe default."""
    with pytest.raises(DemoGuardError):
        assert_demo_environment(_settings(app_env=""))
