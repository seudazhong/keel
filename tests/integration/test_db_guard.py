"""Integration-test database safety guard (no live DB writes)."""

from __future__ import annotations

import pytest
from conftest import _assert_test_database_name, _require_test_database_url


def test_test_database_url_must_be_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KEEL_TEST_DATABASE_URL", raising=False)

    with pytest.raises(pytest.UsageError, match="KEEL_TEST_DATABASE_URL must be set"):
        _require_test_database_url()


def test_live_database_url_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "KEEL_TEST_DATABASE_URL",
        "postgresql+psycopg://keel:keel@localhost:5432/keel",
    )

    with pytest.raises(pytest.UsageError, match="refusing database 'keel'"):
        _require_test_database_url()


def test_explicit_keel_test_url_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    url = "postgresql+psycopg://keel:keel@localhost:5432/keel_test"
    monkeypatch.setenv("KEEL_TEST_DATABASE_URL", url)

    assert _require_test_database_url() == url


def test_destructive_boundary_rechecks_actual_database_name() -> None:
    _assert_test_database_name("keel_test")

    with pytest.raises(pytest.UsageError, match="refusing database 'keel'"):
        _assert_test_database_name("keel")
