"""The eval DB guard refuses a live/unknown database purely from the URL name."""

from __future__ import annotations

import pytest

from keel_worker.evals.database import (
    EvalDatabaseError,
    assert_eval_database_name,
    require_eval_database_url,
)


def test_refuses_live_keel_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEEL_EVAL_DATABASE_URL", "postgresql://localhost:5432/keel")
    with pytest.raises(EvalDatabaseError):
        require_eval_database_url()


def test_requires_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KEEL_EVAL_DATABASE_URL", raising=False)
    with pytest.raises(EvalDatabaseError):
        require_eval_database_url()


def test_allows_keel_test(monkeypatch: pytest.MonkeyPatch) -> None:
    url = "postgresql://localhost:5432/keel_test"
    monkeypatch.setenv("KEEL_EVAL_DATABASE_URL", url)
    assert require_eval_database_url() == url


def test_name_guard_rejects_production() -> None:
    with pytest.raises(EvalDatabaseError):
        assert_eval_database_name("keel")
