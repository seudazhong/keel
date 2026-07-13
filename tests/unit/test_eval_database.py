"""Eval DB guard: only keel_eval/keel_test, never live keel, no silent fallback."""

from __future__ import annotations

import pytest

from keel_worker.evals.database import (
    EvalDatabaseError,
    assert_eval_database_name,
    case_scope,
    require_eval_database_url,
)


@pytest.mark.parametrize("name", ["keel_eval", "keel_test"])
def test_allowed_databases_pass(name: str) -> None:
    assert_eval_database_name(name)  # does not raise


@pytest.mark.parametrize("name", ["keel", "postgres", None, "keel_prod"])
def test_disallowed_databases_refused(name: str | None) -> None:
    with pytest.raises(EvalDatabaseError):
        assert_eval_database_name(name)


def test_require_url_needs_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KEEL_EVAL_DATABASE_URL", raising=False)
    monkeypatch.setenv("KEEL_DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/keel")
    with pytest.raises(EvalDatabaseError, match="KEEL_EVAL_DATABASE_URL"):
        require_eval_database_url()  # never falls back to KEEL_DATABASE_URL


def test_require_url_refuses_live(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "KEEL_EVAL_DATABASE_URL",
        "postgresql+asyncpg://u:p@localhost:5432/keel",
    )
    with pytest.raises(EvalDatabaseError):
        require_eval_database_url()


def test_require_url_accepts_eval(monkeypatch: pytest.MonkeyPatch) -> None:
    url = "postgresql+asyncpg://u:p@localhost:5432/keel_eval"
    monkeypatch.setenv("KEEL_EVAL_DATABASE_URL", url)
    assert require_eval_database_url() == url


def test_case_scope_format() -> None:
    assert case_scope("v1", "con-en-preference") == "eval:v1:con-en-preference"
