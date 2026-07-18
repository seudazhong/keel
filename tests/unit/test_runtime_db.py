"""Unit tests for runtime DB principal reporting + identifier validation (no database)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def test_validate_identifier_accepts_simple_names() -> None:
    from keel_core.runtime_db import _validate_identifier

    assert _validate_identifier("keel_runtime_login", kind="login") == "keel_runtime_login"
    # Surrounding whitespace is trimmed before validation.
    assert _validate_identifier("  keel_runtime  ", kind="group") == "keel_runtime"


@pytest.mark.parametrize(
    "bad",
    ["", "   ", "1role", "has-dash", "has space", "drop;table", 'quote"d', "sneaky--", "a.b"],
)
def test_validate_identifier_rejects_unsafe(bad: str) -> None:
    from keel_core.runtime_db import _validate_identifier

    with pytest.raises(ValueError):
        _validate_identifier(bad, kind="login")


def test_validate_identifier_rejects_overlong() -> None:
    from keel_core.runtime_db import _validate_identifier

    with pytest.raises(ValueError):
        _validate_identifier("a" * 64, kind="login")


def test_runtime_principal_report_least_privilege() -> None:
    from keel_core.runtime_db import RuntimePrincipalReport

    report = RuntimePrincipalReport(
        principal="keel_runtime_login",
        is_superuser=False,
        can_bypass_rls=False,
        owns_tables=False,
    )
    assert report.least_privilege is True
    assert report.describe_violation() == ""


@pytest.mark.parametrize(
    ("is_super", "bypass", "owns", "needle"),
    [
        (True, False, False, "superuser"),
        (False, True, False, "BYPASSRLS"),
        (False, False, True, "owns application tables"),
    ],
)
def test_runtime_principal_report_violations(
    is_super: bool, bypass: bool, owns: bool, needle: str
) -> None:
    from keel_core.runtime_db import RuntimePrincipalReport

    report = RuntimePrincipalReport(
        principal="keel",
        is_superuser=is_super,
        can_bypass_rls=bypass,
        owns_tables=owns,
    )
    assert report.least_privilege is False
    assert needle in report.describe_violation()


async def test_provision_runtime_login_rejects_empty_password() -> None:
    from keel_core.runtime_db import provision_runtime_login

    # Validation happens before the engine is touched, so a mock engine is never used.
    with pytest.raises(ValueError):
        await provision_runtime_login(MagicMock(), password="", login_name="keel_runtime_login")


@pytest.mark.parametrize("bad_login", ["bad-name", "1login", "has space", ""])
async def test_provision_runtime_login_rejects_bad_login_name(bad_login: str) -> None:
    from keel_core.runtime_db import provision_runtime_login

    with pytest.raises(ValueError):
        await provision_runtime_login(MagicMock(), password="secret", login_name=bad_login)


async def test_provision_runtime_login_sanitizes_db_error_without_leaking_password() -> None:
    """A DB error during provisioning must never surface the password.

    SQLAlchemy would otherwise attach the failing ``ALTER ROLE ... PASSWORD`` statement and its
    bound parameters to the exception; ``provision_runtime_login`` converts any ``SQLAlchemyError``
    into a sanitized ``RuntimePrincipalError`` and drops the original (password-bearing) exception
    from the reported traceback chain via ``raise ... from None``.
    """
    import traceback
    from typing import Any

    from sqlalchemy.exc import StatementError

    from keel_core.errors import RuntimePrincipalError
    from keel_core.runtime_db import provision_runtime_login

    leaked_pw = "leak-me-please-secret"  # noqa: S105 - fake secret asserted absent from output

    class _RaisingConn:
        async def scalar(self, *args: object, **kwargs: object) -> int:
            return 1  # group exists -> provisioning proceeds to the (failing) DDL path

        async def execute(self, *args: object, **kwargs: object) -> object:
            # Mimic SQLAlchemy embedding the password in the failing statement + parameters.
            raise StatementError(
                message="boom",
                statement=f"ALTER ROLE x WITH PASSWORD '{leaked_pw}'",
                params={"pw": leaked_pw},
                orig=Exception(f"detail {leaked_pw}"),
            )

        async def __aenter__(self) -> _RaisingConn:
            return self

        async def __aexit__(self, *args: object) -> bool:
            return False

    class _FakeEngine:
        def begin(self) -> _RaisingConn:
            return _RaisingConn()

    engine: Any = _FakeEngine()
    with pytest.raises(RuntimePrincipalError) as excinfo:
        await provision_runtime_login(engine, password=leaked_pw, login_name="keel_runtime_login")

    # The sanitized error names only the exception class, never the secret.
    assert leaked_pw not in str(excinfo.value)
    assert "StatementError" in str(excinfo.value)
    # The formatted traceback (what a log/console would show) must not contain the password either.
    formatted = "".join(
        traceback.format_exception(type(excinfo.value), excinfo.value, excinfo.value.__traceback__)
    )
    assert leaked_pw not in formatted
