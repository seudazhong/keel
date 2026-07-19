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
    ("kwargs", "needle"),
    [
        ({"is_superuser": True}, "superuser"),
        ({"can_bypass_rls": True}, "BYPASSRLS"),
        ({"owns_tables": True}, "owns application tables"),
        ({"can_create_in_schema": True}, "CREATE in schema public"),
        ({"can_delete_identity_tables": True}, "DELETE the global identity tables"),
        ({"can_execute_erase_functions": True}, "keel_erase_* SECURITY DEFINER"),
        ({"can_write_control_table": True}, "alembic_version"),
        ({"unexpected_memberships": ("keel_maintenance_exec",)}, "keel_maintenance_exec"),
    ],
)
def test_runtime_principal_report_violations(kwargs: dict[str, object], needle: str) -> None:
    from keel_core.runtime_db import RuntimePrincipalReport

    base: dict[str, object] = {
        "is_superuser": False,
        "can_bypass_rls": False,
        "owns_tables": False,
    }
    report = RuntimePrincipalReport(principal="keel", **{**base, **kwargs})  # type: ignore[arg-type]
    assert report.least_privilege is False
    assert needle in report.describe_violation()


def test_runtime_principal_report_caps_membership_list() -> None:
    """A superuser owner's SET ROLE closure is every role; the message must cap + count them."""
    from keel_core.runtime_db import RuntimePrincipalReport

    memberships = tuple(f"role_{i:02d}" for i in range(12))
    report = RuntimePrincipalReport(
        principal="keel",
        is_superuser=False,
        can_bypass_rls=False,
        owns_tables=False,
        unexpected_memberships=memberships,
    )
    described = report.describe_violation()
    assert report.least_privilege is False
    assert "role_00" in described and "role_04" in described  # first five shown
    assert "role_05" not in described  # sixth is elided
    assert "+7 more" in described  # 12 total - 5 shown


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


async def test_provision_runtime_login_fails_sanitized_when_membership_revoke_fails() -> None:
    """The membership strip must fail closed: if REVOKEing an extra membership raises, provisioning
    surfaces a sanitized ``RuntimePrincipalError`` (no silent success that would leave the login
    still holding the extra role). The REVOKE runs inside the same transaction as the rest of
    provisioning, so it is caught by the same ``SQLAlchemyError`` sanitizer.
    """
    from typing import Any

    from sqlalchemy.exc import SQLAlchemyError

    from keel_core.errors import RuntimePrincipalError
    from keel_core.runtime_db import provision_runtime_login

    class _Result:
        def __init__(self, rows: list[Any]) -> None:
            self._rows = rows

        def mappings(self) -> _Result:
            return self

        def one(self) -> Any:
            return self._rows[0]

        def scalars(self) -> _Result:
            return self

        def all(self) -> list[Any]:
            return self._rows

    class _Conn:
        def __init__(self) -> None:
            self.calls = 0

        async def scalar(self, *a: object, **k: object) -> int:
            return 1  # group exists

        async def execute(self, *a: object, **k: object) -> _Result:
            self.calls += 1
            if self.calls == 1:  # quote_ident/quote_literal row
                return _Result(
                    [{"id_login": "l", "lit_login": "'l'", "id_group": "g", "lit_pw": "'p'"}]
                )
            if self.calls in (2, 3, 4):  # CREATE DO block, ALTER ROLE, GRANT group
                return _Result([])
            if self.calls == 5:  # membership enumeration -> one extra role to strip
                return _Result(["keel_maintenance_exec"])
            raise SQLAlchemyError("REVOKE keel_maintenance_exec denied")  # the strip REVOKE

        async def __aenter__(self) -> _Conn:
            return self

        async def __aexit__(self, *a: object) -> bool:
            return False

    class _Engine:
        def begin(self) -> _Conn:
            return _Conn()

    engine: Any = _Engine()
    throwaway = "revoke-unit-pw"  # noqa: S105 - fake secret for the mocked provisioning path
    with pytest.raises(RuntimePrincipalError) as excinfo:
        await provision_runtime_login(engine, password=throwaway, login_name="keel_runtime_login")
    assert "keel_runtime_login" in str(excinfo.value)
    assert "SQLAlchemyError" in str(excinfo.value)
