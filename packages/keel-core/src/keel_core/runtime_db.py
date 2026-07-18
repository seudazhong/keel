"""Runtime DB principal verification + least-privilege login provisioning (M3A, WS-DB).

The application data plane is protected by ``FORCE ROW LEVEL SECURITY`` on the scope-/org-
partitioned tables, but ``FORCE`` only binds a *non-owner, non-bypass* connection: a superuser
or the table owner bypasses RLS entirely. The Compose/local default connects as the schema
owner (a superuser), so RLS is not actually a hard boundary there — a real deployment must
point ``KEEL_DATABASE_URL`` at a dedicated **login role** that is a member of only the
non-owner, non-bypass ``keel_runtime`` group (migrations 0011/0013/0015/0019/0020).

This module is the code seam that makes that split real and fail-closed:

* :func:`verify_runtime_principal` — a pure catalog read that asserts the *connected* runtime
  principal is least-privilege (not a superuser, cannot ``BYPASSRLS`` directly or via role
  membership, and does not effectively own the application tables). The server/worker call it
  at startup + readiness in cloud mode so an over-privileged runtime connection is rejected
  rather than silently defeating RLS for every tenant.
* :func:`provision_runtime_login` — an idempotent operator/provisioning primitive that creates
  (or repairs) a dedicated ``LOGIN`` role and grants it the ``keel_runtime`` group. It connects
  as the owner/migrator principal and is the real path an operator uses to mint the runtime
  login the app then connects as. The password is supplied by the caller (env/stdin/secret
  reference) and is quoted server-side; it is never interpolated by hand or logged.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from keel_core.errors import RuntimePrincipalError

# The non-owner, non-bypass group role the runtime login must be a member of (created and
# hardened by migrations 0011/0020). It owns no tables and can neither DDL nor SET ROLE into
# the schema owner / maintenance roles.
RUNTIME_GROUP_ROLE = "keel_runtime"

# Conventional name for the dedicated runtime LOGIN role a deployment points KEEL_DATABASE_URL
# at. Operators may choose another name; the group membership is what matters.
DEFAULT_RUNTIME_LOGIN = "keel_runtime_login"

# Postgres identifiers we are willing to emit into DDL. We additionally quote every identifier
# server-side (``quote_ident``), but validating first turns an unsafe name into a clear error
# instead of a confusing quoted-but-wrong role.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_IDENT_LEN = 63  # Postgres NAMEDATALEN - 1


def _validate_identifier(name: str, *, kind: str) -> str:
    candidate = name.strip()
    if not candidate:
        raise ValueError(f"{kind} must be a non-empty role name")
    if len(candidate) > _MAX_IDENT_LEN:
        raise ValueError(f"{kind} {name!r} exceeds {_MAX_IDENT_LEN} characters")
    if not _IDENT_RE.match(candidate):
        raise ValueError(
            f"{kind} {name!r} is not a simple identifier "
            "([A-Za-z_][A-Za-z0-9_]*); refusing to emit it into DDL"
        )
    return candidate


@dataclass(frozen=True)
class RuntimePrincipalReport:
    """Least-privilege facts about the connected runtime principal (no credentials)."""

    principal: str
    is_superuser: bool
    can_bypass_rls: bool
    owns_tables: bool

    @property
    def least_privilege(self) -> bool:
        """True when RLS actually binds this principal (not super/bypass/owner)."""
        return not (self.is_superuser or self.can_bypass_rls or self.owns_tables)

    def describe_violation(self) -> str:
        """Human summary of why the principal is over-privileged (empty when it is not)."""
        reasons: list[str] = []
        if self.is_superuser:
            reasons.append("is a superuser")
        if self.can_bypass_rls:
            reasons.append("can BYPASSRLS (directly or via role membership)")
        if self.owns_tables:
            reasons.append("effectively owns application tables")
        return ", ".join(reasons)


async def inspect_runtime_principal(conn: AsyncConnection) -> RuntimePrincipalReport:
    """Read the connected principal's privilege facts from the catalog (never raises on policy).

    All checks are pure ``pg_catalog`` reads over the *membership closure* of ``current_user``
    (``pg_has_role(..., 'MEMBER')``), so a principal that could ``SET ROLE`` into a superuser /
    bypass / owner role is treated as over-privileged too, not just one that holds the attribute
    directly. ``MEMBER`` (not ``USAGE``) is deliberate: ``SET ROLE`` capability follows role
    *membership* regardless of ``INHERIT``, so a ``NOINHERIT`` login that is merely a member of a
    superuser / ``BYPASSRLS`` / table-owner role — and could ``SET ROLE`` to escalate — is still
    caught. No credential or connection URL is touched.
    """
    row = (
        (
            await conn.execute(
                text(
                    """
                    SELECT
                      current_user AS principal,
                      COALESCE((
                        SELECT bool_or(r.rolsuper) FROM pg_roles r
                        WHERE pg_has_role(current_user, r.oid, 'MEMBER')
                      ), false) AS is_super,
                      COALESCE((
                        SELECT bool_or(r.rolbypassrls) FROM pg_roles r
                        WHERE pg_has_role(current_user, r.oid, 'MEMBER')
                      ), false) AS can_bypass_rls,
                      COALESCE((
                        SELECT bool_or(pg_has_role(current_user, c.relowner, 'MEMBER'))
                        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'public' AND c.relkind = 'r'
                      ), false) AS owns_tables
                    """
                )
            )
        )
        .mappings()
        .one()
    )
    return RuntimePrincipalReport(
        principal=str(row["principal"]),
        is_superuser=bool(row["is_super"]),
        can_bypass_rls=bool(row["can_bypass_rls"]),
        owns_tables=bool(row["owns_tables"]),
    )


async def verify_runtime_principal(conn: AsyncConnection) -> str:
    """Assert the connected principal is a least-privilege runtime login; return its name.

    Fails closed (:class:`RuntimePrincipalError`) when the principal is a superuser, can
    ``BYPASSRLS``, or effectively owns the application tables — any of which silently defeats
    ``FORCE ROW LEVEL SECURITY``. Used by the server/worker cloud-mode startup + readiness gate
    so the data plane is never served from an RLS-exempt connection.
    """
    report = await inspect_runtime_principal(conn)
    if not report.least_privilege:
        raise RuntimePrincipalError(
            f"runtime principal {report.principal!r} {report.describe_violation()} - refusing "
            "to serve the data plane from an RLS-exempt connection; point KEEL_DATABASE_URL at "
            f"a dedicated login that is a member of only {RUNTIME_GROUP_ROLE} (see "
            "docs/OPERATIONS.md)"
        )
    return report.principal


async def provision_runtime_login(
    engine: AsyncEngine,
    *,
    password: str,
    login_name: str = DEFAULT_RUNTIME_LOGIN,
    group_role: str = RUNTIME_GROUP_ROLE,
) -> str:
    """Idempotently create/repair a least-privilege runtime ``LOGIN`` and grant it the group.

    Runs as the owner/migrator principal behind ``engine`` (see
    :meth:`keel_core.config.Settings.require_migration_database_url`). Safe to re-run: the role
    is created only when absent, then its attributes + password are (re)set and the
    ``keel_runtime`` group membership ensured. Attributes are pinned least-privilege
    (``NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE`` + ``LOGIN`` + ``INHERIT`` so it inherits
    the group's DML). It is deliberately made a member of ONLY ``group_role`` — never the schema
    owner or a maintenance role — so it cannot ``SET ROLE`` to escalate.

    The ``group_role`` must already exist (migration 0011/0020 creates it, or the operator
    provisioned it on managed Postgres); a missing group fails closed with
    :class:`RuntimePrincipalError` rather than minting a login with no privileges. Identifiers are
    validated and quoted server-side; the password is quoted server-side, never hand-interpolated,
    and never surfaced in exception text (a DB error is re-raised sanitized, dropping the original
    from the traceback). Postgres cannot bind a parameter for ``ALTER ROLE ... PASSWORD``, so the
    (server-quoted) secret is unavoidably part of that statement's *text*; the provisioning
    connection must therefore not enable SQLAlchemy statement echo (``echo=True`` / the
    ``sqlalchemy.engine`` logger at INFO) — the provisioning CLI's engine does not. See
    docs/OPERATIONS.md.
    """
    login = _validate_identifier(login_name, kind="runtime login role")
    group = _validate_identifier(group_role, kind="runtime group role")
    if not password:
        raise ValueError("runtime login password must be a non-empty secret")

    try:
        async with engine.begin() as conn:
            group_exists = await conn.scalar(
                text("SELECT 1 FROM pg_roles WHERE rolname = :g"), {"g": group}
            )
            if not group_exists:
                raise RuntimePrincipalError(
                    f"runtime group role {group!r} does not exist; run migrations (0011/0020) or "
                    "provision it manually before creating the runtime login"
                )

            # Quote the login/group identifiers and the password server-side so nothing is
            # hand-interpolated: %I -> a safe identifier, %L -> a safe string literal. The
            # literals are computed by Postgres and only ever used inside this transaction.
            quoted = (
                (
                    await conn.execute(
                        text(
                            "SELECT quote_ident(:login) AS id_login, "
                            "quote_literal(:login) AS lit_login, "
                            "quote_ident(:grp) AS id_group, "
                            "quote_literal(:pw) AS lit_pw"
                        ),
                        {"login": login, "grp": group, "pw": password},
                    )
                )
                .mappings()
                .one()
            )
            id_login = str(quoted["id_login"])
            lit_login = str(quoted["lit_login"])
            id_group = str(quoted["id_group"])
            lit_pw = str(quoted["lit_pw"])

            # Create the login only when it does not already exist (idempotent).
            await conn.execute(
                text(
                    f"""
                    DO $prov$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {lit_login}) THEN
                            CREATE ROLE {id_login} LOGIN;
                        END IF;
                    END
                    $prov$;
                    """
                )
            )
            # Pin least-privilege attributes + (re)set the password every run (idempotent repair).
            await conn.execute(
                text(
                    f"ALTER ROLE {id_login} WITH LOGIN NOSUPERUSER NOBYPASSRLS "
                    f"NOCREATEDB NOCREATEROLE INHERIT PASSWORD {lit_pw}"
                )
            )
            # Ensure the runtime group membership (so it inherits the group's DML). GRANT is a
            # no-op when already a member.
            await conn.execute(text(f"GRANT {id_group} TO {id_login}"))
    except RuntimePrincipalError:
        raise
    except SQLAlchemyError as exc:
        # Never surface the DB error message/statement: the failing statement is the
        # ``ALTER ROLE ... PASSWORD {lit_pw}`` (or the bound ``quote_literal(:pw)`` parameter),
        # so SQLAlchemy would attach the secret via ``[SQL: ...]`` / ``[parameters: ...]``.
        # Re-raise a sanitized error carrying only the exception class name; ``from None`` drops
        # the original (password-bearing) exception from the reported traceback chain.
        raise RuntimePrincipalError(
            f"failed to provision runtime login {login!r} (database error: {type(exc).__name__})"
        ) from None

    return login


__all__ = [
    "DEFAULT_RUNTIME_LOGIN",
    "RUNTIME_GROUP_ROLE",
    "RuntimePrincipalReport",
    "inspect_runtime_principal",
    "provision_runtime_login",
    "verify_runtime_principal",
]
