"""Runtime DB principal verification + least-privilege login provisioning (M3A, WS-DB).

The application data plane is protected by ``FORCE ROW LEVEL SECURITY`` on the scope-/org-
partitioned tables, but ``FORCE`` only binds a *non-owner, non-bypass* connection: a superuser
or the table owner bypasses RLS entirely. The Compose/local default connects as the schema
owner (a superuser), so RLS is not actually a hard boundary there — a real deployment must
point ``KEEL_DATABASE_URL`` at a dedicated **login role** that is a member of only the
non-owner, non-bypass ``keel_runtime`` group (migrations 0011/0013/0015/0019/0020).

This module is the code seam that makes that split real and fail-closed:

* :func:`verify_runtime_principal` — a pure catalog read that asserts the *connected* runtime
  principal is least-privilege: not a superuser, cannot ``BYPASSRLS`` directly or via role
  membership, does not effectively own the application tables, cannot ``CREATE`` in schema
  ``public`` (no DDL, even on an old/upgraded cluster where ``PUBLIC`` retained ``CREATE``),
  is a member of *only* the runtime group (nothing in its ``SET ROLE`` closure but itself and
  ``keel_runtime``), and holds none of the cross-tenant erasure privileges (no effective DELETE
  on the global identity tables, no EXECUTE on the ``keel_erase_*`` functions) nor write access
  to the ``alembic_version`` migration control table. The server/worker call it at startup +
  readiness so an over-privileged runtime connection is rejected rather than silently defeating
  RLS / the identity-erasure split for every tenant.
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

# A superuser owner's SET ROLE closure is effectively *every* role, so cap how many unexpected
# memberships describe_violation() spells out (the count is always reported in full).
_MAX_MEMBERSHIPS_SHOWN = 5

# Global identity tables whose DELETE cascades across orgs: a runtime login must never hold
# effective DELETE here (cross-tenant erasure goes through the keel_erase_* SECURITY DEFINER
# functions only — migrations 0013/0020). Qualified so search_path cannot redirect the check.
_IDENTITY_TABLES = ("public.users", "public.oidc_identities", "public.organizations")

# The cross-tenant erasure functions (SECURITY DEFINER, owned by keel_maintenance); a runtime
# login must not hold effective EXECUTE on either (migrations 0013/0020).
_ERASE_FUNCTIONS = ("public.keel_erase_user(text)", "public.keel_erase_organization(text)")

# The Alembic migration control table: the runtime login may read it but must never mutate it.
_CONTROL_TABLE = "public.alembic_version"


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
    """Least-privilege facts about the connected runtime principal (no credentials).

    Every flag is an *effective* privilege computed over the ``SET ROLE`` membership closure of
    ``current_user`` (``pg_has_role(..., 'MEMBER')`` / ``has_*_privilege``), so a privilege that
    is reachable only by escalating into another group — or granted via ``PUBLIC`` on an
    un-hardened / upgraded cluster — is still counted against the principal. ``least_privilege``
    is the single fail-closed predicate the server/worker gate on.
    """

    principal: str
    is_superuser: bool
    can_bypass_rls: bool
    owns_tables: bool
    # Extended M3A hardening checks (default to the safe/least-privilege value so existing
    # constructors and older catalog rows keep working; inspect_runtime_principal always sets
    # every field explicitly).
    can_create_in_schema: bool = False
    unexpected_memberships: tuple[str, ...] = ()
    can_delete_identity_tables: bool = False
    can_execute_erase_functions: bool = False
    can_write_control_table: bool = False

    @property
    def least_privilege(self) -> bool:
        """True only when RLS *and* the DDL / identity-erasure / control-table boundaries bind."""
        return not (
            self.is_superuser
            or self.can_bypass_rls
            or self.owns_tables
            or self.can_create_in_schema
            or bool(self.unexpected_memberships)
            or self.can_delete_identity_tables
            or self.can_execute_erase_functions
            or self.can_write_control_table
        )

    def describe_violation(self) -> str:
        """Human summary of why the principal is over-privileged (empty when it is not)."""
        reasons: list[str] = []
        if self.is_superuser:
            reasons.append("is a superuser")
        if self.can_bypass_rls:
            reasons.append("can BYPASSRLS (directly or via role membership)")
        if self.owns_tables:
            reasons.append("effectively owns application tables")
        if self.can_create_in_schema:
            reasons.append("can CREATE in schema public (DDL / object creation)")
        if self.unexpected_memberships:
            shown = list(self.unexpected_memberships[:_MAX_MEMBERSHIPS_SHOWN])
            hidden = len(self.unexpected_memberships) - len(shown)
            joined = ", ".join(shown)
            if hidden > 0:
                joined = f"{joined}, +{hidden} more"
            reasons.append(f"is a member of role(s) beyond the runtime group: {joined}")
        if self.can_delete_identity_tables:
            reasons.append("can DELETE the global identity tables (cross-tenant erasure path)")
        if self.can_execute_erase_functions:
            reasons.append("can EXECUTE the keel_erase_* SECURITY DEFINER functions")
        if self.can_write_control_table:
            reasons.append("can write the alembic_version migration control table")
        return ", ".join(reasons)


async def inspect_runtime_principal(
    conn: AsyncConnection, *, group_role: str = RUNTIME_GROUP_ROLE
) -> RuntimePrincipalReport:
    """Read the connected principal's privilege facts from the catalog (never raises on policy).

    All checks are pure ``pg_catalog`` / effective-privilege reads over the *membership closure*
    of ``current_user`` (``pg_has_role(..., 'MEMBER')`` and ``has_*_privilege(current_user, ...)``,
    both of which follow role membership *and* ``PUBLIC``), so a principal that could ``SET ROLE``
    into a superuser / bypass / owner role — or reach a privilege via ``PUBLIC`` on an un-hardened
    cluster — is treated as over-privileged too, not just one that holds it directly. ``MEMBER``
    (not ``USAGE``) is deliberate: ``SET ROLE`` capability follows role *membership* regardless of
    ``INHERIT``, so a ``NOINHERIT`` login that is merely a member of a privileged role — and could
    ``SET ROLE`` to escalate — is still caught. ``group_role`` is the one membership that is
    *expected* (the runtime group); every other role in the closure is reported as unexpected.
    Object-specific checks are guarded by ``to_regclass`` / ``to_regprocedure`` so a cluster on
    which an identity table or erase function does not yet exist reads ``false`` rather than
    erroring. No credential or connection URL is touched.
    """
    group = _validate_identifier(group_role, kind="runtime group role")
    identity_values = ", ".join(f"('{t}')" for t in _IDENTITY_TABLES)
    erase_values = ", ".join(f"('{p}')" for p in _ERASE_FUNCTIONS)
    row = (
        (
            await conn.execute(
                text(
                    f"""
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
                      ), false) AS owns_tables,
                      has_schema_privilege(current_user, 'public', 'CREATE') AS can_create_schema,
                      COALESCE((
                        SELECT bool_or(
                          CASE WHEN to_regclass(v.t) IS NOT NULL
                               THEN has_table_privilege(current_user, to_regclass(v.t), 'DELETE')
                               ELSE false END)
                        FROM (VALUES {identity_values}) AS v(t)
                      ), false) AS can_delete_identity,
                      COALESCE((
                        SELECT bool_or(
                          CASE WHEN to_regprocedure(v.p) IS NOT NULL
                               THEN has_function_privilege(current_user, to_regprocedure(v.p),
                                                           'EXECUTE')
                               ELSE false END)
                        FROM (VALUES {erase_values}) AS v(p)
                      ), false) AS can_execute_erase,
                      COALESCE((
                        SELECT bool_or(
                          CASE WHEN to_regclass('{_CONTROL_TABLE}') IS NOT NULL
                               THEN has_table_privilege(current_user, '{_CONTROL_TABLE}', v.priv)
                               ELSE false END)
                        FROM (VALUES ('INSERT'), ('UPDATE'), ('DELETE')) AS v(priv)
                      ), false) AS can_write_control,
                      COALESCE((
                        SELECT array_agg(r.rolname ORDER BY r.rolname) FROM pg_roles r
                        WHERE pg_has_role(current_user, r.oid, 'MEMBER')
                          AND r.rolname <> current_user
                          AND r.rolname <> :group
                      ), ARRAY[]::text[]) AS extra_roles
                    """
                ),
                {"group": group},
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
        can_create_in_schema=bool(row["can_create_schema"]),
        unexpected_memberships=tuple(row["extra_roles"] or ()),
        can_delete_identity_tables=bool(row["can_delete_identity"]),
        can_execute_erase_functions=bool(row["can_execute_erase"]),
        can_write_control_table=bool(row["can_write_control"]),
    )


async def verify_runtime_principal(
    conn: AsyncConnection, *, group_role: str = RUNTIME_GROUP_ROLE
) -> str:
    """Assert the connected principal is a least-privilege runtime login; return its name.

    Fails closed (:class:`RuntimePrincipalError`) when the principal is a superuser, can
    ``BYPASSRLS``, effectively owns the application tables, can ``CREATE`` in schema ``public``,
    is a member of any role other than ``group_role``, can DELETE the global identity tables /
    EXECUTE the ``keel_erase_*`` functions, or can write ``alembic_version`` — any of which
    defeats ``FORCE ROW LEVEL SECURITY`` or the identity-erasure / migration-control boundaries.
    Used by the server/worker startup + readiness gate so the data plane is never served from an
    RLS-exempt / over-privileged connection.
    """
    report = await inspect_runtime_principal(conn, group_role=group_role)
    if not report.least_privilege:
        raise RuntimePrincipalError(
            f"runtime principal {report.principal!r} {report.describe_violation()} - refusing "
            "to serve the data plane from an RLS-exempt connection; point KEEL_DATABASE_URL at "
            f"a dedicated login that is a member of only {group_role} (see docs/OPERATIONS.md)"
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
    the group's DML). It is made a member of ONLY ``group_role``: after granting the group this
    actively enumerates the login's direct memberships and REVOKEs every *other* one (e.g. a
    stale ``keel_maintenance_exec`` grant from a prior provisioning), so it can never ``SET ROLE``
    into the schema owner or a maintenance role to escalate. A membership that cannot be revoked
    is not swallowed — provisioning fails closed (sanitized) rather than returning an
    over-privileged login.

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

            # Make the login a member of ONLY the runtime group. A pre-existing login might
            # already belong to another (possibly privileged) role -- e.g. keel_maintenance_exec,
            # whose EXECUTE on the cross-tenant erase functions a runtime login must never reach --
            # and merely GRANTing keel_runtime would leave that escalation path in place.
            # Enumerate the login's DIRECT memberships from the catalog and REVOKE every one that
            # is not the target group. quote_ident makes each catalog-sourced role name safe to
            # emit for any name. A failed REVOKE is deliberately NOT caught here: it propagates out
            # of this transaction and provisioning fails closed (sanitized below) rather than
            # returning a login that still holds an extra membership.
            extra_memberships = (
                (
                    await conn.execute(
                        text(
                            "SELECT quote_ident(g.rolname) AS id_extra "
                            "FROM pg_auth_members m "
                            "JOIN pg_roles g ON g.oid = m.roleid "
                            "JOIN pg_roles l ON l.oid = m.member "
                            "WHERE l.rolname = :login AND g.rolname <> :grp "
                            "ORDER BY g.rolname"
                        ),
                        {"login": login, "grp": group},
                    )
                )
                .scalars()
                .all()
            )
            for id_extra in extra_memberships:
                await conn.execute(text(f"REVOKE {str(id_extra)} FROM {id_login}"))
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
