"""Integration: the real least-privilege runtime DB LOGIN under RLS (M3A, WS-DB).

Unlike the ``SET ROLE keel_runtime`` proxies elsewhere, these tests provision a dedicated
``LOGIN`` role (via :func:`keel_core.runtime_db.provision_runtime_login`, the real operator
path), open a **new engine as that login**, and prove the production posture end to end:

* it is least-privilege (not a superuser, cannot ``BYPASSRLS`` directly or via membership, owns
  no application tables) and :func:`verify_runtime_principal` accepts it;
* scoped DML works and cross-scope / no-scope reads+writes are denied by ``FORCE RLS`` — on both
  a pre-0019 table (``connector_outbox`` keyed on ``app.scope_id``) and a 0019 table
  (``patch_proposals`` keyed on ``app.org_id``);
* it cannot run DDL (CREATE/ALTER/DROP) and cannot ``SET ROLE`` into the owner or the
  maintenance roles;
* :func:`verify_runtime_principal` rejects the owner/superuser connection, and provisioning is
  idempotent and fails closed when the runtime group is absent.

Roles are dropped in teardown so the database is left clean.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from keel_core.errors import RuntimePrincipalError
from keel_core.outbox import PostgresOutboundStore
from keel_core.provision_runtime_secret import ensure_runtime_secret
from keel_core.runtime_db import (
    inspect_runtime_principal,
    provision_runtime_login,
    verify_runtime_principal,
)

pytestmark = pytest.mark.integration

_RUNTIME_LOGIN = "keel_runtime_login_test"
_RUNTIME_PASSWORD = "runtime-test-pw"  # noqa: S105 - throwaway local test-role password


async def _runtime_group_available(engine: AsyncEngine) -> bool:
    async with engine.connect() as conn:
        found = await conn.scalar(text("SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime'"))
    return bool(found)


async def _can_create_login(engine: AsyncEngine) -> bool:
    async with engine.connect() as conn:
        can = await conn.scalar(
            text("SELECT rolsuper OR rolcreaterole FROM pg_roles WHERE rolname = current_user")
        )
    return bool(can)


@pytest_asyncio.fixture
async def runtime_login_engine(migrated_db: AsyncEngine):
    """A LOGIN engine for a role provisioned as a member of ONLY ``keel_runtime``."""
    if not await _runtime_group_available(migrated_db):
        pytest.skip("keel_runtime role not provisioned in this database")
    if not await _can_create_login(migrated_db):
        pytest.skip("test principal cannot CREATE ROLE for the runtime-login test")

    # Exercise the real provisioning primitive (also covers its idempotency across reruns).
    login = await provision_runtime_login(
        migrated_db,
        password=_RUNTIME_PASSWORD,
        login_name=_RUNTIME_LOGIN,
        group_role="keel_runtime",
    )
    assert login == _RUNTIME_LOGIN

    url = make_url(os.environ["KEEL_TEST_DATABASE_URL"]).set(
        username=_RUNTIME_LOGIN, password=_RUNTIME_PASSWORD
    )
    engine = create_async_engine(url)
    try:
        yield engine
    finally:
        await engine.dispose()
        async with migrated_db.begin() as conn:
            await conn.execute(text(f"DROP ROLE IF EXISTS {_RUNTIME_LOGIN}"))


async def _seed_org_project(engine: AsyncEngine) -> tuple[str, str]:
    """Seed an organization + project as the owner (superuser bypasses RLS); return their ids."""
    org_id = f"org-{uuid.uuid4().hex}"
    project_id = f"proj-{uuid.uuid4().hex}"
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO organizations (id, slug, display_name) VALUES (:id, :slug, :name)"),
            {"id": org_id, "slug": org_id, "name": "Org"},
        )
        await conn.execute(
            text(
                "INSERT INTO projects (id, org_id, slug, display_name) "
                "VALUES (:id, :org, :slug, :name)"
            ),
            {"id": project_id, "org": org_id, "slug": project_id, "name": "Project"},
        )
    return org_id, project_id


def _insert_proposal_sql() -> str:
    return (
        "INSERT INTO patch_proposals "
        "(id, org_id, project_id, run_id, agent_id, actor, base_ref, idempotency_key, expires_at) "
        "VALUES (:id, :org, :project, :run, :agent, :actor, :base_ref, :idem, :expires)"
    )


def _proposal_params(*, proposal_id: str, org_id: str, project_id: str) -> dict[str, object]:
    return {
        "id": proposal_id,
        "org": org_id,
        "project": project_id,
        "run": f"run-{uuid.uuid4().hex}",
        "agent": "agent-1",
        "actor": "actor-1",
        "base_ref": "main",
        "idem": f"idem-{uuid.uuid4().hex}",
        "expires": datetime.now(UTC) + timedelta(days=1),
    }


async def test_runtime_login_is_least_privilege(runtime_login_engine: AsyncEngine) -> None:
    async with runtime_login_engine.connect() as conn:
        # No super / bypass over the whole membership closure, and it owns no public tables.
        report = await inspect_runtime_principal(conn)
        assert report.principal == _RUNTIME_LOGIN
        assert report.is_superuser is False
        assert report.can_bypass_rls is False
        assert report.owns_tables is False
        assert report.least_privilege is True

        # Raw attribute cross-check (defense in depth): over the full membership closure the gate
        # reads -- pg_has_role(..., 'MEMBER') -- i.e. including roles reachable only via SET ROLE.
        row = (
            await conn.execute(
                text(
                    "SELECT bool_or(r.rolsuper) AS s, bool_or(r.rolbypassrls) AS b "
                    "FROM pg_roles r WHERE pg_has_role(current_user, r.oid, 'MEMBER')"
                )
            )
        ).one()
        assert row.s is False
        assert row.b is False

        # The fail-closed gate accepts this principal and returns its name.
        assert await verify_runtime_principal(conn) == _RUNTIME_LOGIN


async def test_runtime_login_scoped_dml_connector_outbox(
    migrated_db: AsyncEngine, runtime_login_engine: AsyncEngine
) -> None:
    """Pre-0019 table (``app.scope_id``): scoped DML works; no-scope / cross-scope denied."""
    scope_a = f"A:{uuid.uuid4().hex}"
    scope_b = f"B:{uuid.uuid4().hex}"
    store = PostgresOutboundStore(migrated_db)
    await store.claim(scope_a, "email_send", "k-a")
    await store.claim(scope_b, "email_send", "k-b")

    async with runtime_login_engine.connect() as conn:
        # Scoped read: only scope A is visible.
        await conn.execute(text("SELECT set_config('app.scope_id', :s, false)"), {"s": scope_a})
        rows = (
            await conn.execute(
                text("SELECT scope_id FROM connector_outbox WHERE scope_id IN (:a, :b)"),
                {"a": scope_a, "b": scope_b},
            )
        ).fetchall()
        assert {r[0] for r in rows} == {scope_a}

        # Scoped write works (the visible row is updatable).
        result = await conn.execute(
            text("UPDATE connector_outbox SET connector_id = connector_id WHERE scope_id = :a"),
            {"a": scope_a},
        )
        assert result.rowcount == 1

        # Cross-scope write is denied: scope B is invisible, so 0 rows are touched.
        cross = await conn.execute(
            text("UPDATE connector_outbox SET connector_id = connector_id WHERE scope_id = :b"),
            {"b": scope_b},
        )
        assert cross.rowcount == 0
        await conn.rollback()

    async with runtime_login_engine.connect() as conn:
        # No scope set -> deny by default (FORCE RLS, no matching policy row).
        await conn.execute(text("SELECT set_config('app.scope_id', '', false)"))
        none_rows = (
            await conn.execute(
                text("SELECT scope_id FROM connector_outbox WHERE scope_id IN (:a, :b)"),
                {"a": scope_a, "b": scope_b},
            )
        ).fetchall()
        assert none_rows == []


async def test_runtime_login_scoped_dml_patch_proposals(
    migrated_db: AsyncEngine, runtime_login_engine: AsyncEngine
) -> None:
    """0019 table (``app.org_id``): scoped read/insert works; cross-scope insert WITH CHECK deny."""
    org_a, project_a = await _seed_org_project(migrated_db)
    org_b, project_b = await _seed_org_project(migrated_db)
    seeded_a = f"pp-{uuid.uuid4().hex}"
    async with migrated_db.begin() as conn:  # seed as owner (bypasses RLS)
        await conn.execute(
            text(_insert_proposal_sql()),
            _proposal_params(proposal_id=seeded_a, org_id=org_a, project_id=project_a),
        )

    async with runtime_login_engine.connect() as conn:
        # Scoped read sees only org A's proposal.
        await conn.execute(text("SELECT set_config('app.org_id', :o, false)"), {"o": org_a})
        visible = (await conn.execute(text("SELECT id FROM patch_proposals"))).fetchall()
        assert {r[0] for r in visible} == {seeded_a}

        # Scoped insert into the caller's own org succeeds.
        await conn.execute(
            text(_insert_proposal_sql()),
            _proposal_params(
                proposal_id=f"pp-{uuid.uuid4().hex}", org_id=org_a, project_id=project_a
            ),
        )
        await conn.commit()

    async with runtime_login_engine.connect() as conn:
        # Cross-org read: org B invisible while scoped to org A.
        await conn.execute(text("SELECT set_config('app.org_id', :o, false)"), {"o": org_a})
        assert (await conn.execute(text("SELECT count(*) FROM patch_proposals"))).scalar() == 2

        # No scope -> deny by default.
        await conn.execute(text("SELECT set_config('app.org_id', '', false)"))
        assert (await conn.execute(text("SELECT count(*) FROM patch_proposals"))).scalar() == 0

    async with runtime_login_engine.connect() as conn:
        # Cross-org write is denied by the RLS WITH CHECK even though the FK to org B is valid.
        await conn.execute(text("SELECT set_config('app.org_id', :o, false)"), {"o": org_a})
        with pytest.raises((ProgrammingError, DBAPIError)):
            await conn.execute(
                text(_insert_proposal_sql()),
                _proposal_params(
                    proposal_id=f"pp-{uuid.uuid4().hex}", org_id=org_b, project_id=project_b
                ),
            )
        await conn.rollback()


async def test_runtime_login_cannot_run_ddl(runtime_login_engine: AsyncEngine) -> None:
    for stmt in (
        "CREATE TABLE keel_runtime_ddl_probe (id int)",
        "ALTER TABLE connector_outbox ADD COLUMN keel_runtime_probe int",
        "DROP TABLE connector_outbox",
    ):
        async with runtime_login_engine.connect() as conn:
            with pytest.raises((ProgrammingError, DBAPIError)):
                await conn.execute(text(stmt))


async def test_runtime_login_cannot_set_role_to_owner_or_maintenance(
    runtime_login_engine: AsyncEngine,
) -> None:
    for role in ("keel", "keel_maintenance", "keel_maintenance_exec"):
        async with runtime_login_engine.connect() as conn:
            with pytest.raises((ProgrammingError, DBAPIError)):
                await conn.execute(text(f"SET ROLE {role}"))


async def test_verify_runtime_principal_rejects_owner_connection(
    migrated_db: AsyncEngine,
) -> None:
    # The schema owner (superuser + BYPASSRLS + table owner) is exactly what the gate must refuse.
    async with migrated_db.connect() as conn:
        with pytest.raises(RuntimePrincipalError):
            await verify_runtime_principal(conn)


async def test_verify_runtime_principal_rejects_noinherit_member_of_bypassrls(
    migrated_db: AsyncEngine,
) -> None:
    """Regression (membership closure, not just inheritance): a ``NOINHERIT`` login that merely
    *belongs to* a ``BYPASSRLS`` role inherits none of its privileges, yet can ``SET ROLE`` into it
    to defeat ``FORCE RLS``. The gate reads ``pg_has_role(..., 'MEMBER')`` (not ``'USAGE'``) so this
    escalation surface is rejected rather than silently accepted."""
    async with migrated_db.connect() as conn:
        is_super = await conn.scalar(
            text("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
        )
    if not is_super:
        pytest.skip("granting BYPASSRLS requires a superuser test principal")

    login = "keel_runtime_login_escalate_test"
    group = "keel_runtime_bypass_grp_test"
    escalate_pw = "escalate-test-pw"  # noqa: S105 - throwaway local test-role password
    async with migrated_db.begin() as conn:
        await conn.execute(text(f"DROP ROLE IF EXISTS {login}"))
        await conn.execute(text(f"DROP ROLE IF EXISTS {group}"))
        await conn.execute(text(f"CREATE ROLE {group} NOLOGIN BYPASSRLS"))
        # NOINHERIT: the login does NOT inherit the group's BYPASSRLS, so a 'USAGE' closure would
        # (wrongly) treat it as least-privilege -- but it can still SET ROLE into the group.
        await conn.execute(
            text(
                f"CREATE ROLE {login} LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS "
                f"PASSWORD '{escalate_pw}'"
            )
        )
        await conn.execute(text(f"GRANT {group} TO {login}"))

    url = make_url(os.environ["KEEL_TEST_DATABASE_URL"]).set(username=login, password=escalate_pw)
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            report = await inspect_runtime_principal(conn)
            # Caught only because the closure follows MEMBER, not USAGE (NOINHERIT hides it there).
            assert report.can_bypass_rls is True
            with pytest.raises(RuntimePrincipalError):
                await verify_runtime_principal(conn)
    finally:
        await engine.dispose()
        async with migrated_db.begin() as conn:
            await conn.execute(text(f"DROP ROLE IF EXISTS {login}"))
            await conn.execute(text(f"DROP ROLE IF EXISTS {group}"))


async def test_provision_runtime_login_is_idempotent(
    migrated_db: AsyncEngine, runtime_login_engine: AsyncEngine
) -> None:
    # The fixture already provisioned once; a repeat run is a no-op repair that keeps it valid.
    login = await provision_runtime_login(
        migrated_db,
        password=_RUNTIME_PASSWORD,
        login_name=_RUNTIME_LOGIN,
        group_role="keel_runtime",
    )
    assert login == _RUNTIME_LOGIN
    async with runtime_login_engine.connect() as conn:
        assert await verify_runtime_principal(conn) == _RUNTIME_LOGIN


async def test_provision_runtime_login_fails_closed_without_group(
    migrated_db: AsyncEngine,
) -> None:
    if not await _can_create_login(migrated_db):
        pytest.skip("test principal cannot CREATE ROLE")
    with pytest.raises(RuntimePrincipalError):
        await provision_runtime_login(
            migrated_db,
            password=_RUNTIME_PASSWORD,
            login_name="keel_runtime_login_test_orphan",
            group_role="keel_group_that_does_not_exist",
        )
    # Fail-closed means no orphan login was minted.
    async with migrated_db.connect() as conn:
        exists = await conn.scalar(
            text("SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime_login_test_orphan'")
        )
    assert exists is None


async def test_runtime_login_connects_password_less_via_pgpass(
    migrated_db: AsyncEngine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end the exact Compose mechanism: the secret bootstrap generates the password + a
    libpq pgpass file, the login is provisioned with it, and the app connects PASSWORD-LESS (no
    secret in the URL) via ``PGPASSFILE`` — proving current_user is the least-privilege login and
    RLS/DDL still bind through that path."""
    if not await _runtime_group_available(migrated_db):
        pytest.skip("keel_runtime role not provisioned in this database")
    if not await _can_create_login(migrated_db):
        pytest.skip("test principal cannot CREATE ROLE for the runtime-login test")

    base = make_url(os.environ["KEEL_TEST_DATABASE_URL"])
    login = "keel_runtime_login_pgpass_test"
    pw_path = tmp_path / "runtime_db_password"
    pgpass_path = tmp_path / "runtime_pgpass"

    # 1) The bootstrap generates a random password + a pgpass line matching this DB (host/port/db).
    ensure_runtime_secret(
        password_path=pw_path,
        pgpass_path=pgpass_path,
        host=base.host or "localhost",
        port=str(base.port or 5432),
        dbname=base.database or "keel",
        user=login,
    )
    password = pw_path.read_text(encoding="utf-8").strip()

    # 2) Provision the login with that generated password (the operator/provision path).
    await provision_runtime_login(
        migrated_db, password=password, login_name=login, group_role="keel_runtime"
    )

    # 3) Connect PASSWORD-LESS: the URL carries only the username; libpq reads PGPASSFILE.
    monkeypatch.setenv("PGPASSFILE", str(pgpass_path))
    pwless = f"{base.drivername}://{login}@{base.host}:{base.port}/{base.database}"
    engine = create_async_engine(pwless)
    try:
        async with engine.connect() as conn:
            who = await conn.scalar(text("SELECT current_user"))
            assert who == login, "must authenticate as the runtime login via pgpass (no URL pw)"

            report = await inspect_runtime_principal(conn)
            assert report.principal == login
            assert report.is_superuser is False
            assert report.can_bypass_rls is False
            assert report.owns_tables is False
            assert report.least_privilege is True
            assert await verify_runtime_principal(conn) == login

            # No scope set -> FORCE RLS denies by default (same boundary, via the pgpass path).
            await conn.execute(text("SELECT set_config('app.scope_id', '', false)"))
            denied = (await conn.execute(text("SELECT scope_id FROM connector_outbox"))).fetchall()
            assert denied == []

        # DDL is still refused for this least-privilege login.
        async with engine.connect() as conn:
            with pytest.raises((ProgrammingError, DBAPIError)):
                await conn.execute(text("CREATE TABLE keel_runtime_pgpass_probe (id int)"))
    finally:
        await engine.dispose()
        async with migrated_db.begin() as conn:
            await conn.execute(text(f"DROP ROLE IF EXISTS {login}"))


async def _direct_memberships(engine: AsyncEngine, login: str) -> set[str]:
    """The set of roles ``login`` is a DIRECT member of (pg_auth_members), read as the owner."""
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT g.rolname FROM pg_auth_members m "
                    "JOIN pg_roles g ON g.oid = m.roleid "
                    "JOIN pg_roles l ON l.oid = m.member WHERE l.rolname = :login"
                ),
                {"login": login},
            )
        ).fetchall()
    return {r[0] for r in rows}


async def _maintenance_exec_available(engine: AsyncEngine) -> bool:
    async with engine.connect() as conn:
        found = await conn.scalar(
            text("SELECT 1 FROM pg_roles WHERE rolname = 'keel_maintenance_exec'")
        )
    return bool(found)


async def test_runtime_login_public_create_is_detected(
    migrated_db: AsyncEngine, runtime_login_engine: AsyncEngine
) -> None:
    """Simulate an old/upgraded cluster where ``PUBLIC`` retained ``CREATE`` on ``public`` (the
    PostgreSQL < 15 default): the runtime login can then create objects via the ``PUBLIC`` grant
    even though 0020 revoked its direct ``CREATE``. The gate must catch the *effective*
    ``has_schema_privilege(current_user, 'public', 'CREATE')`` and reject; once the PUBLIC grant
    is removed (0020's posture) the same login is least-privilege again."""
    try:
        async with migrated_db.begin() as conn:
            await conn.execute(text("GRANT CREATE ON SCHEMA public TO PUBLIC"))
        async with runtime_login_engine.connect() as conn:
            report = await inspect_runtime_principal(conn)
            assert report.can_create_in_schema is True
            assert report.least_privilege is False
            with pytest.raises(RuntimePrincipalError):
                await verify_runtime_principal(conn)
    finally:
        async with migrated_db.begin() as conn:
            await conn.execute(text("REVOKE CREATE ON SCHEMA public FROM PUBLIC"))

    # Back to the 0020 posture: no effective CREATE, gate accepts again.
    async with runtime_login_engine.connect() as conn:
        report = await inspect_runtime_principal(conn)
        assert report.can_create_in_schema is False
        assert await verify_runtime_principal(conn) == _RUNTIME_LOGIN


async def test_runtime_login_cannot_delete_identity_or_mutate_control(
    runtime_login_engine: AsyncEngine,
) -> None:
    """The least-privilege login holds none of the cross-tenant erasure / migration-control
    privileges: the report flags are all clear, ``alembic_version`` SELECT is retained (schema
    version reads ``0021``), and real identity DELETE / ``keel_erase_*`` EXECUTE / control-table
    DML are all refused at the privilege layer (independent of RLS)."""
    async with runtime_login_engine.connect() as conn:
        report = await inspect_runtime_principal(conn)
        assert report.can_delete_identity_tables is False
        assert report.can_execute_erase_functions is False
        assert report.can_write_control_table is False
        assert report.can_create_in_schema is False
        assert report.unexpected_memberships == ()
        # SELECT on the control table is kept: a read-only view of the current schema version.
        head = await conn.scalar(text("SELECT version_num FROM alembic_version"))
        assert head == "0025_agent_access_sessions"

    denied = (
        "DELETE FROM users",
        "SELECT keel_erase_user('does-not-matter')",
        "SELECT keel_erase_organization('does-not-matter')",
        "INSERT INTO alembic_version (version_num) VALUES ('0000_forged')",
        "UPDATE alembic_version SET version_num = '0000_forged'",
        "DELETE FROM alembic_version",
    )
    for stmt in denied:
        async with runtime_login_engine.connect() as conn:
            with pytest.raises((ProgrammingError, DBAPIError)):
                await conn.execute(text(stmt))


async def test_provision_runtime_login_strips_dangerous_membership(
    migrated_db: AsyncEngine,
) -> None:
    """A login that a prior/mistaken provisioning left a member of ``keel_maintenance_exec`` (a
    ``SET ROLE`` path to the cross-tenant erase functions) is (a) rejected by the gate before
    repair — the report names the extra membership and effective erase EXECUTE — and (b) actively
    stripped back to *only* ``keel_runtime`` by ``provision_runtime_login``. There is no silent
    acceptance of the extra membership; the strip runs inside the provisioning transaction."""
    if not await _runtime_group_available(migrated_db):
        pytest.skip("keel_runtime role not provisioned in this database")
    if not await _maintenance_exec_available(migrated_db):
        pytest.skip("keel_maintenance_exec role not provisioned in this database")
    if not await _can_create_login(migrated_db):
        pytest.skip("test principal cannot CREATE ROLE")

    login = "keel_runtime_login_danger_test"
    pw = "danger-test-pw"  # noqa: S105 - throwaway local test-role password
    async with migrated_db.begin() as conn:
        await conn.execute(text(f"DROP ROLE IF EXISTS {login}"))
        await conn.execute(text(f"CREATE ROLE {login} LOGIN PASSWORD '{pw}'"))
        # Drift: the login belongs to the runtime group AND the maintenance executor.
        await conn.execute(text(f"GRANT keel_runtime TO {login}"))
        await conn.execute(text(f"GRANT keel_maintenance_exec TO {login}"))

    url = make_url(os.environ["KEEL_TEST_DATABASE_URL"]).set(username=login, password=pw)
    engine = create_async_engine(url)
    try:
        # Before repair: the gate rejects it (dangerous membership + effective erase EXECUTE).
        async with engine.connect() as conn:
            report = await inspect_runtime_principal(conn)
            assert "keel_maintenance_exec" in report.unexpected_memberships
            assert report.can_execute_erase_functions is True
            assert report.least_privilege is False
            with pytest.raises(RuntimePrincipalError):
                await verify_runtime_principal(conn)
        await engine.dispose()

        # Repair via the real provisioning primitive: it strips every non-runtime membership.
        await provision_runtime_login(
            migrated_db, password=pw, login_name=login, group_role="keel_runtime"
        )
        assert await _direct_memberships(migrated_db, login) == {"keel_runtime"}

        # After repair: least-privilege and accepted; erase EXECUTE no longer reachable.
        engine = create_async_engine(url)
        async with engine.connect() as conn:
            report = await inspect_runtime_principal(conn)
            assert report.unexpected_memberships == ()
            assert report.can_execute_erase_functions is False
            assert report.least_privilege is True
            assert await verify_runtime_principal(conn) == login
    finally:
        await engine.dispose()
        async with migrated_db.begin() as conn:
            await conn.execute(text(f"DROP ROLE IF EXISTS {login}"))
