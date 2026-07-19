"""runtime-role hardening: pin least-privilege ``keel_runtime`` attributes + authoritative grants.

Revision ID: 0020_runtime_role_hardening
Revises: 0019_patch_proposals
Create Date: 2026-07-19

M3A runtime DB role / RLS security gate (WS-DB). ``FORCE ROW LEVEL SECURITY`` (0011/0013/0015/
0019) only binds a **non-owner, non-bypass** connection — a superuser or the table owner
bypasses RLS entirely. Earlier migrations created ``keel_runtime`` with the *default* role
attributes (``NOSUPERUSER``/``NOBYPASSRLS`` plus the implicit ``NOCREATEDB``/``NOCREATEROLE``),
which is correct but not *explicitly asserted* — a pre-existing / manually provisioned role on
managed Postgres could drift. This migration makes the runtime group's least-privilege posture
explicit and authoritative so a real deployment can point the application connection at a login
that is a member of only ``keel_runtime`` and have RLS be a hard boundary:

* **Pin the attributes.** ``ALTER ROLE keel_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOBYPASSRLS NOINHERIT`` — idempotent repair of drift. ``NOLOGIN`` keeps it a pure group (a
  dedicated login is a *member*). ``NOINHERIT`` is defense-in-depth: the group holds its DML
  privileges directly and grants them to login members (whose own ``INHERIT`` governs
  inheritance — verified: a member login still inherits the group's DML), while ``NOINHERIT``
  on the group itself caps privileges to its *direct* grants, so a future accidental
  ``GRANT <privileged_role> TO keel_runtime`` cannot transitively escalate a runtime login.

* **Authoritative minimal grants.** (Re)grant ``USAGE`` on ``public`` and
  ``SELECT/INSERT/UPDATE/DELETE`` on every current table (covering the 0019 patch tables and
  any table an earlier migration may have missed) + ``USAGE, SELECT`` on all sequences, and
  re-assert the ``ALTER DEFAULT PRIVILEGES`` so owner-created future tables inherit them. The
  identity least-privilege exceptions from 0013 are then re-applied on top (see below) so the
  broad grant never silently restores a deliberately-revoked privilege.

* **No DDL / no ownership.** ``REVOKE CREATE ON SCHEMA public`` from both ``keel_runtime`` *and*
  ``PUBLIC`` — the latter matters on PostgreSQL < 15 (and clusters upgraded from it) where the
  ``public`` schema granted ``CREATE`` to ``PUBLIC`` by default, so any login (including a runtime
  login) could create objects even without a direct grant. After this the effective
  ``has_schema_privilege(keel_runtime, 'public', 'CREATE')`` is ``false`` — exactly what
  :func:`keel_core.runtime_db.verify_runtime_principal` asserts — so the runtime role can never
  create objects; it owns no tables and cannot ``ALTER``/``DROP`` them.

* **Re-assert the identity erasure split (0013).** Cross-tenant deletion stays out of the
  runtime role: ``REVOKE DELETE`` on the global identity tables and ``REVOKE EXECUTE`` on the
  ``keel_erase_*`` SECURITY DEFINER functions, so re-running the broad grant above cannot
  re-open the cross-tenant erasure path. Each revoke runs in its **own** subtransaction so an
  ``insufficient_privilege`` (or a missing erase function) on one cannot roll back the others —
  in particular the critical identity ``DELETE`` revoke must persist even if a later
  function-revoke raises. Anything that still slips through is caught fail-closed by
  ``verify_runtime_principal`` (which checks effective identity ``DELETE`` + erase ``EXECUTE``),
  so a per-statement ``NOTICE`` is not a silent security downgrade.

* **No migration-control writes.** ``REVOKE INSERT, UPDATE, DELETE ON alembic_version`` from
  ``keel_runtime`` (``SELECT`` is kept): the broad table grant above re-granted DML on *every*
  table including Alembic's control table, so a compromised runtime connection could otherwise
  forge the recorded schema version. ``verify_runtime_principal`` checks effective INSERT/UPDATE/
  DELETE here.

Everything is wrapped so a managed Postgres that forbids ``ALTER ROLE`` / ``GRANT`` does not
fail the migration (it emits a ``NOTICE``; operators harden the role manually — see
``docs/OPERATIONS.md``). Reversible: the downgrade restores the pre-0020 ``INHERIT`` default. The
strict revokes (schema ``CREATE`` from ``PUBLIC``, identity ``DELETE``, erase ``EXECUTE``,
``alembic_version`` DML) are intentionally *not* reverted — they are safe, idempotent hardening
that re-asserts 0013's posture (and on PG15+ removes a privilege that was never present), so
re-granting them on downgrade would wrongly over-privilege the role; they are simply re-applied
on the next upgrade.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0020_runtime_role_hardening"
down_revision: str | None = "0019_patch_proposals"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Global identity tables whose DELETE cascades across orgs — cross-tenant erasure must go
# through the SECURITY DEFINER functions, never direct runtime DELETE (0013 invariant).
_GLOBAL_IDENTITY_TABLES = ("users", "oidc_identities", "organizations")


def upgrade() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                RAISE NOTICE 'keel_runtime absent; runtime-role hardening skipped '
                             '(provision it manually - see docs/OPERATIONS.md)';
                RETURN;
            END IF;

            -- 1) Pin least-privilege attributes (idempotent repair of drift).
            BEGIN
                ALTER ROLE keel_runtime
                    NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS NOINHERIT;
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'keel_runtime attribute hardening skipped (insufficient privilege)';
            END;

            -- 2) Authoritative minimal grants (covers the 0019 patch tables + any earlier miss).
            BEGIN
                GRANT USAGE ON SCHEMA public TO keel_runtime;
                GRANT SELECT, INSERT, UPDATE, DELETE
                    ON ALL TABLES IN SCHEMA public TO keel_runtime;
                GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO keel_runtime;
                ALTER DEFAULT PRIVILEGES IN SCHEMA public
                    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO keel_runtime;
                ALTER DEFAULT PRIVILEGES IN SCHEMA public
                    GRANT USAGE, SELECT ON SEQUENCES TO keel_runtime;
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'keel_runtime grant refresh skipped (insufficient privilege)';
            END;

            -- 3) No DDL / no ownership: the runtime role can never create objects. Also revoke
            --    CREATE from PUBLIC so an old/upgraded cluster (PG < 15 granted PUBLIC CREATE on
            --    `public` by default) does not leave the runtime login able to create objects via
            --    the PUBLIC grant. verify_runtime_principal checks the effective
            --    has_schema_privilege(current_user, 'public', 'CREATE').
            BEGIN
                REVOKE CREATE ON SCHEMA public FROM keel_runtime;
                REVOKE CREATE ON SCHEMA public FROM PUBLIC;
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'schema public CREATE revoke skipped (insufficient privilege)';
            END;

            -- 4) Re-assert the identity erasure split (0013): keep cross-tenant deletion out of
            --    the runtime role even after the broad grant above. Each revoke is an INDEPENDENT
            --    subtransaction so a failure (or insufficient_privilege) on one cannot roll back
            --    the others -- in particular the critical global-identity DELETE revoke must
            --    persist even if an erase-function revoke raises. verify_runtime_principal is the
            --    fail-closed backstop, so a per-statement NOTICE here is not a silent downgrade.
            BEGIN
                REVOKE DELETE ON {", ".join(_GLOBAL_IDENTITY_TABLES)} FROM keel_runtime;
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'keel_runtime identity DELETE revoke skipped (insufficient privilege)';
            END;
            BEGIN
                IF to_regprocedure('keel_erase_organization(text)') IS NOT NULL THEN
                    REVOKE EXECUTE ON FUNCTION keel_erase_organization(text) FROM keel_runtime;
                END IF;
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE
                    'keel_runtime erase_organization EXECUTE revoke skipped (insufficient priv)';
            END;
            BEGIN
                IF to_regprocedure('keel_erase_user(text)') IS NOT NULL THEN
                    REVOKE EXECUTE ON FUNCTION keel_erase_user(text) FROM keel_runtime;
                END IF;
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE
                    'keel_runtime erase_user EXECUTE revoke skipped (insufficient privilege)';
            END;

            -- 5) No migration-control writes: the broad table grant re-granted DML on every table
            --    including Alembic's control table. Revoke INSERT/UPDATE/DELETE (keep SELECT) so a
            --    compromised runtime connection cannot forge the recorded schema version.
            --    verify_runtime_principal checks effective INSERT/UPDATE/DELETE here.
            BEGIN
                IF to_regclass('public.alembic_version') IS NOT NULL THEN
                    REVOKE INSERT, UPDATE, DELETE ON alembic_version FROM keel_runtime;
                END IF;
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'keel_runtime alembic_version DML revoke skipped (insufficient priv)';
            END;
        END $$;
        """
    )


def downgrade() -> None:
    # Restore the pre-0020 posture: the role predated this migration with the default INHERIT.
    # Leave the DML grants (they predate 0020) and do NOT re-grant the strict revokes (schema
    # CREATE from PUBLIC/keel_runtime, global-identity DELETE, keel_erase_* EXECUTE, and
    # alembic_version DML): the runtime role never legitimately held them, they re-assert 0013's
    # posture, and on PG15+ some were never present, so restoring them would wrongly
    # over-privilege the role. They are simply re-applied on the next upgrade. Guarded so a
    # restricted Postgres does not fail the downgrade.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'keel_runtime') THEN
                BEGIN
                    ALTER ROLE keel_runtime INHERIT;
                EXCEPTION WHEN insufficient_privilege THEN
                    RAISE NOTICE 'keel_runtime downgrade skipped (insufficient privilege)';
                END;
            END IF;
        END $$;
        """
    )
