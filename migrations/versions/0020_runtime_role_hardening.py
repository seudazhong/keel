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

* **No DDL / no ownership.** ``REVOKE CREATE ON SCHEMA public FROM keel_runtime`` (defense for
  older Postgres / drift where ``PUBLIC`` retained ``CREATE``) so the runtime role can never
  create objects; it owns no tables and cannot ``ALTER``/``DROP`` them.

* **Re-assert the identity erasure split (0013).** Cross-tenant deletion stays out of the
  runtime role: ``REVOKE DELETE`` on the global identity tables and ``REVOKE EXECUTE`` on the
  ``keel_erase_*`` SECURITY DEFINER functions, so re-running the broad grant above cannot
  re-open the cross-tenant erasure path.

Everything is wrapped so a managed Postgres that forbids ``ALTER ROLE`` / ``GRANT`` does not
fail the migration (it emits a ``NOTICE``; operators harden the role manually — see
``docs/OPERATIONS.md``). Reversible: the downgrade restores the pre-0020 ``INHERIT`` default and
re-grants ``CREATE`` on the schema (it leaves the DML grants, which predate this migration).
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

            -- 3) No DDL / no ownership: the runtime role can never create objects.
            BEGIN
                REVOKE CREATE ON SCHEMA public FROM keel_runtime;
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'keel_runtime CREATE revoke skipped (insufficient privilege)';
            END;

            -- 4) Re-assert the identity erasure split (0013): keep cross-tenant deletion out of
            --    the runtime role even after the broad grant above.
            BEGIN
                REVOKE DELETE ON {", ".join(_GLOBAL_IDENTITY_TABLES)} FROM keel_runtime;
                IF to_regprocedure('keel_erase_organization(text)') IS NOT NULL THEN
                    REVOKE EXECUTE ON FUNCTION keel_erase_organization(text) FROM keel_runtime;
                END IF;
                IF to_regprocedure('keel_erase_user(text)') IS NOT NULL THEN
                    REVOKE EXECUTE ON FUNCTION keel_erase_user(text) FROM keel_runtime;
                END IF;
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE NOTICE 'keel_runtime identity revokes skipped (insufficient privilege)';
            END;
        END $$;
        """
    )


def downgrade() -> None:
    # Restore the pre-0020 posture: the role predated this migration with the default INHERIT.
    # Leave the DML grants (they predate 0020) and do NOT re-grant CREATE — the runtime role
    # never held it, so restoring it would wrongly over-privilege the role. Guarded so a
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
