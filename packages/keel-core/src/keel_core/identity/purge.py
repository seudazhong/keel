"""Durable erasure for identity data — organization + data-subject (M3.6, WS-L).

Two GDPR primitives that complement the M3.5 scope/session/project erasure coordinator
(identity is org-partitioned, not scope-partitioned, so it is erased through this dedicated
path rather than the scope ledger — see ``docs/DATA-LIFECYCLE.md``):

* :meth:`IdentityPurgeRepository.erase_organization` — tenant offboarding. Removes every
  org-owned row (resource grants, Agents, memberships) and the organization itself.
* :meth:`IdentityPurgeRepository.erase_user` — a data subject's erasure. Removes the user's
  OIDC links, the Agents it owns, its memberships across every org, the grants it issued,
  and the user row. Erasure never orphans an active organization: it is **blocked** (with
  :class:`UserErasureBlockedError`, atomically, deleting nothing) when the user is the sole
  active owner of an active org that still has other active members, and it atomically
  **archives** an active org the user solely owns and is the only active member of.

Both primitives execute the durable ``keel_erase_organization`` / ``keel_erase_user``
``SECURITY DEFINER`` functions installed by migration 0013 rather than issuing the deletes
directly. This is what makes erasure correct under production RLS: user erasure is inherently
cross-tenant (a user belongs to many orgs) and must delete the global ``users`` row, but the
non-bypass ``keel_runtime`` role under ``FORCE ROW LEVEL SECURITY`` cannot enumerate rows
across orgs — so a direct enumeration would silently skip the sole-owner guard and cascade an
active org into an ownerless state. The definer functions (owned by the ``keel_maintenance``
role, EXECUTE revoked from ``keel_runtime``) enumerate + lock the affected orgs, enforce the
block/archive semantics, and purge every identity row atomically, while normal runtime
principals are denied both the functions and DELETE on the global identity tables.

Identity is not event-sourced, so no projection rebuild can resurrect an erased identity row.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from keel_core.config import Settings
from keel_core.errors import KeelError


class UserErasureBlockedError(KeelError):
    """Erasing a user would orphan an active organization it solely owns.

    A user who is the *sole active owner* of an active org that still has **other** active
    members cannot be erased until ownership is transferred (or those members are removed):
    silently deleting the owner would leave a live, ownerless tenant. The blocking org ids
    are exposed so an operator can act. No rows are deleted when this is raised (the whole
    erasure is atomic — it either fully proceeds or fully aborts).
    """

    def __init__(self, blocking_org_ids: tuple[str, ...]) -> None:
        self.blocking_org_ids = blocking_org_ids
        super().__init__(
            "user erasure blocked: transfer ownership of solely-owned active organizations "
            f"first: {', '.join(blocking_org_ids)}"
        )


class MaintenancePrincipalError(KeelError):
    """The connected principal is not a valid least-privilege maintenance executor.

    Raised by :func:`verify_maintenance_principal` / the factory when the database principal
    behind the maintenance connection does not have EXECUTE on the erasure functions, or holds
    privileges it must not (direct DELETE on identity tables, or ``BYPASSRLS``). This fails
    closed so erasure never runs from an over-privileged connection (e.g. the schema owner or
    the ``keel_maintenance`` *definer* role) that would defeat the definer/executor split.
    Carries only role/privilege facts — never a credential or connection URL.
    """


@dataclass(frozen=True)
class OrganizationErasureResult:
    resource_grants: int
    agents: int
    memberships: int
    organization: int

    @property
    def total(self) -> int:
        return self.resource_grants + self.agents + self.memberships + self.organization


@dataclass(frozen=True)
class UserErasureResult:
    oidc_identities: int
    agents: int
    memberships: int
    resource_grants: int
    user: int
    archived_organizations: int = 0

    @property
    def total(self) -> int:
        return (
            self.oidc_identities + self.agents + self.memberships + self.resource_grants + self.user
        )


async def purge_organization(
    engine: AsyncEngine, org_id: str, *, dry_run: bool = False
) -> OrganizationErasureResult:
    """Erase all rows owned by an organization (idempotent). Returns per-store counts.

    When ``dry_run`` is set the erasure runs inside a transaction that is rolled back, so the
    returned counts are a truthful preview (the SECURITY DEFINER function's real deletes) with
    nothing persisted.
    """
    async with engine.connect() as conn:
        trans = await conn.begin()
        try:
            row = (
                (
                    await conn.execute(
                        text("SELECT * FROM keel_erase_organization(:org)"),
                        {"org": org_id},
                    )
                )
                .mappings()
                .one()
            )
            if dry_run:
                await trans.rollback()
            else:
                await trans.commit()
        except BaseException:
            if trans.is_active:
                await trans.rollback()
            raise
    return OrganizationErasureResult(
        resource_grants=int(row["deleted_grants"] or 0),
        agents=int(row["deleted_agents"] or 0),
        memberships=int(row["deleted_memberships"] or 0),
        organization=int(row["deleted_org"] or 0),
    )


async def purge_user(
    engine: AsyncEngine, user_id: str, *, dry_run: bool = False
) -> UserErasureResult:
    """Erase a user's global identity + every org-owned row it owns/issued (idempotent).

    Never orphans an active organization: if the user is the sole active owner of an active
    org that still has other active members, the whole erasure is aborted with
    :class:`UserErasureBlockedError` (ownership must be transferred first). An active org the
    user solely owns *and* is the only active member of is atomically archived as part of the
    same transaction (nobody else can own it), so lifecycle stays honest and no live tenant
    is left ownerless. The block/archive decision and the purge run inside the durable
    ``keel_erase_user`` ``SECURITY DEFINER`` function so they are correct under production RLS.

    When ``dry_run`` is set the function is executed inside a transaction that is rolled back:
    a would-be-blocked erasure still raises :class:`UserErasureBlockedError` and a would-be
    successful one returns its real counts, but nothing is persisted.
    """
    async with engine.connect() as conn:
        trans = await conn.begin()
        try:
            row = (
                (
                    await conn.execute(
                        text("SELECT * FROM keel_erase_user(:user)"),
                        {"user": user_id},
                    )
                )
                .mappings()
                .one()
            )
            blocked = tuple(row["blocked_org_ids"] or ())
            if dry_run or blocked:
                await trans.rollback()
            else:
                await trans.commit()
        except BaseException:
            if trans.is_active:
                await trans.rollback()
            raise
    if blocked:
        raise UserErasureBlockedError(blocked)
    return UserErasureResult(
        oidc_identities=int(row["deleted_oidc"] or 0),
        agents=int(row["deleted_agents"] or 0),
        memberships=int(row["deleted_memberships"] or 0),
        resource_grants=int(row["deleted_grants"] or 0),
        user=int(row["deleted_users"] or 0),
        archived_organizations=int(row["archived_orgs"] or 0),
    )


# Identity tables the maintenance executor must NOT be able to DELETE directly (the three
# global tables cascade across every org; the tenant tables are RLS-scoped).
_IDENTITY_TABLES = (
    "users",
    "oidc_identities",
    "organizations",
    "memberships",
    "agents",
    "resource_grants",
)


async def verify_maintenance_principal(conn: AsyncConnection) -> str:
    """Assert the connected principal is a least-privilege erasure executor; return its name.

    Fails closed (:class:`MaintenancePrincipalError`) unless the principal:

    * has ``EXECUTE`` on **both** ``keel_erase_user`` and ``keel_erase_organization``
      (the audited entrypoints), and
    * holds **no** direct ``DELETE`` on any identity table (it must go through the definer
      functions, never issue cross-tenant deletes itself), and
    * cannot ``BYPASSRLS`` (directly or via role membership) — i.e. it is not the schema owner
      or the ``keel_maintenance`` definer, so RLS still binds it outside the functions.

    All checks are pure catalog reads; no credential or URL is touched. The check is guarded
    so a database missing the functions reports "no EXECUTE" (fail closed) rather than error.
    """
    tables_sql = ", ".join(f"'{t}'" for t in _IDENTITY_TABLES)
    row = (
        (
            await conn.execute(
                text(
                    f"""
                    SELECT
                      current_user AS principal,
                      CASE WHEN to_regprocedure('keel_erase_user(text)') IS NOT NULL
                           THEN has_function_privilege('keel_erase_user(text)', 'EXECUTE')
                           ELSE false END AS exec_user,
                      CASE WHEN to_regprocedure('keel_erase_organization(text)') IS NOT NULL
                           THEN has_function_privilege('keel_erase_organization(text)', 'EXECUTE')
                           ELSE false END AS exec_org,
                      COALESCE((
                        SELECT bool_or(r.rolbypassrls) FROM pg_roles r
                        WHERE pg_has_role(current_user, r.oid, 'USAGE')
                      ), false) AS bypass_rls,
                      COALESCE((
                        SELECT bool_or(has_table_privilege(c.oid, 'DELETE'))
                        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'public' AND c.relname IN ({tables_sql})
                      ), false) AS can_delete_identity
                    """
                )
            )
        )
        .mappings()
        .one()
    )
    principal = str(row["principal"])
    if not (row["exec_user"] and row["exec_org"]):
        raise MaintenancePrincipalError(
            f"principal {principal!r} lacks EXECUTE on the erasure functions; connect as a "
            "member of keel_maintenance_exec"
        )
    if row["bypass_rls"]:
        raise MaintenancePrincipalError(
            f"principal {principal!r} can BYPASSRLS - refusing to erase from an over-privileged "
            "connection (use the keel_maintenance_exec executor, not the owner/definer)"
        )
    if row["can_delete_identity"]:
        raise MaintenancePrincipalError(
            f"principal {principal!r} holds direct DELETE on identity tables - refusing to erase "
            "from an over-privileged connection (use the keel_maintenance_exec executor)"
        )
    return principal


class IdentityPurgeRepository:
    """Durable identity erasure (organization offboarding + data-subject erasure).

    Prefer :func:`create_identity_purge_repository`, which resolves the dedicated maintenance
    URL (fail-closed), connects, and verifies the principal is a least-privilege executor.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    async def verify_principal(self) -> str:
        """Verify the connected principal is a least-privilege executor; return its name."""
        async with self._engine.connect() as conn:
            return await verify_maintenance_principal(conn)

    async def erase_organization(
        self, org_id: str, *, dry_run: bool = False
    ) -> OrganizationErasureResult:
        return await purge_organization(self._engine, org_id, dry_run=dry_run)

    async def erase_user(self, user_id: str, *, dry_run: bool = False) -> UserErasureResult:
        return await purge_user(self._engine, user_id, dry_run=dry_run)

    async def aclose(self) -> None:
        """Dispose the underlying engine (the factory owns engine creation)."""
        await self._engine.dispose()


async def create_identity_purge_repository(
    settings: Settings, *, verify: bool = True
) -> IdentityPurgeRepository:
    """Build an :class:`IdentityPurgeRepository` on the dedicated maintenance connection.

    Resolves ``KEEL_MAINTENANCE_DATABASE_URL`` via
    :meth:`Settings.require_maintenance_database_url` (which fails closed when unset, and in
    cloud mode refuses a copy of the runtime URL), opens an async engine, and — unless
    ``verify`` is disabled — asserts the connected principal is a least-privilege executor
    (EXECUTE on the functions, no direct identity DELETE, no ``BYPASSRLS``). Any failure
    disposes the engine so no leaked connection remains. The URL is never logged.
    """
    url = settings.require_maintenance_database_url()
    engine = create_async_engine(url, pool_pre_ping=True, future=True)
    repo = IdentityPurgeRepository(engine)
    if verify:
        try:
            await repo.verify_principal()
        except BaseException:
            await engine.dispose()
            raise
    return repo


__all__ = [
    "IdentityPurgeRepository",
    "MaintenancePrincipalError",
    "OrganizationErasureResult",
    "UserErasureBlockedError",
    "UserErasureResult",
    "create_identity_purge_repository",
    "purge_organization",
    "purge_user",
    "verify_maintenance_principal",
]
