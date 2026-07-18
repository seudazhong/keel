"""Live-Postgres verification of migration 0018 (durable IM routing) up/down/up + invariants.

Exercises the real migration against a live database: upgrade to head creates the four IM
routing tables with the right RLS posture (scope/org tables FORCE RLS; the two global indices
do not), the composite ``(agent_id, org_id)`` FK rejects a cross-org Agent binding, the global
route/dispatch indices cascade from their owning rows, and ``purge_scope`` erases a scope's
reply outbox + route rows. Then downgrade one revision drops the tables and re-upgrade proves
reversibility.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.im_routing import (
    ImChannelMapping,
    ImChatKind,
    ImProvider,
    ImReplyIntent,
    ImReplyKind,
    ImReplyPolicy,
    PostgresImMappingStore,
    PostgresImProvisioner,
    PostgresImReplyDispatchIndex,
    PostgresImReplyStore,
    PostgresImRouteIndex,
    RouteConflictError,
    purge_scope,
    reply_idempotency_key,
    route_key,
)

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_HEAD = "0018_im_routing"
_PREV = "0017_web_routing_isolation"

_IM_TABLES = {
    "im_channel_mappings",
    "im_route_index",
    "im_reply_intents",
    "im_reply_dispatch_index",
}


def _alembic_cfg(url: str):  # type: ignore[no-untyped-def]
    from alembic.config import Config

    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


def _run_to(url: str, revision: str) -> None:
    from alembic import command

    cfg = _alembic_cfg(url)
    if revision == "head":
        command.upgrade(cfg, "head")
    else:
        command.downgrade(cfg, revision)


def _sync_url() -> str:
    raw = os.environ.get("KEEL_TEST_DATABASE_URL")
    if not raw:
        pytest.skip("KEEL_TEST_DATABASE_URL not set")
    return (
        raw.replace("+psycopg", "")
        .replace("+asyncpg", "")
        .replace("postgresql", "postgresql+psycopg", 1)
    )


async def _im_table_names(engine: AsyncEngine) -> set[str]:
    async with engine.begin() as conn:
        rows = (
            await conn.execute(text("SELECT tablename FROM pg_tables WHERE tablename LIKE 'im_%'"))
        ).all()
    return {r.tablename for r in rows}


async def _seed_org_agent(engine: AsyncEngine) -> tuple[str, str, str]:
    org_a, org_b, agent = "org-a", "org-b", "agent-1"
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text("INSERT INTO users(id,display_name,status) VALUES('u1','U','active')")
        )
        for org in (org_a, org_b):
            await conn.execute(
                text(
                    "INSERT INTO organizations(id,slug,display_name,status) "
                    "VALUES(:id,:slug,:id,'active')"
                ),
                {"id": org, "slug": org},
            )
        await conn.execute(
            text(
                "INSERT INTO agents(id,org_id,kind,owner_user_id,name,status,version) "
                "VALUES(:a,:o,'personal','u1','Support','active',1)"
            ),
            {"a": agent, "o": org_a},
        )
    return org_a, org_b, agent


def _mapping(org_id: str, agent_id: str) -> ImChannelMapping:
    return ImChannelMapping(
        id=uuid.uuid4().hex,
        org_id=org_id,
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="4242",
        chat_kind=ImChatKind.group,
        agent_id=agent_id,
        scope_id=f"agent:{org_id}/{agent_id}",
        policy=ImReplyPolicy(reply_enabled=True),
        created_by="u1",
    )


async def test_migration_0018_up_down_up_and_invariants(migrated_db: AsyncEngine) -> None:
    engine = migrated_db  # already at head (truncated clean slate)
    # 1) The four IM tables exist at head with the right FORCE-RLS posture.
    assert _IM_TABLES <= await _im_table_names(engine)
    async with engine.begin() as conn:
        force = {
            r.relname: r.relforcerowsecurity
            for r in (
                await conn.execute(
                    text(
                        "SELECT relname, relforcerowsecurity FROM pg_class "
                        "WHERE relname = ANY(:names)"
                    ),
                    {"names": list(_IM_TABLES)},
                )
            ).all()
        }
    assert force["im_channel_mappings"] is True
    assert force["im_reply_intents"] is True
    assert force["im_route_index"] is False  # global index — no RLS
    assert force["im_reply_dispatch_index"] is False

    org_a, org_b, agent = await _seed_org_agent(engine)

    # 2) A mapping binds an Agent in its own org; the route index round-trips.
    mstore = PostgresImMappingStore(engine, org_a)
    mapping = await mstore.create(_mapping(org_a, agent))
    route = PostgresImRouteIndex(engine)
    await route.put(mapping.route_entry())
    found = await route.lookup(route_key("telegram", "bot-9", "4242"))
    assert found is not None and found.mapping_id == mapping.id

    # 3) Cross-org Agent binding is rejected by the composite FK (no cross-org binding).
    with pytest.raises(Exception):  # noqa: B017,PT011 - IntegrityError from the composite FK
        await PostgresImMappingStore(engine, org_b).create(_mapping(org_b, agent))

    # 4) Deleting the mapping cascades its global route row (route invalid immediately).
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text("DELETE FROM im_channel_mappings WHERE id = :id"), {"id": mapping.id}
        )
    assert await route.lookup(route_key("telegram", "bot-9", "4242")) is None

    # 5) The reply outbox + global dispatch index round-trip and cascade + purge cleanly.
    scope = f"agent:{org_a}/{agent}"
    replies = PostgresImReplyStore(engine, scope)
    dispatch = PostgresImReplyDispatchIndex(engine)
    idem = reply_idempotency_key(
        run_id="run-1",
        provider="telegram",
        external_bot_id="bot-9",
        external_chat_id="4242",
        external_message_id="m7",
        kind="final",
    )
    intent = ImReplyIntent(
        id=uuid.uuid4().hex,
        scope_id=scope,
        run_id="run-1",
        org_id=org_a,
        provider=ImProvider.telegram,
        external_bot_id="bot-9",
        external_chat_id="4242",
        external_message_id="m7",
        chat_kind=ImChatKind.group,
        reply_kind=ImReplyKind.final,
        idempotency_key=idem,
        key_id="v1",
        ciphertext="ENC",
    )
    stored, created = await replies.record_intent(intent)
    assert created is True
    await dispatch.record(stored.id, scope)
    assert scope in await dispatch.active_scopes()
    # Deleting the reply intent cascades its global dispatch pointer.
    removed = await purge_scope(engine, scope)
    assert removed >= 1
    assert await dispatch.active_scopes() == set()

    # 6) Reversibility: downgrade drops the tables, re-upgrade restores them.
    url = _sync_url()
    await asyncio.to_thread(_run_to, url, _PREV)
    assert _IM_TABLES & await _im_table_names(engine) == set()
    await asyncio.to_thread(_run_to, url, "head")
    assert _IM_TABLES <= await _im_table_names(engine)


async def _seed_two_orgs_two_agents(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        await conn.execute(
            text("INSERT INTO users(id,display_name,status) VALUES('u1','U','active')")
        )
        for org in ("org-a", "org-b"):
            await conn.execute(
                text(
                    "INSERT INTO organizations(id,slug,display_name,status) "
                    "VALUES(:id,:slug,:id,'active')"
                ),
                {"id": org, "slug": org},
            )
            await conn.execute(
                text(
                    "INSERT INTO agents(id,org_id,kind,owner_user_id,name,status,version) "
                    "VALUES(:a,:o,'personal','u1','Support','active',1)"
                ),
                {"a": f"agent-{org}", "o": org},
            )


async def test_provisioner_claim_is_single_winner_and_idempotent(migrated_db: AsyncEngine) -> None:
    """The transactional provisioner enforces one owner per route; the loser leaves no orphan."""
    engine = migrated_db
    await _seed_two_orgs_two_agents(engine)
    provisioner = PostgresImProvisioner(engine)
    route = PostgresImRouteIndex(engine)

    created = await provisioner.provision(_mapping("org-a", "agent-org-a"))
    key = route_key("telegram", "bot-9", "4242")
    entry = await route.lookup(key)
    assert entry is not None and entry.org_id == "org-a" and entry.mapping_id == created.id

    # A competing claim by another org for the same chat fails closed and rolls its mapping back.
    with pytest.raises(RouteConflictError):
        await provisioner.provision(_mapping("org-b", "agent-org-b"))
    # The existing owner's route is untouched; the loser left no orphan mapping row.
    still = await route.lookup(key)
    assert still is not None and still.org_id == "org-a" and still.mapping_id == created.id
    async with engine.begin() as conn:
        await conn.execute(text("SET row_security = off"))
        orphans = await conn.scalar(
            text("SELECT count(*) FROM im_channel_mappings WHERE org_id = 'org-b'")
        )
    assert orphans == 0

    # Re-claiming the *same* mapping is idempotent (no conflict, refreshes the opaque row).
    await route.claim(created.route_entry())
    assert (await route.lookup(key)).mapping_id == created.id  # type: ignore[union-attr]
