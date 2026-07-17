"""Postgres schema, RLS, repository, replay, and secret-metadata checks."""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.connector_contracts import (
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorItemDraft,
    ConnectorResourceDraft,
    ConnectorTargetKind,
)
from keel_core.connector_repository import PostgresConnectorRepository

pytestmark = pytest.mark.integration
_ROOT = Path(__file__).resolve().parents[2]


async def test_connector_foundation_schema_and_rls(migrated_db: AsyncEngine) -> None:
    tables = {
        "connector_bindings",
        "connector_binding_targets",
        "connector_resources",
        "connector_items",
        "connector_cursors",
        "connector_deliveries",
    }
    async with migrated_db.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT relname, relrowsecurity, relforcerowsecurity FROM pg_class "
                    "WHERE relname = ANY(:tables)"
                ),
                {"tables": sorted(tables)},
            )
        ).all()
        assert {row.relname for row in rows} == tables
        assert all(row.relrowsecurity and row.relforcerowsecurity for row in rows)
        head = await conn.scalar(text("SELECT version_num FROM alembic_version"))
        assert head == "0015_connector_foundation"


async def test_repository_scope_isolation_cursor_and_delivery_replay(
    migrated_db: AsyncEngine,
) -> None:
    scope_a = f"connector:a:{uuid.uuid4().hex}"
    scope_b = f"connector:b:{uuid.uuid4().hex}"
    repo_a = PostgresConnectorRepository(migrated_db, scope_a)
    repo_b = PostgresConnectorRepository(migrated_db, scope_b)
    binding = await repo_a.upsert_binding(
        "fixture",
        ConnectorBindingDraft(display_name="Fixture"),
        ConnectorBindingStatus.connected,
    )
    await repo_b.upsert_binding(
        "fixture",
        ConnectorBindingDraft(display_name="Other scope"),
        ConnectorBindingStatus.connected,
    )
    resources = await repo_a.upsert_resources(
        "fixture",
        binding.id,
        (ConnectorResourceDraft("repo-1", "repository", "Repo", selected=True),),
    )
    targets = await repo_a.replace_targets(
        "fixture",
        binding.id,
        {ConnectorTargetKind.knowledge: "kb_00000000000000000000000000000000"},
    )
    assert targets[0].kind is ConnectorTargetKind.knowledge
    items = await repo_a.upsert_items(
        "fixture",
        binding.id,
        (
            ConnectorItemDraft(
                "doc-1",
                "document",
                "Document",
                resource_id=resources[0].id,
                destination_kind=ConnectorTargetKind.knowledge,
                destination_target_id="kb_00000000000000000000000000000000",
                destination_id="doc_00000000000000000000000000000000",
            ),
        ),
    )
    assert items[0].resource_id == resources[0].id
    await repo_a.put_cursor("fixture", binding.id, "documents", "next", resource_id=resources[0].id)
    await repo_a.put_cursor("fixture", binding.id, "global", "global-next")
    cursor = await repo_a.get_cursor(
        "fixture", binding.id, "documents", resource_id=resources[0].id
    )
    assert cursor is not None and cursor.value == "next"
    assert len(await repo_a.list_cursors("fixture", binding.id)) == 2
    assert (await repo_a.list_bindings())[0].display_name == "Fixture"
    assert (await repo_b.get_binding("fixture")).display_name == "Other scope"  # type: ignore[union-attr]
    assert await repo_a.claim_delivery("fixture", binding.id, "delivery-1", "a" * 64)
    assert not await repo_a.claim_delivery("fixture", binding.id, "delivery-1", "a" * 64)
    with pytest.raises(ValueError, match="different payload"):
        await repo_a.claim_delivery("fixture", binding.id, "delivery-1", "b" * 64)
    await repo_a.finish_delivery(
        "fixture", "delivery-1", error_code="temporary", error_summary="retry"
    )
    assert await repo_a.claim_delivery("fixture", binding.id, "delivery-1", "a" * 64)
    await repo_a.finish_delivery("fixture", "delivery-1")

    async with migrated_db.connect() as conn:
        await conn.execute(text("SET ROLE keel_runtime"))
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, false)"),
            {"scope": scope_a},
        )
        rows = (await conn.execute(text("SELECT scope_id FROM connector_bindings"))).all()
        assert {row.scope_id for row in rows} == {scope_a}
        await conn.execute(text("RESET app.scope_id"))
        assert (await conn.execute(text("SELECT scope_id FROM connector_bindings"))).all() == []
        await conn.execute(text("RESET ROLE"))


async def test_database_rejects_top_level_plaintext_secret_metadata(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"connector:secret:{uuid.uuid4().hex}"
    with pytest.raises(IntegrityError):
        async with migrated_db.begin() as conn:
            await conn.execute(
                text("SELECT set_config('app.scope_id', :scope, true)"),
                {"scope": scope},
            )
            await conn.execute(
                text(
                    "INSERT INTO connector_bindings "
                    "(id, scope_id, connector_id, status, metadata) "
                    "VALUES ('bad', :scope, 'fixture', 'connected', "
                    '\'{"access_token":"plaintext"}\'::jsonb)'
                ),
                {"scope": scope},
            )


async def test_connector_migration_downgrade_and_upgrade(migrated_db: AsyncEngine) -> None:
    url = os.environ["KEEL_TEST_DATABASE_URL"]
    cfg = Config(str(_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    await asyncio.to_thread(command.downgrade, cfg, "0014_durable_runs")
    async with migrated_db.connect() as conn:
        assert not await conn.scalar(
            text("SELECT to_regclass('public.connector_bindings') IS NOT NULL")
        )
    scope = f"connector:migrate:{uuid.uuid4().hex}"
    async with migrated_db.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"),
            {"scope": scope},
        )
        await conn.execute(
            text(
                "INSERT INTO connector_tokens "
                "(scope_id, connector_id, ciphertext, key_id) "
                "VALUES (:scope, 'gmail', 'encrypted', 'v1')"
            ),
            {"scope": scope},
        )
    await asyncio.to_thread(command.upgrade, cfg, "head")
    async with migrated_db.connect() as conn:
        assert await conn.scalar(
            text("SELECT to_regclass('public.connector_bindings') IS NOT NULL")
        )
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"),
            {"scope": scope},
        )
        binding = (
            await conn.execute(
                text(
                    "SELECT status FROM connector_bindings "
                    "WHERE scope_id = :scope AND connector_id = 'gmail'"
                ),
                {"scope": scope},
            )
        ).one()
        assert binding.status == "connected"
