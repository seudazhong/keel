"""Postgres schema, RLS, repository, replay, and secret-metadata checks."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
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
    ConnectorHealth,
    ConnectorHealthStatus,
    ConnectorIngressFailure,
    ConnectorItemDraft,
    ConnectorRenewalPolicy,
    ConnectorResourceDraft,
    ConnectorTargetKind,
)
from keel_core.connector_credentials import ConnectorCredentialStore, CredentialEnvelope
from keel_core.connector_repository import (
    ConnectorDeliveryClaimLostError,
    PostgresConnectorRepository,
)
from keel_core.secrets import EnvelopeCipher
from keel_core.tokens import PostgresTokenStore

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
        assert await conn.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns "
                "WHERE table_name = 'connector_tokens' AND column_name = 'version')"
            )
        )
        schedule_columns = (
            await conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'connector_bindings' "
                    "AND column_name = ANY(:columns)"
                ),
                {
                    "columns": [
                        "next_sync_at",
                        "next_renewal_at",
                        "schedule_lease_token",
                        "schedule_lease_expires_at",
                    ]
                },
            )
        ).scalars()
        assert set(schedule_columns) == {
            "next_sync_at",
            "next_renewal_at",
            "schedule_lease_token",
            "schedule_lease_expires_at",
        }
        delivery_columns = (
            await conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'connector_deliveries' "
                    "AND column_name = ANY(:columns)"
                ),
                {
                    "columns": [
                        "claim_token",
                        "error_code",
                        "error_summary",
                        "error_retryable",
                    ]
                },
            )
        ).scalars()
        assert set(delivery_columns) == {
            "claim_token",
            "error_code",
            "error_summary",
            "error_retryable",
        }
        head = await conn.scalar(text("SELECT version_num FROM alembic_version"))
        assert head == "0016_connector_foundation"


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
    binding_b = await repo_b.upsert_binding(
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
    first = await repo_a.claim_delivery("fixture", binding.id, "delivery-1", "a" * 64)
    assert first is not None
    assert await repo_a.claim_delivery("fixture", binding.id, "delivery-1", "a" * 64) is None
    with pytest.raises(ValueError, match="different payload"):
        await repo_a.claim_delivery("fixture", binding.id, "delivery-1", "b" * 64)
    await repo_a.finish_delivery(
        first,
        failure=ConnectorIngressFailure("temporary", "retry", retryable=True),
    )
    async with migrated_db.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"),
            {"scope": scope_a},
        )
        persisted_failure = (
            await conn.execute(
                text(
                    "SELECT status, claim_token, error_code, error_summary, error_retryable "
                    "FROM connector_deliveries WHERE scope_id = :scope "
                    "AND connector_id = 'fixture' AND delivery_id = 'delivery-1'"
                ),
                {"scope": scope_a},
            )
        ).one()
    assert (
        persisted_failure.status,
        persisted_failure.claim_token,
        persisted_failure.error_code,
        persisted_failure.error_summary,
        persisted_failure.error_retryable,
    ) == ("failed", first.token, "temporary", "retry", True)
    failed_health = await repo_a.get_delivery_health("fixture", binding.id)
    assert failed_health is not None
    assert (failed_health.unresolved_count, failed_health.summary) == (1, "retry")
    retry = await repo_a.claim_delivery("fixture", binding.id, "delivery-1", "a" * 64)
    assert retry is not None and retry.token != first.token
    with pytest.raises(ConnectorDeliveryClaimLostError, match="claim was lost"):
        await repo_a.finish_delivery(first)
    preserved_health = await repo_a.get_delivery_health("fixture", binding.id)
    assert preserved_health is not None and preserved_health.summary == "retry"
    await repo_a.finish_delivery(retry)
    assert await repo_a.get_delivery_health("fixture", binding.id) is None
    async with migrated_db.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"),
            {"scope": scope_a},
        )
        persisted_success = (
            await conn.execute(
                text(
                    "SELECT status, claim_token, error_code, error_summary, error_retryable "
                    "FROM connector_deliveries WHERE scope_id = :scope "
                    "AND connector_id = 'fixture' AND delivery_id = 'delivery-1'"
                ),
                {"scope": scope_a},
            )
        ).one()
    assert (
        persisted_success.status,
        persisted_success.claim_token,
        persisted_success.error_code,
        persisted_success.error_summary,
        persisted_success.error_retryable,
    ) == ("processed", retry.token, None, None, None)

    stale = await repo_a.claim_delivery("fixture", binding.id, "delivery-stale", "c" * 64)
    assert stale is not None
    async with migrated_db.begin() as conn:
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, true)"),
            {"scope": scope_a},
        )
        await conn.execute(
            text(
                "UPDATE connector_deliveries SET updated_at = now() - interval '6 minutes' "
                "WHERE scope_id = :scope AND connector_id = 'fixture' "
                "AND delivery_id = 'delivery-stale'"
            ),
            {"scope": scope_a},
        )
    stale_retry = await repo_a.claim_delivery("fixture", binding.id, "delivery-stale", "c" * 64)
    assert stale_retry is not None and stale_retry.token != stale.token
    await repo_a.finish_delivery(stale_retry)

    other_scope_claim = await repo_b.claim_delivery(
        "fixture",
        binding_b.id,
        "delivery-1",
        "d" * 64,
    )
    assert other_scope_claim is not None
    with pytest.raises(ConnectorDeliveryClaimLostError, match="claim was lost"):
        await repo_a.finish_delivery(other_scope_claim)
    await repo_b.finish_delivery(other_scope_claim)

    async with migrated_db.connect() as conn:
        await conn.execute(text("SET ROLE keel_runtime"))
        await conn.execute(
            text("SELECT set_config('app.scope_id', :scope, false)"),
            {"scope": scope_a},
        )
        rows = (await conn.execute(text("SELECT scope_id FROM connector_bindings"))).all()
        assert {row.scope_id for row in rows} == {scope_a}
        delivery_rows = (
            await conn.execute(text("SELECT scope_id FROM connector_deliveries"))
        ).all()
        assert {row.scope_id for row in delivery_rows} == {scope_a}
        await conn.execute(text("RESET app.scope_id"))
        assert (await conn.execute(text("SELECT scope_id FROM connector_bindings"))).all() == []
        assert (await conn.execute(text("SELECT scope_id FROM connector_deliveries"))).all() == []
        await conn.execute(text("RESET ROLE"))


async def test_delivery_binding_fence_survives_replacement(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"connector:replacement:{uuid.uuid4().hex}"
    repository = PostgresConnectorRepository(migrated_db, scope)
    original = await repository.upsert_binding(
        "fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    claim = await repository.claim_delivery("fixture", original.id, "delivery", "a" * 64)
    assert claim is not None
    await repository.delete_connector("fixture")
    replacement = await repository.upsert_binding(
        "fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    assert replacement.id != original.id
    with pytest.raises(ConnectorDeliveryClaimLostError, match="claim was lost"):
        await repository.finish_delivery(
            claim,
            failure=ConnectorIngressFailure("late", "Late worker failure", True),
        )
    assert await repository.get_delivery_health("fixture", replacement.id) is None
    assert (
        await repository.record_health(
            "fixture",
            original.id,
            ConnectorHealth(
                ConnectorHealthStatus.error,
                datetime.now(UTC),
                "old binding health",
            ),
        )
        is None
    )
    current = await repository.get_binding("fixture")
    assert current is not None and current.status is ConnectorBindingStatus.connected


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


async def test_repository_pruning_cascades_and_is_scope_isolated(
    migrated_db: AsyncEngine,
) -> None:
    scope_a = f"connector:prune:a:{uuid.uuid4().hex}"
    scope_b = f"connector:prune:b:{uuid.uuid4().hex}"
    repo_a = PostgresConnectorRepository(migrated_db, scope_a)
    repo_b = PostgresConnectorRepository(migrated_db, scope_b)
    binding_a = await repo_a.upsert_binding(
        "fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    binding_b = await repo_b.upsert_binding(
        "fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    resource_a = (
        await repo_a.upsert_resources(
            "fixture",
            binding_a.id,
            (ConnectorResourceDraft("shared", "repository", "A", selected=True),),
        )
    )[0]
    resource_b = (
        await repo_b.upsert_resources(
            "fixture",
            binding_b.id,
            (ConnectorResourceDraft("shared", "repository", "B", selected=True),),
        )
    )[0]
    item_a = (
        await repo_a.upsert_items(
            "fixture",
            binding_a.id,
            (ConnectorItemDraft("item", "document", "A", resource_id=resource_a.id),),
        )
    )[0]
    await repo_b.upsert_items(
        "fixture",
        binding_b.id,
        (ConnectorItemDraft("item", "document", "B", resource_id=resource_b.id),),
    )
    await repo_a.put_cursor("fixture", binding_a.id, "documents", "a", resource_id=resource_a.id)
    await repo_b.put_cursor("fixture", binding_b.id, "documents", "b", resource_id=resource_b.id)

    assert await repo_a.prune_resources("fixture", binding_a.id, set()) == 1
    assert await repo_a.list_resources("fixture") == []
    assert await repo_a.list_items("fixture") == []
    assert await repo_a.list_cursors("fixture", binding_a.id) == []
    assert [item.external_id for item in await repo_b.list_resources("fixture")] == ["shared"]
    assert [item.external_id for item in await repo_b.list_items("fixture")] == ["item"]
    assert len(await repo_b.list_cursors("fixture", binding_b.id)) == 1

    recreated = (
        await repo_a.upsert_items(
            "fixture",
            binding_a.id,
            (ConnectorItemDraft("item", "document", "Recreated"),),
        )
    )[0]
    assert recreated.id != item_a.id
    assert await repo_a.delete_item("fixture", binding_a.id, "item")
    assert not await repo_a.delete_item("fixture", binding_b.id, "item")


async def test_postgres_credential_updates_are_versioned_compare_and_set(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"connector:credential:{uuid.uuid4().hex}"
    store = ConnectorCredentialStore(PostgresTokenStore(migrated_db, scope, EnvelopeCipher("key")))
    await store.put("fixture", CredentialEnvelope("oauth", {"access_token": "old"}))
    initial = await store.get_versioned("fixture")
    assert initial is not None and initial.version == 1
    assert (
        await store.put_if_version(
            "fixture",
            CredentialEnvelope("oauth", {"access_token": "stale"}),
            0,
        )
        is None
    )
    assert (
        await store.put_if_version(
            "fixture",
            CredentialEnvelope("oauth", {"access_token": "rotated"}),
            1,
        )
        == 2
    )
    updated = await store.get_versioned("fixture")
    assert updated is not None
    assert updated.version == 2
    assert updated.envelope.values["access_token"] == "rotated"


async def test_postgres_recurring_schedule_claim_reclaims_and_fences(
    migrated_db: AsyncEngine,
) -> None:
    scope = f"connector:schedule:{uuid.uuid4().hex}"
    first_worker = PostgresConnectorRepository(migrated_db, scope)
    second_worker = PostgresConnectorRepository(migrated_db, scope)
    binding = await first_worker.upsert_binding(
        "fixture",
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
        sync_cadence_seconds=30,
        renewal=ConnectorRenewalPolicy(60),
        renewal_expires_at=datetime.now(UTC) + timedelta(minutes=10),
    )
    assert binding.next_sync_at is not None
    due = binding.next_sync_at
    first = (await first_worker.claim_due_schedules(due, limit=1, lease_seconds=10))[0]
    assert (
        await second_worker.claim_due_schedules(
            due + timedelta(seconds=5),
            limit=1,
            lease_seconds=10,
        )
        == []
    )
    reclaimed = (
        await second_worker.claim_due_schedules(
            due + timedelta(seconds=11),
            limit=1,
            lease_seconds=10,
        )
    )[0]
    with pytest.raises(RuntimeError, match="lease was lost"):
        await first_worker.complete_schedule(
            first,
            next_at=due + timedelta(seconds=30),
        )
    await second_worker.complete_schedule(
        reclaimed,
        next_at=due + timedelta(seconds=30),
    )
    updated = await first_worker.get_binding("fixture")
    assert updated is not None
    assert updated.next_sync_at == due + timedelta(seconds=30)
    assert updated.sync_failures == 0


async def test_connector_migration_downgrade_and_upgrade(migrated_db: AsyncEngine) -> None:
    url = os.environ["KEEL_TEST_DATABASE_URL"]
    cfg = Config(str(_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    await asyncio.to_thread(command.downgrade, cfg, "0015_projects_github")
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
