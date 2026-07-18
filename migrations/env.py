"""Alembic environment — reads the DB URL from keel_core config.

M0 baseline enables required Postgres extensions. ORM metadata (``target_metadata``)
is wired when the event/state schema lands in M1 (WS-D).
"""

from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

from keel_core.config import get_settings

config = context.config
if not config.get_main_option("sqlalchemy.url"):
    # Alembic must connect as the privileged owner/migrator, never the non-owner runtime login.
    # ``require_migration_database_url`` enforces the M3A split: it uses KEEL_MIGRATION_DATABASE_URL
    # when set, falls back to KEEL_DATABASE_URL only outside cloud mode (the local single-owner
    # profile), and fails closed in cloud mode when unset — rather than silently running DDL /
    # CREATE ROLE on the runtime login (which would only fail later with a confusing permission
    # error). The test harness sets ``sqlalchemy.url`` explicitly, so this branch is CLI-only.
    config.set_main_option("sqlalchemy.url", get_settings().require_migration_database_url())

target_metadata = None


def run_migrations_offline() -> None:
    """Emit SQL without a live connection."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
