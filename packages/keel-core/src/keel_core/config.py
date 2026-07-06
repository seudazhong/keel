"""Layered application configuration (Pydantic Settings).

Precedence (low -> high): field defaults -> ``.env`` file -> process env
(``KEEL_`` prefix) -> explicit overrides passed to ``Settings(...)``.
A mounted-config-file source (deploy/config/) is a documented M1 extension point.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process-wide settings shared by every Keel service."""

    model_config = SettingsConfigDict(
        env_prefix="KEEL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = "dev"
    log_level: str = "INFO"

    # Default model surfaces use when none is given (LiteLLM model id).
    default_model: str = "gpt-4o-mini"

    # psycopg3 driver works for both sync (Alembic) and async (app) engines.
    database_url: str = "postgresql+psycopg://keel:keel@localhost:5432/keel"
    redis_url: str = "redis://localhost:6379/0"

    server_host: str = "0.0.0.0"
    server_port: int = 8000

    @property
    def sync_database_url(self) -> str:
        """SQLAlchemy URL for the synchronous engine (Alembic uses this)."""
        return self.database_url


@lru_cache
def get_settings() -> Settings:
    """Return a cached :class:`Settings` instance."""
    return Settings()
