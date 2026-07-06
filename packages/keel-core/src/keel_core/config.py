"""Layered application configuration (Pydantic Settings).

Precedence (low -> high): field defaults -> ``.env`` file -> process env
(``KEEL_`` prefix) -> explicit overrides passed to ``Settings(...)``.
A mounted-config-file source (deploy/config/) is a documented M1 extension point.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

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


def load_env_file(path: str | Path | None = None) -> str | None:
    """Load a ``.env`` file into ``os.environ`` and return the path used (or None).

    :class:`Settings` already reads ``.env`` for ``KEEL_``-prefixed values, but
    third-party libraries (LiteLLM) read provider API keys such as
    ``OPENAI_API_KEY`` straight from the process environment. This makes one
    ``.env`` carry both: it injects every key/value into ``os.environ`` with
    ``override=False`` so a real environment variable always wins.

    Call this from a **process entry point** (CLI ``main``, a service ``startup``)
    — never at import time, so tests and library importers don't inherit a
    developer's local secrets. When ``path`` is omitted, the nearest ``.env`` at
    or above the current working directory is used.
    """
    try:
        from dotenv import find_dotenv, load_dotenv
    except ImportError:  # pragma: no cover - python-dotenv ships with pydantic-settings
        return None
    target = str(path) if path is not None else find_dotenv(usecwd=True)
    if not target or not Path(target).is_file():
        return None
    load_dotenv(target, override=False)
    return target
