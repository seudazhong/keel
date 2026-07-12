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

    # Ordered fallback models (comma-separated LiteLLM ids) tried when a provider call
    # fails before emitting any output (B2 failover/routing). Empty -> no failover.
    fallback_models: str = ""

    # Embeddings for archival memory / RAG (ADR-0007). Empty embedding_model -> archival off
    # (core memory still works, it is embedding-free).
    embedding_model: str = "ollama/bge-m3"
    embedding_dim: int = 1024
    embedding_send_dimensions: bool = False  # OpenAI text-embedding-3 needs it; Ollama rejects it
    embedding_timeout_seconds: float = 10.0  # hard deadline for each provider embed call
    memory_block_max_chars: int = 2000
    session_embedding_batch_size: int = 64
    session_embedding_catchup_limit: int = 500

    # Consolidation settings (memory compression & archival).
    consolidation_min_messages: int = 10
    consolidation_batch_messages: int = 50
    consolidation_input_max_chars: int = 20_000
    consolidation_message_max_chars: int = 4_000
    consolidation_archival_min_confidence: float = 0.8
    consolidation_lease_seconds: int = 600
    consolidation_token_budget: int = 4_000

    # RBAC (B3): comma-separated ``key:role`` pairs (roles: viewer|operator|admin).
    # Empty -> open single-user mode (every request is an implicit admin).
    api_keys: str = ""

    # Durable event store backend: "postgres" (default) or "memory". The in-memory
    # store is a single-process "lite" profile — and the way to run the server on a
    # Windows host, where uvicorn's Proactor loop can't drive psycopg's async driver.
    event_store: str = "postgres"

    # Envelope-encryption key for connector OAuth tokens at rest (G18). Any string;
    # a Fernet key is derived from it. Empty -> the token store fails closed.
    secret_key: str = ""

    # Langfuse tracing (WS-H). Tracing is enabled only when both keys are set;
    # otherwise a no-op tracer is used (zero overhead, no network).
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    langfuse_host: str = "https://cloud.langfuse.com"

    # OneBot (QQ) IM gateway (WS-E/J). Empty api_base -> the gateway is disabled.
    onebot_api_base: str = ""
    onebot_access_token: str = ""
    onebot_self_id: int = 0
    im_rate_limit: int = 5  # max messages per chat per minute

    # Telegram IM gateway (WS-E/J). Empty bot token -> the gateway is disabled. The
    # username (without '@') is used for group @-mention wake detection.
    telegram_bot_token: str = ""
    telegram_bot_username: str = ""

    # psycopg3 driver works for both sync (Alembic) and async (app) engines.
    database_url: str = "postgresql+psycopg://keel:keel@localhost:5432/keel"
    redis_url: str = "redis://localhost:6379/0"

    server_host: str = "0.0.0.0"
    server_port: int = 8000

    # Autonomy (M2 slice): scheduler tick cadence + how long an unattended approval
    # stays open before it fails closed (invariant: fail-closed on timeout).
    scheduler_tick_seconds: int = 30
    approval_timeout_hours: int = 24

    # Gmail read connector (WS-G, real inbox). Disabled -> the digest uses the fake
    # in-memory inbox (default/test path). When enabled the worker loads the OAuth
    # refresh token from the connector token store (scope-bound, encrypted).
    gmail_enabled: bool = False
    gmail_client_secrets_path: str = ".secrets/gmail_client.json"
    gmail_max_messages: int = 5
    # Real outbound send (gmail.send). Off by default even when reading is enabled, so
    # the safe posture is "read real mail, send is mocked + approval-gated". Turning
    # this on requires re-authorizing (the send scope) via scripts/gmail_authorize.py.
    gmail_send_enabled: bool = False

    @property
    def sync_database_url(self) -> str:
        """SQLAlchemy URL for the synchronous engine (Alembic uses this)."""
        return self.database_url

    @property
    def fallback_model_list(self) -> list[str]:
        """Parsed, de-blanked ``fallback_models`` (order preserved)."""
        return [m.strip() for m in self.fallback_models.split(",") if m.strip()]


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
