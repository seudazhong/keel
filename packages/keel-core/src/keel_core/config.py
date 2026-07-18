"""Layered application configuration (Pydantic Settings).

Precedence (low -> high): field defaults -> ``.env`` file -> process env
(``KEEL_`` prefix) -> explicit overrides passed to ``Settings(...)``.
A mounted-config-file source (deploy/config/) is a documented M1 extension point.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from keel_core.errors import MaintenanceDatabaseNotConfigured, MigrationDatabaseNotConfigured

if TYPE_CHECKING:
    from keel_core.review.pricing import PriceBook as ReviewPriceBook


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

    # Scope-bound Knowledge Base ingestion and retrieval limits.
    knowledge_document_max_bytes: int = Field(default=1_048_576, gt=0)
    knowledge_title_max_chars: int = Field(default=300, gt=0)
    knowledge_description_max_chars: int = Field(default=2_000, gt=0)
    knowledge_search_query_max_chars: int = Field(default=2_000, gt=0)
    knowledge_search_k_max: int = Field(default=10, gt=0, le=10)
    knowledge_chunk_target_chars: int = Field(default=1_600, gt=0)
    knowledge_chunk_overlap_chars: int = Field(default=200, gt=0)
    knowledge_embedding_batch_size: int = Field(default=32, gt=0)
    knowledge_tool_output_max_chars: int = Field(default=8_000, gt=0)

    # Consolidation settings (memory compression & archival).
    consolidation_min_messages: int = 10
    consolidation_batch_messages: int = 50
    consolidation_input_max_chars: int = 20_000
    consolidation_message_max_chars: int = 4_000
    consolidation_archival_min_confidence: float = 0.8
    consolidation_lease_seconds: int = 600
    consolidation_token_budget: int = 4_000
    # Semantic retry dedupe: merge a rephrased archival write into an existing
    # consolidation row only when it cites the *exact same* source events and its
    # embedding is within this cosine distance (calibrated from live replay, max
    # observed 0.0468). 0 disables the semantic pass; exact content-hash dedupe stays.
    consolidation_semantic_dedupe_distance: float = 0.05

    # Durable background jobs (ADR-0010).
    job_lease_seconds: int = Field(default=300, gt=0)
    job_execution_timeout_seconds: int = Field(default=3600, gt=0)
    job_dispatch_limit: int = Field(default=100, gt=0, le=100)
    job_retry_base_seconds: int = Field(default=5, gt=0)
    job_retry_max_seconds: int = Field(default=300, gt=0)
    job_payload_max_bytes: int = Field(default=65_536, gt=0)
    job_result_max_bytes: int = Field(default=65_536, gt=0)
    job_progress_message_max_chars: int = Field(default=1_000, gt=0)
    job_result_message_max_chars: int = Field(default=8_000, gt=0)
    job_error_message_max_chars: int = Field(default=2_000, gt=0)
    connector_schedule_batch_size: int = Field(default=50, gt=0, le=100)
    connector_schedule_lease_seconds: int = Field(default=120, gt=0)

    # RBAC (B3): comma-separated ``key:role`` pairs (roles: viewer|operator|admin).
    # Keys are hashed at rest (never compared in plaintext) and verified in constant
    # time. Empty -> open single-user mode, allowed ONLY when ``cloud_mode`` is off
    # (local/self-hosted posture). See ``cloud_mode`` below.
    api_keys: str = ""

    # Cloud-safety posture (M3.3). When true the server fails **closed**: an empty
    # ``api_keys`` is a hard misconfiguration (every request is rejected) instead of the
    # implicit-admin open mode. Leave false only for local/self-hosted single-user use.
    cloud_mode: bool = False

    # Legacy API-key migration mapping (M3.6 review finding 5). A pre-identity API key of the
    # bare ``key:role`` form (no ``org=``/``agent=`` binding) carries no tenant, so in cloud
    # mode it fails closed — there is no ambient data-plane scope. To let an existing cloud
    # deployment keep working while operators migrate those keys to scoped ``key:role:org=..:
    # agent=..`` credentials, set BOTH of these to a real org id/slug and Agent id: every
    # unbound, non-global machine credential then binds to exactly that one org+Agent (never an
    # ambient scope, and a spoofed ``X-Keel-Org``/``X-Keel-Agent`` that selects a different
    # tenant is still rejected). This is an explicit, cloud-only migration aid: leave both empty
    # (the default) to keep bare keys failing closed. Setting only one is a hard misconfiguration
    # (see ``_validate_legacy_machine_binding``): the mapping must be a complete org+Agent pair.
    legacy_machine_org_id: str = ""
    legacy_machine_agent_id: str = ""

    # OIDC identity (M3.6). Human users authenticate with a provider-issued bearer JWT that
    # the server verifies against the issuer's JWKS. Disabled -> only the API-key/local
    # actor paths are available (identity APIs then require the local single-operator user
    # outside cloud mode). ``oidc_audience`` accepts a comma-separated allow-list.
    oidc_enabled: bool = False
    oidc_issuer: str = ""
    oidc_audience: str = ""
    oidc_jwks_uri: str = ""
    oidc_algorithms: str = ""  # comma-separated; empty -> safe asymmetric defaults
    # Expected authorized party (``azp``). When a token carries multiple audiences its
    # ``azp`` MUST equal this client id (multi-audience confused-deputy defense). Empty ->
    # derived from ``oidc_audience`` when exactly one audience is configured.
    oidc_client_id: str = ""
    oidc_leeway_seconds: int = Field(default=60, ge=0)
    oidc_jwks_cache_ttl_seconds: int = Field(default=3600, gt=0)
    # Minimum interval between attacker-driven (unknown-kid) JWKS refreshes. Bounds outbound
    # JWKS requests so a flood of tokens with bogus ``kid``s cannot amplify into a matching
    # flood of network calls (unknown-kid amplification defense).
    oidc_jwks_min_refresh_interval_seconds: float = Field(default=60.0, ge=0)
    # Cooldown after a failed / degenerate JWKS fetch during which further fetches are
    # suppressed (negative provider cache). Bounds retry amplification against a flapping or
    # down provider; the provider recovers automatically once the cooldown elapses.
    oidc_jwks_failure_cooldown_seconds: float = Field(default=30.0, ge=0)
    # Just-in-time user provisioning: a first-seen verified subject is auto-provisioned a
    # durable user. Off by default (fail closed): an unlinked subject must be linked
    # explicitly before it can act.
    identity_allow_jit_provisioning: bool = False

    # Durable event store backend: "postgres" (default) or "memory". The in-memory
    # store is a single-process "lite" profile — and the way to run the server on a
    # Windows host, where uvicorn's Proactor loop can't drive psycopg's async driver.
    event_store: str = "postgres"

    # Envelope-encryption key for connector OAuth tokens at rest (G18). Any string;
    # a Fernet key is derived from it. Empty -> the token store fails closed. This is
    # the legacy single-key form; it is registered under key id ``v1``.
    secret_key: str = ""

    # Versioned envelope keys for rotation (M3.3). Comma-separated ``key_id:secret``
    # pairs; every id that has ever encrypted a stored token MUST stay listed so old
    # ciphertext still decrypts. ``secret_key_active_id`` selects which id encrypts new
    # writes (default ``v1``; falls back to the sole configured id when unset). When
    # empty the legacy ``secret_key`` is used as key id ``v1``.
    secret_keys: str = ""
    secret_key_active_id: str = ""

    # Durable OAuth CSRF ``state`` lifetime (M3.3). A connect ``state`` is single-use
    # and expires after this many seconds; the callback rejects unknown/expired states.
    oauth_state_ttl_seconds: int = Field(default=600, gt=0)

    # Durable webhook replay-protection window (M3.3): how long a seen delivery id is
    # remembered so a replayed IM webhook is dropped. Also the outbound-idempotency and
    # oauth-state sweeper horizon.
    webhook_replay_ttl_seconds: int = Field(default=86_400, gt=0)

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
    # Inbound webhook signing secret (M3.3). When set, ``POST /v1/gateway/onebot``
    # requires a valid ``X-Signature: sha1=<hmac>`` over the raw body; when
    # ``cloud_mode`` is on it is REQUIRED (unsigned requests are rejected).
    onebot_signing_secret: str = ""

    # Telegram IM gateway (WS-E/J). Empty bot token -> the gateway is disabled. The
    # username (without '@') is used for group @-mention wake detection.
    telegram_bot_token: str = ""
    telegram_bot_username: str = ""
    # Inbound webhook secret (M3.3): compared to Telegram's
    # ``X-Telegram-Bot-Api-Secret-Token`` header in constant time. Required when
    # ``cloud_mode`` is on.
    telegram_webhook_secret: str = ""

    # GitHub App (M3.7, WS-P). Managed-project GitHub synchronization. The integration is
    # disabled unless ``github_app_id`` is set. The private key is supplied by *reference*
    # (a filesystem path or an env var name resolved out-of-band, never the PEM inline in
    # this settings object) so the raw key material is not held in a broadly-shared config;
    # ``github_webhook_secret`` verifies inbound ``X-Hub-Signature-256`` over the raw body.
    # ``github_allowed_hosts`` is a comma-separated host allowlist for clone/API URLs
    # (SSRF defense); loopback/private/reserved addresses are always rejected.
    github_app_id: int = Field(default=0, ge=0)
    github_client_id: str = ""
    github_private_key_ref: str = ""
    github_webhook_secret: SecretStr = SecretStr("")
    github_api_base_url: str = "https://api.github.com"
    github_web_base_url: str = "https://github.com"
    github_allowed_hosts: str = "github.com,api.github.com,codeload.github.com"
    # Installation access tokens are minted just-in-time and cached only briefly (well under
    # GitHub's ~1h token lifetime) and never persisted; this is the in-process cache horizon.
    github_token_cache_seconds: int = Field(default=300, ge=0)

    # Shared durable project/coding storage root (WS-R). Server and worker MUST resolve the
    # SAME filesystem path (a shared/RWX volume in cloud) so a worker-written review artifact is
    # readable by the server's report APIs. Empty -> a container-local ``./.keel/projects``
    # default that is ONLY valid for single-host local development (readiness fails in cloud).
    project_storage_root: str = ""

    # Read-only code review (WS-R). The model MUST come from this allowlist, never an arbitrary
    # caller string; ``default_model`` is always allowed. Budgets are always enforced and are
    # never unlimited (fail closed).
    #
    # ``review_enabled`` gates whether this process participates in read-only review at all. When
    # true (default) a worker that cannot reach the shared project-storage volume the review
    # reports live on FAILS STARTUP (crash-loops) rather than run without review handlers, and the
    # server offers the review API. When explicitly false, a process neither enqueues nor consumes
    # ``review.run`` jobs — a local profile with review disabled never touches shared storage.
    review_enabled: bool = True
    review_model_allowlist: str = ""
    review_token_budget: int = Field(default=200_000, gt=0, le=2_000_000)
    review_output_max_tokens: int = Field(default=8_000, gt=0, le=32_000)
    review_cost_ceiling_usd: float = Field(default=1.0, gt=0.0, le=50.0)
    review_max_provider_attempts: int = Field(default=2, ge=1, le=4)
    # Explicit retention window (days) for review report artifacts — never indefinite.
    review_report_retention_days: int = Field(default=90, ge=1, le=3650)
    # Age (hours) after which a review worktree is considered crash-orphaned and reaped. A live
    # review's worktree is short-lived (materialized then disposed within one job), so a healthy
    # worktree is always far younger than this; the reaper only reclaims worktrees left behind by
    # a crash. Active worktrees (younger than the cutoff) are never removed.
    review_worktree_stale_hours: int = Field(default=6, ge=1, le=168)
    # Authoritative per-model review pricing for the cost ceiling. Format:
    # ``model=input/output`` (USD per 1,000,000 tokens), comma-separated, e.g.
    # ``gpt-4o=2.5/10,my-model=1.0/3.0``. A review model that is neither in this override map
    # nor in the built-in known price map is rejected at admission (fail closed) so a cloud
    # review never runs under an unenforceable budget.
    review_model_prices: str = ""

    # psycopg3 driver works for both sync (Alembic) and async (app) engines.
    database_url: str = "postgresql+psycopg://keel:keel@localhost:5432/keel"
    redis_url: str = "redis://localhost:6379/0"

    # Separate maintenance/erasure connection (M3.6, WS-L). Identity erasure
    # (``keel_core.identity.purge``) is a privileged, cross-tenant operation run through the
    # ``keel_erase_user`` / ``keel_erase_organization`` SECURITY DEFINER functions. It MUST
    # connect as a dedicated least-privilege login that is a member of ONLY the
    # ``keel_maintenance_exec`` executor role — never the runtime login. This is intentionally
    # a distinct URL: it never falls back to ``database_url`` in cloud mode, and when unset
    # identity erasure fails closed. Leave empty for local/self-hosted use where erasure is
    # not exercised. See ``require_maintenance_database_url`` and ``docs/OPERATIONS.md``.
    maintenance_database_url: str = ""

    # Separate schema-owner / migrator connection (M3A runtime-role split, WS-DB). Alembic
    # migrations and DB role provisioning must run as a privileged owner/migrator principal
    # (able to CREATE/ALTER schema, tables, and roles). The server/worker instead connect on
    # ``database_url`` as the least-privilege, non-owner ``keel_runtime`` login so
    # ``FORCE ROW LEVEL SECURITY`` is a hard boundary (a superuser/owner would bypass RLS).
    #
    # * When set, Alembic (``migrations/env.py``) and the provisioning CLI use THIS url via
    #   ``require_migration_database_url``.
    # * When empty, they fall back to ``database_url`` — correct for local/self-hosted preview
    #   where a single owner login both migrates and serves. In ``cloud_mode`` that fallback is
    #   refused (``require_migration_database_url``) because ``database_url`` is then the
    #   non-owner runtime login, so migrating/provisioning on it would fail confusingly; set
    #   this to the dedicated owner/migrator url instead. The url is never logged.
    migration_database_url: str = ""

    server_host: str = "0.0.0.0"
    server_port: int = 8000

    # Built-in command/file tools execute through the isolated sandbox RPC service.
    # The in-process backend is only for explicitly trusted local previews.
    execution_backend: str = "sandbox"
    sandbox_url: str = "http://keel-sandbox:8090"
    sandbox_rpc_secret: SecretStr = SecretStr("")
    sandbox_rpc_local_test_mode: bool = False
    trusted_preview_allow_unsafe_execution: bool = False
    trusted_preview_shell_workspace_sanitized: bool = False

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

    @model_validator(mode="after")
    def _validate_knowledge_chunk_settings(self) -> Self:
        if self.knowledge_chunk_overlap_chars >= self.knowledge_chunk_target_chars:
            raise ValueError("knowledge_chunk_overlap_chars must be less than target")
        return self

    @model_validator(mode="after")
    def _validate_legacy_machine_binding(self) -> Self:
        """The legacy migration mapping must be a complete org+Agent pair (or both empty).

        Binding pre-identity ``key:role`` credentials to only an org (without an Agent) — or
        vice versa — cannot derive a data-plane scope, so a half-configured pair is a silent
        no-op that would leave those credentials failing closed while looking configured. Reject
        it explicitly instead so the misconfiguration surfaces at startup.
        """
        org = self.legacy_machine_org_id.strip()
        agent = self.legacy_machine_agent_id.strip()
        if bool(org) != bool(agent):
            raise ValueError(
                "KEEL_LEGACY_MACHINE_ORG_ID and KEEL_LEGACY_MACHINE_AGENT_ID must be set "
                "together (a complete org+Agent pair) or both left empty"
            )
        return self

    @property
    def legacy_machine_binding(self) -> tuple[str, str] | None:
        """The configured legacy org+Agent migration binding, or ``None`` when unset."""
        org = self.legacy_machine_org_id.strip()
        agent = self.legacy_machine_agent_id.strip()
        if org and agent:
            return org, agent
        return None

    @property
    def sync_database_url(self) -> str:
        """SQLAlchemy URL for a synchronous owner/migrator engine (convenience accessor).

        Prefers the dedicated owner/migrator ``migration_database_url`` when set so schema
        migrations run as a privileged principal even when ``database_url`` is the least-privilege
        runtime login; falls back to ``database_url`` for the local/self-hosted single-owner
        profile. Alembic (``migrations/env.py``) resolves its URL through
        :meth:`require_migration_database_url` instead, which additionally fails closed in
        ``cloud_mode`` when no migrator url is configured.
        """
        return self.migration_database_url.strip() or self.database_url

    def require_migration_database_url(self) -> str:
        """Resolve the owner/migrator URL for migrations + role provisioning, failing closed.

        Migrations and DB-role provisioning need a privileged owner/migrator principal. This
        resolver enforces the runtime/owner split operationally:

        * When ``migration_database_url`` is set it is always used (the explicit owner/migrator).
        * When it is empty, it falls back to ``database_url`` ONLY outside ``cloud_mode`` — the
          local/self-hosted profile where that url is itself the owner login.
        * In ``cloud_mode`` a missing ``migration_database_url`` fails closed: ``database_url``
          is the non-owner runtime login there, so silently running DDL / ``CREATE ROLE`` on it
          would fail confusingly (or, worse, imply the runtime login is over-privileged).

        Raising :class:`MigrationDatabaseNotConfigured` (a ``KeelError``) keeps the failure
        typed and auditable. The URL itself is never logged by callers.
        """
        url = self.migration_database_url.strip()
        if url:
            return url
        if self.cloud_mode:
            raise MigrationDatabaseNotConfigured(
                "migrations and DB role provisioning require KEEL_MIGRATION_DATABASE_URL (a "
                "dedicated owner/migrator login) in cloud mode; it is unset and KEEL_DATABASE_URL "
                "is the non-owner runtime login, so it fails closed"
            )
        return self.database_url

    def require_maintenance_database_url(self) -> str:
        """Resolve the identity-erasure maintenance URL, failing closed.

        Identity erasure is privileged and cross-tenant; it must connect as a dedicated
        least-privilege login (a member of only ``keel_maintenance_exec``), never the runtime
        login. This resolver enforces that operationally:

        * A missing ``maintenance_database_url`` always fails closed — erasure is refused
          rather than silently reusing the runtime connection.
        * In ``cloud_mode`` it must additionally differ from ``database_url`` (reusing the
          runtime URL would defeat the privilege split), so an accidental copy is rejected.

        Raising :class:`MaintenanceDatabaseNotConfigured` (a ``KeelError``) keeps the failure
        typed and auditable. The URL itself is never logged by callers.
        """
        url = self.maintenance_database_url.strip()
        if not url:
            raise MaintenanceDatabaseNotConfigured(
                "identity erasure requires KEEL_MAINTENANCE_DATABASE_URL (a dedicated "
                "keel_maintenance_exec login); it is unset, so erasure fails closed"
            )
        if self.cloud_mode and url == self.database_url:
            raise MaintenanceDatabaseNotConfigured(
                "KEEL_MAINTENANCE_DATABASE_URL must not equal KEEL_DATABASE_URL in cloud "
                "mode: erasure must use a separate least-privilege maintenance login"
            )
        return url

    @property
    def fallback_model_list(self) -> list[str]:
        """Parsed, de-blanked ``fallback_models`` (order preserved)."""
        return [m.strip() for m in self.fallback_models.split(",") if m.strip()]

    @property
    def review_allowed_models(self) -> frozenset[str]:
        """Models a caller may request for a review — the allowlist plus ``default_model``.

        The default model is always permitted so a minimally-configured deployment still works;
        any additional models must be explicitly allowlisted. A caller-supplied model outside
        this set is rejected (never passed through to the provider).
        """
        allowed = {m.strip() for m in self.review_model_allowlist.split(",") if m.strip()}
        allowed.add(self.default_model)
        return frozenset(allowed)

    @property
    def review_price_book(self) -> ReviewPriceBook:
        """Authoritative review price book (``KEEL_REVIEW_MODEL_PRICES`` overrides + known map)."""
        from keel_core.review.pricing import PriceBook

        return PriceBook.from_settings(self.review_model_prices)

    def review_priced_models(self) -> frozenset[str]:
        """Allowed review models that also have an authoritative price (fail closed otherwise).

        A model may be on the allowlist yet lack a price; such a model is *not* usable for a
        review because its cost ceiling could not be enforced. Callers reject a requested model
        that is not in this set (especially in cloud).
        """
        book = self.review_price_book
        return frozenset(m for m in self.review_allowed_models if book.is_priced(m))


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
