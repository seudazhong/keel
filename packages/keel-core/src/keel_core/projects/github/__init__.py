"""GitHub App integration: JWT auth, JIT installation tokens, HTTP client, webhooks."""

from keel_core.projects.github.auth import (
    AppJwtMinter,
    GitHubAppConfigError,
    InstallationToken,
    InstallationTokenService,
    resolve_private_key,
)
from keel_core.projects.github.client import (
    GitHubAuthError,
    GitHubClient,
    GitHubError,
    GitHubNotFoundError,
    GitHubRateLimitError,
    GitHubResponse,
    GitHubTransport,
    HttpxGitHubTransport,
    RetryPolicy,
)
from keel_core.projects.github.urls import (
    UntrustedUrlError,
    normalize_clone_url,
    normalize_https_url,
)
from keel_core.projects.github.webhooks import (
    ALLOWED_EVENTS,
    WebhookEvent,
    WebhookVerificationError,
    delivery_id,
    event_name,
    parse_event,
    verify_signature,
)

__all__ = [
    "ALLOWED_EVENTS",
    "AppJwtMinter",
    "GitHubAppConfigError",
    "GitHubAuthError",
    "GitHubClient",
    "GitHubError",
    "GitHubNotFoundError",
    "GitHubRateLimitError",
    "GitHubResponse",
    "GitHubTransport",
    "HttpxGitHubTransport",
    "InstallationToken",
    "InstallationTokenService",
    "RetryPolicy",
    "UntrustedUrlError",
    "WebhookEvent",
    "WebhookVerificationError",
    "delivery_id",
    "event_name",
    "normalize_clone_url",
    "normalize_https_url",
    "parse_event",
    "resolve_private_key",
    "verify_signature",
]
