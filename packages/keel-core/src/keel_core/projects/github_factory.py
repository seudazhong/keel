"""Shared GitHub App integration factory (server + worker).

Both the API server (project import/sync) and the worker (read-only review PR resolution) need
the same GitHub App control-plane integration. This factory centralizes construction so the two
processes build it identically from :class:`~keel_core.config.Settings` and disable the feature
(returning ``None``) rather than crashing when it is unconfigured or mis-configured.
"""

from __future__ import annotations

import logging

from keel_core.config import Settings

from .github import (
    AppJwtMinter,
    GitHubClient,
    HttpxGitHubTransport,
    InstallationTokenService,
    resolve_private_key,
)
from .github.urls import UntrustedUrlError, normalize_https_url
from .service import GitHubIntegration

logger = logging.getLogger("keel.projects.github")


def build_github_integration(settings: Settings) -> GitHubIntegration | None:
    """Build the GitHub App integration when configured (else ``None`` — feature disabled)."""
    if settings.github_app_id <= 0 or not settings.github_private_key_ref:
        return None
    try:
        hosts = frozenset(
            h.strip().lower() for h in settings.github_allowed_hosts.split(",") if h.strip()
        )
        # Validate the configured GitHub API base URL BEFORE any authenticated client is built:
        # it must be HTTPS, carry no embedded credentials, resolve to a non-private/non-loopback
        # host on the explicit allowlist, and use the default port with no query/fragment. A
        # mis-configured base (plain HTTP, an internal/private host, an off-allowlist host) would
        # otherwise send a just-in-time installation token to an arbitrary endpoint — fail closed
        # and disable the feature instead.
        try:
            api_base_url = normalize_https_url(settings.github_api_base_url, allowed_hosts=hosts)
        except UntrustedUrlError:
            logger.warning(
                "github_api_base_url failed HTTPS/allowlist validation; GitHub App disabled"
            )
            return None
        minter = AppJwtMinter(
            app_id=settings.github_app_id,
            private_key_loader=lambda: resolve_private_key(settings.github_private_key_ref),
        )
        transport = HttpxGitHubTransport()
        client = GitHubClient(transport, api_base_url=api_base_url)

        async def _mint(installation_id: int, app_jwt: str) -> object:
            return await client.mint_installation_token(
                installation_id=installation_id, app_jwt=app_jwt
            )

        tokens = InstallationTokenService(
            minter, _mint, cache_seconds=settings.github_token_cache_seconds
        )
        return GitHubIntegration(tokens=tokens, client=client, allowed_hosts=hosts)
    except Exception:  # noqa: BLE001 - misconfiguration disables the feature, never crashes boot
        logger.warning("GitHub App configured but could not be initialized; feature disabled")
        return None


__all__ = ["build_github_integration"]
