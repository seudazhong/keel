"""URL normalization + allowlist for GitHub clone/API endpoints (M3.7, WS-P).

Every remote a project fetches from, and every GitHub API/web base URL the control plane
talks to, is normalized and checked against an explicit host allowlist *before* use, so an
attacker-controlled repository payload can never point the control plane at an internal
service (SSRF), a loopback/private address, or a credential-bearing URL. Redirects are never
followed by the fetch/clone helpers (see :mod:`keel_core.coding` and
:mod:`keel_core.projects.github.client`); this module is the URL gate in front of them.
"""

from __future__ import annotations

import ipaddress
from urllib.parse import urlsplit


class UntrustedUrlError(ValueError):
    """A URL failed normalization / allowlist checks (fail closed, non-sensitive message)."""


_ALLOWED_SCHEMES = frozenset({"https"})


def _is_ip_literal_disallowed(hostname: str) -> bool:
    """True if ``hostname`` is an IP literal that is loopback/private/link-local/reserved."""
    try:
        ip = ipaddress.ip_address(hostname)
    except ValueError:
        return False
    return (
        ip.is_loopback
        or ip.is_private
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def normalize_https_url(raw: str, *, allowed_hosts: frozenset[str]) -> str:
    """Return a canonical ``https://host[:443]/path`` URL or raise :class:`UntrustedUrlError`.

    Rejects: non-HTTPS schemes; embedded credentials; a missing/loopback/private/reserved
    host; a non-443 explicit port; query/fragment components; and any host not on the
    ``allowed_hosts`` allowlist. The result carries no credentials and no redirect.
    """
    if not raw or not raw.strip():
        raise UntrustedUrlError("empty URL")
    parsed = urlsplit(raw.strip())
    if parsed.scheme.lower() not in _ALLOWED_SCHEMES:
        raise UntrustedUrlError("only https URLs are allowed")
    if parsed.username or parsed.password:
        raise UntrustedUrlError("URLs must not embed credentials")
    hostname = parsed.hostname
    if not hostname:
        raise UntrustedUrlError("URL is missing a host")
    host = hostname.lower()
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".localhost"):
        raise UntrustedUrlError("loopback hosts are not allowed")
    if _is_ip_literal_disallowed(host):
        raise UntrustedUrlError("private, loopback, or reserved addresses are not allowed")
    if parsed.query or parsed.fragment:
        raise UntrustedUrlError("URLs must not carry query or fragment components")
    port = parsed.port
    if port not in (None, 443):
        raise UntrustedUrlError("only the default https port is allowed")
    if host not in allowed_hosts:
        raise UntrustedUrlError("host is not on the allowlist")
    path = parsed.path or "/"
    return f"https://{host}{path}"


def normalize_clone_url(
    raw: str, *, allowed_hosts: frozenset[str], repo_full_name: str | None = None
) -> str:
    """Normalize a GitHub clone URL and (optionally) pin it to an expected ``owner/repo``.

    On top of :func:`normalize_https_url`, when ``repo_full_name`` is supplied the URL path
    must be exactly ``/<owner>/<repo>(.git)?`` for that repository, so a webhook payload can
    never redirect a fetch to a *different* repository on an allowed host.
    """
    normalized = normalize_https_url(raw, allowed_hosts=allowed_hosts)
    if repo_full_name is not None:
        parsed = urlsplit(normalized)
        path = parsed.path.rstrip("/")
        if path.endswith(".git"):
            path = path[: -len(".git")]
        if path.lstrip("/") != repo_full_name.strip("/"):
            raise UntrustedUrlError("clone URL does not match the expected repository")
    return normalized


__all__ = ["UntrustedUrlError", "normalize_clone_url", "normalize_https_url"]
