"""Sandbox access policy — path allow-list + egress deny (spike S3, ADR-0005).

Pure policy functions (no container yet): prove that, by default, the sandbox
denies network egress and confines file access to the workspace, blocking
sensitive names (``.git``/``.env``), path-escape (``..``), loopback and
SSRF/metadata targets. The two-level container sandbox wraps these in M1.
"""

from __future__ import annotations

import ipaddress
import posixpath
import re

_DENY_NAMES = frozenset({".git", ".env"})
_DENY_HOSTNAMES = frozenset({"localhost", "metadata.google.internal"})


class PathPolicy:
    """Confine file access to a workspace root; deny sensitive names and escape."""

    def __init__(self, workspace: str, deny_names: frozenset[str] = _DENY_NAMES) -> None:
        self.workspace = posixpath.normpath(workspace)
        self.deny_names = deny_names

    def is_allowed(self, path: str) -> bool:
        canonical = path.replace("\\", "/")
        if re.match(r"^[A-Za-z]:/", canonical) or ".." in canonical.split("/"):
            return False
        raw = canonical if posixpath.isabs(canonical) else posixpath.join(self.workspace, canonical)
        norm = posixpath.normpath(raw)
        if any(segment in self.deny_names for segment in norm.split("/")):
            return False
        return norm == self.workspace or norm.startswith(self.workspace + "/")

    def is_allowed_pattern(self, pattern: str) -> bool:
        """Allow only relative patterns without traversal or sensitive components."""
        canonical = pattern.replace("\\", "/")
        if not canonical or posixpath.isabs(canonical) or re.match(r"^[A-Za-z]:/", canonical):
            return False
        return not any(
            segment == ".." or segment in self.deny_names for segment in canonical.split("/")
        )


class EgressPolicy:
    """Default-deny network egress; loopback/link-local/private are never allowed."""

    def __init__(
        self, allow_hosts: frozenset[str] = frozenset(), network_enabled: bool = False
    ) -> None:
        self.allow_hosts = allow_hosts
        self.network_enabled = network_enabled

    def is_allowed(self, host: str) -> bool:
        if not self.network_enabled:
            return False
        if self._is_blocked(host):
            return False
        return host in self.allow_hosts

    @staticmethod
    def _is_blocked(host: str) -> bool:
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            return host in _DENY_HOSTNAMES
        return ip.is_loopback or ip.is_link_local or ip.is_private or ip.is_reserved
