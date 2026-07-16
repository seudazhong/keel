"""Fail-closed guard for opt-in local dev/demo tooling (e.g. demo data bootstrap).

Any script that mutates state outside of the normal product surfaces (HTTP API,
worker jobs) must prove — before touching anything — that it is pointed at an
unmistakable local development/demo target: a dev-labelled ``app_env`` and
loopback Postgres/Redis hosts. There is deliberately no fallback: a missing or
unrecognised setting refuses rather than assuming a safe default (mirrors the
eval harness's fail-closed database selection in ``keel_worker.evals.database``).
"""

from __future__ import annotations

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from .config import Settings

# app_env values that unambiguously mean "not a real deployment".
ALLOWED_DEMO_APP_ENVS = frozenset({"dev", "development", "local", "demo", "test"})

# Hostnames that unambiguously mean "this process, or a container on this
# machine" — never a remote/production host.
ALLOWED_DEMO_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

_PRODUCTION_MARKERS = ("prod",)


class DemoGuardError(Exception):
    """Raised when the current configuration is not a safe demo/dev target."""


def _host_is_loopback(url: str, *, label: str) -> str | None:
    """Return a refusal reason for ``url``'s host, or ``None`` if it is safe."""
    try:
        parsed = make_url(url)
    except ArgumentError:
        return f"{label} is not a valid connection URL"
    host = parsed.host
    if host is None:
        # A socket-style URL with no host is local-only by construction.
        return None
    if host.lower() not in ALLOWED_DEMO_HOSTS:
        return f"{label} host {host!r} is not a loopback address"
    return None


def _database_name_is_safe(url: str, *, label: str) -> str | None:
    try:
        parsed = make_url(url)
    except ArgumentError:
        return f"{label} is not a valid connection URL"
    database = (parsed.database or "").lower()
    for marker in _PRODUCTION_MARKERS:
        if marker in database:
            return f"{label} database name {parsed.database!r} looks production-like"
    return None


def assert_demo_environment(settings: Settings) -> None:
    """Raise :class:`DemoGuardError` unless ``settings`` is an unmistakable demo target.

    Checks (all must pass):
      * ``app_env`` is one of :data:`ALLOWED_DEMO_APP_ENVS` (case-insensitive).
      * ``database_url`` and ``redis_url`` resolve to a loopback host.
      * ``database_url``'s database name does not look production-like.

    Never silently substitutes a "safe" default — every failure names the exact
    setting and reason so the caller can fail closed with a clear message.
    """
    app_env = (settings.app_env or "").strip().lower()
    if app_env not in ALLOWED_DEMO_APP_ENVS:
        raise DemoGuardError(
            f"refusing: KEEL_APP_ENV={settings.app_env!r} is not a recognized dev/demo "
            f"environment (expected one of {sorted(ALLOWED_DEMO_APP_ENVS)})"
        )

    for reason in (
        _host_is_loopback(settings.database_url, label="KEEL_DATABASE_URL"),
        _host_is_loopback(settings.redis_url, label="KEEL_REDIS_URL"),
        _database_name_is_safe(settings.database_url, label="KEEL_DATABASE_URL"),
    ):
        if reason is not None:
            raise DemoGuardError(f"refusing: {reason}")


__all__ = [
    "ALLOWED_DEMO_APP_ENVS",
    "ALLOWED_DEMO_HOSTS",
    "DemoGuardError",
    "assert_demo_environment",
]
