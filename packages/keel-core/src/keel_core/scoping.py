"""Canonical data-plane scope derivation (M3.6, org/Agent isolation).

The durable data plane (events, memory, Knowledge, connectors, runs, approvals) is keyed
by an opaque :class:`~keel_core.types.ScopeId`. Before M3.6 every authenticated Web request
shared a single ambient ``web:local`` scope, so two organizations reusing the same session
id would read and write each other's data. This module is the **single** place a scope id is
derived from an org + persisted Agent and validated, so the value is identical everywhere it
is used (admission, the worker's store construction, and every authenticated read/stream).

* An authenticated request derives ``agent:<org_id>/<agent_id>`` from the *resolved* org id
  and the *persisted* Agent id (never the raw, caller-supplied header, which may be a slug).
  Two orgs therefore never collide, and the same session id under two Agents is two logically
  distinct sessions.
* The open-mode local-preview single operator keeps the explicit, non-cloud ``web:local``
  scope. It is never reused across organizations because it is only reachable when there is
  no authenticated user/org (non-cloud mode).

Validation is deny-by-default: each segment must be a short, opaque, traversal-free token, so
a derived scope can never smuggle a path separator, ``..``, whitespace, or an empty segment
into the downstream Postgres ``app.scope_id`` GUC / RLS predicate or a workspace path.
"""

from __future__ import annotations

import re

from keel_core.errors import KeelError
from keel_core.types import ScopeId

# The explicit, non-cloud single-operator preview scope. Kept distinct from every derived
# per-Agent scope and never shared across organizations.
LOCAL_PREVIEW_SCOPE: ScopeId = "web:local"

_AGENT_SCOPE_PREFIX = "agent:"
# A scope segment is an opaque, traversal-free token: letters, digits and ``._-`` only, so it
# cannot contain a path separator, ``..`` as a whole segment, whitespace, or the ``:``/``/``
# used by the scheme itself.
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class ScopeValidationError(KeelError):
    """A scope id (or one of its segments) is malformed (fail closed)."""


def _validate_segment(kind: str, value: str) -> str:
    token = (value or "").strip()
    if not _SEGMENT_RE.match(token) or token == ".." or token.startswith(".."):
        raise ScopeValidationError(f"invalid {kind} segment for scope derivation: {value!r}")
    return token


def derive_agent_scope(org_id: str, agent_id: str) -> ScopeId:
    """Derive the canonical ``agent:<org_id>/<agent_id>`` data-plane scope (validated).

    Callers must pass the *resolved* org id and the *persisted* Agent id (both already
    authorized), never a raw header value. A malformed segment fails closed with
    :class:`ScopeValidationError` rather than producing a smuggled/ambiguous scope.
    """
    org = _validate_segment("org", org_id)
    agent = _validate_segment("agent", agent_id)
    return f"{_AGENT_SCOPE_PREFIX}{org}/{agent}"


def is_local_preview_scope(scope_id: ScopeId) -> bool:
    """Whether ``scope_id`` is the explicit local-preview single-operator scope."""
    return scope_id == LOCAL_PREVIEW_SCOPE


def is_agent_scope(scope_id: ScopeId) -> bool:
    """Whether ``scope_id`` is a derived per-Agent scope (``agent:<org>/<agent>``)."""
    return scope_id.startswith(_AGENT_SCOPE_PREFIX)


def parse_agent_scope(scope_id: ScopeId) -> tuple[str, str] | None:
    """Best-effort ``(org_id, agent_id)`` recovered from a derived per-Agent scope.

    ``None`` for the local-preview scope or anything malformed — never raises, so a
    read-only/audit caller (e.g. the R1B Effect ledger's traceability columns,
    :mod:`keel_core.connectors`) can use it without re-validating the scope itself."""
    if not is_agent_scope(scope_id):
        return None
    remainder = scope_id[len(_AGENT_SCOPE_PREFIX) :]
    org, sep, agent = remainder.partition("/")
    if not sep or not org or not agent:
        return None
    return org, agent


def validate_scope_id(scope_id: ScopeId) -> ScopeId:
    """Re-validate a scope id centrally (the worker revalidates ``record.scope_id``).

    Accepts the local-preview scope and any well-formed derived per-Agent scope; anything
    else (an ambient, empty, or malformed value) fails closed. Used by the worker before it
    constructs per-scope stores/tools from a run record so a corrupted/foreign scope can
    never be adopted.
    """
    if is_local_preview_scope(scope_id):
        return scope_id
    if is_agent_scope(scope_id):
        remainder = scope_id[len(_AGENT_SCOPE_PREFIX) :]
        org, sep, agent = remainder.partition("/")
        if sep:
            _validate_segment("org", org)
            _validate_segment("agent", agent)
            return scope_id
    raise ScopeValidationError(f"malformed scope id: {scope_id!r}")


def workspace_namespace(scope_id: ScopeId) -> str:
    """A filesystem-safe, opaque workspace namespace for ``scope_id`` (no traversal).

    Derived by hashing the validated scope id so the on-disk / sandbox workspace name is a
    fixed-length, traversal-free token that still maps one-to-one to the scope. Distinct
    scopes always get distinct namespaces; the local-preview scope maps to a stable name.
    """
    import hashlib

    validate_scope_id(scope_id)
    digest = hashlib.sha256(scope_id.encode("utf-8")).hexdigest()[:32]
    return f"ws_{digest}"


__all__ = [
    "LOCAL_PREVIEW_SCOPE",
    "ScopeValidationError",
    "derive_agent_scope",
    "is_agent_scope",
    "is_local_preview_scope",
    "parse_agent_scope",
    "validate_scope_id",
    "workspace_namespace",
]
