"""Immutable, schema-versioned Agent configuration snapshot captured at run admission (R1B).

Every durable run admits with a **typed** snapshot of the exact Agent configuration bound to
that run — the persisted Agent's optimistic ``version``, its name/persona, the selected model,
the bounded execution budget (``max_iterations`` / ``token_budget``), a permission-profile
identifier, the tool-name set admitted, a (currently minimal) memory-policy object, and any
admitted resource-grant descriptors (non-secret: type/id/capability only, never credential
material). It is persisted verbatim (canonical JSON + a content hash) alongside the run row so a
worker reconstructs the run's name/persona/model/bounded config from **this** frozen record
rather than the Agent's later-mutated fields (INVARIANTS.md C8).

This is intentionally *not* the full future Agent/Connection/Routine model (docs/ARCHITECTURE.md
3.3): it captures the fields the current interactive runtime actually models. Later work can
extend :class:`AgentConfigSnapshot` additively (new optional fields, a bumped
``schema_version``) without breaking a snapshot already persisted — :meth:`from_canonical_json`
defaults every field absent from an older payload.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

# The current snapshot payload shape. Bump when a field's *meaning* changes incompatibly (an
# additive new field with a safe default does not require a bump).
SNAPSHOT_SCHEMA_VERSION = 1

# The empty/legacy sentinel persisted-hash value: a run admitted before this migration (or by a
# caller that never captured a snapshot) carries this, never a real content hash, so it is never
# confused with a genuinely empty-but-captured snapshot (which always hashes non-empty content).
NO_SNAPSHOT_HASH = ""


class AgentConfigSnapshotError(ValueError):
    """Raised when a persisted snapshot payload fails canonical decoding or hash validation."""


@dataclass(frozen=True)
class MemoryPolicySnapshot:
    """The bounded memory capability policy in effect at admission.

    Extensible placeholder (docs/MEMORY.md: "Agent records do not yet own a versioned memory
    policy") — it captures the caps the current interactive runtime actually enforces (core
    memory edit tools + optional archival recall), not a future first-class Agent-owned policy
    object. A later PR can add fields here additively.
    """

    core_memory_enabled: bool = True
    archival_enabled: bool = False
    memory_block_max_chars: int = 2000

    def to_dict(self) -> dict[str, Any]:
        return {
            "core_memory_enabled": self.core_memory_enabled,
            "archival_enabled": self.archival_enabled,
            "memory_block_max_chars": self.memory_block_max_chars,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> MemoryPolicySnapshot:
        data = data or {}
        return cls(
            core_memory_enabled=bool(data.get("core_memory_enabled", True)),
            archival_enabled=bool(data.get("archival_enabled", False)),
            memory_block_max_chars=int(data.get("memory_block_max_chars", 2000)),
        )


@dataclass(frozen=True, order=True)
class ResourceGrantSnapshot:
    """One admitted resource-grant descriptor bound at admission.

    Non-secret by construction: only the ``(resource_type, resource_id, capability)`` identity
    is captured — never a credential, token, or other secret material (C7).
    """

    resource_type: str
    resource_id: str
    capability: str

    def to_dict(self) -> dict[str, str]:
        return {
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "capability": self.capability,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ResourceGrantSnapshot:
        return cls(
            resource_type=str(data.get("resource_type", "")),
            resource_id=str(data.get("resource_id", "")),
            capability=str(data.get("capability", "")),
        )


@dataclass(frozen=True)
class AgentConfigSnapshot:
    """The immutable Agent configuration a run is admitted (and executes) against.

    Construct with keyword arguments; ``tools`` and ``resource_grants`` are normalized to a
    stable sorted, de-duplicated order in ``__post_init__`` so two snapshots with the same
    logical content always produce an identical :meth:`canonical_json` / :meth:`content_hash` —
    the property the admission fingerprint and idempotency checks depend on.
    """

    schema_version: int = SNAPSHOT_SCHEMA_VERSION
    agent_id: str = ""
    agent_version: int = 0
    agent_name: str = ""
    persona: str = ""
    model: str = ""
    max_iterations: int = 40
    token_budget: int | None = None
    permission_profile: str = "default"
    tools: tuple[str, ...] = ()
    memory_policy: MemoryPolicySnapshot = field(default_factory=MemoryPolicySnapshot)
    resource_grants: tuple[ResourceGrantSnapshot, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "tools", tuple(sorted(set(self.tools))))
        object.__setattr__(self, "resource_grants", tuple(sorted(set(self.resource_grants))))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "agent_id": self.agent_id,
            "agent_version": self.agent_version,
            "agent_name": self.agent_name,
            "persona": self.persona,
            "model": self.model,
            "max_iterations": self.max_iterations,
            "token_budget": self.token_budget,
            "permission_profile": self.permission_profile,
            "tools": list(self.tools),
            "memory_policy": self.memory_policy.to_dict(),
            "resource_grants": [grant.to_dict() for grant in self.resource_grants],
        }

    def canonical_json(self) -> str:
        """Stable JSON (sorted keys, no whitespace) — the exact bytes persisted + hashed."""
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    def content_hash(self) -> str:
        """A sha256 over :meth:`canonical_json` — the identity bound into admission fingerprints."""
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()

    def restrict_tools(self, currently_available: Iterable[str]) -> tuple[str, ...]:
        """The tool names safe to actually grant a claimed run: never more than admitted.

        Intersects the snapshot's admitted tool-name set with what is *currently* available
        (a connector removed, a grant revoked, a tool renamed): a tool the snapshot admitted but
        that no longer exists is silently dropped (fails safe, never substituted); a tool that
        exists now but was **not** in the admitted set is never added — authority can only
        shrink after admission, never expand (docs/INVARIANTS.md C8).
        """
        available = set(currently_available)
        return tuple(name for name in self.tools if name in available)

    @classmethod
    def from_canonical_json(
        cls, payload: str, *, expected_hash: str | None = None
    ) -> AgentConfigSnapshot:
        """Reconstruct a snapshot from its persisted JSON, validating ``expected_hash`` if given.

        Every field is defaulted so a payload from an older (additive) schema version decodes
        cleanly. Raises :class:`AgentConfigSnapshotError` on malformed JSON/shape or — the
        integrity check callers actually rely on — a hash mismatch (tamper or corruption)."""
        try:
            data = json.loads(payload) if payload else {}
        except (TypeError, json.JSONDecodeError) as exc:
            raise AgentConfigSnapshotError(f"invalid snapshot payload: {exc}") from exc
        if not isinstance(data, dict):
            raise AgentConfigSnapshotError("snapshot payload must decode to a JSON object")
        token_budget_raw = data.get("token_budget")
        snapshot = cls(
            schema_version=int(data.get("schema_version", SNAPSHOT_SCHEMA_VERSION)),
            agent_id=str(data.get("agent_id", "")),
            agent_version=int(data.get("agent_version", 0)),
            agent_name=str(data.get("agent_name", "")),
            persona=str(data.get("persona", "")),
            model=str(data.get("model", "")),
            max_iterations=int(data.get("max_iterations", 40)),
            token_budget=(int(token_budget_raw) if token_budget_raw is not None else None),
            permission_profile=str(data.get("permission_profile", "default")),
            tools=tuple(str(name) for name in data.get("tools", ())),
            memory_policy=MemoryPolicySnapshot.from_dict(data.get("memory_policy")),
            resource_grants=tuple(
                ResourceGrantSnapshot.from_dict(g) for g in data.get("resource_grants", ())
            ),
        )
        if expected_hash and snapshot.content_hash() != expected_hash:
            raise AgentConfigSnapshotError("snapshot content hash mismatch (tamper/corruption)")
        return snapshot


__all__ = [
    "NO_SNAPSHOT_HASH",
    "SNAPSHOT_SCHEMA_VERSION",
    "AgentConfigSnapshot",
    "AgentConfigSnapshotError",
    "MemoryPolicySnapshot",
    "ResourceGrantSnapshot",
]
