"""Durable OneBot/Telegram (IM) channel routing, safe-agent policy, and reply outbox.

Every IM surface is **untrusted**: an inbound provider event (OneBot QQ, Telegram) is
authenticated + replay-checked by the provider adapter *before* any tenant is known, then a
minimal, opaque **global route index** (no credentials, no content) maps the provider + bot +
chat identity to exactly one org, persisted Agent, canonical data-plane scope and reply policy.
An unknown or revoked mapping **fails closed** in cloud mode; local preview must bind an
*explicit* mapping. The resolved run is admitted through the same
:class:`~keel_core.run_service.DurableRunService` the Web surface uses (``surface="im"``), and
the worker rebuilds an **IM-safe Agent**: the read-only toolset only (plus read-only
memory/Knowledge the grants allow), never write/edit/shell or an outbound connector action
unless an explicit approved policy lists it. Repo + message content is tainted.

Terminal replies are **durable**: the worker persists an idempotent reply *intent* bound to the
run + provider + account + chat + message id, with the reply text stored **encrypted** (envelope
cipher) at rest; a **global** reply-dispatch index carries only a routing pointer (``reply_id``
+ ``scope_id``), never content. A restart-safe sender leases each intent with a fencing token,
sends through the existing OneBot/Telegram adapters keyed by a durable idempotency key, and
records the delivery result — so a crash after terminal / before intent and after send / before
ack are repaired without a duplicate user-visible reply where the provider supports idempotency
(otherwise at-least-once, documented).

The global indices (:class:`ImRouteEntry` rows, reply-dispatch rows) are intentionally **not**
under row-level security — they are the one thing read *across* tenants before a scope is bound —
so they expose only opaque routing keys + a coarse status, nothing sensitive.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.agents import AgentSpec, Scope
from keel_core.errors import KeelError
from keel_core.permissions import Rule, RuleBasedPermissionEngine
from keel_core.protocols import EventStore, Tool
from keel_core.secrets import EncryptedSecret, KeyRing
from keel_core.tools import ExecutionEnvironment, GlobTool, GrepTool, LsTool, ReadTool
from keel_core.types import (
    PermissionDecision,
    RunId,
    ScopeId,
    ScopeKind,
    SessionId,
    TrustLevel,
)


def _now() -> datetime:
    return datetime.now(UTC)


# --------------------------------------------------------------------------- enums


class ImProvider(StrEnum):
    """The IM providers with a durable webhook + reply adapter."""

    onebot = "onebot"
    telegram = "telegram"


class ImChatKind(StrEnum):
    """A mapping targets a single DM peer (personal) or a group/channel (group)."""

    personal = "personal"
    group = "group"


class ImMappingStatus(StrEnum):
    """Lifecycle of a channel mapping (revoked/disabled fail closed at lookup)."""

    active = "active"
    disabled = "disabled"
    revoked = "revoked"


class ImReplyKind(StrEnum):
    """A durable reply intent is the run's terminal (final) reply, or a partial stream chunk."""

    final = "final"
    partial = "partial"


class ImReplyStatus(StrEnum):
    """Lifecycle of a durable reply intent in the outbox."""

    pending = "pending"
    leased = "leased"
    sent = "sent"
    failed = "failed"


# --------------------------------------------------------------------------- errors


class ImRoutingError(KeelError):
    """Base class for fail-closed IM routing errors."""


class UnknownMappingError(ImRoutingError):
    """No active mapping resolves the inbound provider + bot + chat identity (fail closed)."""


class RevokedMappingError(ImRoutingError):
    """The resolved mapping is revoked/disabled — the run is refused (fail closed)."""


# --------------------------------------------------------------------------- hashing


def route_key(provider: str, external_bot_id: str, external_chat_id: str) -> str:
    """The opaque, pseudonymous global lookup key for a provider + bot + chat identity.

    A SHA-256 over canonical JSON of the tuple — the value stored in the **global** route index
    so a pre-tenant webhook can discover *which* mapping (org/scope/agent) owns a chat without
    the index ever holding the plaintext external ids, any credential, or any message content.
    """
    canonical = json.dumps(
        {"provider": provider, "bot": external_bot_id, "chat": external_chat_id},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def pseudonymous_trace_id(*parts: str) -> str:
    """A short pseudonymous hash for tracing (org/agent/run/provider/chat) — no id leakage."""
    canonical = "\u0000".join(parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def reply_idempotency_key(
    *,
    run_id: RunId,
    provider: str,
    external_bot_id: str,
    external_chat_id: str,
    external_message_id: str,
    kind: str,
) -> str:
    """The stable idempotency key binding a reply intent to run/provider/account/chat/message.

    Recording an intent twice (a retried terminalization, a repaired crash window) collapses to
    one row; the sender reuses the same key as the provider-facing idempotency token so a resend
    after a lost ack does not produce a second user-visible reply where the provider dedupes.
    """
    canonical = json.dumps(
        {
            "run": run_id,
            "provider": provider,
            "bot": external_bot_id,
            "chat": external_chat_id,
            "message": external_message_id,
            "kind": kind,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- models


@dataclass(frozen=True)
class ImReplyPolicy:
    """The per-mapping reply/tool policy (the only place extra IM capabilities are granted).

    ``reply_enabled`` gates whether the run may emit a durable reply at all; ``partial_replies``
    opts into streaming partial chunks (default: final response only). ``allow_tools`` is the
    **explicit** allow-list of otherwise-forbidden tools (write/edit/shell/connector actions)
    the org has approved for this channel — empty means the read-only safe set only.
    """

    reply_enabled: bool = True
    partial_replies: bool = False
    allow_tools: tuple[str, ...] = ()

    def to_json(self) -> dict[str, object]:
        return {
            "reply_enabled": self.reply_enabled,
            "partial_replies": self.partial_replies,
            "allow_tools": list(self.allow_tools),
        }

    @classmethod
    def from_json(cls, raw: object) -> ImReplyPolicy:
        data = raw if isinstance(raw, dict) else {}
        allow = data.get("allow_tools") or []
        return cls(
            reply_enabled=bool(data.get("reply_enabled", True)),
            partial_replies=bool(data.get("partial_replies", False)),
            allow_tools=tuple(str(name) for name in allow if isinstance(name, str)),
        )


@dataclass(frozen=True)
class ImChannelMapping:
    """An org-owned binding from a provider chat identity to an exact Agent + scope + policy."""

    id: str
    org_id: str
    provider: ImProvider
    external_bot_id: str
    external_chat_id: str
    chat_kind: ImChatKind
    agent_id: str
    scope_id: ScopeId
    policy: ImReplyPolicy = field(default_factory=ImReplyPolicy)
    status: ImMappingStatus = ImMappingStatus.active
    version: int = 1
    created_by: str = ""
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)
    revoked_by: str = ""
    revoked_at: datetime | None = None

    @property
    def route_key(self) -> str:
        return route_key(self.provider.value, self.external_bot_id, self.external_chat_id)

    def route_entry(self) -> ImRouteEntry:
        """The minimal, opaque global index row this mapping publishes."""
        return ImRouteEntry(
            route_key=self.route_key,
            org_id=self.org_id,
            scope_id=self.scope_id,
            mapping_id=self.id,
            agent_id=self.agent_id,
            chat_kind=self.chat_kind,
            status=self.status,
            reply_allowed=self.policy.reply_enabled,
        )


@dataclass(frozen=True)
class ImRouteEntry:
    """A single opaque row in the **global** route index (no credentials, no content).

    Carries only what a pre-tenant webhook needs to resolve a chat to its owner: the opaque
    :func:`route_key`, the org + canonical scope + Agent it binds, a coarse status, and a
    capability flag. Deliberately **not** under RLS — the one index read across tenants.
    """

    route_key: str
    org_id: str
    scope_id: ScopeId
    mapping_id: str
    agent_id: str
    chat_kind: ImChatKind
    status: ImMappingStatus
    reply_allowed: bool


@dataclass(frozen=True)
class ImReplyIntent:
    """A durable, idempotent terminal reply intent (encrypted payload, scope-partitioned)."""

    id: str
    scope_id: ScopeId
    run_id: RunId
    org_id: str
    provider: ImProvider
    external_bot_id: str
    external_chat_id: str
    external_message_id: str
    chat_kind: ImChatKind
    reply_kind: ImReplyKind
    idempotency_key: str
    key_id: str
    ciphertext: str
    status: ImReplyStatus = ImReplyStatus.pending
    attempts: int = 0
    provider_message_id: str = ""
    error: str = ""
    lease_token: str = ""
    lease_expires_at: datetime | None = None
    next_attempt_at: datetime = field(default_factory=_now)
    delivered_at: datetime | None = None
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)


# --------------------------------------------------------------------------- resolution


def resolve_inbound_route(entry: ImRouteEntry | None, *, cloud_mode: bool) -> ImRouteEntry:
    """Resolve a looked-up route to a runnable binding, or fail closed.

    ``cloud_mode`` is informational for the error text — the policy is identical in both
    postures: an **unknown** chat (no index row) is refused with :class:`UnknownMappingError`
    and a **revoked/disabled** mapping with :class:`RevokedMappingError`. Local-preview callers
    must have *explicitly* published a mapping (there is no implicit ambient binding), so an
    unmapped chat is always a hard failure rather than a silent default.
    """
    if entry is None:
        raise UnknownMappingError(
            "no active IM mapping for this chat" + (" (cloud fail-closed)" if cloud_mode else "")
        )
    if entry.status is not ImMappingStatus.active:
        raise RevokedMappingError(f"IM mapping {entry.mapping_id} is {entry.status.value}")
    return entry


# --------------------------------------------------------------------------- admission context


@dataclass(frozen=True)
class ImInboundContext:
    """The durable IM provider/chat context bound to a run at admission.

    Carried in the admission event payload (no schema migration) so the worker can rebuild the
    reply target (provider + account + chat + message id) and the mapping's reply policy from
    the durable log — the run row alone only knows org/actor/agent/session/surface.
    """

    provider: ImProvider
    external_bot_id: str
    external_chat_id: str
    external_message_id: str
    chat_kind: ImChatKind
    mapping_id: str
    policy: ImReplyPolicy = field(default_factory=ImReplyPolicy)

    def to_admission_extra(self) -> dict[str, object]:
        """The ``extra`` payload merged into the admission turn (namespaced under ``im``)."""
        return {
            "im_context": {
                "provider": self.provider.value,
                "bot": self.external_bot_id,
                "chat": self.external_chat_id,
                "message": self.external_message_id,
                "chat_kind": self.chat_kind.value,
                "mapping_id": self.mapping_id,
                "policy": self.policy.to_json(),
            }
        }

    @classmethod
    def from_payload(cls, raw: object) -> ImInboundContext | None:
        if not isinstance(raw, dict):
            return None
        try:
            return cls(
                provider=ImProvider(str(raw["provider"])),
                external_bot_id=str(raw["bot"]),
                external_chat_id=str(raw["chat"]),
                external_message_id=str(raw.get("message", "")),
                chat_kind=ImChatKind(str(raw["chat_kind"])),
                mapping_id=str(raw.get("mapping_id", "")),
                policy=ImReplyPolicy.from_json(raw.get("policy")),
            )
        except (KeyError, ValueError):
            return None


async def im_context_in_log(
    store: EventStore, session_id: SessionId, run_id: RunId
) -> ImInboundContext | None:
    """The IM provider/chat context recorded on ``run_id``'s admission turn, if any."""
    async for event in store.read(session_id):
        if event.payload.get("admission_run") == run_id:
            return ImInboundContext.from_payload(event.payload.get("im_context"))
    return None


async def final_reply_text_in_log(store: EventStore, session_id: SessionId, run_id: RunId) -> str:
    """The run's final assistant reply text — the last complete (non-partial) turn.

    A multi-turn run (tool use) emits one complete assistant message per turn; the terminal
    reply is the last non-empty one. Partial (streaming) deltas are ignored — the durable reply
    minimum is the final response. Empty if the run produced no assistant text."""
    latest = ""
    async for event in store.read(session_id):
        if event.run_id != run_id:
            continue
        payload = event.payload
        if payload.get("role") != "assistant" or payload.get("partial"):
            continue
        text_value = payload.get("text")
        if isinstance(text_value, str) and text_value.strip():
            latest = text_value
    return latest


# --------------------------------------------------------------------------- safe agent


# The read-only file toolset an untrusted IM surface may always use (never write/edit/shell).
IM_SAFE_TOOLS: tuple[str, ...] = ("read", "ls", "glob", "grep")


def im_safe_tools(environment: ExecutionEnvironment) -> list[Tool]:
    """The read-only file tools built over a fail-closed execution environment."""
    return [
        ReadTool(environment),
        LsTool(environment),
        GlobTool(environment),
        GrepTool(environment),
    ]


def im_safe_permissions(
    policy: ImReplyPolicy, *, read_only_extra: tuple[str, ...] = ()
) -> RuleBasedPermissionEngine:
    """Fail-closed permissions for an untrusted IM run.

    The read-only file tools and any grant-allowed read-only memory/Knowledge tools
    (``read_only_extra``) are allowed. Every other tool — write/edit/shell, an outbound
    connector action, an arbitrary project tool — is **denied** by default, so tainted external
    text can never reach a mutating capability. Only a tool the mapping's policy *explicitly*
    approves is downgraded from deny to ``ask`` (a durable approval), never silently allowed.
    """
    rules = [Rule(name, PermissionDecision.allow) for name in IM_SAFE_TOOLS]
    rules += [Rule(name, PermissionDecision.allow) for name in read_only_extra]
    rules += [
        Rule(name, PermissionDecision.ask)
        for name in policy.allow_tools
        if name not in IM_SAFE_TOOLS and name not in read_only_extra
    ]
    return RuleBasedPermissionEngine(rules, default=PermissionDecision.deny)


def build_im_safe_agent(
    *,
    scope_id: ScopeId,
    model: str,
    agent_id: str,
    name: str,
    chat_kind: ImChatKind,
    persona: str = "",
    policy: ImReplyPolicy | None = None,
    read_only_extra: tuple[str, ...] = (),
    max_iterations: int = 20,
    token_budget: int | None = None,
) -> AgentSpec:
    """Build the untrusted IM :class:`AgentSpec` bound to the persisted Agent + safe toolset.

    The scope is **untrusted** (an IM surface); the toolset is the read-only safe tools plus any
    grant-allowed read-only memory/Knowledge names and any explicitly policy-approved tools. The
    Agent id/name/persona come from the *persisted* selected Agent so the worker-owned run
    executes as the mapped Agent — with no mutating capability unless the policy approved it.
    """
    policy = policy or ImReplyPolicy()
    kind = ScopeKind.group if chat_kind is ImChatKind.group else ScopeKind.personal
    scope = Scope(id=scope_id, kind=kind, trust=TrustLevel.untrusted)
    toolset = (
        list(IM_SAFE_TOOLS)
        + list(read_only_extra)
        + [
            name
            for name in policy.allow_tools
            if name not in IM_SAFE_TOOLS and name not in read_only_extra
        ]
    )
    return AgentSpec(
        id=agent_id,
        name=name,
        model=model,
        scope=scope,
        persona=persona,
        toolset=toolset,
        max_iterations=max_iterations,
        token_budget=token_budget,
    )


# --------------------------------------------------------------------------- reply payload


def encrypt_reply_payload(keyring: KeyRing, text_payload: str) -> EncryptedSecret:
    """Encrypt a reply's text with the active envelope key (payload never stored in clear)."""
    return keyring.encrypt(text_payload)


def decrypt_reply_payload(keyring: KeyRing, intent: ImReplyIntent) -> str:
    """Decrypt a reply intent's payload with the key id it was written under."""
    return keyring.decrypt(intent.key_id, intent.ciphertext)


class ReplySender(Protocol):
    """Send a reply through a provider adapter; returns the provider message id (or "")."""

    async def send(self, intent: ImReplyIntent, text_payload: str) -> str: ...


async def persist_terminal_reply(
    reply_store: ImReplyStore,
    reply_dispatch: ImReplyDispatchIndex,
    keyring: KeyRing,
    *,
    reply_id: str,
    scope_id: ScopeId,
    run_id: RunId,
    org_id: str,
    context: ImInboundContext,
    text_payload: str,
    now: datetime | None = None,
) -> str | None:
    """Idempotently persist a run's terminal reply intent + its global dispatch pointer.

    Returns the durable reply id (existing or new), or ``None`` when the mapping policy disables
    replies or the run produced no text. The payload is encrypted at rest; the dispatch pointer
    carries only ``(reply_id, scope_id)``. Re-recording (a repaired crash window, a retried
    terminalization) collapses on the idempotency key so a user never sees a duplicate reply."""
    if not context.policy.reply_enabled or not text_payload.strip():
        return None
    now = now or _now()
    secret = encrypt_reply_payload(keyring, text_payload)
    idempotency = reply_idempotency_key(
        run_id=run_id,
        provider=context.provider.value,
        external_bot_id=context.external_bot_id,
        external_chat_id=context.external_chat_id,
        external_message_id=context.external_message_id,
        kind=ImReplyKind.final.value,
    )
    intent = ImReplyIntent(
        id=reply_id,
        scope_id=scope_id,
        run_id=run_id,
        org_id=org_id,
        provider=context.provider,
        external_bot_id=context.external_bot_id,
        external_chat_id=context.external_chat_id,
        external_message_id=context.external_message_id,
        chat_kind=context.chat_kind,
        reply_kind=ImReplyKind.final,
        idempotency_key=idempotency,
        key_id=secret.key_id,
        ciphertext=secret.ciphertext,
        created_at=now,
        updated_at=now,
        next_attempt_at=now,
    )
    stored, _created = await reply_store.record_intent(intent)
    await reply_dispatch.record(stored.id, scope_id, now=now)
    return stored.id


async def deliver_reply(
    reply_store: ImReplyStore,
    reply_dispatch: ImReplyDispatchIndex,
    keyring: KeyRing,
    sender: ReplySender,
    *,
    reply_id: str,
    worker_id: str,
    lease_seconds: int = 60,
    retry_delay_seconds: int = 60,
    now: datetime | None = None,
) -> bool:
    """Deliver one durable reply under a fenced lease; returns True when the reply is sent.

    Restart-safe: the intent is claimed with a fencing token, the encrypted payload is decrypted
    in-memory only, and the provider send reuses the durable idempotency key so a resend after a
    lost ack does not produce a second user-visible reply where the provider dedupes (otherwise
    at-least-once). On success the intent is marked ``sent`` and its dispatch pointer retired; on
    failure the intent is rescheduled for a later attempt (no duplicate, no lost reply)."""
    now = now or _now()
    current = await reply_store.get(reply_id)
    if current is None:
        await reply_dispatch.remove(reply_id)  # nothing to send (erased) — retire the pointer
        return False
    if current.status is ImReplyStatus.sent:
        await reply_dispatch.remove(reply_id)  # already delivered — idempotent no-op
        return True
    claimed = await reply_store.claim(
        reply_id, worker_id=worker_id, lease_seconds=lease_seconds, now=now
    )
    if claimed is None:
        await reply_dispatch.reschedule(reply_id, delay_seconds=lease_seconds, now=now)
        return False
    try:
        text_payload = decrypt_reply_payload(keyring, claimed)
        provider_message_id = await sender.send(claimed, text_payload)
    except Exception:  # noqa: BLE001 - a failed send is retried, never a duplicate/lost reply
        await reply_store.mark_failed(
            reply_id,
            lease_token=claimed.lease_token,
            error="send failed",
            retry_delay_seconds=retry_delay_seconds,
            now=now,
        )
        await reply_dispatch.reschedule(reply_id, delay_seconds=retry_delay_seconds, now=now)
        return False
    await reply_store.mark_sent(
        reply_id, lease_token=claimed.lease_token, provider_message_id=provider_message_id, now=now
    )
    await reply_dispatch.remove(reply_id)
    return True


# --------------------------------------------------------------------------- store protocols


class ImMappingStore(Protocol):
    """CRUD/list/status for org-owned channel mappings (org-partitioned)."""

    async def create(self, mapping: ImChannelMapping) -> ImChannelMapping: ...

    async def get(self, mapping_id: str) -> ImChannelMapping | None: ...

    async def list_for_org(self, org_id: str) -> list[ImChannelMapping]: ...

    async def set_status(
        self,
        mapping_id: str,
        status: ImMappingStatus,
        *,
        actor: str,
        now: datetime | None = None,
    ) -> ImChannelMapping | None: ...


class ImRouteIndexStore(Protocol):
    """The global opaque route index (no RLS): publish/lookup/remove/purge."""

    async def put(self, entry: ImRouteEntry, *, now: datetime | None = None) -> None: ...

    async def lookup(self, key: str) -> ImRouteEntry | None: ...

    async def remove_for_mapping(self, mapping_id: str) -> None: ...

    async def purge_scope(self, scope_id: ScopeId) -> int: ...


class ImReplyStore(Protocol):
    """The durable, scope-partitioned reply outbox (encrypted payloads)."""

    async def record_intent(self, intent: ImReplyIntent) -> tuple[ImReplyIntent, bool]: ...

    async def get(self, reply_id: str) -> ImReplyIntent | None: ...

    async def claim(
        self,
        reply_id: str,
        *,
        worker_id: str,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> ImReplyIntent | None: ...

    async def mark_sent(
        self,
        reply_id: str,
        *,
        lease_token: str,
        provider_message_id: str,
        now: datetime | None = None,
    ) -> bool: ...

    async def mark_failed(
        self,
        reply_id: str,
        *,
        lease_token: str,
        error: str,
        retry_delay_seconds: int,
        now: datetime | None = None,
    ) -> bool: ...

    async def purge_scope(self, scope_id: ScopeId) -> int: ...


@dataclass(frozen=True)
class ReplyDispatchIntent:
    """A single open reply-dispatch pointer: which reply, in which scope, needs sending."""

    reply_id: str
    scope_id: ScopeId
    attempts: int = 0


class ImReplyDispatchIndex(Protocol):
    """The global reply-dispatch index (no RLS, no content) a sender scans across scopes."""

    async def record(
        self, reply_id: str, scope_id: ScopeId, *, now: datetime | None = None
    ) -> None: ...

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 200,
        lease_seconds: int = 60,
    ) -> list[ReplyDispatchIntent]: ...

    async def reschedule(
        self, reply_id: str, *, delay_seconds: int = 60, now: datetime | None = None
    ) -> None: ...

    async def remove(self, reply_id: str) -> None: ...

    async def active_scopes(self) -> set[ScopeId]: ...


# --------------------------------------------------------------------------- in-memory doubles


class InMemoryImMappingStore:
    """Process-local mapping store double for unit tests (org-scoped semantics)."""

    def __init__(self) -> None:
        self._rows: dict[str, ImChannelMapping] = {}

    async def create(self, mapping: ImChannelMapping) -> ImChannelMapping:
        self._rows[mapping.id] = mapping
        return mapping

    async def get(self, mapping_id: str) -> ImChannelMapping | None:
        return self._rows.get(mapping_id)

    async def list_for_org(self, org_id: str) -> list[ImChannelMapping]:
        return [m for m in self._rows.values() if m.org_id == org_id]

    async def set_status(
        self,
        mapping_id: str,
        status: ImMappingStatus,
        *,
        actor: str,
        now: datetime | None = None,
    ) -> ImChannelMapping | None:
        now = now or _now()
        current = self._rows.get(mapping_id)
        if current is None:
            return None
        revoked_at = now if status is ImMappingStatus.revoked else current.revoked_at
        updated = ImChannelMapping(
            id=current.id,
            org_id=current.org_id,
            provider=current.provider,
            external_bot_id=current.external_bot_id,
            external_chat_id=current.external_chat_id,
            chat_kind=current.chat_kind,
            agent_id=current.agent_id,
            scope_id=current.scope_id,
            policy=current.policy,
            status=status,
            version=current.version + 1,
            created_by=current.created_by,
            created_at=current.created_at,
            updated_at=now,
            revoked_by=actor if status is ImMappingStatus.revoked else current.revoked_by,
            revoked_at=revoked_at,
        )
        self._rows[mapping_id] = updated
        return updated


class InMemoryImRouteIndex:
    """Process-local global route index double for unit tests."""

    def __init__(self) -> None:
        self._by_key: dict[str, ImRouteEntry] = {}

    async def put(self, entry: ImRouteEntry, *, now: datetime | None = None) -> None:
        self._by_key[entry.route_key] = entry

    async def lookup(self, key: str) -> ImRouteEntry | None:
        return self._by_key.get(key)

    async def remove_for_mapping(self, mapping_id: str) -> None:
        self._by_key = {k: v for k, v in self._by_key.items() if v.mapping_id != mapping_id}

    async def purge_scope(self, scope_id: ScopeId) -> int:
        before = len(self._by_key)
        self._by_key = {k: v for k, v in self._by_key.items() if v.scope_id != scope_id}
        return before - len(self._by_key)


class InMemoryImReplyStore:
    """Process-local reply outbox double for unit tests (fenced claim/mark semantics)."""

    def __init__(self) -> None:
        self._by_id: dict[str, ImReplyIntent] = {}
        self._by_key: dict[str, str] = {}

    async def record_intent(self, intent: ImReplyIntent) -> tuple[ImReplyIntent, bool]:
        existing_id = self._by_key.get(intent.idempotency_key)
        if existing_id is not None:
            return self._by_id[existing_id], False
        self._by_id[intent.id] = intent
        self._by_key[intent.idempotency_key] = intent.id
        return intent, True

    async def get(self, reply_id: str) -> ImReplyIntent | None:
        return self._by_id.get(reply_id)

    async def claim(
        self,
        reply_id: str,
        *,
        worker_id: str,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> ImReplyIntent | None:
        now = now or _now()
        current = self._by_id.get(reply_id)
        if current is None or current.status is ImReplyStatus.sent:
            return None
        lease_free = current.lease_expires_at is None or current.lease_expires_at <= now
        if not lease_free:
            return None
        import uuid

        claimed = _replace_intent(
            current,
            status=ImReplyStatus.leased,
            attempts=current.attempts + 1,
            lease_token=uuid.uuid4().hex,
            lease_expires_at=now + timedelta(seconds=lease_seconds),
            updated_at=now,
        )
        self._by_id[reply_id] = claimed
        return claimed

    async def mark_sent(
        self,
        reply_id: str,
        *,
        lease_token: str,
        provider_message_id: str,
        now: datetime | None = None,
    ) -> bool:
        now = now or _now()
        current = self._by_id.get(reply_id)
        if current is None or current.lease_token != lease_token:
            return False
        self._by_id[reply_id] = _replace_intent(
            current,
            status=ImReplyStatus.sent,
            provider_message_id=provider_message_id,
            delivered_at=now,
            lease_token="",
            lease_expires_at=None,
            updated_at=now,
        )
        return True

    async def mark_failed(
        self,
        reply_id: str,
        *,
        lease_token: str,
        error: str,
        retry_delay_seconds: int,
        now: datetime | None = None,
    ) -> bool:
        now = now or _now()
        current = self._by_id.get(reply_id)
        if current is None or current.lease_token != lease_token:
            return False
        self._by_id[reply_id] = _replace_intent(
            current,
            status=ImReplyStatus.pending,
            error=error[:500],
            lease_token="",
            lease_expires_at=None,
            next_attempt_at=now + timedelta(seconds=retry_delay_seconds),
            updated_at=now,
        )
        return True

    async def purge_scope(self, scope_id: ScopeId) -> int:
        victims = [rid for rid, row in self._by_id.items() if row.scope_id == scope_id]
        for rid in victims:
            row = self._by_id.pop(rid)
            self._by_key.pop(row.idempotency_key, None)
        return len(victims)


class InMemoryImReplyDispatchIndex:
    """Process-local reply-dispatch index double (mirrors the run dispatch outbox semantics)."""

    @dataclass
    class _Row:
        scope_id: ScopeId
        attempts: int
        next_attempt_at: datetime
        lease_owner: str | None
        lease_expires_at: datetime | None

    def __init__(self) -> None:
        self._intents: dict[str, InMemoryImReplyDispatchIndex._Row] = {}

    async def record(
        self, reply_id: str, scope_id: ScopeId, *, now: datetime | None = None
    ) -> None:
        now = now or _now()
        if reply_id not in self._intents:
            self._intents[reply_id] = self._Row(scope_id, 0, now, None, None)

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 200,
        lease_seconds: int = 60,
    ) -> list[ReplyDispatchIntent]:
        now = now or _now()
        claimed: list[ReplyDispatchIntent] = []
        for reply_id, row in sorted(self._intents.items(), key=lambda kv: kv[1].next_attempt_at):
            if len(claimed) >= limit:
                break
            due = row.next_attempt_at <= now
            lease_free = row.lease_expires_at is None or row.lease_expires_at <= now
            if not (due and lease_free):
                continue
            row.lease_owner = worker_id
            row.lease_expires_at = now + timedelta(seconds=lease_seconds)
            row.attempts += 1
            claimed.append(
                ReplyDispatchIntent(reply_id=reply_id, scope_id=row.scope_id, attempts=row.attempts)
            )
        return claimed

    async def reschedule(
        self, reply_id: str, *, delay_seconds: int = 60, now: datetime | None = None
    ) -> None:
        now = now or _now()
        row = self._intents.get(reply_id)
        if row is not None:
            row.lease_owner = None
            row.lease_expires_at = None
            row.next_attempt_at = now + timedelta(seconds=delay_seconds)

    async def remove(self, reply_id: str) -> None:
        self._intents.pop(reply_id, None)

    async def active_scopes(self) -> set[ScopeId]:
        return {row.scope_id for row in self._intents.values()}


def _replace_intent(intent: ImReplyIntent, **changes: object) -> ImReplyIntent:
    """Return a copy of ``intent`` with the given fields replaced (dataclasses.replace typed)."""
    import dataclasses

    return dataclasses.replace(intent, **changes)  # type: ignore[arg-type]


async def purge_scope(engine: AsyncEngine, scope_id: ScopeId) -> int:
    """Erase a scope's durable IM reply outbox + its global route/dispatch pointers.

    Deletes the scope's ``im_reply_intents`` (which cascades the global ``im_reply_dispatch_index``
    rows) and its ``im_route_index`` rows, so an org/scope/user erasure removes every reply row +
    global index and the webhook route is invalid immediately. Returns the total rows removed."""
    async with engine.begin() as conn:
        await conn.execute(_SET_SCOPE, {"scope": scope_id})
        replies = await conn.execute(
            text("DELETE FROM im_reply_intents WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
        routes = await conn.execute(
            text("DELETE FROM im_route_index WHERE scope_id = :scope"),
            {"scope": scope_id},
        )
    return (replies.rowcount or 0) + (routes.rowcount or 0)


# --------------------------------------------------------------------------- postgres stores

_SET_ORG = text("SELECT set_config('app.org_id', :org, true)")
_SET_SCOPE = text("SELECT set_config('app.scope_id', :scope, true)")


def _mapping_from_row(row: object) -> ImChannelMapping:
    r = row  # sqlalchemy Row is attribute-accessible
    return ImChannelMapping(
        id=r.id,  # type: ignore[attr-defined]
        org_id=r.org_id,  # type: ignore[attr-defined]
        provider=ImProvider(r.provider),  # type: ignore[attr-defined]
        external_bot_id=r.external_bot_id,  # type: ignore[attr-defined]
        external_chat_id=r.external_chat_id,  # type: ignore[attr-defined]
        chat_kind=ImChatKind(r.chat_kind),  # type: ignore[attr-defined]
        agent_id=r.agent_id,  # type: ignore[attr-defined]
        scope_id=r.scope_id,  # type: ignore[attr-defined]
        policy=ImReplyPolicy.from_json(r.policy),  # type: ignore[attr-defined]
        status=ImMappingStatus(r.status),  # type: ignore[attr-defined]
        version=r.version,  # type: ignore[attr-defined]
        created_by=r.created_by,  # type: ignore[attr-defined]
        created_at=r.created_at,  # type: ignore[attr-defined]
        updated_at=r.updated_at,  # type: ignore[attr-defined]
        revoked_by=r.revoked_by or "",  # type: ignore[attr-defined]
        revoked_at=r.revoked_at,  # type: ignore[attr-defined]
    )


def _intent_from_row(row: object) -> ImReplyIntent:
    r = row
    return ImReplyIntent(
        id=r.id,  # type: ignore[attr-defined]
        scope_id=r.scope_id,  # type: ignore[attr-defined]
        run_id=r.run_id,  # type: ignore[attr-defined]
        org_id=r.org_id,  # type: ignore[attr-defined]
        provider=ImProvider(r.provider),  # type: ignore[attr-defined]
        external_bot_id=r.external_bot_id,  # type: ignore[attr-defined]
        external_chat_id=r.external_chat_id,  # type: ignore[attr-defined]
        external_message_id=r.external_message_id,  # type: ignore[attr-defined]
        chat_kind=ImChatKind(r.chat_kind),  # type: ignore[attr-defined]
        reply_kind=ImReplyKind(r.reply_kind),  # type: ignore[attr-defined]
        idempotency_key=r.idempotency_key,  # type: ignore[attr-defined]
        key_id=r.key_id,  # type: ignore[attr-defined]
        ciphertext=r.ciphertext,  # type: ignore[attr-defined]
        status=ImReplyStatus(r.status),  # type: ignore[attr-defined]
        attempts=r.attempts,  # type: ignore[attr-defined]
        provider_message_id=r.provider_message_id or "",  # type: ignore[attr-defined]
        error=r.error or "",  # type: ignore[attr-defined]
        lease_token=r.lease_token or "",  # type: ignore[attr-defined]
        lease_expires_at=r.lease_expires_at,  # type: ignore[attr-defined]
        next_attempt_at=r.next_attempt_at,  # type: ignore[attr-defined]
        delivered_at=r.delivered_at,  # type: ignore[attr-defined]
        created_at=r.created_at,  # type: ignore[attr-defined]
        updated_at=r.updated_at,  # type: ignore[attr-defined]
    )


class PostgresImMappingStore:
    """Durable org-owned mapping store; every access sets ``app.org_id`` for RLS."""

    def __init__(self, engine: AsyncEngine, org_id: str) -> None:
        self._engine = engine
        self._org_id = org_id

    async def create(self, mapping: ImChannelMapping) -> ImChannelMapping:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": self._org_id})
            row = (
                await conn.execute(
                    text(
                        "INSERT INTO im_channel_mappings "
                        "(id, org_id, provider, external_bot_id, external_chat_id, chat_kind, "
                        " agent_id, scope_id, policy, status, version, created_by, "
                        " created_at, updated_at) "
                        "VALUES (:id, :org_id, :provider, :bot, :chat, :chat_kind, :agent_id, "
                        " :scope_id, CAST(:policy AS jsonb), :status, :version, :created_by, "
                        " :created_at, :updated_at) "
                        "RETURNING id, org_id, provider, external_bot_id, external_chat_id, "
                        " chat_kind, agent_id, scope_id, policy, status, version, created_by, "
                        " created_at, updated_at, revoked_by, revoked_at"
                    ),
                    {
                        "id": mapping.id,
                        "org_id": mapping.org_id,
                        "provider": mapping.provider.value,
                        "bot": mapping.external_bot_id,
                        "chat": mapping.external_chat_id,
                        "chat_kind": mapping.chat_kind.value,
                        "agent_id": mapping.agent_id,
                        "scope_id": mapping.scope_id,
                        "policy": json.dumps(mapping.policy.to_json()),
                        "status": mapping.status.value,
                        "version": mapping.version,
                        "created_by": mapping.created_by,
                        "created_at": mapping.created_at,
                        "updated_at": mapping.updated_at,
                    },
                )
            ).one()
        return _mapping_from_row(row)

    async def get(self, mapping_id: str) -> ImChannelMapping | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": self._org_id})
            row = (
                await conn.execute(
                    text(
                        "SELECT id, org_id, provider, external_bot_id, external_chat_id, "
                        " chat_kind, agent_id, scope_id, policy, status, version, created_by, "
                        " created_at, updated_at, revoked_by, revoked_at "
                        "FROM im_channel_mappings WHERE id = :id"
                    ),
                    {"id": mapping_id},
                )
            ).first()
        return _mapping_from_row(row) if row is not None else None

    async def list_for_org(self, org_id: str) -> list[ImChannelMapping]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": self._org_id})
            rows = (
                await conn.execute(
                    text(
                        "SELECT id, org_id, provider, external_bot_id, external_chat_id, "
                        " chat_kind, agent_id, scope_id, policy, status, version, created_by, "
                        " created_at, updated_at, revoked_by, revoked_at "
                        "FROM im_channel_mappings WHERE org_id = :org ORDER BY created_at"
                    ),
                    {"org": org_id},
                )
            ).all()
        return [_mapping_from_row(row) for row in rows]

    async def set_status(
        self,
        mapping_id: str,
        status: ImMappingStatus,
        *,
        actor: str,
        now: datetime | None = None,
    ) -> ImChannelMapping | None:
        now = now or _now()
        revoked = status is ImMappingStatus.revoked
        async with self._engine.begin() as conn:
            await conn.execute(_SET_ORG, {"org": self._org_id})
            row = (
                await conn.execute(
                    text(
                        "UPDATE im_channel_mappings SET "
                        " status = :status, version = version + 1, updated_at = :now, "
                        " revoked_by = CASE WHEN :revoked THEN :actor ELSE revoked_by END, "
                        " revoked_at = CASE WHEN :revoked THEN :now ELSE revoked_at END "
                        "WHERE id = :id "
                        "RETURNING id, org_id, provider, external_bot_id, external_chat_id, "
                        " chat_kind, agent_id, scope_id, policy, status, version, created_by, "
                        " created_at, updated_at, revoked_by, revoked_at"
                    ),
                    {
                        "status": status.value,
                        "now": now,
                        "revoked": revoked,
                        "actor": actor,
                        "id": mapping_id,
                    },
                )
            ).first()
        return _mapping_from_row(row) if row is not None else None


class PostgresImRouteIndex:
    """Durable global route index over Postgres (no RLS — the pre-tenant lookup index)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def put(self, entry: ImRouteEntry, *, now: datetime | None = None) -> None:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO im_route_index "
                    "(route_key, org_id, scope_id, mapping_id, agent_id, chat_kind, status, "
                    " reply_allowed, created_at, updated_at) "
                    "VALUES (:route_key, :org_id, :scope_id, :mapping_id, :agent_id, :chat_kind, "
                    " :status, :reply_allowed, :now, :now) "
                    "ON CONFLICT (route_key) DO UPDATE SET "
                    " org_id = EXCLUDED.org_id, scope_id = EXCLUDED.scope_id, "
                    " mapping_id = EXCLUDED.mapping_id, agent_id = EXCLUDED.agent_id, "
                    " chat_kind = EXCLUDED.chat_kind, status = EXCLUDED.status, "
                    " reply_allowed = EXCLUDED.reply_allowed, updated_at = EXCLUDED.updated_at"
                ),
                {
                    "route_key": entry.route_key,
                    "org_id": entry.org_id,
                    "scope_id": entry.scope_id,
                    "mapping_id": entry.mapping_id,
                    "agent_id": entry.agent_id,
                    "chat_kind": entry.chat_kind.value,
                    "status": entry.status.value,
                    "reply_allowed": entry.reply_allowed,
                    "now": now,
                },
            )

    async def lookup(self, key: str) -> ImRouteEntry | None:
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT route_key, org_id, scope_id, mapping_id, agent_id, chat_kind, "
                        " status, reply_allowed FROM im_route_index WHERE route_key = :key"
                    ),
                    {"key": key},
                )
            ).first()
        if row is None:
            return None
        return ImRouteEntry(
            route_key=row.route_key,
            org_id=row.org_id,
            scope_id=row.scope_id,
            mapping_id=row.mapping_id,
            agent_id=row.agent_id,
            chat_kind=ImChatKind(row.chat_kind),
            status=ImMappingStatus(row.status),
            reply_allowed=row.reply_allowed,
        )

    async def remove_for_mapping(self, mapping_id: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM im_route_index WHERE mapping_id = :id"),
                {"id": mapping_id},
            )

    async def purge_scope(self, scope_id: ScopeId) -> int:
        async with self._engine.begin() as conn:
            result = await conn.execute(
                text("DELETE FROM im_route_index WHERE scope_id = :scope"),
                {"scope": scope_id},
            )
        return result.rowcount or 0


class PostgresImReplyStore:
    """Durable, scope-partitioned reply outbox; every access sets ``app.scope_id`` for RLS."""

    _COLS = (
        "id, scope_id, run_id, org_id, provider, external_bot_id, external_chat_id, "
        "external_message_id, chat_kind, reply_kind, idempotency_key, key_id, ciphertext, "
        "status, attempts, provider_message_id, error, lease_token, lease_expires_at, "
        "next_attempt_at, delivered_at, created_at, updated_at"
    )

    def __init__(self, engine: AsyncEngine, scope_id: ScopeId) -> None:
        self._engine = engine
        self._scope_id = scope_id

    async def record_intent(self, intent: ImReplyIntent) -> tuple[ImReplyIntent, bool]:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            inserted = (
                await conn.execute(
                    text(
                        f"INSERT INTO im_reply_intents ({self._COLS}) VALUES "
                        "(:id, :scope_id, :run_id, :org_id, :provider, :bot, :chat, :message, "
                        " :chat_kind, :reply_kind, :idem, :key_id, :ciphertext, :status, "
                        " :attempts, :pmid, :error, :lease_token, :lease_expires_at, "
                        " :next_attempt_at, :delivered_at, :created_at, :updated_at) "
                        "ON CONFLICT (scope_id, idempotency_key) DO NOTHING "
                        f"RETURNING {self._COLS}"
                    ),
                    {
                        "id": intent.id,
                        "scope_id": intent.scope_id,
                        "run_id": intent.run_id,
                        "org_id": intent.org_id,
                        "provider": intent.provider.value,
                        "bot": intent.external_bot_id,
                        "chat": intent.external_chat_id,
                        "message": intent.external_message_id,
                        "chat_kind": intent.chat_kind.value,
                        "reply_kind": intent.reply_kind.value,
                        "idem": intent.idempotency_key,
                        "key_id": intent.key_id,
                        "ciphertext": intent.ciphertext,
                        "status": intent.status.value,
                        "attempts": intent.attempts,
                        "pmid": intent.provider_message_id,
                        "error": intent.error,
                        "lease_token": intent.lease_token,
                        "lease_expires_at": intent.lease_expires_at,
                        "next_attempt_at": intent.next_attempt_at,
                        "delivered_at": intent.delivered_at,
                        "created_at": intent.created_at,
                        "updated_at": intent.updated_at,
                    },
                )
            ).first()
            if inserted is not None:
                return _intent_from_row(inserted), True
            existing = (
                await conn.execute(
                    text(
                        f"SELECT {self._COLS} FROM im_reply_intents WHERE idempotency_key = :idem"
                    ),
                    {"idem": intent.idempotency_key},
                )
            ).one()
        return _intent_from_row(existing), False

    async def get(self, reply_id: str) -> ImReplyIntent | None:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(f"SELECT {self._COLS} FROM im_reply_intents WHERE id = :id"),
                    {"id": reply_id},
                )
            ).first()
        return _intent_from_row(row) if row is not None else None

    async def claim(
        self,
        reply_id: str,
        *,
        worker_id: str,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> ImReplyIntent | None:
        import uuid

        now = now or _now()
        token = uuid.uuid4().hex
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            row = (
                await conn.execute(
                    text(
                        "UPDATE im_reply_intents SET "
                        " status = 'leased', attempts = attempts + 1, lease_token = :token, "
                        " lease_expires_at = :expires, updated_at = :now "
                        "WHERE id = :id AND status <> 'sent' "
                        " AND (lease_expires_at IS NULL OR lease_expires_at <= :now) "
                        f"RETURNING {self._COLS}"
                    ),
                    {
                        "token": token,
                        "expires": now + timedelta(seconds=lease_seconds),
                        "now": now,
                        "id": reply_id,
                    },
                )
            ).first()
        return _intent_from_row(row) if row is not None else None

    async def mark_sent(
        self,
        reply_id: str,
        *,
        lease_token: str,
        provider_message_id: str,
        now: datetime | None = None,
    ) -> bool:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE im_reply_intents SET "
                    " status = 'sent', provider_message_id = :pmid, delivered_at = :now, "
                    " lease_token = '', lease_expires_at = NULL, updated_at = :now "
                    "WHERE id = :id AND lease_token = :token"
                ),
                {"pmid": provider_message_id, "now": now, "id": reply_id, "token": lease_token},
            )
        return (result.rowcount or 0) > 0

    async def mark_failed(
        self,
        reply_id: str,
        *,
        lease_token: str,
        error: str,
        retry_delay_seconds: int,
        now: datetime | None = None,
    ) -> bool:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": self._scope_id})
            result = await conn.execute(
                text(
                    "UPDATE im_reply_intents SET "
                    " status = 'pending', error = :error, lease_token = '', "
                    " lease_expires_at = NULL, next_attempt_at = :retry, updated_at = :now "
                    "WHERE id = :id AND lease_token = :token"
                ),
                {
                    "error": error[:500],
                    "retry": now + timedelta(seconds=retry_delay_seconds),
                    "now": now,
                    "id": reply_id,
                    "token": lease_token,
                },
            )
        return (result.rowcount or 0) > 0

    async def purge_scope(self, scope_id: ScopeId) -> int:
        async with self._engine.begin() as conn:
            await conn.execute(_SET_SCOPE, {"scope": scope_id})
            result = await conn.execute(
                text("DELETE FROM im_reply_intents WHERE scope_id = :scope"),
                {"scope": scope_id},
            )
        return result.rowcount or 0


_INSERT_REPLY_INTENT = text(
    "INSERT INTO im_reply_dispatch_index "
    "(reply_id, scope_id, state, attempts, next_attempt_at, created_at, updated_at) "
    "VALUES (:reply_id, :scope_id, 'pending', 0, :now, :now, :now) "
    "ON CONFLICT (reply_id) DO NOTHING"
)


class PostgresImReplyDispatchIndex:
    """Durable global reply-dispatch index over Postgres (no RLS, no content)."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def record(
        self, reply_id: str, scope_id: ScopeId, *, now: datetime | None = None
    ) -> None:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(
                _INSERT_REPLY_INTENT,
                {"reply_id": reply_id, "scope_id": scope_id, "now": now},
            )

    async def claim_due(
        self,
        *,
        worker_id: str,
        now: datetime | None = None,
        limit: int = 200,
        lease_seconds: int = 60,
    ) -> list[ReplyDispatchIntent]:
        now = now or _now()
        lease_until = now + timedelta(seconds=lease_seconds)
        async with self._engine.begin() as conn:
            rows = (
                await conn.execute(
                    text(
                        "UPDATE im_reply_dispatch_index SET "
                        " state = 'leased', lease_owner = :worker, lease_expires_at = :lease, "
                        " attempts = attempts + 1, updated_at = :now "
                        "WHERE reply_id IN ("
                        " SELECT reply_id FROM im_reply_dispatch_index "
                        " WHERE next_attempt_at <= :now "
                        "  AND (lease_expires_at IS NULL OR lease_expires_at <= :now) "
                        " ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED LIMIT :limit) "
                        "RETURNING reply_id, scope_id, attempts"
                    ),
                    {"worker": worker_id, "lease": lease_until, "now": now, "limit": limit},
                )
            ).all()
        return [
            ReplyDispatchIntent(reply_id=row.reply_id, scope_id=row.scope_id, attempts=row.attempts)
            for row in rows
        ]

    async def reschedule(
        self, reply_id: str, *, delay_seconds: int = 60, now: datetime | None = None
    ) -> None:
        now = now or _now()
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE im_reply_dispatch_index SET "
                    " state = 'pending', lease_owner = NULL, lease_expires_at = NULL, "
                    " next_attempt_at = :next_at, updated_at = :now WHERE reply_id = :reply_id"
                ),
                {
                    "reply_id": reply_id,
                    "next_at": now + timedelta(seconds=delay_seconds),
                    "now": now,
                },
            )

    async def remove(self, reply_id: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM im_reply_dispatch_index WHERE reply_id = :reply_id"),
                {"reply_id": reply_id},
            )

    async def active_scopes(self) -> set[ScopeId]:
        async with self._engine.begin() as conn:
            rows = (
                await conn.execute(text("SELECT DISTINCT scope_id FROM im_reply_dispatch_index"))
            ).all()
        return {row.scope_id for row in rows}


__all__ = [
    "IM_SAFE_TOOLS",
    "ImChannelMapping",
    "ImChatKind",
    "ImInboundContext",
    "ImMappingStatus",
    "ImMappingStore",
    "ImProvider",
    "ImReplyDispatchIndex",
    "ImReplyIntent",
    "ImReplyKind",
    "ImReplyPolicy",
    "ImReplyStatus",
    "ImReplyStore",
    "ImRouteEntry",
    "ImRouteIndexStore",
    "ImRoutingError",
    "InMemoryImMappingStore",
    "InMemoryImReplyDispatchIndex",
    "InMemoryImReplyStore",
    "InMemoryImRouteIndex",
    "PostgresImMappingStore",
    "PostgresImReplyDispatchIndex",
    "PostgresImReplyStore",
    "PostgresImRouteIndex",
    "ReplyDispatchIntent",
    "ReplySender",
    "RevokedMappingError",
    "UnknownMappingError",
    "build_im_safe_agent",
    "decrypt_reply_payload",
    "deliver_reply",
    "encrypt_reply_payload",
    "final_reply_text_in_log",
    "im_context_in_log",
    "im_safe_permissions",
    "im_safe_tools",
    "persist_terminal_reply",
    "pseudonymous_trace_id",
    "purge_scope",
    "reply_idempotency_key",
    "resolve_inbound_route",
    "route_key",
]
