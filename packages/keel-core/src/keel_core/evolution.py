"""Versioned event decoding and deterministic, fail-closed upcasting.

Events are immutable facts.  Their stored version is never rewritten; consumers
upgrade a copy to the current version before using it.  Every intermediate
version needs an explicit one-step upcaster, which makes upgrades reviewable and
replay deterministic.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from pydantic import ValidationError

from keel_core.events import Event, EventType

type EventPayload = dict[str, Any]
type Upcaster = Callable[[EventPayload], EventPayload]


class EventEvolutionError(ValueError):
    """Base class for a persisted event that cannot safely be consumed."""


class UnknownEventTypeError(EventEvolutionError):
    """The stored event type is not understood by this binary."""


class UnknownEventVersionError(EventEvolutionError):
    """A historical event has no complete upgrade path."""


class FutureEventVersionError(EventEvolutionError):
    """A stored event was produced by a newer binary."""


class MalformedEventPayloadError(EventEvolutionError):
    """A historical event does not satisfy the payload expected by its upcaster."""


class EventUpcasterRegistry:
    """Current versions and one-step upcasters for each known event type.

    The registry deliberately requires one upcaster per version transition.
    Skipping versions would make the result dependent on registration order.
    """

    def __init__(self, current_versions: Mapping[EventType, int]) -> None:
        self._current_versions = dict(current_versions)
        self._upcasters: dict[tuple[EventType, int], Upcaster] = {}
        if set(self._current_versions) != set(EventType):
            missing = set(EventType) - set(self._current_versions)
            unexpected = set(self._current_versions) - set(EventType)
            raise ValueError(
                f"current versions must cover EventType (missing={missing}, extra={unexpected})"
            )
        if any(version < 1 for version in self._current_versions.values()):
            raise ValueError("event versions must be positive")

    @property
    def current_versions(self) -> Mapping[EventType, int]:
        """A copyable read-only view of the version contract."""
        return self._current_versions.copy()

    def current_version(self, event_type: EventType | str) -> int:
        """Return the current version, rejecting unknown types explicitly."""
        try:
            normalized = EventType(event_type)
        except ValueError as exc:
            raise UnknownEventTypeError(f"unknown event type: {event_type!r}") from exc
        try:
            return self._current_versions[normalized]
        except KeyError as exc:
            raise UnknownEventTypeError(f"event type is not registered: {event_type!r}") from exc

    def register(self, event_type: EventType, from_version: int, upcaster: Upcaster) -> None:
        """Register the deterministic transition ``from_version -> from_version + 1``."""
        if from_version < 1:
            raise ValueError("event versions must be positive")
        if from_version >= self.current_version(event_type):
            raise ValueError("an upcaster must lead to a registered future version")
        key = (event_type, from_version)
        if key in self._upcasters:
            raise ValueError(f"upcaster already registered for {event_type} v{from_version}")
        self._upcasters[key] = upcaster

    def upcast(self, event: Event) -> Event:
        """Return an event at its current schema version without mutating storage."""
        current = self.current_version(event.type)
        if event.version > current:
            raise FutureEventVersionError(
                f"{event.type} v{event.version} is newer than supported v{current}"
            )
        if event.version < 1:
            raise UnknownEventVersionError(f"{event.type} has invalid version {event.version}")

        payload = event.payload.copy()
        version = event.version
        while version < current:
            try:
                upcaster = self._upcasters[(event.type, version)]
            except KeyError as exc:
                raise UnknownEventVersionError(
                    f"no upcaster registered for {event.type} v{version} -> v{version + 1}"
                ) from exc
            try:
                payload = upcaster(payload.copy())
            except EventEvolutionError:
                raise
            except (KeyError, TypeError, ValueError) as exc:
                raise MalformedEventPayloadError(
                    f"malformed {event.type} v{version} payload"
                ) from exc
            if not isinstance(payload, dict):
                raise MalformedEventPayloadError(
                    f"upcaster for {event.type} v{version} did not return an object payload"
                )
            version += 1
        return event.model_copy(update={"version": version, "payload": payload})

    def decode(self, raw: str | bytes | Mapping[str, Any]) -> Event:
        """Decode and upcast an envelope, naming unknown event types precisely."""
        try:
            envelope = json.loads(raw) if isinstance(raw, (str, bytes)) else dict(raw)
        except (TypeError, ValueError) as exc:
            raise EventEvolutionError("event envelope is not valid JSON") from exc
        event_type = envelope.get("type")
        self.current_version(str(event_type))
        try:
            event = Event.model_validate(envelope)
        except ValidationError as exc:
            raise EventEvolutionError("event envelope is invalid") from exc
        return self.upcast(event)


def _message_token_v1_to_v2(payload: EventPayload) -> EventPayload:
    """Add explicit streaming state while preserving the v1 message representation."""
    role = payload.get("role")
    text = payload.get("text")
    if role not in {"user", "assistant", "system"} or not isinstance(text, str):
        raise MalformedEventPayloadError("message.token v1 requires string role and text")
    return {**payload, "partial": bool(payload.get("partial", False))}


CURRENT_EVENT_VERSIONS: dict[EventType, int] = {
    event_type: (2 if event_type is EventType.message_token else 1) for event_type in EventType
}
"""The current persisted schema version for every event type."""

EVENT_UPCASTERS = EventUpcasterRegistry(CURRENT_EVENT_VERSIONS)
EVENT_UPCASTERS.register(EventType.message_token, 1, _message_token_v1_to_v2)


def current_event_version(event_type: EventType) -> int:
    """Return the version new event writers must persist."""
    return EVENT_UPCASTERS.current_version(event_type)


def upcast_event(event: Event) -> Event:
    """Upgrade one event through the process-wide registry."""
    return EVENT_UPCASTERS.upcast(event)
