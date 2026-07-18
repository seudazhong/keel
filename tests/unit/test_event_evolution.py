"""Historical event and projection-rebuild compatibility coverage."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from keel_core.events import Event, EventType
from keel_core.evolution import (
    CURRENT_EVENT_VERSIONS,
    EVENT_UPCASTERS,
    EventUpcasterRegistry,
    FutureEventVersionError,
    MalformedEventPayloadError,
    UnknownEventTypeError,
    UnknownEventVersionError,
    current_event_version,
    upcast_event,
)
from keel_core.projections import project_messages
from keel_core.rebuild import InMemoryProjectionCheckpoints, ProjectionRebuilder
from keel_core.state import InMemoryEventStore

FIXTURES = Path(__file__).parents[1] / "fixtures" / "events"


def _event(seq: int, *, version: int = 1, payload: dict[str, Any] | None = None) -> Event:
    return Event(
        type=EventType.message_token,
        version=version,
        seq=seq,
        session_id="session-1",
        scope_id="scope-1",
        ts=datetime.now(UTC),
        payload=payload or {"role": "user", "text": "hello"},
    )


def test_every_registered_historical_version_has_a_fixture() -> None:
    fixtures = [
        json.loads(path.read_text(encoding="utf-8")) for path in sorted(FIXTURES.glob("*.json"))
    ]
    historical = {(EventType(raw["type"]), int(raw["version"])) for raw in fixtures}
    expected = {
        (event_type, version)
        for event_type, current in CURRENT_EVENT_VERSIONS.items()
        for version in range(1, current)
    }
    assert historical == expected

    for raw in fixtures:
        event = EVENT_UPCASTERS.decode(raw)
        assert event.version == CURRENT_EVENT_VERSIONS[event.type]


def test_v1_message_fixture_preserves_current_projection() -> None:
    raw = json.loads((FIXTURES / "message-token-v1.json").read_text(encoding="utf-8"))
    historical = EVENT_UPCASTERS.decode(raw)
    current = _event(
        1,
        version=2,
        payload={"role": "user", "text": "hello from v1", "partial": False},
    )

    assert historical.payload["partial"] is False
    assert project_messages([historical]) == project_messages([current])


def test_upcasting_is_idempotent_and_store_reads_are_upcast() -> None:
    event = _event(1)
    upgraded = upcast_event(event)
    assert upgraded.version == 2
    assert upcast_event(upgraded) == upgraded

    async def exercise() -> None:
        store = InMemoryEventStore()
        await store.append(event)
        replayed = [item async for item in store.read("session-1")]
        assert replayed == [upgraded]

    import asyncio

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("event", "error"),
    [
        (_event(1, version=3), FutureEventVersionError),
        (_event(1, payload={"role": "user"}), MalformedEventPayloadError),
    ],
)
def test_future_versions_and_malformed_historical_payloads_fail_closed(
    event: Event, error: type[Exception]
) -> None:
    with pytest.raises(error):
        upcast_event(event)


def test_unknown_type_and_missing_transition_fail_closed() -> None:
    with pytest.raises(UnknownEventTypeError):
        EVENT_UPCASTERS.decode({"type": "unknown.event"})

    registry = EventUpcasterRegistry({event_type: 2 for event_type in EventType})
    with pytest.raises(UnknownEventVersionError):
        registry.upcast(_event(1))


def test_new_writers_persist_current_version_and_survive_round_trip() -> None:
    """New writers must stamp the current version with a valid payload (M0 contract).

    This guards the event fan-out regression: a ``message.token`` written at the
    current version with a legal payload must decode unchanged, so producers never
    depend on the v1->v2 upcaster to backfill missing ``role``/``text``.
    """
    current = current_event_version(EventType.message_token)
    written = _event(
        1,
        version=current,
        payload={"role": "assistant", "text": "streamed", "partial": True},
    )
    decoded = EVENT_UPCASTERS.decode(written.model_dump(mode="json"))
    assert decoded.version == current
    assert decoded == upcast_event(written)
    assert decoded.payload == {"role": "assistant", "text": "streamed", "partial": True}


def test_default_constructed_message_token_fails_closed() -> None:
    """A default ``Event`` (version=1, empty payload) is a malformed historical write.

    The fail-closed decode is intentional; producers must supply the current
    version and payload rather than emitting an unupgradeable v1 envelope.
    """
    malformed = Event(
        type=EventType.message_token,
        seq=1,
        session_id="session-1",
        scope_id="scope-1",
        ts=datetime.now(UTC),
    )
    assert malformed.version == 1
    assert malformed.payload == {}
    with pytest.raises(MalformedEventPayloadError):
        EVENT_UPCASTERS.decode(malformed.model_dump(mode="json"))


def test_projection_rebuild_dry_run_checkpoint_resume_and_tombstones() -> None:
    class Sink:
        def __init__(self) -> None:
            self.events: list[Event] = []

        async def apply(self, event: Event) -> None:
            self.events.append(event)

    async def exercise() -> None:
        events = InMemoryEventStore()
        await events.append(_event(1))
        await events.append(_event(2, payload={"role": "assistant", "text": "hidden"}))
        checkpoints = InMemoryProjectionCheckpoints()
        sink = Sink()
        rebuilder = ProjectionRebuilder(
            events,
            checkpoints,
            tombstone_hook=lambda event: event.seq == 2,
        )

        dry_run = await rebuilder.rebuild("messages", "session-1", sink, dry_run=True)
        assert dry_run.processed == 2
        assert dry_run.skipped_tombstones == 1
        assert sink.events == []
        assert await checkpoints.load("messages", "session-1") is None

        rebuilt = await rebuilder.rebuild("messages", "session-1", sink)
        assert rebuilt.processed == 2
        assert rebuilt.skipped_tombstones == 1
        assert [event.seq for event in sink.events] == [1]
        assert (await checkpoints.load("messages", "session-1")).seq == 2  # type: ignore[union-attr]

        assert (await rebuilder.rebuild("messages", "session-1", sink)).processed == 0
        await events.append(_event(3))
        resumed = await rebuilder.rebuild("messages", "session-1", sink)
        assert resumed.processed == 1
        assert [event.seq for event in sink.events] == [1, 3]

    import asyncio

    asyncio.run(exercise())
