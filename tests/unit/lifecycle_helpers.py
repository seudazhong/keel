"""Shared in-memory doubles for lifecycle/erasure unit tests (M3.5)."""

from __future__ import annotations

from keel_core.lifecycle.coordinator import ExternalStepOutcome
from keel_core.lifecycle.models import ErasureTarget, StepStatus

SCOPE = "web:local"


class FakePurge:
    """A duck-typed ScopePurgeRepository that records calls and can fail once per step."""

    def __init__(
        self,
        *,
        sessions: tuple[str, ...] = (),
        spill: tuple[str, ...] = (),
        fail_on: str | None = None,
    ) -> None:
        self._sessions = list(sessions)
        self._spill = list(spill)
        self.calls: list[str] = []
        self._fail_on = fail_on
        self._failed = False

    async def session_ids(self, scope_id: str) -> list[str]:
        return list(self._sessions)

    async def spill_paths(self, scope_id: str, session_id: str | None = None) -> list[str]:
        return list(self._spill)

    async def _op(self, name: str) -> int:
        if self._fail_on == name and not self._failed:
            self._failed = True
            raise RuntimeError(f"boom during {name}")
        self.calls.append(name)
        return 1

    async def message_embeddings(self, scope_id: str) -> int:
        return await self._op("message_embeddings")

    async def events_and_sessions(self, scope_id: str) -> int:
        return await self._op("events_and_sessions")

    async def archival(self, scope_id: str) -> int:
        return await self._op("archival")

    async def memory(self, scope_id: str) -> int:
        return await self._op("memory")

    async def memory_proposals(self, scope_id: str) -> int:
        return await self._op("memory_proposals")

    async def consolidation_cursor(self, scope_id: str) -> int:
        return await self._op("consolidation_cursor")

    async def knowledge(self, scope_id: str) -> int:
        return await self._op("knowledge")

    async def connector_tokens(self, scope_id: str) -> int:
        return await self._op("connector_tokens")

    async def connector_state(self, scope_id: str) -> int:
        return await self._op("connector_state")

    async def connector_outbox(self, scope_id: str) -> int:
        return await self._op("connector_outbox")

    async def effects(self, scope_id: str) -> int:
        return await self._op("effects")

    async def oauth_states(self, scope_id: str) -> int:
        return await self._op("oauth_states")

    async def schedules(self, scope_id: str) -> int:
        return await self._op("schedules")

    async def approvals(self, scope_id: str) -> int:
        return await self._op("approvals")

    async def jobs(self, scope_id: str, *, exclude_job_id: str | None = None) -> int:
        self.calls.append("jobs")
        return 1

    async def runs(self, scope_id: str) -> int:
        return await self._op("runs")

    async def im_routing(self, scope_id: str) -> int:
        return await self._op("im_routing")

    async def session_message_embeddings(self, scope_id: str, session_id: str) -> int:
        return await self._op("session_message_embeddings")

    async def session_events(self, scope_id: str, session_id: str) -> int:
        return await self._op("session_events")


class RecordingRedis:
    def __init__(self) -> None:
        self.purged: list[str] = []

    async def purge_sessions(self, session_ids: list[str]) -> int:
        self.purged.extend(session_ids)
        return len(list(session_ids))


class RecordingProjectPurger:
    def __init__(self, removed: bool = True) -> None:
        self.removed = removed
        self.projects: list[str] = []

    def purge_project(self, project_id: str) -> bool:
        self.projects.append(project_id)
        return self.removed


class FailingExternalStep:
    name = "provider_telemetry"

    async def erase(self, target: ErasureTarget) -> ExternalStepOutcome:
        return ExternalStepOutcome(StepStatus.failed, "provider returned 500")
