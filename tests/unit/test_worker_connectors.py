"""The worker wires one generic connector sync kind, independent of provider ids."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from keel_core.config import Settings
from keel_core.connector_contracts import (
    BaseConnectorProvider,
    ConnectorAction,
    ConnectorActionApproval,
    ConnectorActionContext,
    ConnectorActionIdempotency,
    ConnectorActionManifest,
    ConnectorActionSemantics,
    ConnectorAuthKind,
    ConnectorBindingDraft,
    ConnectorBindingStatus,
    ConnectorCapability,
    ConnectorManifest,
    ConnectorOperationContext,
    ConnectorRenewalExpiryBehavior,
    ConnectorRenewalPolicy,
    ConnectorRenewalResult,
    ConnectorScheduleOperation,
    ConnectorSetupResult,
    ConnectorSyncResult,
)
from keel_core.connector_registry import ConnectorRegistration, ConnectorRegistry
from keel_core.connector_repository import (
    ConnectorScheduleLease,
    ConnectorScheduleLeaseLostError,
    InMemoryConnectorRepository,
)
from keel_core.connector_schedule_index import InMemoryConnectorScheduleIndex
from keel_core.connector_service import (
    CONNECTOR_RENEW_JOB_KIND,
    CONNECTOR_SYNC_JOB_KIND,
    ConnectorService,
)
from keel_core.job_dispatch import InMemoryJobDispatchOutbox
from keel_core.jobs import InMemoryJobStore, JobStatus
from keel_core.protocols import ToolContext
from keel_worker.connectors import reconcile_connectors_tick, register_connector_jobs
from keel_worker.jobs import JobRegistry, reconcile_job_dispatch_tick, run_job
from keel_worker.main import _connector_actions

ACTION = ConnectorActionManifest(
    name="calendar_create",
    description="Create a calendar event.",
    input_schema={
        "type": "object",
        "properties": {"idempotency_key": {"type": "string"}},
    },
    semantics=ConnectorActionSemantics.outbound,
    idempotency=ConnectorActionIdempotency.required,
    approval=ConnectorActionApproval.tainted,
)


class Provider(BaseConnectorProvider):
    manifest = ConnectorManifest(
        id="worker_fixture",
        name="Worker fixture",
        description="test",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
        actions=(ACTION,),
    )

    async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
        return ConnectorSyncResult()

    def build_actions(self, context: ConnectorActionContext) -> tuple[ConnectorAction, ...]:
        async def create(args: dict[str, Any], tool_context: ToolContext) -> str:
            state = await context.load_state("worker_fixture")
            assert state.binding is not None
            return state.binding.id

        return (ConnectorAction(ACTION, create),)


def test_register_connector_jobs_adds_provider_agnostic_sync_and_renew_kinds() -> None:
    provider_registry = ConnectorRegistry(
        (ConnectorRegistration(Provider.manifest, Provider, "tests.worker_fixture"),)
    )
    service = ConnectorService(
        provider_registry,
        InMemoryConnectorRepository("scope:test"),
    )
    jobs = JobRegistry()
    register_connector_jobs(jobs, service, Settings())
    assert jobs.kinds() == (CONNECTOR_RENEW_JOB_KIND, CONNECTOR_SYNC_JOB_KIND)


async def test_worker_discovers_provider_local_actions_without_provider_branches() -> None:
    provider_registry = ConnectorRegistry(
        (ConnectorRegistration(Provider.manifest, Provider, "tests.worker_fixture"),)
    )
    repository = InMemoryConnectorRepository("scope:test")
    binding = await repository.upsert_binding(
        "worker_fixture",
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
    )
    ctx = {
        "connector_registry": provider_registry,
        "connector_repository": repository,
    }
    assert _connector_actions(ctx, Settings(), "scope:test") == ()

    actions = _connector_actions(
        {
            **ctx,
            "connector_action_credentials": object(),
        },
        Settings(),
        "scope:test",
    )
    assert [action.manifest.name for action in actions] == ["calendar_create"]
    assert actions[0].manifest.idempotency is ConnectorActionIdempotency.required
    assert actions[0].manifest.approval is ConnectorActionApproval.tainted
    assert (
        await actions[0].action(
            {},
            ToolContext(scope_id="scope:test", session_id="session"),
        )
        == binding.id
    )


async def test_recurring_reconciliation_obeys_cadence_and_deduplicates_dispatch() -> None:
    manifest = ConnectorManifest(
        id="recurring",
        name="Recurring",
        description="recurring fixture",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
        default_sync_cadence_seconds=60,
        renewal=ConnectorRenewalPolicy(120),
    )

    class RecurringProvider(BaseConnectorProvider):
        async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
            return ConnectorSyncResult()

        async def renew(self, context: ConnectorOperationContext) -> ConnectorRenewalResult:
            return ConnectorRenewalResult()

    RecurringProvider.manifest = manifest
    registry = ConnectorRegistry(
        (ConnectorRegistration(manifest, RecurringProvider, "tests.recurring"),)
    )
    repository = InMemoryConnectorRepository("scope:test")
    binding = await repository.upsert_binding(
        "recurring",
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
        sync_cadence_seconds=manifest.default_sync_cadence_seconds,
        renewal=manifest.renewal,
    )
    assert binding.next_sync_at is not None
    assert binding.next_renewal_at is not None
    jobs = InMemoryJobStore("scope:test")
    service = ConnectorService(registry, repository, jobs=jobs)
    now = binding.next_sync_at
    ctx = {
        "connector_sync_service": service,
        "connector_clock": lambda: now,
        "job_settings": Settings(),
    }
    assert await reconcile_connectors_tick(ctx) == 1
    assert await reconcile_connectors_tick(ctx) == 0
    queued = await jobs.list()
    assert len(queued) == 1
    assert queued[0].kind == CONNECTOR_SYNC_JOB_KIND
    advanced = await repository.get_binding("recurring")
    assert advanced is not None
    assert advanced.next_sync_at == now + timedelta(seconds=60)
    ctx["connector_clock"] = lambda: binding.next_renewal_at
    assert await reconcile_connectors_tick(ctx) == 1
    queued = await jobs.list()
    assert {job.kind for job in queued} == {
        CONNECTOR_RENEW_JOB_KIND,
        CONNECTOR_SYNC_JOB_KIND,
    }


async def test_recurring_lease_reclaims_crash_without_duplicate_job() -> None:
    manifest = ConnectorManifest(
        id="reclaim",
        name="Reclaim",
        description="reclaim fixture",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
        default_sync_cadence_seconds=30,
    )

    class ReclaimProvider(BaseConnectorProvider):
        async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
            return ConnectorSyncResult()

    ReclaimProvider.manifest = manifest
    registry = ConnectorRegistry(
        (ConnectorRegistration(manifest, ReclaimProvider, "tests.reclaim"),)
    )
    repository = InMemoryConnectorRepository("scope:test")
    binding = await repository.upsert_binding(
        "reclaim",
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
        sync_cadence_seconds=30,
    )
    assert binding.next_sync_at is not None
    due = binding.next_sync_at
    first = (await repository.claim_due_schedules(due, limit=1, lease_seconds=10))[0]
    assert first.operation is ConnectorScheduleOperation.sync
    assert (
        await repository.claim_due_schedules(
            due + timedelta(seconds=5),
            limit=1,
            lease_seconds=10,
        )
        == []
    )
    reclaimed = (
        await repository.claim_due_schedules(
            due + timedelta(seconds=11),
            limit=1,
            lease_seconds=10,
        )
    )[0]
    jobs = InMemoryJobStore("scope:test")
    service = ConnectorService(registry, repository, jobs=jobs)
    first_job = await service.enqueue_recurring(first)
    reclaimed_job = await service.enqueue_recurring(reclaimed)
    assert reclaimed_job.id == first_job.id
    assert len(await jobs.list()) == 1


async def test_recurring_skips_configured_and_disabled_bindings() -> None:
    manifest = ConnectorManifest(
        id="disabled",
        name="Disabled",
        description="disabled fixture",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
        default_sync_cadence_seconds=30,
    )

    class DisabledProvider(BaseConnectorProvider):
        async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
            raise AssertionError("disabled connector must not run")

    DisabledProvider.manifest = manifest
    registry = ConnectorRegistry(
        (
            ConnectorRegistration(
                manifest,
                DisabledProvider,
                "tests.disabled",
                enabled=lambda: False,
            ),
        )
    )
    repository = InMemoryConnectorRepository("scope:test")
    configured = await repository.upsert_binding(
        "configured",
        ConnectorBindingDraft(),
        ConnectorBindingStatus.configured,
        sync_cadence_seconds=30,
    )
    disabled = await repository.upsert_binding(
        "disabled",
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
        sync_cadence_seconds=30,
    )
    assert configured.next_sync_at is None
    assert disabled.next_sync_at is not None
    jobs = InMemoryJobStore("scope:test")
    service = ConnectorService(registry, repository, jobs=jobs)
    assert (
        await service.reconcile_recurring(
            disabled.next_sync_at,
            limit=10,
            lease_seconds=10,
            retry_base_seconds=5,
            retry_max_seconds=60,
        )
        == 0
    )
    assert await jobs.list() == []


async def test_recurring_dispatch_failure_backs_off_and_records_degraded_health() -> None:
    manifest = ConnectorManifest(
        id="unavailable",
        name="Unavailable",
        description="unavailable fixture",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
        default_sync_cadence_seconds=30,
    )

    class UnavailableProvider(BaseConnectorProvider):
        pass

    UnavailableProvider.manifest = manifest

    def unavailable() -> None:
        raise ModuleNotFoundError("missing optional", name="missing_sdk")

    registry = ConnectorRegistry(
        (
            ConnectorRegistration(
                manifest,
                UnavailableProvider,
                "tests.unavailable",
                availability=unavailable,
            ),
        )
    )
    repository = InMemoryConnectorRepository("scope:test")
    binding = await repository.upsert_binding(
        "unavailable",
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
        sync_cadence_seconds=30,
    )
    assert binding.next_sync_at is not None
    service = ConnectorService(
        registry,
        repository,
        jobs=InMemoryJobStore("scope:test"),
    )
    assert (
        await service.reconcile_recurring(
            binding.next_sync_at,
            limit=10,
            lease_seconds=10,
            retry_base_seconds=5,
            retry_max_seconds=60,
        )
        == 0
    )
    updated = await repository.get_binding("unavailable")
    assert updated is not None
    assert updated.status is ConnectorBindingStatus.degraded
    assert updated.sync_failures == 1
    assert updated.next_sync_at == binding.next_sync_at + timedelta(seconds=5)
    assert updated.error_code == "connector_schedule_dispatch_failed"


async def test_recurring_lease_loss_while_recording_failure_does_not_abort_batch() -> None:
    first_manifest = ConnectorManifest(
        id="a_unavailable",
        name="First unavailable",
        description="first unavailable fixture",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
        default_sync_cadence_seconds=30,
    )
    second_manifest = ConnectorManifest(
        id="b_unavailable",
        name="Second unavailable",
        description="second unavailable fixture",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
        default_sync_cadence_seconds=30,
    )

    class FirstUnavailableProvider(BaseConnectorProvider):
        pass

    class SecondUnavailableProvider(BaseConnectorProvider):
        pass

    FirstUnavailableProvider.manifest = first_manifest
    SecondUnavailableProvider.manifest = second_manifest

    def unavailable() -> None:
        raise ModuleNotFoundError("missing optional", name="missing_sdk")

    registry = ConnectorRegistry(
        (
            ConnectorRegistration(
                first_manifest,
                FirstUnavailableProvider,
                "tests.first_unavailable",
                availability=unavailable,
            ),
            ConnectorRegistration(
                second_manifest,
                SecondUnavailableProvider,
                "tests.second_unavailable",
                availability=unavailable,
            ),
        )
    )

    class LeaseLossRepository(InMemoryConnectorRepository):
        def __init__(self, scope_id: str) -> None:
            super().__init__(scope_id)
            self.fail_calls = 0

        async def fail_schedule(
            self,
            lease: ConnectorScheduleLease,
            *,
            retry_at: datetime,
            error_code: str,
            error_summary: str,
        ) -> None:
            self.fail_calls += 1
            if self.fail_calls == 1:
                raise ConnectorScheduleLeaseLostError("connector schedule lease was lost")
            await super().fail_schedule(
                lease,
                retry_at=retry_at,
                error_code=error_code,
                error_summary=error_summary,
            )

    repository = LeaseLossRepository("scope:test")
    first = await repository.upsert_binding(
        first_manifest.id,
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
        sync_cadence_seconds=30,
    )
    second = await repository.upsert_binding(
        second_manifest.id,
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
        sync_cadence_seconds=30,
    )
    assert first.next_sync_at is not None
    assert second.next_sync_at is not None
    due_at = max(first.next_sync_at, second.next_sync_at)

    service = ConnectorService(
        registry,
        repository,
        jobs=InMemoryJobStore("scope:test"),
    )
    assert (
        await service.reconcile_recurring(
            due_at,
            limit=10,
            lease_seconds=10,
            retry_base_seconds=5,
            retry_max_seconds=60,
        )
        == 0
    )

    assert repository.fail_calls == 2
    updated = await repository.get_binding(second_manifest.id)
    assert updated is not None
    assert updated.status is ConnectorBindingStatus.degraded
    assert updated.sync_failures == 1


async def test_renewal_expiry_behavior_revokes_before_dispatch() -> None:
    repository = InMemoryConnectorRepository("scope:test")
    binding = await repository.upsert_binding(
        "expiring",
        ConnectorBindingDraft(),
        ConnectorBindingStatus.connected,
        renewal=ConnectorRenewalPolicy(
            30,
            ConnectorRenewalExpiryBehavior.revoked,
        ),
        renewal_expires_at=datetime.now(UTC) + timedelta(seconds=1),
    )
    assert binding.next_renewal_at is not None
    assert (
        await repository.claim_due_schedules(
            binding.next_renewal_at,
            limit=10,
            lease_seconds=10,
        )
        == []
    )
    expired = await repository.get_binding("expiring")
    assert expired is not None
    assert expired.status is ConnectorBindingStatus.revoked
    assert expired.error_code == "connector_renewal_expired"


# --- Agent-scoped connector jobs + cross-scope reconciliation (review finding 1) -------------

_SYNC_SCOPE_A = "agent:orga/ag1"
_SYNC_SCOPE_B = "agent:orgb/ag2"


def _sync_registry() -> ConnectorRegistry:
    return ConnectorRegistry(
        (ConnectorRegistration(Provider.manifest, Provider, "tests.worker_fixture"),)
    )


async def _connected_service(
    scope: str,
    *,
    outbox: InMemoryJobDispatchOutbox | None = None,
    schedule_index: InMemoryConnectorScheduleIndex | None = None,
    dispatched: list[tuple[str, str]] | None = None,
) -> ConnectorService:
    repository = InMemoryConnectorRepository(scope)
    await repository.upsert_binding(
        "worker_fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )

    async def _dispatch(dispatch_scope: str, job_id: str) -> None:
        if dispatched is not None:
            dispatched.append((dispatch_scope, job_id))

    return ConnectorService(
        _sync_registry(),
        repository,
        jobs=InMemoryJobStore(scope),
        dispatch_job=_dispatch,
        dispatch_outbox=outbox,
        schedule_index=schedule_index,
    )


async def test_agent_scoped_sync_records_dispatch_intent() -> None:
    # Finding 1: a connector sync enqueued under a per-Agent scope records a global dispatch
    # intent keyed by that scope (never web:local) so the cross-scope reconciler can recover it.
    outbox = InMemoryJobDispatchOutbox()
    dispatched: list[tuple[str, str]] = []
    service = await _connected_service(_SYNC_SCOPE_A, outbox=outbox, dispatched=dispatched)
    job = await service.enqueue_sync("worker_fixture")
    intents = await outbox.claim_due(worker_id="w1")
    assert [(i.job_id, i.scope_id, i.kind) for i in intents] == [
        (job.id, _SYNC_SCOPE_A, CONNECTOR_SYNC_JOB_KIND)
    ]
    assert dispatched[-1] == (_SYNC_SCOPE_A, job.id)


async def test_run_job_executes_agent_scoped_connector_sync() -> None:
    # run_job binds the job's own per-Agent scope and runs connector.sync there (no web:local).
    scope = _SYNC_SCOPE_A
    service = await _connected_service(scope)
    job = await service.enqueue_sync("worker_fixture")
    registry = JobRegistry()
    register_connector_jobs(registry, service, Settings())
    enqueued: list[tuple[str, tuple[Any, ...]]] = []

    async def _enqueue(name: str, *args: object, **_o: object) -> None:
        enqueued.append((name, args))

    ctx: dict[str, Any] = {
        "durable_scope": "web:local",
        "job_settings": Settings(),
        "jobs": service.jobs,
        "job_registry": registry,
        "enqueue": _enqueue,
    }
    assert await run_job(ctx, scope, job.id) == JobStatus.succeeded.value


async def test_lost_connector_enqueue_is_recovered_by_reconciler() -> None:
    # Crash/retry: a lost immediate dispatch leaves the intent, and the cross-scope job reconciler
    # re-dispatches run_job with the job's own scope (finding 1).
    scope = _SYNC_SCOPE_A
    outbox = InMemoryJobDispatchOutbox()

    async def _failing_dispatch(dispatch_scope: str, job_id: str) -> None:
        raise RuntimeError("queue down")

    repository = InMemoryConnectorRepository(scope)
    await repository.upsert_binding(
        "worker_fixture", ConnectorBindingDraft(), ConnectorBindingStatus.connected
    )
    jobs = InMemoryJobStore(scope)
    service = ConnectorService(
        _sync_registry(),
        repository,
        jobs=jobs,
        dispatch_job=_failing_dispatch,
        dispatch_outbox=outbox,
    )
    try:
        job = await service.enqueue_sync("worker_fixture")
    except RuntimeError:
        job = (await jobs.list())[0]
    # The intent survived the failed enqueue.
    assert scope in await outbox.active_scopes()

    enqueued: list[tuple[str, tuple[Any, ...]]] = []

    async def _enqueue(name: str, *args: object, **_o: object) -> None:
        enqueued.append((name, args))

    ctx: dict[str, Any] = {
        "durable_scope": "web:local",
        "job_dispatch_outbox": outbox,
        "jobs": jobs,
        "enqueue": _enqueue,
        "job_settings": Settings(),
    }
    dispatched = await reconcile_job_dispatch_tick(ctx)
    assert dispatched == 1
    assert ("run_job", (scope, job.id)) in enqueued


async def test_reconcile_connectors_enumerates_two_scopes_from_index() -> None:
    # Finding 1: the recurring reconciler fires due schedules across every scope in the global
    # schedule index — not just the pinned durable_scope.
    manifest = ConnectorManifest(
        id="recurring",
        name="Recurring",
        description="recurring fixture",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
        default_sync_cadence_seconds=60,
    )

    class RecurringProvider(BaseConnectorProvider):
        async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
            return ConnectorSyncResult()

    RecurringProvider.manifest = manifest
    registry = ConnectorRegistry(
        (ConnectorRegistration(manifest, RecurringProvider, "tests.recurring"),)
    )
    index = InMemoryConnectorScheduleIndex()
    services: dict[str, ConnectorService] = {}
    due_times: list[datetime] = []
    for scope in (_SYNC_SCOPE_A, _SYNC_SCOPE_B):
        repository = InMemoryConnectorRepository(scope)
        binding = await repository.upsert_binding(
            "recurring",
            ConnectorBindingDraft(),
            ConnectorBindingStatus.connected,
            sync_cadence_seconds=manifest.default_sync_cadence_seconds,
        )
        assert binding.next_sync_at is not None
        due_times.append(binding.next_sync_at)
        services[scope] = ConnectorService(
            registry, repository, jobs=InMemoryJobStore(scope), schedule_index=index
        )
        await index.record(scope)

    assert await index.active_scopes() == {_SYNC_SCOPE_A, _SYNC_SCOPE_B}
    now = max(due_times)
    ctx: dict[str, Any] = {
        "job_settings": Settings(),
        "connector_clock": lambda: now,
        "connector_schedule_index": index,
        "connector_scope_factory": lambda scope: services[scope],
    }
    # One due sync schedule fires in each of the two scopes.
    assert await reconcile_connectors_tick(ctx) == 2
    for scope in (_SYNC_SCOPE_A, _SYNC_SCOPE_B):
        service_jobs = services[scope].jobs
        assert service_jobs is not None
        queued = await service_jobs.list()
        assert [job.kind for job in queued] == [CONNECTOR_SYNC_JOB_KIND]


async def test_setup_registers_scope_and_revoke_discards_it() -> None:
    # A binding that arms a recurring schedule registers its scope in the global index; revoking
    # the last scheduled binding discards it (self-healing).
    manifest = ConnectorManifest(
        id="recurring",
        name="Recurring",
        description="recurring fixture",
        auth_kind=ConnectorAuthKind.url,
        capabilities=(ConnectorCapability.sync,),
        default_sync_cadence_seconds=60,
    )

    class RecurringProvider(BaseConnectorProvider):
        async def sync(self, context: ConnectorOperationContext) -> ConnectorSyncResult:
            return ConnectorSyncResult()

        async def revoke(self, context: ConnectorOperationContext) -> None:
            return None

    RecurringProvider.manifest = manifest
    registry = ConnectorRegistry(
        (ConnectorRegistration(manifest, RecurringProvider, "tests.recurring"),)
    )
    index = InMemoryConnectorScheduleIndex()
    repository = InMemoryConnectorRepository(_SYNC_SCOPE_A)
    service = ConnectorService(
        registry, repository, jobs=InMemoryJobStore(_SYNC_SCOPE_A), schedule_index=index
    )
    await service.save_setup(
        "recurring",
        ConnectorSetupResult(
            binding=ConnectorBindingDraft(),
            status=ConnectorBindingStatus.connected,
        ),
    )
    assert await index.active_scopes() == {_SYNC_SCOPE_A}
    await service.revoke("recurring")
    assert await index.active_scopes() == set()
