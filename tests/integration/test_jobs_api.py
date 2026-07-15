"""Integration: scope-bound jobs list/detail/cancel API and RBAC."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport
from sqlalchemy.ext.asyncio import AsyncEngine

from keel_core.jobs import (
    CancelMode,
    JobError,
    JobStatus,
    JobTerminalIntent,
    PostgresJobStore,
)
from keel_server.api.v1 import router
from keel_server.auth import parse_api_keys

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)


@pytest_asyncio.fixture
async def jobs_client(
    migrated_db: AsyncEngine,
) -> AsyncIterator[tuple[httpx.AsyncClient, str, PostgresJobStore]]:
    scope = "api:jobs"
    store = PostgresJobStore(migrated_db, scope)
    app = FastAPI()
    app.include_router(router)
    app.state.durable_scope = scope
    app.state.jobs = store
    app.state.api_keys = parse_api_keys("vw:viewer,op:operator")
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client, scope, store


async def _seed(
    store: PostgresJobStore,
    key: str,
    *,
    kind: str = "test.echo",
    now: datetime = _NOW,
    cancel_mode: CancelMode = CancelMode.immediate,
) -> str:
    row, _ = await store.enqueue_once(
        kind=kind,
        payload={"private": "not exposed"},
        target_session_id=None,
        idempotency_key=key,
        max_attempts=3,
        cancel_mode=cancel_mode,
        now=now,
    )
    return row.id


async def test_viewer_lists_filters_and_reads_detail(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, store = jobs_client
    older = await _seed(store, "older", kind="test.a")
    newer = await _seed(store, "newer", kind="test.b", now=_NOW + timedelta(seconds=1))
    headers = {"X-API-Key": "vw"}

    response = await client.get("/v1/jobs", headers=headers)
    assert response.status_code == 200
    assert [row["id"] for row in response.json()] == [newer, older]
    assert "payload" not in response.json()[0]
    assert "scope_id" not in response.json()[0]
    assert "lease_token" not in response.json()[0]
    assert "idempotency_key" not in response.json()[0]

    filtered = await client.get(
        "/v1/jobs",
        params={"kind": "test.a", "status": "queued", "limit": 1},
        headers=headers,
    )
    assert [row["id"] for row in filtered.json()] == [older]
    assert (await client.get("/v1/jobs", params={"limit": 0}, headers=headers)).status_code == 422
    assert (await client.get("/v1/jobs", params={"limit": 101}, headers=headers)).status_code == 422
    assert (
        await client.get("/v1/jobs", params={"status": "unknown"}, headers=headers)
    ).status_code == 422
    detail = await client.get(f"/v1/jobs/{older}", headers=headers)
    assert detail.status_code == 200
    assert detail.json()["status"] == "queued"
    assert detail.json()["cancel_mode"] == "immediate"
    assert detail.json()["cancel_requested"] is False


async def test_jobs_api_hides_missing_cross_scope_and_malformed_ids(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
    migrated_db: AsyncEngine,
) -> None:
    client, _, _ = jobs_client
    other_id = await _seed(PostgresJobStore(migrated_db, "api:other"), "other")
    viewer = {"X-API-Key": "vw"}
    operator = {"X-API-Key": "op"}

    for job_id in ("missing", other_id, "%00", "%ED%A0%80"):
        assert (await client.get(f"/v1/jobs/{job_id}", headers=viewer)).status_code == 404
        assert (await client.post(f"/v1/jobs/{job_id}/cancel", headers=operator)).status_code == 404


@pytest.mark.parametrize(
    ("configure_store", "configured_scope"),
    [(True, None), (True, "api:wrong"), (False, "api:expected")],
)
async def test_jobs_api_requires_explicit_matching_store_and_scope(
    migrated_db: AsyncEngine,
    configure_store: bool,
    configured_scope: str | None,
) -> None:
    app = FastAPI()
    app.include_router(router)
    app.state.api_keys = parse_api_keys("vw:viewer")
    if configure_store:
        app.state.jobs = PostgresJobStore(migrated_db, "api:expected")
    if configured_scope is not None:
        app.state.durable_scope = configured_scope
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/v1/jobs", headers={"X-API-Key": "vw"})
    assert response.status_code == 503


async def test_viewer_cannot_cancel_operator_can_and_terminal_is_idempotent(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, store = jobs_client
    job_id = await _seed(store, "cancel")
    assert (
        await client.post(f"/v1/jobs/{job_id}/cancel", headers={"X-API-Key": "vw"})
    ).status_code == 403

    first = await client.post(f"/v1/jobs/{job_id}/cancel", headers={"X-API-Key": "op"})
    second = await client.post(f"/v1/jobs/{job_id}/cancel", headers={"X-API-Key": "op"})
    assert first.status_code == 200
    assert first.json()["status"] == "cancelled"
    assert second.json()["status"] == "cancelled"


async def test_running_cancel_response_exposes_request_flag(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, store = jobs_client
    job_id = await _seed(store, "running-cancel")
    assert await store.claim(job_id, _NOW, 60) is not None
    response = await client.post(f"/v1/jobs/{job_id}/cancel", headers={"X-API-Key": "op"})
    assert response.status_code == 200
    assert response.json()["status"] == "running"
    assert response.json()["cancel_requested"] is True


async def test_final_attempt_expiry_cancel_maps_to_conflict_without_mutation(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, store = jobs_client
    claimed_at = datetime.now(UTC) - timedelta(seconds=2)
    row, _ = await store.enqueue_once(
        kind="test.echo",
        payload={},
        target_session_id=None,
        idempotency_key="cancel-finalizing",
        max_attempts=1,
        cancel_mode=CancelMode.cooperative,
        now=claimed_at,
    )
    assert await store.claim(row.id, claimed_at, 1) is not None
    before = await store.get(row.id)

    response = await client.post(
        f"/v1/jobs/{row.id}/cancel",
        headers={"X-API-Key": "op"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "job is finalizing and cannot be cancelled"
    assert await store.get(row.id) == before


async def test_failed_terminal_reservation_cancel_maps_to_conflict(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, store = jobs_client
    claimed_at = datetime.now(UTC)
    job_id = await _seed(
        store,
        "cancel-failed-reservation",
        now=claimed_at,
        cancel_mode=CancelMode.cooperative,
    )
    lease = await store.claim(job_id, claimed_at, 60)
    assert lease is not None
    reserved = await store.reserve_terminal(
        lease,
        JobTerminalIntent.failed,
        now=claimed_at,
        error=JobError("permanent", "Permanent failure."),
    )

    response = await client.post(
        f"/v1/jobs/{job_id}/cancel",
        headers={"X-API-Key": "op"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "job is finalizing and cannot be cancelled"
    assert await store.get(job_id) == reserved


async def test_cooperative_queued_cancel_response_stays_queued_and_exposes_mode(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, store = jobs_client
    job_id = await _seed(
        store,
        "cooperative-cancel",
        now=_NOW + timedelta(minutes=5),
        cancel_mode=CancelMode.cooperative,
    )

    response = await client.post(
        f"/v1/jobs/{job_id}/cancel",
        headers={"X-API-Key": "op"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["cancel_mode"] == "cooperative"
    assert response.json()["cancel_requested"] is True
    row = await store.get(job_id)
    assert row is not None
    assert row.next_attempt_at <= row.cancel_requested_at  # type: ignore[operator]


async def test_disabled_cancel_maps_to_conflict_and_leaves_job_queued(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, store = jobs_client
    job_id = await _seed(
        store,
        "disabled-cancel",
        cancel_mode=CancelMode.disabled,
    )

    response = await client.post(
        f"/v1/jobs/{job_id}/cancel",
        headers={"X-API-Key": "op"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "job does not allow cancellation"
    row = await store.get(job_id)
    assert row is not None
    assert row.status is JobStatus.queued
    assert row.cancel_requested_at is None


async def test_no_generic_create_retry_or_inject_routes(
    jobs_client: tuple[httpx.AsyncClient, str, PostgresJobStore],
) -> None:
    client, _, _ = jobs_client
    headers = {"X-API-Key": "op"}
    assert (await client.post("/v1/jobs", headers=headers, json={})).status_code == 405
    assert (await client.post("/v1/jobs/job_1/retry", headers=headers)).status_code == 404
    assert (await client.post("/v1/jobs/job_1/inject", headers=headers)).status_code == 404
