"""Unit tests for the erasure governance API (``/v1/erasure``, M3.5)."""

from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient
from lifecycle_helpers import FakePurge, RecordingRedis

from keel_core.jobs import InMemoryJobStore
from keel_core.lifecycle.coordinator import ErasureCoordinator, UnsupportedExternalStep
from keel_core.lifecycle.jobs import ErasureJobHandlers
from keel_core.lifecycle.service import ErasureService
from keel_core.lifecycle.store import InMemoryErasureStore
from keel_server.app import create_app

_SCOPE = "web:local"


class _WorkerCtx:
    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        self.scope_id = _SCOPE

    async def checkpoint(self) -> None:
        return None


def _make_service() -> tuple[ErasureService, ErasureCoordinator]:
    store = InMemoryErasureStore(_SCOPE)
    coord = ErasureCoordinator(
        None,
        store,
        purge=FakePurge(sessions=("s1",)),  # type: ignore[arg-type]
        redis_cleaner=RecordingRedis(),  # type: ignore[arg-type]
        external_steps=(UnsupportedExternalStep("provider_telemetry"),),
    )
    return ErasureService(coord, InMemoryJobStore(_SCOPE)), coord


def test_erasure_endpoints_require_service() -> None:
    with TestClient(create_app()) as client:
        client.app.state.erasure = None  # lite/memory profile has no erasure service
        resp = client.post(
            "/v1/erasure/requests",
            json={"target_kind": "scope", "idempotency_key": "k1"},
        )
        assert resp.status_code == 503


def test_submit_dedupes_and_lists() -> None:
    service, _coord = _make_service()
    with TestClient(create_app()) as client:
        client.app.state.erasure = service

        created = client.post(
            "/v1/erasure/requests",
            json={"target_kind": "scope", "idempotency_key": "k1", "reason": "gdpr"},
        )
        assert created.status_code == 202
        body = created.json()
        request_id = body["request"]["id"]
        assert body["request"]["status"] == "pending"
        assert body["request"]["requested_by"]
        assert body["job_id"]

        again = client.post(
            "/v1/erasure/requests",
            json={"target_kind": "scope", "idempotency_key": "k1"},
        )
        assert again.json()["request"]["id"] == request_id

        listing = client.get("/v1/erasure/requests")
        assert listing.status_code == 200
        assert any(r["id"] == request_id for r in listing.json())


def test_submit_rejects_session_without_target_id() -> None:
    service, _coord = _make_service()
    with TestClient(create_app()) as client:
        client.app.state.erasure = service
        resp = client.post(
            "/v1/erasure/requests",
            json={"target_kind": "session", "idempotency_key": "k2"},
        )
        assert resp.status_code == 400


def test_status_reports_partial_and_retry_enqueues() -> None:
    service, coord = _make_service()
    with TestClient(create_app()) as client:
        client.app.state.erasure = service
        created = client.post(
            "/v1/erasure/requests",
            json={"target_kind": "scope", "idempotency_key": "k1"},
        )
        request_id = created.json()["request"]["id"]
        job_id = created.json()["job_id"]

        # Execute the erasure via the job handler (as the worker would), out-of-band.
        handler = ErasureJobHandlers(coord)
        asyncio.run(handler.erase(_WorkerCtx(job_id), {"request_id": request_id}))

        status = client.get(f"/v1/erasure/requests/{request_id}")
        assert status.status_code == 200
        payload = status.json()
        assert payload["request"]["status"] == "partial"
        assert payload["request"]["external_incomplete"] is True
        telemetry = next(s for s in payload["steps"] if s["step"] == "provider_telemetry")
        assert telemetry["status"] == "unsupported"

        retry = client.post(f"/v1/erasure/requests/{request_id}/retry")
        assert retry.status_code == 200
        assert retry.json()["job_id"]


def test_status_404_for_unknown_request() -> None:
    service, _coord = _make_service()
    with TestClient(create_app()) as client:
        client.app.state.erasure = service
        assert client.get("/v1/erasure/requests/missing").status_code == 404
