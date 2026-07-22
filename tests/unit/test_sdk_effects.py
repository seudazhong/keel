"""Unit tests for the typed SDK's Effect ledger methods (R1B) — request shape + typed
response parsing, against a mocked transport (no live server)."""

from __future__ import annotations

import httpx
import pytest

from keel_sdk import KeelClient
from keel_sdk.models import EffectRetryEligibility, EffectSummary

_EFFECT = {
    "id": "eff-1",
    "org_id": "org-a",
    "agent_id": "agent-1",
    "actor_id": "user-1",
    "run_id": "run-1",
    "tool_name": "email_send",
    "provider": "gmail",
    "resource_id": "",
    "action_name": "email_send",
    "action_hash": "hash-1",
    "idempotency_key": "k1",
    "canonical_args": '{"to":"a@example.com"}',
    "status": "confirmed",
    "attempt": 1,
    "provider_ref": "msg-1",
    "result": "sent",
    "error": "",
    "reconciliation_attempts": 0,
    "reconciled_at": None,
    "created_at": "2026-07-22T00:00:00Z",
    "updated_at": "2026-07-22T00:00:01Z",
}


def _client(handler: httpx.MockTransport) -> KeelClient:
    return KeelClient(
        "http://test", client=httpx.AsyncClient(transport=handler, base_url="http://test")
    )


async def test_list_effects_sends_scope_headers_and_parses_typed_rows() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["org"] = request.headers.get("X-Keel-Org")
        seen["agent"] = request.headers.get("X-Keel-Agent")
        seen["status_param"] = request.url.params.get("status")
        return httpx.Response(200, json=[_EFFECT])

    client = _client(httpx.MockTransport(handler))
    rows = await client.list_effects("org-a", "agent-1", status="confirmed")
    await client.aclose()

    assert seen == {
        "path": "/v1/effects",
        "org": "org-a",
        "agent": "agent-1",
        "status_param": "confirmed",
    }
    assert rows == [EffectSummary.model_validate(_EFFECT)]


async def test_get_effect_parses_typed_row() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/effects/eff-1"
        return httpx.Response(200, json=_EFFECT)

    client = _client(httpx.MockTransport(handler))
    effect = await client.get_effect("org-a", "agent-1", "eff-1")
    await client.aclose()
    assert effect.id == "eff-1"
    assert effect.status == "confirmed"


async def test_reconcile_effect_posts_to_reconcile_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/v1/effects/eff-1/reconcile"
        return httpx.Response(200, json={**_EFFECT, "status": "reconciled_confirmed"})

    client = _client(httpx.MockTransport(handler))
    effect = await client.reconcile_effect("org-a", "agent-1", "eff-1")
    await client.aclose()
    assert effect.status == "reconciled_confirmed"


async def test_check_effect_retry_eligibility_parses_nested_effect() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/effects/eff-1/retry"
        return httpx.Response(
            200,
            json={
                "effect": {**_EFFECT, "status": "failed"},
                "retryable": True,
                "detail": "eligible: invoke the owning tool again with the same idempotency_key",
            },
        )

    client = _client(httpx.MockTransport(handler))
    eligibility = await client.check_effect_retry_eligibility("org-a", "agent-1", "eff-1")
    await client.aclose()
    assert isinstance(eligibility, EffectRetryEligibility)
    assert eligibility.retryable is True
    assert eligibility.effect.status == "failed"


async def test_reconcile_effect_raises_on_conflict_status() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": "not unknown"})

    client = _client(httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError):
        await client.reconcile_effect("org-a", "agent-1", "eff-1")
    await client.aclose()
