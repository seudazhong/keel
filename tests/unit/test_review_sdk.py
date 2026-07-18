"""SDK typed surface tests for the read-only review endpoints."""

from __future__ import annotations

import json

import httpx

from keel_sdk import CreateReviewRequest, KeelClient


def _status_body(review_id: str, status: str = "completed") -> dict[str, object]:
    return {
        "review_id": review_id,
        "org_id": "org-1",
        "project_id": "prj-1",
        "run_id": review_id,
        "status": status,
        "source": "branch",
        "head": "main",
        "base": None,
        "model": "m",
        "created_at": "2024-01-01T00:00:00+00:00",
        "updated_at": "2024-01-01T00:00:01+00:00",
        "finding_count": 1,
        "severity_counts": {"high": 1},
        "prompt_tokens": 10,
        "completion_tokens": 5,
        "cost_usd": 0.01,
        "report_json_sha256": "abc",
        "report_markdown_sha256": "def",
        "error_kind": None,
        "error_message": None,
    }


async def test_create_review_sends_idempotency_and_org() -> None:
    seen: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["org"] = request.headers.get("X-Keel-Org")
        seen["idem"] = request.headers.get("Idempotency-Key")
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            202,
            json={
                "review_id": "rev_1",
                "run_id": "rev_1",
                "status": "pending",
                "idempotency_key": "k-1",
                "created": True,
            },
        )

    async with httpx.AsyncClient(
        base_url="https://keel.test", transport=httpx.MockTransport(handler)
    ) as transport:
        client = KeelClient("https://ignored.test", client=transport)
        resp = await client.create_review(
            "org-1", "prj-1", CreateReviewRequest(head="main", idempotency_key="k-1")
        )

    assert resp.review_id == "rev_1"
    assert resp.created is True
    assert seen["path"] == "/v1/projects/prj-1/reviews"
    assert seen["org"] == "org-1"
    assert seen["idem"] == "k-1"
    assert seen["body"]["head"] == "main"


async def test_get_review_and_reports() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/report.md"):
            return httpx.Response(
                200, text="# Code Review", headers={"content-type": "text/markdown"}
            )
        if path.endswith("/report"):
            return httpx.Response(200, json={"review_id": "rev_1", "findings": []})
        if path == "/v1/projects/prj-1/reviews":
            return httpx.Response(200, json=[_status_body("rev_1")])
        return httpx.Response(200, json=_status_body("rev_1"))

    async with httpx.AsyncClient(
        base_url="https://keel.test", transport=httpx.MockTransport(handler)
    ) as transport:
        client = KeelClient("https://ignored.test", client=transport)
        status = await client.get_review("org-1", "prj-1", "rev_1")
        listing = await client.list_reviews("org-1", "prj-1")
        report = await client.get_review_report("org-1", "prj-1", "rev_1")
        markdown = await client.get_review_report_markdown("org-1", "prj-1", "rev_1")

    assert status.status == "completed"
    assert status.finding_count == 1
    assert listing[0].review_id == "rev_1"
    assert report["review_id"] == "rev_1"
    assert markdown.startswith("# Code Review")
