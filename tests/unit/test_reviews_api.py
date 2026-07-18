"""Read-only review REST API: trigger, status, report, and read authorization."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from oidc_helpers import make_rsa_key, sign_token
from review_support import CapturingProvider, build_source_repo, finding_json

from keel_core.coding import LocalArtifactStore, LocalCodingStorage, LocalWorktreeStore
from keel_core.identity import (
    IdentityService,
    InMemoryIdentityStore,
    OIDCConfig,
    OIDCVerifier,
    StaticJWKSProvider,
)
from keel_core.projects import InMemoryProjectStore, ProjectService
from keel_core.review import ReviewCoordinator, ReviewService
from keel_core.review.jobs import ReviewJobPayload
from keel_core.runs import InMemoryRunStore
from keel_core.state import InMemoryEventStore
from keel_server.app import create_app

_ISSUER = "https://issuer.example"
_AUD = "keel"
_KEY = make_rsa_key()


def _auth(subject: str, org: str | None = None) -> dict[str, str]:
    token = sign_token(
        _KEY, issuer=_ISSUER, audience=_AUD, subject=subject, email=f"{subject}@example.com"
    )
    headers = {"Authorization": f"Bearer {token}"}
    if org is not None:
        headers["X-Keel-Org"] = org
    return headers


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from keel_core.config import get_settings

    get_settings.cache_clear()
    app = create_app()
    identity_store = InMemoryIdentityStore()
    app.state.identity = IdentityService(identity_store, allow_jit_provisioning=True)
    app.state.oidc_verifier = OIDCVerifier(
        OIDCConfig(issuer=_ISSUER, audiences=frozenset({_AUD})),
        StaticJWKSProvider([_KEY.jwk]),
    )
    app.state.auth_required = False
    app.state.api_keys = {}
    projects = ProjectService(InMemoryProjectStore(), identity_store)
    app.state.projects = projects

    client = TestClient(app)

    # Bootstrap actor + org + project.
    client.get("/v1/identity/me", headers=_auth("alice"))
    org = client.post(
        "/v1/identity/organizations",
        headers=_auth("alice"),
        json={"slug": "acme", "display_name": "Acme"},
    ).json()["organization"]["id"]
    project = client.post(
        "/v1/projects",
        headers=_auth("alice", org),
        json={"slug": "proj-x", "display_name": "P"},
    ).json()
    handle = project["id"]

    # Import a real repo into the coding storage under the project handle.
    build_source_repo(tmp_path)
    storage = LocalCodingStorage(tmp_path / "coding", allow_local_remotes=True)
    storage.import_project(handle, tmp_path / "source", default_branch="main")

    provider = CapturingProvider(
        responses=[
            finding_json(
                file_path="app.py",
                line_start=2,
                line_end=2,
                snippet="return a - b  # BUG: subtraction",
            )
        ]
    )
    review_service = ReviewService(
        worktrees=LocalWorktreeStore(storage),
        artifacts=LocalArtifactStore(storage),
        provider=provider,
    )
    coordinator = ReviewCoordinator(
        projects=projects,
        runs=InMemoryRunStore(),
        review_service=review_service,
        artifacts=LocalArtifactStore(storage),
        scope_id="web:local",
        events=InMemoryEventStore(),
    )
    app.state.review_coordinator = coordinator

    async def _enqueue_review(payload: dict, idem: str) -> None:
        parsed = ReviewJobPayload.model_validate(payload)
        await coordinator.execute_review(parsed.to_request(), run_id=parsed.run_id)

    app.state.enqueue_review = _enqueue_review
    return client, org, project["id"]


def test_trigger_review_and_read_report(env) -> None:
    client, org, project_id = env
    resp = client.post(
        f"/v1/projects/{project_id}/reviews",
        headers={**_auth("alice", org), "Idempotency-Key": "rk-1"},
        json={"source": "branch", "head": "main"},
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["created"] is True
    review_id = body["review_id"]

    # Status now reflects the completed review (enqueue ran the worker inline).
    status_resp = client.get(
        f"/v1/projects/{project_id}/reviews/{review_id}", headers=_auth("alice", org)
    )
    assert status_resp.status_code == 200, status_resp.text
    status = status_resp.json()
    assert status["status"] == "completed"
    assert status["finding_count"] == 1

    # JSON report.
    report = client.get(
        f"/v1/projects/{project_id}/reviews/{review_id}/report", headers=_auth("alice", org)
    )
    assert report.status_code == 200, report.text
    assert report.json()["head_sha"]
    assert len(report.json()["findings"]) == 1

    # Markdown report.
    md = client.get(
        f"/v1/projects/{project_id}/reviews/{review_id}/report.md", headers=_auth("alice", org)
    )
    assert md.status_code == 200
    assert md.headers["content-type"].startswith("text/markdown")
    assert "Code Review" in md.text

    # Listing includes the review.
    listing = client.get(f"/v1/projects/{project_id}/reviews", headers=_auth("alice", org))
    assert listing.status_code == 200
    assert any(item["review_id"] == review_id for item in listing.json())


def test_trigger_is_idempotent(env) -> None:
    client, org, project_id = env
    headers = {**_auth("alice", org), "Idempotency-Key": "same-key"}
    first = client.post(
        f"/v1/projects/{project_id}/reviews", headers=headers, json={"head": "main"}
    ).json()
    second = client.post(
        f"/v1/projects/{project_id}/reviews", headers=headers, json={"head": "main"}
    ).json()
    assert first["run_id"] == second["run_id"]
    assert second["created"] is False


def test_non_member_denied(env) -> None:
    client, org, project_id = env
    # Bob is a valid user but not a member of alice's org.
    client.get("/v1/identity/me", headers=_auth("bob"))
    resp = client.post(
        f"/v1/projects/{project_id}/reviews",
        headers=_auth("bob", org),
        json={"head": "main"},
    )
    assert resp.status_code in (403, 404)


def test_disallowed_model_rejected(env) -> None:
    client, org, project_id = env
    resp = client.post(
        f"/v1/projects/{project_id}/reviews",
        headers=_auth("alice", org),
        json={"head": "main", "model": "totally-unlisted-model"},
    )
    assert resp.status_code == 422, resp.text


def test_review_of_other_project_is_404(env) -> None:
    client, org, project_id = env
    # Trigger a review under the real project.
    review_id = client.post(
        f"/v1/projects/{project_id}/reviews",
        headers={**_auth("alice", org), "Idempotency-Key": "xp-1"},
        json={"head": "main"},
    ).json()["review_id"]
    # Create a second project and try to read the first project's review through it.
    other = client.post(
        "/v1/projects",
        headers=_auth("alice", org),
        json={"slug": "proj-y", "display_name": "Y"},
    ).json()["id"]
    resp = client.get(f"/v1/projects/{other}/reviews/{review_id}", headers=_auth("alice", org))
    assert resp.status_code == 404, resp.text
