"""Change-set reference resolution for read-only reviews (WS-R).

A review must review **exact commit SHAs**, never a symbolic PR number treated as a Git ref.
This module owns the resolution policy that turns a :class:`~keel_core.review.models.ReviewRequest`
into a concrete :class:`MaterializationPlan`:

* **branch** — materialize the branch tip; an omitted base derives the *merge-base* against the
  project default branch (the branch's own changes), after a safe fetch.
* **commit** — materialize the commit; an omitted base defaults to its first parent (documented
  and validated; the empty tree for a root commit).
* **pull_request** — the PR number is resolved on the **control plane** (GitHub App) to exact
  base/head SHAs and the bound repository, the required refs are fetched with a JIT token that
  never enters the sandbox/worktree/logs, and the review runs against those SHAs. When no PR
  resolver is configured or GitHub is unavailable, resolution fails **explicitly** — a PR number
  is never passed through as a Git ref.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .errors import ReviewValidationError
from .models import ReviewRequest, ReviewSource


@dataclass(frozen=True, slots=True)
class ResolvedPullRequest:
    """Exact PR endpoints resolved on the control plane from a PR number."""

    base_sha: str
    head_sha: str
    repo_full_name: str


@dataclass(frozen=True, slots=True)
class MaterializationPlan:
    """How the review service should materialize + bound the change set.

    ``materialize_ref`` is what to check out (a branch name, a commit sha, or a PR head sha —
    never a PR number). ``base_sha_hint``/``head_sha_hint`` are pre-resolved control-plane SHAs
    (PR path); when absent the service resolves locally. ``derive_base_from_default`` requests a
    merge-base against ``default_branch`` when the request omits an explicit base.
    """

    materialize_ref: str
    base_ref: str | None
    default_branch: str | None
    derive_base_from_default: bool
    base_sha_hint: str | None = None
    head_sha_hint: str | None = None


@runtime_checkable
class PullRequestResolver(Protocol):
    """Control-plane PR resolver: PR number -> exact SHAs + repo, refs fetched, token isolated."""

    async def resolve(
        self, *, org_id: str, project_id: str, agent_id: str | None, pr_number: int
    ) -> ResolvedPullRequest: ...


async def build_materialization_plan(
    request: ReviewRequest,
    *,
    default_branch: str | None,
    pr_resolver: PullRequestResolver | None,
) -> MaterializationPlan:
    """Resolve a request into a concrete, SHA-exact materialization plan (fail closed)."""
    if request.source is ReviewSource.pull_request:
        if pr_resolver is None:
            raise ReviewValidationError(
                "pull-request review requires GitHub PR resolution, which is not available"
            )
        if not request.head.isdigit():
            raise ReviewValidationError("pull_request head must be the PR number")
        resolved = await pr_resolver.resolve(
            org_id=request.org_id,
            project_id=request.project_id,
            agent_id=request.agent_id,
            pr_number=int(request.head),
        )
        # Materialize + review the EXACT head sha; base is the exact PR base sha. Never the PR
        # number as a ref.
        return MaterializationPlan(
            materialize_ref=resolved.head_sha,
            base_ref=resolved.base_sha,
            default_branch=default_branch,
            derive_base_from_default=False,
            base_sha_hint=resolved.base_sha,
            head_sha_hint=resolved.head_sha,
        )

    if request.source is ReviewSource.commit:
        # Commit review: materialize the commit; an omitted base defaults to its first parent.
        return MaterializationPlan(
            materialize_ref=request.head,
            base_ref=request.base,
            default_branch=default_branch,
            derive_base_from_default=False,
        )

    # Branch review: materialize the branch tip; an omitted base derives the merge-base against
    # the project default branch (the branch's own changes).
    return MaterializationPlan(
        materialize_ref=request.head,
        base_ref=request.base,
        default_branch=default_branch,
        derive_base_from_default=request.base is None,
    )


__all__ = [
    "MaterializationPlan",
    "PullRequestResolver",
    "ResolvedPullRequest",
    "build_materialization_plan",
]
