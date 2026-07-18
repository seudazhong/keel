"""Concrete control-plane GitHub App pull-request resolver for read-only reviews (WS-R).

Turns a PR *number* into the exact ``base``/``head`` commit SHAs of the project's bound
repository, using the GitHub App on the control plane. The just-in-time installation token is
used only for the metadata call (and an optional ref fetch); it never reaches the review
service, the worktree, or the model. A PR number is never used as a Git ref.

Binding is verified two ways before any SHA is trusted:

* the project must be bound to a GitHub repository (installation + ``full_name``), and
* the PR's **base** repository ``full_name`` must equal that bound repository — a PR whose base
  is a different repo (or whose payload is malformed) is rejected.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from keel_core.projects.github.client import (
    GitHubAuthError,
    GitHubError,
    GitHubNotFoundError,
    GitHubRateLimitError,
)
from keel_core.projects.service import GitHubIntegration, ProjectService

from .errors import ReviewProviderUnavailable, ReviewValidationError
from .refs import ResolvedPullRequest

_SHA = re.compile(r"\A[0-9a-f]{40}\Z")

# Exception class-name fragments that indicate a *transient* transport failure (timeout,
# connection reset, network unreachable) — safe to retry rather than terminalize the review.
_TRANSIENT_TRANSPORT_MARKERS = (
    "timeout",
    "connect",
    "connection",
    "network",
    "readtimeout",
    "writetimeout",
    "pooltimeout",
    "remoteprotocol",
    "temporar",
    "unavailable",
)


def _classify_github_failure(exc: Exception, pr_number: int) -> Exception:
    """Map a GitHub resolution failure to a retryable or permanent typed review error.

    Rate-limit (429), upstream 5xx, and transport/timeout/network failures are *transient*
    (:class:`ReviewProviderUnavailable`, retryable, carrying any ``Retry-After`` backoff hint);
    a not-found PR, an auth/binding failure, or any other 4xx is *permanent*
    (:class:`ReviewValidationError`) — retrying can never make it succeed.
    """
    if isinstance(exc, GitHubRateLimitError):
        return ReviewProviderUnavailable(
            f"GitHub rate limit while resolving pull request #{pr_number}",
            retry_after=getattr(exc, "retry_after", None),
        )
    if isinstance(exc, GitHubNotFoundError | GitHubAuthError):
        return ReviewValidationError(
            f"could not resolve pull request #{pr_number} from GitHub: {exc.__class__.__name__}"
        )
    if isinstance(exc, GitHubError):
        if getattr(exc, "is_transient", False):
            return ReviewProviderUnavailable(
                f"GitHub temporarily unavailable while resolving pull request #{pr_number}",
                retry_after=getattr(exc, "retry_after", None),
            )
        return ReviewValidationError(
            f"could not resolve pull request #{pr_number} from GitHub: {exc.__class__.__name__}"
        )
    name = f"{exc.__class__.__module__}.{exc.__class__.__name__}".lower()
    if any(marker in name for marker in _TRANSIENT_TRANSPORT_MARKERS):
        return ReviewProviderUnavailable(
            f"GitHub transport failure while resolving pull request #{pr_number}: "
            f"{exc.__class__.__name__}"
        )
    return ReviewValidationError(
        f"could not resolve pull request #{pr_number} from GitHub: {exc.__class__.__name__}"
    )


@runtime_checkable
class RefMaterializer(Protocol):
    """Control-plane hook that fetches a PR's exact commit SHAs into the authoritative repo.

    It runs entirely on the control plane and is responsible for keeping the JIT installation
    token off the review sandbox/worktree/logs. A PR head that lives in a fork is fetchable from
    the base repository (GitHub exposes the PR head commit there), so both SHAs are fetched from
    the project's bound repository.
    """

    async def ensure_commits(
        self,
        *,
        org_id: str,
        project_id: str,
        installation_id: int,
        repo_full_name: str,
        clone_url: str,
        base_sha: str,
        head_sha: str,
    ) -> None: ...


def _endpoint_sha(payload: dict[str, Any], side: str) -> str:
    endpoint = payload.get(side)
    if not isinstance(endpoint, dict):
        raise ReviewValidationError(f"pull-request payload missing '{side}'")
    sha = str(endpoint.get("sha", "")).strip().lower()
    if not _SHA.fullmatch(sha):
        raise ReviewValidationError(f"pull-request '{side}' sha is not a resolved commit")
    return sha


def _endpoint_repo(payload: dict[str, Any], side: str) -> str:
    endpoint = payload.get(side)
    repo = endpoint.get("repo") if isinstance(endpoint, dict) else None
    full_name = repo.get("full_name") if isinstance(repo, dict) else None
    return str(full_name or "").strip()


@dataclass
class GitHubPullRequestResolver:
    """Resolve a PR number to exact base/head SHAs via the GitHub App (token control-plane)."""

    projects: ProjectService
    github: GitHubIntegration
    ensure_refs: RefMaterializer | None = None

    async def resolve(
        self, *, org_id: str, project_id: str, agent_id: str | None, pr_number: int
    ) -> ResolvedPullRequest:
        repo = await self.projects.get_project_repository(org_id, project_id)
        if repo is None:
            raise ReviewValidationError("project is not bound to a GitHub repository")
        try:
            payload = await self.github.resolve_pull_request(
                repo.installation_id, repo.full_name, pr_number
            )
        except (ReviewProviderUnavailable, ReviewValidationError):
            raise
        except Exception as exc:  # noqa: BLE001 — classify transient vs permanent GitHub failure
            raise _classify_github_failure(exc, pr_number) from exc
        if not isinstance(payload, dict):
            raise ReviewValidationError("unexpected pull-request payload")

        base_sha = _endpoint_sha(payload, "base")
        head_sha = _endpoint_sha(payload, "head")
        base_repo = _endpoint_repo(payload, "base")
        # The PR's base repository MUST be the project's bound repository (association check).
        if base_repo.lower() != repo.full_name.lower():
            raise ReviewValidationError(
                "pull-request base repository does not match the project's bound repository"
            )
        # Make the resolved SHAs materializable in the authoritative repo (control plane). The
        # JIT token used here never leaves the control plane — it is passed to git only via an
        # environment-supplied ``http.extraHeader`` and is never handed to the review sandbox.
        if self.ensure_refs is not None:
            await self.ensure_refs.ensure_commits(
                org_id=org_id,
                project_id=project_id,
                installation_id=repo.installation_id,
                repo_full_name=repo.full_name,
                clone_url=repo.clone_url,
                base_sha=base_sha,
                head_sha=head_sha,
            )
        return ResolvedPullRequest(
            base_sha=base_sha, head_sha=head_sha, repo_full_name=repo.full_name
        )


__all__ = ["GitHubPullRequestResolver", "RefMaterializer"]
