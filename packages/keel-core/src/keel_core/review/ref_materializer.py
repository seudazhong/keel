"""Control-plane PR ref materialization for read-only reviews (WS-R).

Turns a PR's exact base/head commit SHAs into objects that are actually present in the
project's authoritative repository, so the review can materialize a worktree at those exact
commits — never by treating a PR *number* as a Git ref.

Token isolation is the whole point of doing this on the control plane:

* the JIT GitHub installation token is minted here and passed to ``git`` **only** through an
  environment-supplied ``http.extraHeader`` (:meth:`LocalCodingStorage.fetch_commits`), so it
  never appears in a command argument, in on-disk repository config, or in a log line;
* the review sandbox/worktree only ever sees the (already fetched) objects, never the token.

The remote URL is normalized + host-allow-listed (SSRF defense) and redirects are refused. A PR
head that lives in a fork is fetchable from the base repository because GitHub exposes the PR
head commit there, so both SHAs are fetched from the project's bound repository.
"""

from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass

from keel_core.coding.local import LocalCodingStorage
from keel_core.coding.models import ProjectId
from keel_core.projects.service import GitHubIntegration, ProjectService

from .errors import ReviewValidationError


def _basic_auth_header(token: str) -> str:
    """A GitHub installation-token ``Authorization`` header (``x-access-token`` basic auth)."""
    if not token or "\r" in token or "\n" in token:
        raise ReviewValidationError("invalid installation token")
    encoded = base64.b64encode(f"x-access-token:{token}".encode()).decode("ascii")
    return f"Basic {encoded}"


@dataclass
class GitHubRefMaterializer:
    """Fetch a PR's exact commits into the authoritative repo with an isolated JIT token."""

    projects: ProjectService
    github: GitHubIntegration
    storage: LocalCodingStorage

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
    ) -> None:
        handle = await self.projects.get_active_git_handle(org_id, project_id)
        if handle is None:
            raise ReviewValidationError(
                "project has no authoritative repository to fetch review refs into"
            )
        # Normalize + allow-list the remote (SSRF defense) before any token is minted or used.
        safe_url = self.github.safe_clone_url(clone_url, repo_full_name)
        token = await self.github.tokens.get_token(installation_id)
        auth_header = _basic_auth_header(token.token)
        # The fetch is synchronous (subprocess); run it off the event loop.
        await asyncio.to_thread(
            self.storage.fetch_commits,
            ProjectId(handle),
            safe_url,
            [base_sha, head_sha],
            auth_header=auth_header,
        )


__all__ = ["GitHubRefMaterializer"]
