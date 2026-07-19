"""Controlled patch-proposal error hierarchy (WS-PP).

Security-relevant failures fail closed (P5): authorization, base-drift detection, bundle
integrity, approval binding and forbidden-path validation raise rather than degrade silently.
Errors never carry a GitHub token, a snippet of source, deploy secrets, or the raw development
task text in their message.
"""

from __future__ import annotations

from keel_core.errors import KeelError


class PatchError(KeelError):
    """Base class for controlled patch-proposal domain errors."""


class PatchValidationError(PatchError, ValueError):
    """A proposal request, bundle, or transition failed strict validation."""


class PatchBoundsExceeded(PatchError):
    """A bounded input/output (diff bytes, changed files, field length) was exceeded."""


class PatchPolicyViolation(PatchError):
    """The generated change violates policy (forbidden path, submodule escape, binary/oversize).

    Fail-closed: a proposal that touches ``.git``/``.env``/secret material, escapes the worktree
    via a symlink/submodule, or exceeds the binary/size policy is never persisted or approvable.
    """


class PatchProviderError(PatchError):
    """The generation provider failed, or produced output that could not be used.

    Carries the optional partial ``usage`` already consumed (mirroring
    :class:`PatchProviderUnavailable`) so the coordinator can durably charge a *permanent* failure
    onto the run/proposal as a fenced delta — a terminal failure neither loses nor double-counts the
    tokens a cost-ceiling stop, malformed completion, or permanent transfer rejection spent.
    """

    def __init__(self, message: str, *, usage: object | None = None) -> None:
        super().__init__(message)
        self.usage = usage


class PatchProviderUnavailable(PatchError):
    """A *transient* provider/upstream failure (transport, timeout, rate limit, 5xx) — retryable.

    Carries optional retry metadata and the partial ``usage`` already consumed so the coordinator
    can durably charge it (a subsequent attempt gets only the *remaining* token/cost budget).
    """

    def __init__(
        self,
        message: str,
        *,
        retry_after: float | None = None,
        usage: object | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after = retry_after
        self.usage = usage


class PatchLeaseLost(PatchError):
    """The run lease was lost mid-generation (reclaimed/expired) — abort and allow reclaim."""


class PatchStateError(PatchError):
    """An illegal state-machine transition or optimistic-fence conflict was attempted."""


class PatchStaleError(PatchError):
    """The proposal's approved base drifted, or the proposal changed after a decision.

    A stale proposal can never be silently rebased or reuse an old approval: it must be
    regenerated and re-approved.
    """


class PatchApprovalError(PatchError):
    """The durable approval binding is missing, mismatched, or cannot authorize the writeback."""


class PatchWritebackError(PatchError):
    """A control-plane writeback step (verify/push/PR) failed structurally."""


class PatchRemoteUnavailable(PatchError):
    """A *transient* remote (GitHub/Git) failure during writeback — retryable.

    Distinct from :class:`PatchWritebackError` (a permanent, structural refusal): a rate limit or
    5xx must not terminalize the proposal, the durable job retries.
    """

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class PatchNotFound(PatchError):
    """The requested proposal does not exist in this org."""


__all__ = [
    "PatchApprovalError",
    "PatchBoundsExceeded",
    "PatchError",
    "PatchLeaseLost",
    "PatchNotFound",
    "PatchPolicyViolation",
    "PatchProviderError",
    "PatchProviderUnavailable",
    "PatchRemoteUnavailable",
    "PatchStaleError",
    "PatchStateError",
    "PatchValidationError",
    "PatchWritebackError",
]
