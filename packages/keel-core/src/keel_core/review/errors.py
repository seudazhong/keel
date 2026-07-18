"""Read-only code-review error hierarchy (managed-code review MVP, WS-R).

Security-relevant failures fail closed (P5): authorization, evidence verification, and
provider-contract violations raise rather than degrade silently. Errors never carry a
GitHub token, a snippet of source, or a system prompt in their message.
"""

from __future__ import annotations

from keel_core.errors import KeelError


class ReviewError(KeelError):
    """Base class for read-only review domain errors."""


class ReviewValidationError(ReviewError, ValueError):
    """A review request, finding, or report failed strict validation."""


class ReviewBoundsExceeded(ReviewError):
    """A bounded input/output (diff bytes, finding count, field length) was exceeded."""


class ReviewProviderError(ReviewError):
    """The provider failed, or returned output that could not be repaired into a contract."""


class ReviewEvidenceError(ReviewError):
    """A finding's cited evidence could not be verified against the reviewed diff/file."""


class ReviewNotFound(ReviewError):
    """The requested review does not exist in this scope/org."""


__all__ = [
    "ReviewBoundsExceeded",
    "ReviewError",
    "ReviewEvidenceError",
    "ReviewNotFound",
    "ReviewProviderError",
    "ReviewValidationError",
]
