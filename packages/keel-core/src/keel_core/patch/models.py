"""Typed contracts for controlled patch proposals + human-approved Draft PRs (WS-PP).

Pure, transport-free dataclasses + enums + validators + the fail-closed lifecycle state machine
for the *controlled development -> immutable proposal -> human-approved remote branch + GitHub
Draft PR* phase:

* :class:`PatchProposalRequest` — an authorized request to generate a patch for an explicit
  development task against an approved base ref of a project.
* :class:`ChangedFile` — one bounded, content-addressed changed path in the proposed tree.
* :class:`PatchBundleManifest` — the immutable, content-addressable manifest describing the
  whole proposal (diff/commits/tree/tests/base+head/changed paths + hashes). Its ``bundle_sha256``
  is what an approval is bound to; the heavy bytes live in artifacts, never a mutable DB blob.
* :class:`PatchProposal` — the durable proposal record (org/project/run binding + refs + status).
* :class:`PatchRecord` — the lightweight status projection returned by the read APIs.
* :class:`PatchStatus` + :data:`LEGAL_TRANSITIONS` — the legal, fail-closed lifecycle.

Every field is strictly validated and bounded (P5 fail-closed). The generation model output is
*tainted* and never trusted: a changed path that escapes the tree, names ``.git``/``.env``, or is
a submodule/symlink escape is rejected before a bundle is ever produced.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Any

from .errors import PatchBoundsExceeded, PatchStateError, PatchValidationError

# --- Identifiers ---------------------------------------------------------------------

_PROPOSAL_PREFIX = "pp_"

type ProposalId = str


def new_proposal_id() -> ProposalId:
    return f"{_PROPOSAL_PREFIX}{uuid.uuid4().hex}"


# --- Bounds (fail closed) ------------------------------------------------------------

MAX_TASK_CHARS = 20_000
MAX_TITLE_CHARS = 200
MAX_SUMMARY_CHARS = 8_000
MAX_PATH_CHARS = 1_024
MAX_REF_CHARS = 255
MAX_MODEL_CHARS = 128
MAX_CHANGED_FILES = 200
MAX_COMMITS = 50
MAX_COMMIT_MESSAGE_CHARS = 4_000
# Default ceiling on the proposal diff captured into the bundle. Larger diffs fail the proposal
# closed (a controlled patch must be reviewable) rather than being silently truncated.
DEFAULT_MAX_DIFF_BYTES = 2_000_000
MAX_DIFF_BYTES = 20_000_000
# A single changed file may not exceed this many bytes (oversize policy). Binary files are
# rejected outright (a controlled patch is a reviewable text change).
MAX_FILE_BYTES = 2_000_000
MAX_TEST_COMMANDS = 20
MAX_TEST_COMMAND_CHARS = 500

# --- Budget policy (fail closed; never unlimited) ------------------------------------
DEFAULT_PATCH_TOKEN_BUDGET = 400_000
MAX_PATCH_TOKEN_BUDGET = 4_000_000
DEFAULT_PATCH_OUTPUT_MAX_TOKENS = 16_000
MAX_PATCH_OUTPUT_MAX_TOKENS = 64_000
DEFAULT_PATCH_COST_CEILING_USD = 5.0
MAX_PATCH_COST_CEILING_USD = 100.0
DEFAULT_PATCH_MAX_ITERATIONS = 30
MAX_PATCH_MAX_ITERATIONS = 100

# Paths a controlled patch may never touch (verbatim segment or prefix match). The sandbox path
# policy also blocks these; this is the belt-and-braces domain check on the *captured* tree.
FORBIDDEN_PATH_SEGMENTS = frozenset({".git", ".env", ".ssh", ".aws", ".gnupg"})
FORBIDDEN_PATH_SUFFIXES = (
    ".pem",
    ".key",
    ".p12",
    ".pfx",
    "id_rsa",
    "id_ed25519",
    ".env",
)
_SECRET_NAME_RE = re.compile(r"(^|/)\.env(\.|$)|secrets?\.(ya?ml|json|toml|txt)$", re.IGNORECASE)

# A dedicated writeback branch namespace. Never the default branch; validated against this.
RUN_BRANCH_PREFIX = "keel/patch/"
_BRANCH_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._-]+$")


# --- Ordered enums -------------------------------------------------------------------


class ChangeKind(StrEnum):
    """How a path changed in the proposed tree."""

    added = "added"
    modified = "modified"
    deleted = "deleted"
    renamed = "renamed"


class TestStatus(StrEnum):
    """Aggregate outcome of the proposal's required tests."""

    __test__ = False

    unknown = "unknown"
    passed = "passed"
    failed = "failed"
    skipped = "skipped"


class PatchStatus(StrEnum):
    """Fail-closed lifecycle of a controlled patch proposal.

    ``generating`` -> ``ready`` -> ``approval_pending`` -> ``approved`` / ``denied`` / ``expired``
    -> ``writing`` -> ``draft_pr_created``; any step may fall to ``failed`` / ``stale`` /
    ``cancelled``. Transitions are enforced by :data:`LEGAL_TRANSITIONS` and the store's optimistic
    version fence.
    """

    generating = "generating"
    ready = "ready"
    approval_pending = "approval_pending"
    approved = "approved"
    denied = "denied"
    expired = "expired"
    writing = "writing"
    draft_pr_created = "draft_pr_created"
    failed = "failed"
    stale = "stale"
    cancelled = "cancelled"


TERMINAL_STATUSES: frozenset[PatchStatus] = frozenset(
    {
        PatchStatus.denied,
        PatchStatus.expired,
        PatchStatus.draft_pr_created,
        PatchStatus.failed,
        PatchStatus.stale,
        PatchStatus.cancelled,
    }
)

# The single source of truth for legal state-machine edges. Anything not listed is illegal and
# fails closed (:class:`PatchStateError`). ``stale`` is reachable from any non-terminal state
# because a base drift / mutation can be detected at any point before the PR exists.
LEGAL_TRANSITIONS: Mapping[PatchStatus, frozenset[PatchStatus]] = {
    PatchStatus.generating: frozenset(
        {PatchStatus.ready, PatchStatus.failed, PatchStatus.cancelled, PatchStatus.stale}
    ),
    PatchStatus.ready: frozenset(
        {
            PatchStatus.approval_pending,
            PatchStatus.failed,
            PatchStatus.cancelled,
            PatchStatus.stale,
            PatchStatus.expired,
        }
    ),
    PatchStatus.approval_pending: frozenset(
        {
            PatchStatus.approved,
            PatchStatus.denied,
            PatchStatus.expired,
            PatchStatus.cancelled,
            PatchStatus.stale,
        }
    ),
    PatchStatus.approved: frozenset(
        {
            PatchStatus.writing,
            PatchStatus.failed,
            PatchStatus.cancelled,
            PatchStatus.stale,
            PatchStatus.expired,
        }
    ),
    PatchStatus.writing: frozenset(
        {
            PatchStatus.draft_pr_created,
            PatchStatus.failed,
            PatchStatus.stale,
            PatchStatus.cancelled,
        }
    ),
    # Terminal states have no outgoing edges.
    PatchStatus.draft_pr_created: frozenset(),
    PatchStatus.denied: frozenset(),
    PatchStatus.expired: frozenset(),
    PatchStatus.failed: frozenset(),
    PatchStatus.stale: frozenset(),
    PatchStatus.cancelled: frozenset(),
}


def can_transition(current: PatchStatus, target: PatchStatus) -> bool:
    """Whether ``current -> target`` is a legal state-machine edge."""
    return target in LEGAL_TRANSITIONS.get(current, frozenset())


def ensure_transition(current: PatchStatus, target: PatchStatus) -> None:
    """Raise :class:`PatchStateError` unless ``current -> target`` is legal (fail closed)."""
    if current == target:
        return
    if not can_transition(current, target):
        raise PatchStateError(
            f"illegal patch-proposal transition {current.value} -> {target.value}"
        )


# --- Validation helpers --------------------------------------------------------------


def _bounded_text(value: Any, *, field_name: str, max_chars: int, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise PatchValidationError(f"{field_name} must be a string")
    if "\x00" in value:
        raise PatchValidationError(f"{field_name} must not contain null bytes")
    text = value if allow_empty else value.strip()
    if not allow_empty and not text:
        raise PatchValidationError(f"{field_name} must not be empty")
    if len(value) > max_chars:
        raise PatchBoundsExceeded(f"{field_name} exceeds {max_chars} characters")
    return text


def _enum_value(enum_cls: type[StrEnum], value: Any, *, field_name: str) -> Any:
    if isinstance(value, enum_cls):
        return value
    if not isinstance(value, str):
        raise PatchValidationError(f"{field_name} must be one of {[e.value for e in enum_cls]}")
    try:
        return enum_cls(value)
    except ValueError as exc:
        raise PatchValidationError(
            f"{field_name} must be one of {[e.value for e in enum_cls]}"
        ) from exc


def normalize_repo_path(value: Any, *, field_name: str = "path") -> str:
    """A repo-relative, forward-slash, traversal-free path a proposed tree can name.

    Generation output is untrusted; a path that escapes the tree, is absolute, contains a
    traversal component, or names forbidden/secret material is rejected outright.
    """
    text = _bounded_text(value, field_name=field_name, max_chars=MAX_PATH_CHARS)
    normalized = text.replace("\\", "/").strip()
    if normalized.startswith("/") or normalized.startswith("~"):
        raise PatchValidationError(f"{field_name} must be repository-relative")
    if len(normalized) >= 2 and normalized[1] == ":":
        raise PatchValidationError(f"{field_name} must not be a drive-absolute path")
    parts = normalized.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise PatchValidationError(f"{field_name} must not contain empty or traversal components")
    return normalized


def is_forbidden_path(path: str) -> bool:
    """Whether a (normalized) repo-relative path is forbidden for a controlled patch."""
    parts = path.split("/")
    if any(part in FORBIDDEN_PATH_SEGMENTS for part in parts):
        return True
    lowered = path.lower()
    if any(lowered.endswith(suffix) for suffix in FORBIDDEN_PATH_SUFFIXES):
        return True
    if _SECRET_NAME_RE.search(path):
        return True
    return False


def validate_commit_sha(value: Any, *, field_name: str = "sha", allow_empty: bool = False) -> str:
    if allow_empty and (value is None or value == ""):
        return ""
    text = _bounded_text(value, field_name=field_name, max_chars=64)
    if not re.fullmatch(r"[0-9a-fA-F]{7,64}", text):
        raise PatchValidationError(f"{field_name} must be a hex git object id")
    return text.lower()


def validate_run_branch(value: Any, *, default_branch: str) -> str:
    """A dedicated, safe writeback branch in the reserved namespace — never the default branch."""
    text = _bounded_text(value, field_name="remote_branch", max_chars=MAX_REF_CHARS)
    if not text.startswith(RUN_BRANCH_PREFIX):
        raise PatchValidationError(f"remote_branch must be in the '{RUN_BRANCH_PREFIX}' namespace")
    if text == default_branch or text == f"refs/heads/{default_branch}":
        raise PatchValidationError("remote_branch must never be the default branch")
    tail = text[len(RUN_BRANCH_PREFIX) :]
    segments = tail.split("/")
    if not segments or any(not _BRANCH_SEGMENT_RE.fullmatch(seg) for seg in segments):
        raise PatchValidationError("remote_branch contains an invalid namespace segment")
    if ".." in text or text.endswith("/") or text.endswith(".lock"):
        raise PatchValidationError("remote_branch is not a valid git ref")
    return text


def run_branch_for(proposal_id: str) -> str:
    """The canonical dedicated writeback branch name for a proposal (collision-free, idempotent)."""
    safe = re.sub(r"[^A-Za-z0-9._-]", "-", proposal_id)
    return f"{RUN_BRANCH_PREFIX}{safe}"


def task_digest(task: str) -> str:
    """A stable, non-reversible digest of the (tainted) development task text (audit binding)."""
    return hashlib.sha256(task.encode("utf-8")).hexdigest()


# --- Changed files -------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ChangedFile:
    """One bounded, content-addressed changed path in the proposed tree."""

    path: str
    change_kind: ChangeKind
    blob_sha: str
    size_bytes: int
    old_path: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", normalize_repo_path(self.path, field_name="path"))
        object.__setattr__(
            self, "change_kind", _enum_value(ChangeKind, self.change_kind, field_name="change_kind")
        )
        old = self.old_path
        if self.change_kind is ChangeKind.renamed:
            object.__setattr__(
                self, "old_path", normalize_repo_path(old or self.path, field_name="old_path")
            )
        elif old:
            object.__setattr__(self, "old_path", normalize_repo_path(old, field_name="old_path"))
        # Deleted files carry the empty tree hash and zero size; others carry the real blob sha.
        object.__setattr__(
            self,
            "blob_sha",
            validate_commit_sha(
                self.blob_sha,
                field_name="blob_sha",
                allow_empty=self.change_kind is ChangeKind.deleted,
            ),
        )
        if isinstance(self.size_bytes, bool) or not isinstance(self.size_bytes, int):
            raise PatchValidationError("size_bytes must be an integer")
        if self.size_bytes < 0:
            raise PatchValidationError("size_bytes must be >= 0")
        if self.size_bytes > MAX_FILE_BYTES:
            raise PatchBoundsExceeded(f"changed file exceeds {MAX_FILE_BYTES} bytes")
        if is_forbidden_path(self.path) or (self.old_path and is_forbidden_path(self.old_path)):
            from .errors import PatchPolicyViolation

            raise PatchPolicyViolation(f"changed path is forbidden: {self.path}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "change_kind": self.change_kind.value,
            "blob_sha": self.blob_sha,
            "size_bytes": self.size_bytes,
            "old_path": self.old_path,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ChangedFile:
        return cls(
            path=data["path"],
            change_kind=data["change_kind"],
            blob_sha=data.get("blob_sha", ""),
            size_bytes=int(data.get("size_bytes", 0)),
            old_path=data.get("old_path", ""),
        )


def changed_path_digest(files: Sequence[ChangedFile]) -> str:
    """A stable digest binding an approval to the EXACT set of changed (path, blob) pairs.

    Sorted, canonical, and content-addressed: if any changed path or blob changes after approval,
    the digest changes and the old approval can no longer authorize the writeback (fail closed).
    """
    canonical = sorted(
        (f.change_kind.value, f.path, f.old_path, f.blob_sha, str(f.size_bytes)) for f in files
    )
    payload = "\n".join("\x1f".join(row) for row in canonical)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --- Commits -------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ProposedCommit:
    """One proposed commit in the disposable worktree (message + tree/commit shas)."""

    sha: str
    message: str
    tree_sha: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "sha", validate_commit_sha(self.sha, field_name="commit.sha"))
        object.__setattr__(
            self, "tree_sha", validate_commit_sha(self.tree_sha, field_name="commit.tree_sha")
        )
        object.__setattr__(
            self,
            "message",
            _bounded_text(
                self.message, field_name="commit.message", max_chars=MAX_COMMIT_MESSAGE_CHARS
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {"sha": self.sha, "message": self.message, "tree_sha": self.tree_sha}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ProposedCommit:
        return cls(sha=data["sha"], message=data["message"], tree_sha=data["tree_sha"])


# --- Test results --------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TestResult:
    """The recorded outcome of one required test command (log stored as an artifact ref)."""

    __test__ = False

    command: str
    exit_code: int
    passed: bool
    log_sha256: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "command",
            _bounded_text(
                self.command, field_name="test.command", max_chars=MAX_TEST_COMMAND_CHARS
            ),
        )
        if isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int):
            raise PatchValidationError("test.exit_code must be an integer")
        if self.log_sha256:
            validate_commit_sha(self.log_sha256, field_name="test.log_sha256")

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": self.command,
            "exit_code": self.exit_code,
            "passed": self.passed,
            "log_sha256": self.log_sha256,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TestResult:
        return cls(
            command=data["command"],
            exit_code=int(data["exit_code"]),
            passed=bool(data["passed"]),
            log_sha256=data.get("log_sha256", ""),
        )


# --- Budget --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PatchBudget:
    """The bounded token/output/cost/iteration envelope every generation runs under."""

    token_budget: int = DEFAULT_PATCH_TOKEN_BUDGET
    output_max_tokens: int = DEFAULT_PATCH_OUTPUT_MAX_TOKENS
    cost_ceiling_usd: float = DEFAULT_PATCH_COST_CEILING_USD
    max_iterations: int = DEFAULT_PATCH_MAX_ITERATIONS

    def __post_init__(self) -> None:
        if not 1 <= self.token_budget <= MAX_PATCH_TOKEN_BUDGET:
            raise PatchValidationError("token_budget out of bounds")
        if not 1 <= self.output_max_tokens <= MAX_PATCH_OUTPUT_MAX_TOKENS:
            raise PatchValidationError("output_max_tokens out of bounds")
        if not 0.0 < self.cost_ceiling_usd <= MAX_PATCH_COST_CEILING_USD:
            raise PatchValidationError("cost_ceiling_usd out of bounds")
        if not 1 <= self.max_iterations <= MAX_PATCH_MAX_ITERATIONS:
            raise PatchValidationError("max_iterations out of bounds")


# --- Request -------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PatchProposalRequest:
    """An authorized request to generate a controlled patch for a development task."""

    org_id: str
    project_id: str
    actor: str
    task: str
    base_ref: str
    model: str
    idempotency_key: str
    agent_id: str | None = None
    source_ref: str = ""
    test_commands: tuple[str, ...] = ()
    max_diff_bytes: int = DEFAULT_MAX_DIFF_BYTES
    token_budget: int = DEFAULT_PATCH_TOKEN_BUDGET
    output_max_tokens: int = DEFAULT_PATCH_OUTPUT_MAX_TOKENS
    cost_ceiling_usd: float = DEFAULT_PATCH_COST_CEILING_USD
    max_iterations: int = DEFAULT_PATCH_MAX_ITERATIONS

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "org_id", _bounded_text(self.org_id, field_name="org_id", max_chars=255)
        )
        object.__setattr__(
            self,
            "project_id",
            _bounded_text(self.project_id, field_name="project_id", max_chars=255),
        )
        object.__setattr__(
            self, "actor", _bounded_text(self.actor, field_name="actor", max_chars=255)
        )
        object.__setattr__(
            self, "task", _bounded_text(self.task, field_name="task", max_chars=MAX_TASK_CHARS)
        )
        object.__setattr__(
            self,
            "base_ref",
            _bounded_text(self.base_ref, field_name="base_ref", max_chars=MAX_REF_CHARS),
        )
        object.__setattr__(
            self, "model", _bounded_text(self.model, field_name="model", max_chars=MAX_MODEL_CHARS)
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _bounded_text(self.idempotency_key, field_name="idempotency_key", max_chars=200),
        )
        if self.agent_id is not None:
            object.__setattr__(
                self, "agent_id", _bounded_text(self.agent_id, field_name="agent_id", max_chars=128)
            )
        object.__setattr__(
            self,
            "source_ref",
            _bounded_text(
                self.source_ref, field_name="source_ref", max_chars=255, allow_empty=True
            ),
        )
        if len(self.test_commands) > MAX_TEST_COMMANDS:
            raise PatchBoundsExceeded(f"at most {MAX_TEST_COMMANDS} test commands")
        cleaned = tuple(
            _bounded_text(cmd, field_name="test_command", max_chars=MAX_TEST_COMMAND_CHARS)
            for cmd in self.test_commands
        )
        object.__setattr__(self, "test_commands", cleaned)
        if not 1 <= self.max_diff_bytes <= MAX_DIFF_BYTES:
            raise PatchValidationError("max_diff_bytes out of bounds")
        # Validate the budget envelope eagerly (fail closed on an out-of-bounds request).
        self.budget()

    def budget(self) -> PatchBudget:
        return PatchBudget(
            token_budget=self.token_budget,
            output_max_tokens=self.output_max_tokens,
            cost_ceiling_usd=self.cost_ceiling_usd,
            max_iterations=self.max_iterations,
        )


# --- Immutable proposal bundle manifest ----------------------------------------------


@dataclass(frozen=True, slots=True)
class PatchBundleManifest:
    """The immutable, content-addressable description of a whole proposal.

    Heavy bytes (the unified diff, per-file blobs, test logs) live in the artifact store keyed by
    the hashes recorded here; this manifest is itself stored as an artifact and its canonical
    sha256 (:meth:`content_hash`) is the ``bundle_sha256`` an approval binds to.
    """

    proposal_id: str
    org_id: str
    project_id: str
    run_id: str
    base_ref: str
    base_sha: str
    head_sha: str
    diff_sha256: str
    diff_bytes: int
    files: tuple[ChangedFile, ...]
    commits: tuple[ProposedCommit, ...]
    tests: tuple[TestResult, ...]
    test_status: TestStatus
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "base_sha", validate_commit_sha(self.base_sha, field_name="base_sha")
        )
        object.__setattr__(
            self, "head_sha", validate_commit_sha(self.head_sha, field_name="head_sha")
        )
        object.__setattr__(
            self, "diff_sha256", validate_commit_sha(self.diff_sha256, field_name="diff_sha256")
        )
        if len(self.files) > MAX_CHANGED_FILES:
            raise PatchBoundsExceeded(f"at most {MAX_CHANGED_FILES} changed files")
        if not self.files:
            raise PatchValidationError("a proposal must change at least one file")
        if len(self.commits) > MAX_COMMITS:
            raise PatchBoundsExceeded(f"at most {MAX_COMMITS} commits")
        if self.diff_bytes < 0 or self.diff_bytes > MAX_DIFF_BYTES:
            raise PatchBoundsExceeded("diff_bytes out of bounds")
        object.__setattr__(
            self, "test_status", _enum_value(TestStatus, self.test_status, field_name="test_status")
        )

    @property
    def changed_path_digest(self) -> str:
        return changed_path_digest(self.files)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "keel.patch.bundle.v1",
            "proposal_id": self.proposal_id,
            "org_id": self.org_id,
            "project_id": self.project_id,
            "run_id": self.run_id,
            "base_ref": self.base_ref,
            "base_sha": self.base_sha,
            "head_sha": self.head_sha,
            "diff_sha256": self.diff_sha256,
            "diff_bytes": self.diff_bytes,
            "changed_path_digest": self.changed_path_digest,
            "files": [f.to_dict() for f in self.files],
            "commits": [c.to_dict() for c in self.commits],
            "tests": [t.to_dict() for t in self.tests],
            "test_status": self.test_status.value,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PatchBundleManifest:
        return cls(
            proposal_id=data["proposal_id"],
            org_id=data["org_id"],
            project_id=data["project_id"],
            run_id=data["run_id"],
            base_ref=data["base_ref"],
            base_sha=data["base_sha"],
            head_sha=data["head_sha"],
            diff_sha256=data["diff_sha256"],
            diff_bytes=int(data["diff_bytes"]),
            files=tuple(ChangedFile.from_dict(f) for f in data.get("files", ())),
            commits=tuple(ProposedCommit.from_dict(c) for c in data.get("commits", ())),
            tests=tuple(TestResult.from_dict(t) for t in data.get("tests", ())),
            test_status=data.get("test_status", TestStatus.unknown.value),
            created_at=_parse_dt(data["created_at"]),
        )


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


# --- Durable proposal record ---------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PatchProposal:
    """The durable proposal record projected from the ``patch_proposals`` row."""

    id: str
    org_id: str
    project_id: str
    run_id: str
    run_attempt: int
    agent_id: str
    actor: str
    base_ref: str
    base_sha: str
    head_sha: str
    bundle_sha256: str
    diff_sha256: str
    changed_path_digest: str
    changed_files: int
    test_status: TestStatus
    status: PatchStatus
    version: int
    idempotency_key: str
    fingerprint: str
    expires_at: datetime
    created_at: datetime
    updated_at: datetime
    source_ref: str = ""
    task_digest: str = ""
    approval_id: str = ""
    remote_branch: str = ""
    pr_number: int | None = None
    pr_url: str = ""
    pr_node_id: str = ""
    cost_usd: float = 0.0
    error_kind: str = ""
    error_message: str = ""
    ready_at: datetime | None = None
    decided_at: datetime | None = None
    written_at: datetime | None = None

    def with_status(self, status: PatchStatus) -> PatchProposal:
        ensure_transition(self.status, status)
        return replace(self, status=status, version=self.version + 1)


# --- Status projection ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PatchRecord:
    """The lightweight status projection returned by list/get APIs."""

    proposal_id: str
    org_id: str
    project_id: str
    run_id: str
    status: PatchStatus
    base_ref: str
    base_sha: str
    head_sha: str
    changed_files: int
    test_status: TestStatus
    bundle_sha256: str
    changed_path_digest: str
    approval_id: str
    remote_branch: str
    pr_number: int | None
    pr_url: str
    cost_usd: float
    error_kind: str
    error_message: str
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_proposal(cls, proposal: PatchProposal) -> PatchRecord:
        return cls(
            proposal_id=proposal.id,
            org_id=proposal.org_id,
            project_id=proposal.project_id,
            run_id=proposal.run_id,
            status=proposal.status,
            base_ref=proposal.base_ref,
            base_sha=proposal.base_sha,
            head_sha=proposal.head_sha,
            changed_files=proposal.changed_files,
            test_status=proposal.test_status,
            bundle_sha256=proposal.bundle_sha256,
            changed_path_digest=proposal.changed_path_digest,
            approval_id=proposal.approval_id,
            remote_branch=proposal.remote_branch,
            pr_number=proposal.pr_number,
            pr_url=proposal.pr_url,
            cost_usd=proposal.cost_usd,
            error_kind=proposal.error_kind,
            error_message=proposal.error_message,
            created_at=proposal.created_at,
            updated_at=proposal.updated_at,
        )


__all__ = [
    "ChangeKind",
    "ChangedFile",
    "LEGAL_TRANSITIONS",
    "PatchBudget",
    "PatchBundleManifest",
    "PatchProposal",
    "PatchProposalRequest",
    "PatchRecord",
    "PatchStatus",
    "ProposalId",
    "ProposedCommit",
    "TERMINAL_STATUSES",
    "TestResult",
    "TestStatus",
    "can_transition",
    "changed_path_digest",
    "ensure_transition",
    "is_forbidden_path",
    "new_proposal_id",
    "normalize_repo_path",
    "run_branch_for",
    "task_digest",
    "validate_commit_sha",
    "validate_run_branch",
]
