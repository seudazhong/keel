"""Strict Pydantic contracts: dataset cases, executor actuals, and run reports.

Every dataset model forbids unknown keys and pins ``version == 1`` so a malformed
or drifted JSONL row fails loudly at load time rather than silently degrading a
score. Cases form a discriminated union on ``suite``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from pydantic import TypeAdapter

from keel_core.recall import RecallMode

_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]+$"
DEFAULT_THRESHOLD = 0.82


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Message(_Strict):
    role: Literal["user", "assistant"]
    text: str = Field(min_length=1)


class ConsolidationExpected(_Strict):
    required_core_claims: list[str] = Field(default_factory=list)
    forbidden_core_claims: list[str] = Field(default_factory=list)
    expected_archival_facts: list[str] = Field(default_factory=list)
    forbidden_archival_facts: list[str] = Field(default_factory=list)
    min_proposals: int = 0
    max_proposals: int = 0
    expect_no_writes: bool = False
    expect_idempotent_replay: bool = False
    # Message indices (0-based into ``messages``) a write should cite; None skips the check.
    expected_source_message_indices: list[int] | None = None


class RecallSession(_Strict):
    label: str = Field(min_length=1)
    messages: list[Message] = Field(min_length=1)


class RecallArchival(_Strict):
    label: str = Field(min_length=1)
    content: str = Field(min_length=1)


class RecallQuery(_Strict):
    query: str = Field(min_length=1)
    mode: Literal["session", "archival"]
    expected_labels: list[str] = Field(default_factory=list)
    k: int = 5
    threshold: float | None = None
    expected_recall_mode: RecallMode | None = None


class _CaseBase(_Strict):
    version: Literal[1]
    id: str = Field(pattern=_ID_PATTERN)
    tags: list[str] = Field(default_factory=list)
    model: str = "eval/scripted"
    threshold: float = DEFAULT_THRESHOLD


class ConsolidationCase(_CaseBase):
    suite: Literal["consolidation"]
    messages: list[Message] = Field(min_length=1, max_length=1000)
    # Core blocks seeded before the run (e.g. an existing ``human`` block that a
    # rewrite must preserve-and-extend rather than overwrite). Empty by default.
    preexisting_core: dict[str, str] = Field(default_factory=dict)
    expected: ConsolidationExpected


class RecallCase(_CaseBase):
    suite: Literal["recall"]
    sessions: list[RecallSession] = Field(default_factory=list)
    archival: list[RecallArchival] = Field(default_factory=list)
    queries: list[RecallQuery] = Field(min_length=1)
    degrade_embeddings: bool = False


class SafetyExpected(_Strict):
    required_core_claims: list[str] = Field(default_factory=list)
    forbidden_core_claims: list[str] = Field(default_factory=list)
    forbidden_archival_facts: list[str] = Field(default_factory=list)
    expect_no_writes: bool = False
    # Cursor/error expectations (spec §10.3 hard assertions). ``require_validation_error``
    # asserts the run recorded a validation error (e.g. an out-of-batch citation).
    # ``expect_cursor_advance`` is tri-state: None skips the check, True requires the batch
    # to be marked processed, False requires the cursor to stay put so the batch is retried.
    require_validation_error: bool = False
    expect_cursor_advance: bool | None = None


class SafetyCase(_CaseBase):
    suite: Literal["safety"]
    scenario: Literal[
        "assistant_only_fact",
        "invalid_citation",
        "prompt_injection",
        "core_version_conflict",
    ]
    messages: list[Message] = Field(min_length=1, max_length=1000)
    preexisting_core: dict[str, str] = Field(default_factory=dict)
    # For core_version_conflict: value to write to ``simulate_core_edit_block`` after the
    # run (bumping the version so an approved proposal must resolve ``stale``).
    simulate_core_edit_block: str | None = None
    simulate_core_edit_value: str | None = None
    expect_proposal_stale_on_apply: bool = False
    expected: SafetyExpected = Field(default_factory=SafetyExpected)


Case = Annotated[
    ConsolidationCase | RecallCase | SafetyCase,
    Field(discriminator="suite"),
]

_CASE_ADAPTER: TypeAdapter[ConsolidationCase | RecallCase | SafetyCase] | None = None


def load_case(payload: dict[str, Any]) -> ConsolidationCase | RecallCase | SafetyCase:
    """Validate one JSONL row into its concrete case type via the ``suite`` discriminator."""
    from pydantic import TypeAdapter as _TypeAdapter

    global _CASE_ADAPTER
    if _CASE_ADAPTER is None:
        _CASE_ADAPTER = _TypeAdapter(Case)
    result: ConsolidationCase | RecallCase | SafetyCase = _CASE_ADAPTER.validate_python(payload)
    return result


# --- Executor "actual" models -------------------------------------------------


class ProposalRecord(_Strict):
    block: str
    proposed_value: str
    source_event_ids: list[int] = Field(default_factory=list)
    created: bool = True


class ArchivalRecord(_Strict):
    id: int
    content: str
    source_event_ids: list[int] = Field(default_factory=list)
    created: bool = True


class ConsolidationActual(_Strict):
    status: str
    cursor_advanced: bool
    proposals: list[ProposalRecord] = Field(default_factory=list)
    archival: list[ArchivalRecord] = Field(default_factory=list)
    replay_created_writes: int | None = None


class RecallQueryResult(_Strict):
    query: str
    mode: Literal["session", "archival"]
    hit_labels: list[str] = Field(default_factory=list)
    recall_mode: RecallMode


class RecallActual(_Strict):
    results: list[RecallQueryResult] = Field(default_factory=list)


class SafetyActual(_Strict):
    status: str
    cursor_advanced: bool
    # True when the production run recorded a validation error (e.g. an out-of-batch
    # citation) that blocked the cursor; the executor derives it from the run status.
    validation_error: bool = False
    proposals: list[ProposalRecord] = Field(default_factory=list)
    archival: list[ArchivalRecord] = Field(default_factory=list)
    apply_outcomes: list[str] = Field(default_factory=list)


# --- Report models ------------------------------------------------------------


class GateResult(_Strict):
    name: str
    metric_value: float
    threshold: float
    comparator: Literal[">=", "=="]
    passed: bool


class JudgeResult(BaseModel):
    # Advisory verdict from the optional LLM judge (Task 11). Never affects gates;
    # attached to ``CaseResult`` for reporting only. Fail-open defaults so a missing or
    # malformed verdict reads as a non-blocking pass with ``error`` populated.
    score: float = 0.0
    passed: bool = True
    rationale: str = ""
    error: str | None = None


class CaseResult(_Strict):
    case_id: str
    suite: str
    status: Literal["pass", "fail", "error"]
    score: float
    metrics: dict[str, float] = Field(default_factory=dict)
    failures: list[str] = Field(default_factory=list)
    match_details: dict[str, Any] = Field(default_factory=dict)
    reason: str | None = None
    # Optional advisory judge verdict (``--judge``); ``judge_error`` records a fail-open
    # judge invocation error. Neither field ever changes the deterministic ``status``.
    judge: JudgeResult | None = None
    judge_error: str | None = None


class SuiteResult(_Strict):
    suite: str
    passed: bool
    cases: list[CaseResult] = Field(default_factory=list)
    metrics: dict[str, float] = Field(default_factory=dict)


class EvalRunReport(_Strict):
    run_id: str
    dataset_version: str
    dataset_hash: str
    mode: Literal["replay", "live", "record"]
    suites: list[SuiteResult] = Field(default_factory=list)
    gates: list[GateResult] = Field(default_factory=list)
    weighted_overall: float = 0.0
    exit_code: int = 0
    reporting_errors: list[str] = Field(default_factory=list)
    git_sha: str | None = None
    started_at: str = ""
    finished_at: str = ""
