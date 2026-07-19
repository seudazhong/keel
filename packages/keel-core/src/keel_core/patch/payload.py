"""Durable, scope-partitioned patch generation-request payload contract (WS-PP, P4a).

A ``patch.generate`` job carries the full authorized request (the tainted dev ``task`` text +
build params + budgets), but the ``patch_proposals`` row persists only a ``task_digest`` and the
global dispatch pointer deliberately carries no sensitive payload. So a proposal admitted into
``generating`` needs a durable, reconstructable copy of the exact request the fenced reconciler
re-dispatches after a lost enqueue.

This module (it imports only :mod:`keel_core.patch.models`/:mod:`keel_core.patch.errors` and the
scoping helper, so the store/coordinator/jobs can all depend on it without a cycle) owns:

* :class:`PatchGenerateJobPayload` — the strict, immutable, bounded payload carried by a
  ``patch.generate`` job and persisted per proposal;
* :func:`canonical_payload_json` / :func:`request_fingerprint` — a deterministic canonical-JSON
  serialization + SHA-256 digest an idempotent re-submission is verified against; and
* :class:`PatchGenerationRequestRecord` — the immutable per-proposal request row (payload +
  fingerprint + the canonical per-Agent scope it executes under), built from an authorized
  :class:`~keel_core.patch.models.PatchProposalRequest` and validated fail-closed on read.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
)

from keel_core.scoping import derive_agent_scope, validate_scope_id

from .errors import PatchValidationError
from .models import (
    DEFAULT_MAX_DIFF_BYTES,
    DEFAULT_PATCH_AGENT_ID,
    DEFAULT_PATCH_COST_CEILING_USD,
    DEFAULT_PATCH_MAX_ITERATIONS,
    DEFAULT_PATCH_OUTPUT_MAX_TOKENS,
    DEFAULT_PATCH_TOKEN_BUDGET,
    PatchProposalRequest,
)

# A 64-char lowercase-hex SHA-256 digest (mirrors the schema CHECK on the persisted column).
_FINGERPRINT_LEN = 64
# The per-attempt identifiers excluded from the request-identity fingerprint: an idempotent retry
# mints a fresh proposal_id/run_id but is the same logical request (deduped by idempotency key).
_FINGERPRINT_EXCLUDE = frozenset({"proposal_id", "run_id"})


class PatchGenerateJobPayload(BaseModel):
    """The durable payload carried by a ``patch.generate`` job (and persisted per proposal).

    Reconstructs the authorized :class:`~keel_core.patch.models.PatchProposalRequest` (the tainted
    development ``task`` is bounded storage-safe text) plus the ``run_id`` the server bound to the
    proposal, so the worker can resume generation idempotently after a crash/retry."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True, strict=True)

    proposal_id: StrictStr
    run_id: StrictStr
    org_id: StrictStr
    project_id: StrictStr
    actor: StrictStr
    task: StrictStr
    base_ref: StrictStr
    model: StrictStr
    idempotency_key: StrictStr
    agent_id: StrictStr | None = None
    source_ref: StrictStr = ""
    test_commands: tuple[StrictStr, ...] = ()
    max_diff_bytes: StrictInt = Field(default=DEFAULT_MAX_DIFF_BYTES)
    token_budget: StrictInt = Field(default=DEFAULT_PATCH_TOKEN_BUDGET)
    output_max_tokens: StrictInt = Field(default=DEFAULT_PATCH_OUTPUT_MAX_TOKENS)
    cost_ceiling_usd: StrictFloat = Field(default=DEFAULT_PATCH_COST_CEILING_USD)
    max_iterations: StrictInt = Field(default=DEFAULT_PATCH_MAX_ITERATIONS)

    @field_validator("test_commands", mode="before")
    @classmethod
    def _coerce_test_commands(cls, value: object) -> object:
        # A durable job payload survives a JSONB store/read round-trip, which lowers the immutable
        # ``tuple`` to a JSON ``list``. Coerce a list back to a tuple BEFORE strict validation so a
        # reconstructed/enqueued payload validates, while every element is still checked as a
        # ``StrictStr`` (a non-list, e.g. a bare string, is left untouched and fails closed).
        if isinstance(value, list):
            return tuple(value)
        return value

    def to_request(self) -> PatchProposalRequest:
        return PatchProposalRequest(
            org_id=self.org_id,
            project_id=self.project_id,
            actor=self.actor,
            task=self.task,
            base_ref=self.base_ref,
            model=self.model,
            idempotency_key=self.idempotency_key,
            agent_id=self.agent_id,
            source_ref=self.source_ref,
            test_commands=self.test_commands,
            max_diff_bytes=self.max_diff_bytes,
            token_budget=self.token_budget,
            output_max_tokens=self.output_max_tokens,
            cost_ceiling_usd=self.cost_ceiling_usd,
            max_iterations=self.max_iterations,
        )

    @classmethod
    def from_request(
        cls, request: PatchProposalRequest, *, proposal_id: str, run_id: str
    ) -> PatchGenerateJobPayload:
        return cls(
            proposal_id=proposal_id,
            run_id=run_id,
            org_id=request.org_id,
            project_id=request.project_id,
            actor=request.actor,
            task=request.task,
            base_ref=request.base_ref,
            model=request.model,
            idempotency_key=request.idempotency_key,
            agent_id=request.agent_id,
            source_ref=request.source_ref,
            test_commands=tuple(request.test_commands),
            max_diff_bytes=request.max_diff_bytes,
            token_budget=request.token_budget,
            output_max_tokens=request.output_max_tokens,
            cost_ceiling_usd=request.cost_ceiling_usd,
            max_iterations=request.max_iterations,
        )


def canonical_payload_json(payload: PatchGenerateJobPayload) -> str:
    """Deterministic canonical JSON for the FULL payload (sorted keys, compact, UTF-8 preserving).

    Used for the stored ``payload`` column. Stable across a JSONB store/read round trip (which may
    reorder keys) and independent of Python dict insertion order.
    """
    return json.dumps(
        payload.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def request_fingerprint(payload: PatchGenerateJobPayload) -> str:
    """A 64-char lowercase-hex SHA-256 over the payload's *request-identity* fields.

    The digest deliberately EXCLUDES the per-attempt ``proposal_id``/``run_id`` (a retried
    idempotent request mints fresh ones but is the same logical request), so it is the stable
    identity an idempotent re-submission is verified against: two submissions carrying the same
    authorized request (same idempotency key + task + params) produce the same fingerprint, and a
    diverging task/params under the same idempotency key produces a different one (a conflict).
    """
    identity = payload.model_dump(mode="json", exclude=set(_FINGERPRINT_EXCLUDE))
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class PatchGenerationRequestRecord:
    """One immutable per-proposal generation-request row (payload + fingerprint + canonical scope).

    Written in the SAME transaction as the proposal + dispatch pointer, so a committed
    ``generating`` proposal always implies a committed, reconstructable request. The
    ``scope_id`` is the canonical per-Agent scope (``agent:<org>/<agent>``) the request executes
    under; RLS partitions the durable row on it.
    """

    proposal_id: str
    org_id: str
    scope_id: str
    payload: PatchGenerateJobPayload
    fingerprint: str
    created_at: datetime

    @classmethod
    def from_request(
        cls,
        request: PatchProposalRequest,
        *,
        proposal_id: str,
        run_id: str,
        scope_id: str,
        now: datetime | None = None,
    ) -> PatchGenerationRequestRecord:
        """Build the immutable record from an authorized request under an explicit scope.

        Defense-in-depth: ``scope_id`` must be a well-formed scope AND must equal the canonical
        per-Agent scope derived from the immutable ``org_id`` + (effective) ``agent_id`` — the same
        scope the coordinator authorized the run under. A mismatch fails closed rather than
        persisting a payload the reconciler could never re-derive the scope for.
        """
        validate_scope_id(scope_id)
        canonical = derive_agent_scope(request.org_id, request.agent_id or DEFAULT_PATCH_AGENT_ID)
        if scope_id != canonical:
            raise PatchValidationError(
                "generation request scope does not match the canonical Agent scope"
            )
        payload = PatchGenerateJobPayload.from_request(
            request, proposal_id=proposal_id, run_id=run_id
        )
        return cls(
            proposal_id=proposal_id,
            org_id=request.org_id,
            scope_id=scope_id,
            payload=payload,
            fingerprint=request_fingerprint(payload),
            created_at=now or datetime.now(UTC),
        )

    @classmethod
    def from_stored(
        cls,
        *,
        proposal_id: str,
        org_id: str,
        scope_id: str,
        payload: Mapping[str, object],
        fingerprint: str,
        created_at: datetime,
    ) -> PatchGenerationRequestRecord:
        """Reconstruct + verify a persisted row, fail closed on corruption.

        The stored ``payload`` JSON is re-validated against the strict payload contract and its
        fingerprint recomputed and compared to the stored digest, so a payload that no longer
        validates (a schema drift) or whose digest was tampered raises :class:`PatchValidationError`
        rather than returning a fabricated/defaulted request. The message never carries the
        raw task text.
        """
        try:
            model = PatchGenerateJobPayload.model_validate(payload)
        except ValidationError as exc:
            raise PatchValidationError(
                "stored patch generation request failed schema validation"
            ) from exc
        # The identity fingerprint excludes the per-attempt ids, so pin them structurally against
        # the row they were looked up by (a tampered proposal_id/org_id fails closed).
        if model.proposal_id != proposal_id or model.org_id != org_id:
            raise PatchValidationError(
                "stored patch generation request payload binding is inconsistent"
            )
        if request_fingerprint(model) != fingerprint:
            raise PatchValidationError(
                "stored patch generation request fingerprint does not match its payload"
            )
        return cls(
            proposal_id=proposal_id,
            org_id=org_id,
            scope_id=scope_id,
            payload=model,
            fingerprint=fingerprint,
            created_at=created_at,
        )


__all__ = [
    "PatchGenerateJobPayload",
    "PatchGenerationRequestRecord",
    "canonical_payload_json",
    "request_fingerprint",
]
