"""Unit tests for the durable patch generation-request payload contract (M4, WS-PP, P4a).

Covers the leaf :mod:`keel_core.patch.payload` module in isolation:

* :class:`PatchGenerateJobPayload` round-trips to/from a :class:`PatchProposalRequest` and coerces a
  JSONB-lowered ``list`` back to the immutable ``tuple`` under strict checks (a bare string fails
  closed, an extra key is forbidden);
* :func:`request_fingerprint` is a deterministic *request-identity* digest -- stable across the
  per-attempt ``proposal_id``/``run_id`` (an idempotent retry mints fresh ones) but divergent for a
  changed task/param under the same idempotency key;
* :func:`canonical_payload_json` is order-independent and stable across a JSONB round trip; and
* :class:`PatchGenerationRequestRecord` builds from an authorized request under the canonical
  per-Agent scope (a mismatched or malformed scope fails closed) and reconstructs a stored row
  fail-closed (schema drift, a tampered digest, or an inconsistent id/org all raise), never leaking
  the raw task text in the error.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from keel_core.patch.errors import PatchValidationError
from keel_core.patch.models import DEFAULT_PATCH_AGENT_ID, PatchProposalRequest
from keel_core.patch.payload import (
    PatchGenerateJobPayload,
    PatchGenerationRequestRecord,
    canonical_payload_json,
    request_fingerprint,
)
from keel_core.scoping import ScopeValidationError, derive_agent_scope

_ORG = "org-a"
_SCOPE = derive_agent_scope(_ORG, DEFAULT_PATCH_AGENT_ID)
_T0 = datetime(2026, 7, 19, 12, 0, tzinfo=UTC)


def _request(**over: object) -> PatchProposalRequest:
    kw: dict[str, object] = dict(
        org_id=_ORG,
        project_id="proj-a",
        actor="u",
        task="apply the change",
        base_ref="main",
        model="m",
        idempotency_key="idem-1",
        agent_id=DEFAULT_PATCH_AGENT_ID,
    )
    kw.update(over)
    return PatchProposalRequest(**kw)  # type: ignore[arg-type]


def _payload(
    *, proposal_id: str = "pp-1", run_id: str = "run-1", **over: object
) -> PatchGenerateJobPayload:
    return PatchGenerateJobPayload.from_request(
        _request(**over), proposal_id=proposal_id, run_id=run_id
    )


# --- payload contract ---------------------------------------------------------------------------


def test_payload_round_trips_to_request() -> None:
    request = _request(test_commands=("pytest -q", "ruff check ."))
    payload = PatchGenerateJobPayload.from_request(request, proposal_id="pp-1", run_id="run-1")
    assert payload.to_request() == request
    assert payload.proposal_id == "pp-1" and payload.run_id == "run-1"


def test_payload_coerces_jsonb_list_to_tuple() -> None:
    payload = _payload(test_commands=("a", "b"))
    dumped = payload.model_dump(mode="json")
    assert isinstance(dumped["test_commands"], list)  # JSON lowers the tuple to a list
    reloaded = PatchGenerateJobPayload.model_validate(dumped)
    assert reloaded == payload
    assert isinstance(reloaded.test_commands, tuple)


def test_payload_rejects_bare_string_test_commands() -> None:
    data = _payload().model_dump(mode="json")
    data["test_commands"] = "pytest"  # a non-list is left untouched and fails closed under strict
    with pytest.raises(ValidationError):
        PatchGenerateJobPayload.model_validate(data)


def test_payload_forbids_extra_fields() -> None:
    data = _payload().model_dump(mode="json")
    data["surprise"] = 1
    with pytest.raises(ValidationError):
        PatchGenerateJobPayload.model_validate(data)


# --- fingerprint (request identity) -------------------------------------------------------------


def test_fingerprint_is_deterministic_hex_sha256() -> None:
    fp = request_fingerprint(_payload())
    assert fp == request_fingerprint(_payload())
    assert len(fp) == 64 and all(c in "0123456789abcdef" for c in fp)


def test_fingerprint_is_stable_across_attempt_ids() -> None:
    # An idempotent retry mints a fresh proposal_id/run_id but is the same logical request: the two
    # payloads differ, yet their request-identity fingerprint is identical.
    a = PatchGenerateJobPayload.from_request(_request(), proposal_id="pp-1", run_id="run-1")
    b = PatchGenerateJobPayload.from_request(_request(), proposal_id="pp-2", run_id="run-2")
    assert a != b
    assert request_fingerprint(a) == request_fingerprint(b)


def test_fingerprint_diverges_on_changed_task_or_param() -> None:
    base = request_fingerprint(_payload())
    assert request_fingerprint(_payload(task="something else")) != base
    assert request_fingerprint(_payload(model="other-model")) != base
    assert request_fingerprint(_payload(token_budget=999)) != base


# --- canonical payload JSON ---------------------------------------------------------------------


def test_canonical_payload_json_is_order_independent_and_stable() -> None:
    payload = _payload(test_commands=("a", "b"))
    canonical = canonical_payload_json(payload)
    reparsed = PatchGenerateJobPayload.model_validate(json.loads(canonical))
    assert canonical_payload_json(reparsed) == canonical  # stable across a JSONB round trip
    assert json.loads(canonical)["task"] == "apply the change"


# --- record: build from an authorized request ---------------------------------------------------


def test_record_from_request_binds_canonical_scope() -> None:
    record = PatchGenerationRequestRecord.from_request(
        _request(), proposal_id="pp-1", run_id="run-1", scope_id=_SCOPE, now=_T0
    )
    assert record.proposal_id == "pp-1"
    assert record.org_id == _ORG
    assert record.scope_id == _SCOPE
    assert record.created_at == _T0
    assert record.fingerprint == request_fingerprint(record.payload)
    assert record.payload.run_id == "run-1"


def test_record_from_request_defaults_agent_to_patch_scope() -> None:
    # agent_id=None falls back to DEFAULT_PATCH_AGENT_ID for the canonical scope derivation.
    record = PatchGenerationRequestRecord.from_request(
        _request(agent_id=None), proposal_id="pp-1", run_id="run-1", scope_id=_SCOPE
    )
    assert record.scope_id == _SCOPE


def test_record_from_request_rejects_non_canonical_scope() -> None:
    other = derive_agent_scope(_ORG, "other-agent")
    with pytest.raises(PatchValidationError):
        PatchGenerationRequestRecord.from_request(
            _request(), proposal_id="pp-1", run_id="run-1", scope_id=other
        )


def test_record_from_request_rejects_malformed_scope() -> None:
    with pytest.raises(ScopeValidationError):
        PatchGenerationRequestRecord.from_request(
            _request(), proposal_id="pp-1", run_id="run-1", scope_id="not-a-scope"
        )


# --- record: reconstruct + verify a stored row --------------------------------------------------


def _stored(record: PatchGenerationRequestRecord) -> dict[str, object]:
    return json.loads(canonical_payload_json(record.payload))


def test_from_stored_round_trips() -> None:
    record = PatchGenerationRequestRecord.from_request(
        _request(test_commands=("pytest -q",)),
        proposal_id="pp-1",
        run_id="run-1",
        scope_id=_SCOPE,
        now=_T0,
    )
    restored = PatchGenerationRequestRecord.from_stored(
        proposal_id=record.proposal_id,
        org_id=record.org_id,
        scope_id=record.scope_id,
        payload=_stored(record),
        fingerprint=record.fingerprint,
        created_at=record.created_at,
    )
    assert restored.payload == record.payload
    assert restored.fingerprint == record.fingerprint
    assert restored.payload.test_commands == ("pytest -q",)  # tuple restored from a JSON list


def test_from_stored_rejects_tampered_fingerprint() -> None:
    record = PatchGenerationRequestRecord.from_request(
        _request(), proposal_id="pp-1", run_id="run-1", scope_id=_SCOPE
    )
    with pytest.raises(PatchValidationError):
        PatchGenerationRequestRecord.from_stored(
            proposal_id="pp-1",
            org_id=_ORG,
            scope_id=_SCOPE,
            payload=_stored(record),
            fingerprint="0" * 64,
            created_at=record.created_at,
        )


@pytest.mark.parametrize(("field", "value"), [("proposal_id", "pp-2"), ("org_id", "other-org")])
def test_from_stored_rejects_inconsistent_binding(field: str, value: str) -> None:
    # The identity fingerprint excludes the per-attempt ids, so the proposal_id/org_id are pinned
    # structurally against the row they were looked up by; a tampered lookup key fails closed.
    record = PatchGenerationRequestRecord.from_request(
        _request(), proposal_id="pp-1", run_id="run-1", scope_id=_SCOPE
    )
    kwargs: dict[str, object] = dict(
        proposal_id="pp-1",
        org_id=_ORG,
        scope_id=_SCOPE,
        payload=_stored(record),
        fingerprint=record.fingerprint,
        created_at=record.created_at,
    )
    kwargs[field] = value
    with pytest.raises(PatchValidationError):
        PatchGenerationRequestRecord.from_stored(**kwargs)  # type: ignore[arg-type]


def test_from_stored_rejects_schema_drift() -> None:
    record = PatchGenerationRequestRecord.from_request(
        _request(), proposal_id="pp-1", run_id="run-1", scope_id=_SCOPE
    )
    payload = _stored(record)
    del payload["model"]  # a schema drift no longer validates against the strict contract
    with pytest.raises(PatchValidationError):
        PatchGenerationRequestRecord.from_stored(
            proposal_id="pp-1",
            org_id=_ORG,
            scope_id=_SCOPE,
            payload=payload,
            fingerprint=record.fingerprint,
            created_at=record.created_at,
        )


def test_from_stored_error_never_leaks_raw_task() -> None:
    secret = "SUPER-SECRET-TASK-TEXT"
    record = PatchGenerationRequestRecord.from_request(
        _request(task=secret), proposal_id="pp-1", run_id="run-1", scope_id=_SCOPE
    )
    with pytest.raises(PatchValidationError) as excinfo:
        PatchGenerationRequestRecord.from_stored(
            proposal_id="pp-1",
            org_id=_ORG,
            scope_id=_SCOPE,
            payload=_stored(record),
            fingerprint="0" * 64,
            created_at=record.created_at,
        )
    assert secret not in str(excinfo.value)
