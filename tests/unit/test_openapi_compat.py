"""OpenAPI additive-compatibility policy tests."""

from __future__ import annotations

import runpy
from pathlib import Path

import pytest

_SCRIPT = runpy.run_path(
    str(Path(__file__).resolve().parents[2] / "scripts" / "check_openapi_compat.py")
)
CompatibilityError = _SCRIPT["CompatibilityError"]
check = _SCRIPT["check"]
_BASELINE_PATH = _SCRIPT["BASELINE"]
_load_current = _SCRIPT["_schema"]


def test_current_schema_is_additive_over_committed_baseline() -> None:
    """The live app schema must remain backward compatible with the committed baseline.

    This is the real regression guard: it validates against the ``main`` baseline (never a
    branch-regenerated snapshot), so a breaking change to an existing ``/v1`` contract fails
    here even though new paths and new optional parameters are allowed.
    """
    import json

    baseline = json.loads(_BASELINE_PATH.read_text(encoding="utf-8"))
    check(baseline, _load_current())


def test_documentation_metadata_can_change() -> None:
    baseline = {
        "paths": {
            "/v1/items": {
                "get": {
                    "summary": "Old summary",
                    "description": "Old description",
                    "responses": {"200": {"description": "Old response description"}},
                }
            }
        }
    }
    current = {
        "paths": {
            "/v1/items": {
                "get": {
                    "summary": "New summary",
                    "description": "New description",
                    "responses": {"200": {"description": "New response description"}},
                }
            }
        }
    }

    check(baseline, current)


def _op_with_params(params: list[dict[str, object]]) -> dict[str, object]:
    return {"paths": {"/v1/items": {"get": {"parameters": params}}}}


_ORG_HEADER = {
    "name": "X-Keel-Org",
    "in": "header",
    "required": False,
    "schema": {"type": "string"},
}
_ID_QUERY = {"name": "id", "in": "query", "required": True, "schema": {"type": "string"}}


def test_additive_optional_parameter_allowed() -> None:
    # Adding a new optional header to an existing endpoint is backward compatible.
    baseline = _op_with_params([_ID_QUERY])
    current = _op_with_params([_ID_QUERY, _ORG_HEADER])
    check(baseline, current)


def test_added_required_parameter_fails() -> None:
    baseline = _op_with_params([_ID_QUERY])
    new_required = {
        "name": "tenant",
        "in": "header",
        "required": True,
        "schema": {"type": "string"},
    }
    current = _op_with_params([_ID_QUERY, new_required])
    with pytest.raises(CompatibilityError):
        check(baseline, current)


def test_removed_parameter_fails() -> None:
    baseline = _op_with_params([_ID_QUERY, _ORG_HEADER])
    current = _op_with_params([_ID_QUERY])
    with pytest.raises(CompatibilityError):
        check(baseline, current)


def test_parameter_becoming_required_fails() -> None:
    baseline = _op_with_params([{**_ORG_HEADER, "required": False}])
    current = _op_with_params([{**_ORG_HEADER, "required": True}])
    with pytest.raises(CompatibilityError):
        check(baseline, current)


def test_parameter_type_change_fails() -> None:
    baseline = _op_with_params([_ID_QUERY])
    current = _op_with_params([{**_ID_QUERY, "schema": {"type": "integer"}}])
    with pytest.raises(CompatibilityError):
        check(baseline, current)


def test_parameter_reorder_is_compatible() -> None:
    # Identity is (name, in), not position: reordering optional params is not a break.
    baseline = _op_with_params([_ID_QUERY, _ORG_HEADER])
    current = _op_with_params([_ORG_HEADER, _ID_QUERY])
    check(baseline, current)


def test_new_path_is_additive() -> None:
    baseline = {"paths": {"/v1/items": {"get": {"responses": {"200": {}}}}}}
    current = {
        "paths": {
            "/v1/items": {"get": {"responses": {"200": {}}}},
            "/v1/widgets": {"get": {"responses": {"200": {}}}},
        }
    }
    check(baseline, current)


def test_removed_path_fails() -> None:
    baseline = {"paths": {"/v1/items": {"get": {}}, "/v1/widgets": {"get": {}}}}
    current = {"paths": {"/v1/items": {"get": {}}}}
    with pytest.raises(CompatibilityError):
        check(baseline, current)


def test_contract_values_cannot_change() -> None:
    baseline = {
        "paths": {
            "/v1/items": {
                "get": {
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {"type": "string"},
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    current = {
        "paths": {
            "/v1/items": {
                "get": {
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {"type": "integer"},
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    with pytest.raises(CompatibilityError):
        check(baseline, current)
