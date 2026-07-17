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
