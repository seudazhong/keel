"""Check that the checked-in `/v1` OpenAPI baseline evolves additively.

Run `uv run python scripts/check_openapi_compat.py --write-baseline` deliberately
when accepting an additive contract change, then commit the resulting snapshot.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "tests" / "fixtures" / "openapi-v1-baseline.json"


class CompatibilityError(ValueError):
    """A current schema removed or changed a baseline contract value."""


def _schema() -> dict[str, Any]:
    sys.path[:0] = [
        str(ROOT / "packages" / "keel-core" / "src"),
        str(ROOT / "packages" / "keel-scheduler" / "src"),
        str(ROOT / "packages" / "keel-server" / "src"),
    ]
    from keel_server.app import create_app

    return create_app().openapi()


def _require_additive(baseline: Any, current: Any, path: str = "$") -> None:
    """Require every old node to remain; new mapping keys are additive."""
    if isinstance(baseline, dict):
        if not isinstance(current, dict):
            raise CompatibilityError(f"{path} changed from an object")
        for key, old_value in baseline.items():
            if key not in current:
                raise CompatibilityError(f"{path}.{key} was removed")
            _require_additive(old_value, current[key], f"{path}.{key}")
    elif isinstance(baseline, list):
        if not isinstance(current, list) or baseline != current:
            raise CompatibilityError(f"{path} changed")
    elif baseline != current:
        raise CompatibilityError(f"{path} changed from {baseline!r} to {current!r}")


def check(baseline: dict[str, Any], current: dict[str, Any]) -> None:
    """Validate the frozen surface while allowing new paths and optional fields."""
    _require_additive(baseline, current)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-baseline", action="store_true")
    args = parser.parse_args()
    current = _schema()
    if args.write_baseline:
        BASELINE.parent.mkdir(parents=True, exist_ok=True)
        BASELINE.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return 0
    try:
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"OpenAPI baseline is missing: {BASELINE}", file=sys.stderr)
        return 2
    try:
        check(baseline, current)
    except CompatibilityError as exc:
        print(f"OpenAPI compatibility check failed: {exc}", file=sys.stderr)
        return 1
    print("OpenAPI compatibility check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
