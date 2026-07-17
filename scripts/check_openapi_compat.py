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
_DOCUMENTATION_KEYS = frozenset(
    {"description", "summary", "title", "example", "examples", "externalDocs"}
)


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


def _param_identity(param: Any) -> tuple[str, str] | None:
    """The stable ``(name, in)`` identity of an OpenAPI parameter object, or ``None``.

    Two parameter descriptions are the *same* parameter iff they share this identity, so an
    endpoint may grow additional optional parameters (a different identity) without breaking a
    client that never sent them, while a rename/removal or an incompatible change to an
    existing one is still a breaking change.
    """
    if isinstance(param, dict):
        name, location = param.get("name"), param.get("in")
        if isinstance(name, str) and isinstance(location, str):
            return (name, location)
    return None


def _param_type(param: dict[str, Any]) -> Any:
    schema = param.get("schema")
    return schema.get("type") if isinstance(schema, dict) else None


def _require_additive_parameters(baseline: list[Any], current: list[Any], path: str) -> None:
    """Compare an OpenAPI ``parameters`` array by stable ``(name, in)`` identity.

    Additive, backward-compatible evolution is allowed; contract-narrowing is not:

    * a baseline parameter that is **removed** (its identity is gone) fails;
    * a matched parameter whose ``required`` flag is **tightened** (optional -> required) or
      whose ``schema`` **changes non-additively** (its ``type`` changes, or any schema value is
      removed/altered) fails;
    * a **newly added** parameter is allowed only when it is optional (``required`` falsy) — a
      new required parameter would break an existing caller that never sent it;
    * a **duplicate** ``(name, in)`` identity — in either the baseline or the current list — is
      rejected: a parameters array with two descriptions of the same parameter is ambiguous and
      cannot be reasoned about additively.

    Parameters without a resolvable identity (no ``name``/``in``) fall back to the strict,
    order-sensitive list comparison so nothing is silently skipped.
    """
    if not isinstance(current, list):
        raise CompatibilityError(f"{path} changed from a parameter list")
    base_ids = {_param_identity(p) for p in baseline}
    if None in base_ids or any(_param_identity(p) is None for p in current):
        # An unidentifiable parameter (no name/in) — fall back to exact comparison so we never
        # under-report a change we cannot reason about by identity.
        if baseline != current:
            raise CompatibilityError(f"{path} changed")
        return
    # Reject duplicate identities on either side: an ambiguous parameters array (the same
    # (name, in) described twice) is not additively comparable.
    _reject_duplicate_parameters(baseline, path, "baseline")
    _reject_duplicate_parameters(current, path, "current")
    cur_by_id: dict[tuple[str, str], dict[str, Any]] = {}
    for param in current:
        pid = _param_identity(param)
        if pid is not None and isinstance(param, dict):
            cur_by_id[pid] = param
    for param in baseline:
        pid = _param_identity(param)
        assert pid is not None and isinstance(param, dict)  # narrowed above
        current_param = cur_by_id.get(pid)
        if current_param is None:
            raise CompatibilityError(f"{path}[{pid[1]}:{pid[0]}] was removed")
        if not bool(param.get("required", False)) and bool(current_param.get("required", False)):
            raise CompatibilityError(f"{path}[{pid[1]}:{pid[0]}] became required")
        old_type, new_type = _param_type(param), _param_type(current_param)
        if old_type is not None and new_type is not None and old_type != new_type:
            raise CompatibilityError(
                f"{path}[{pid[1]}:{pid[0]}] type changed from {old_type!r} to {new_type!r}"
            )
        # A parameter's ``schema`` must evolve additively too (format/enum/$ref/nested changes,
        # or a removed constraint, are breaking); new schema keys remain additive.
        old_schema, new_schema = param.get("schema"), current_param.get("schema")
        if isinstance(old_schema, dict):
            _require_additive(old_schema, new_schema, f"{path}[{pid[1]}:{pid[0]}].schema")
    for pid, param in cur_by_id.items():
        if pid not in base_ids and bool(param.get("required", False)):
            raise CompatibilityError(f"{path}[{pid[1]}:{pid[0]}] added as a required parameter")


def _reject_duplicate_parameters(params: list[Any], path: str, side: str) -> None:
    """Raise if any ``(name, in)`` identity appears more than once in ``params``."""
    seen: set[tuple[str, str]] = set()
    for param in params:
        pid = _param_identity(param)
        if pid is None:
            continue
        if pid in seen:
            raise CompatibilityError(
                f"{path}[{pid[1]}:{pid[0]}] is duplicated in the {side} parameter list"
            )
        seen.add(pid)


def _require_additive(baseline: Any, current: Any, path: str = "$", key: str | None = None) -> None:
    """Require every old node to remain; new mapping keys are additive."""
    if key == "parameters" and isinstance(baseline, list):
        _require_additive_parameters(baseline, current, path)
        return
    if isinstance(baseline, dict):
        if not isinstance(current, dict):
            raise CompatibilityError(f"{path} changed from an object")
        for key, old_value in baseline.items():
            if key in _DOCUMENTATION_KEYS:
                continue
            if key not in current:
                raise CompatibilityError(f"{path}.{key} was removed")
            _require_additive(old_value, current[key], f"{path}.{key}", key)
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
