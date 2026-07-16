"""Idempotent, non-destructive demo data bootstrap (scripts/seed_demo_data.py)."""

from __future__ import annotations

from .runner import (
    DEFAULT_SCOPE_ID,
    DemoBootstrapError,
    DemoBootstrapResult,
    describe_plan,
    render_result,
    run_bootstrap,
)

__all__ = [
    "DEFAULT_SCOPE_ID",
    "DemoBootstrapError",
    "DemoBootstrapResult",
    "describe_plan",
    "render_result",
    "run_bootstrap",
]
