#!/usr/bin/env python
"""Repo-root entry point for the idempotent, non-destructive demo data bootstrap.

    uv run python scripts/seed_demo_data.py --dry-run
    uv run python scripts/seed_demo_data.py --yes

See ``keel_worker.demo_bootstrap`` for the implementation. Refuses to run
outside a recognized dev/demo environment and refuses to mutate anything
without ``--yes`` (or ``--dry-run`` to only preview).
"""

from __future__ import annotations

from keel_worker.demo_bootstrap.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
