#!/usr/bin/env python
"""Repo-root entry point for the Knowledge eval suite."""

from __future__ import annotations

from keel_worker.knowledge_evals.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
