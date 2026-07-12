"""Memory consolidation subsystem (spec 2026-07-12-memory-consolidation).

A scheduled, unattended agent reviews recent conversation and durably records
lasting facts: it *proposes* core-memory rewrites (human-reviewed) and inserts
deduplicated, provenance-tagged archival passages. The public surface is
finalized in the package re-exports below (see the individual submodules).
"""

from __future__ import annotations
