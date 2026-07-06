"""Deterministic testing helpers (NFR-12).

M0 ships the provider **record/replay** skeleton so core tests never hit a live
model. Record mode (calling the real LiteLLM gateway and saving) lands with the
ProviderGateway in M1; the cassette format and replay are frozen here.
"""

from __future__ import annotations

from keel_core.testing.record_replay import Cassette, ReplayProviderGateway, request_key

__all__ = ["Cassette", "ReplayProviderGateway", "request_key"]
