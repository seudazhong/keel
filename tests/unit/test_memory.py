"""Core-memory formatting (pure)."""

from __future__ import annotations

from keel_core.memory import format_core_memory


def test_renders_defaults_and_extras() -> None:
    out = format_core_memory({"human": "name is X", "notes": "likes tea"})
    assert out.startswith("<core_memory>")
    assert out.rstrip().endswith("</core_memory>")
    assert "<persona></persona>" in out  # empty default still shown
    assert "<human>name is X</human>" in out
    assert "<notes>likes tea</notes>" in out  # extras rendered after defaults
    assert out.index("<persona>") < out.index("<notes>")  # defaults first
