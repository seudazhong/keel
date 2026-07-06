"""Output bounding + spill tests."""

from __future__ import annotations

from pathlib import Path

from keel_core.tools.bounding import bound_output


def test_small_output_unchanged() -> None:
    result = bound_output("hello")
    assert result.truncated is False
    assert result.text == "hello"
    assert result.spill_path is None


def test_large_output_truncated_and_spilled(tmp_path: Path) -> None:
    text = "\n".join(f"line {i}" for i in range(5000))
    result = bound_output(text, max_lines=100, spill_dir=tmp_path)
    assert result.truncated is True
    assert result.spill_path is not None
    assert Path(result.spill_path).read_text(encoding="utf-8") == text  # full output retained
    assert "truncated" in result.text


def test_byte_budget(tmp_path: Path) -> None:
    text = "x" * 100_000  # one long line
    result = bound_output(text, max_bytes=1_000, spill_dir=tmp_path)
    assert result.truncated is True
    assert len(result.text.encode("utf-8")) < 2_000  # clipped near budget (+ note)
