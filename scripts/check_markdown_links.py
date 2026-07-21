"""Validate local Markdown links and heading anchors.

The check includes tracked and untracked, non-ignored Markdown files so newly created
documentation is validated before it is staged.
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
FENCED_CODE = re.compile(r"(^|\n)(```|~~~).*?\n\2(?=\n|$)", re.DOTALL)
INLINE_LINK = re.compile(r"!?\[(?:[^\[\]]|\[[^\]]*])*\]\(([^)]+)\)")
REFERENCE_LINK = re.compile(r"^\s*\[[^\]]+]:\s*(\S+)", re.MULTILINE)
HEADING = re.compile(r"^ {0,3}(#{1,6})\s+(.+?)\s*#*\s*$", re.MULTILINE)
HTML_ID = re.compile(r"\b(?:id|name)=[\"']([^\"']+)[\"']", re.IGNORECASE)


def markdown_files() -> list[Path]:
    result = subprocess.run(
        [
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            "*.md",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return [ROOT / line for line in result.stdout.splitlines() if line]


def strip_code(text: str) -> str:
    return FENCED_CODE.sub("\n", text)


def link_target(raw: str) -> str:
    target = raw.strip()
    if target.startswith("<") and ">" in target:
        return target[1 : target.index(">")]
    # Markdown permits an optional quoted title after the destination.
    match = re.match(r"(\S+)(?:\s+[\"'(].*)?$", target)
    return match.group(1) if match else target


def github_slug(text: str) -> str:
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"\[([^\]]+)]\([^)]*\)", r"\1", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s", "-", text)
    return text


def anchors(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    result = {unquote(value).lower() for value in HTML_ID.findall(text)}
    seen: defaultdict[str, int] = defaultdict(int)
    for match in HEADING.finditer(strip_code(text)):
        base = github_slug(match.group(2))
        if not base:
            continue
        suffix = seen[base]
        seen[base] += 1
        result.add(base if suffix == 0 else f"{base}-{suffix}")
    return result


def targets(text: str) -> list[str]:
    clean = strip_code(text)
    return [link_target(raw) for raw in INLINE_LINK.findall(clean)] + [
        link_target(raw) for raw in REFERENCE_LINK.findall(clean)
    ]


def validate() -> list[str]:
    errors: list[str] = []
    anchor_cache: dict[Path, set[str]] = {}
    for source in markdown_files():
        text = source.read_text(encoding="utf-8")
        for target in targets(text):
            if not target:
                continue
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc or target.startswith("mailto:"):
                continue

            relative = unquote(parsed.path)
            resolved = source if not relative else (source.parent / relative).resolve()
            try:
                display = source.relative_to(ROOT)
            except ValueError:
                display = source

            if not resolved.exists():
                errors.append(f"{display}: missing path {relative or target}")
                continue

            fragment = unquote(parsed.fragment).lower()
            if not fragment or resolved.suffix.lower() != ".md":
                continue
            known = anchor_cache.setdefault(resolved, anchors(resolved))
            if fragment not in known:
                try:
                    destination = resolved.relative_to(ROOT)
                except ValueError:
                    destination = resolved
                errors.append(f"{display}: missing anchor #{fragment} in {destination}")
    return errors


def main() -> int:
    errors = validate()
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("Markdown links and anchors: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
