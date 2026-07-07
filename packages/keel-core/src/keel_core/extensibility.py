"""Extensibility: skills, MCP tools, and discovery — behind an import guard (WS-G).

ADR-0009 / DESIGN-REVIEW G6 headline: **import ≠ trust** (invariant I8). Anything
imported at discovery time — an MCP server's tool description, a skill's
instructions, a plugin manifest — is untrusted text. Before it can influence the
loop it must clear :class:`ImportGuard`:

1. **allow-list** — only permitted sources (MCP servers / skill origins) are admitted;
2. **injection scan** (G6) — descriptions/instructions are scanned for prompt-injection
   and exfiltration patterns; a hit is **quarantined**, never registered.

Skills use **progressive disclosure**: only names + descriptions are exposed for
discovery (``tool_search``); a skill's full instructions load on activation.
MCP tools are surfaced through the one tool interface (P3).
"""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from keel_core.protocols import ToolContext, ToolResult

# --- injection scanning (G6) ---------------------------------------------------

_INJECTION_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"ignore\s+(all\s+|any\s+)?(the\s+)?(previous|prior|earlier|above)\s+(instructions|prompts?)",
        r"disregard\s+(all\s+)?(the\s+)?(previous|prior|above|system)",
        r"(reveal|print|show|leak|repeat)\s+(your\s+|the\s+)?(system\s+)?prompt",
        r"override\s+(your\s+)?(instructions|guidelines|rules)",
        r"exfiltrat",
        r"\b(send|email|e-mail|post|upload|forward|leak)\b.{0,60}"
        r"\b(secret|secrets|password|passwords|token|tokens|api[_\s-]?keys?|credentials?)\b",
    )
)


def scan_for_injection(text: str) -> list[str]:
    """Return the injection/exfiltration patterns found in ``text`` (empty = clean)."""
    return [pattern.pattern for pattern in _INJECTION_PATTERNS if pattern.search(text)]


# --- import guard (I8) ---------------------------------------------------------


@dataclass
class ImportVerdict:
    """Outcome of importing an untrusted tool/skill description."""

    admitted: bool
    reason: str = ""

    @property
    def quarantined(self) -> bool:
        return not self.admitted


class ImportGuard:
    """Admit imported artifacts only if allow-listed **and** injection-clean (I8)."""

    def __init__(self, allowlist: Sequence[str]) -> None:
        self._allow = frozenset(allowlist)

    def admit(self, *, source: str, name: str, text: str) -> ImportVerdict:
        """Decide whether an imported ``(source, name, text)`` may be registered."""
        if source not in self._allow:
            return ImportVerdict(False, f"source not allow-listed: {source!r}")
        findings = scan_for_injection(text)
        if findings:
            return ImportVerdict(False, f"quarantined (injection scan): {name!r}")
        return ImportVerdict(True)


# --- skills (progressive disclosure) -------------------------------------------


@dataclass
class Skill:
    """A skill: a name + short description (always visible) and full instructions
    (loaded on activation — progressive disclosure)."""

    name: str
    description: str
    instructions: str
    source: str = "local"


class SkillCatalog:
    """Load skills from a directory of ``*.json`` manifests, guarded on import."""

    def __init__(self, guard: ImportGuard) -> None:
        self._guard = guard
        self._skills: dict[str, Skill] = {}
        self.quarantined: list[str] = []

    def load_dir(self, directory: Path) -> None:
        for path in sorted(Path(directory).glob("*.json")):
            data = json.loads(path.read_text(encoding="utf-8"))
            self.add(
                Skill(
                    name=str(data["name"]),
                    description=str(data.get("description", "")),
                    instructions=str(data.get("instructions", "")),
                    source=str(data.get("source", "local")),
                )
            )

    def add(self, skill: Skill) -> ImportVerdict:
        # The scanned text spans everything the skill could inject into the loop.
        verdict = self._guard.admit(
            source=skill.source,
            name=skill.name,
            text=f"{skill.description}\n{skill.instructions}",
        )
        if verdict.admitted:
            self._skills[skill.name] = skill
        else:
            self.quarantined.append(f"{skill.name}: {verdict.reason}")
        return verdict

    def describe(self) -> list[CatalogEntry]:
        """Names + descriptions only (progressive disclosure — no instructions)."""
        return [CatalogEntry(s.name, s.description) for s in self._skills.values()]

    def activate(self, name: str) -> str:
        """Return a skill's full instructions (loaded only when activated)."""
        skill = self._skills.get(name)
        if skill is None:
            raise KeyError(f"unknown or quarantined skill: {name!r}")
        return skill.instructions


# --- MCP client (tools behind the guard, P3) -----------------------------------


@dataclass
class MCPToolSpec:
    """A tool advertised by an MCP server."""

    name: str
    description: str
    input_schema: dict[str, Any] = field(default_factory=dict)


ListToolsFn = Callable[[], Awaitable[Sequence[MCPToolSpec]]]
CallToolFn = Callable[[str, dict[str, Any]], Awaitable[str]]


class MCPClient:
    """A client for one MCP server. The transport (stdio / HTTP+SSE) is injected as
    ``list_tools``/``call_tool`` callables so the framework stays transport-agnostic.
    """

    def __init__(self, server: str, list_tools: ListToolsFn, call_tool: CallToolFn) -> None:
        self.server = server
        self._list_tools = list_tools
        self._call_tool = call_tool

    async def import_tools(self, guard: ImportGuard) -> tuple[list[MCPTool], list[str]]:
        """List the server's tools; return the admitted ones + quarantine reasons (I8)."""
        admitted: list[MCPTool] = []
        quarantined: list[str] = []
        for spec in await self._list_tools():
            verdict = guard.admit(source=self.server, name=spec.name, text=spec.description)
            if verdict.admitted:
                admitted.append(MCPTool(self, spec))
            else:
                quarantined.append(f"{self.server}/{spec.name}: {verdict.reason}")
        return admitted, quarantined

    async def call(self, name: str, args: dict[str, Any]) -> str:
        return await self._call_tool(name, args)


class MCPTool:
    """An imported MCP tool surfaced through the one tool interface (P3)."""

    writes = True  # remote side effects unknown -> schedule conservatively

    def __init__(self, client: MCPClient, spec: MCPToolSpec) -> None:
        self._client = client
        self.name = spec.name
        self.description = spec.description
        self._schema = spec.input_schema or {"type": "object"}

    def input_schema(self) -> dict[str, Any]:
        return self._schema

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        return ToolResult(ok=True, output=await self._client.call(self.name, args))


# --- discovery (tool_search) ---------------------------------------------------


@dataclass
class CatalogEntry:
    """A discoverable capability: name + description (no payload)."""

    name: str
    description: str


_STOPWORDS = frozenset(
    {"the", "and", "for", "with", "from", "over", "into", "your", "you", "are", "how", "use"}
)


def _terms(text: str) -> set[str]:
    return {t for t in re.findall(r"\w+", text.lower()) if len(t) > 2 and t not in _STOPWORDS}


def tool_search(query: str, catalog: Sequence[CatalogEntry], *, k: int = 5) -> list[CatalogEntry]:
    """Rank catalog entries by lexical overlap with ``query`` (progressive discovery)."""
    terms = _terms(query)
    if not terms:
        return list(catalog[:k])

    def score(entry: CatalogEntry) -> int:
        return len(terms & _terms(f"{entry.name} {entry.description}"))

    ranked = sorted(catalog, key=score, reverse=True)
    return [entry for entry in ranked if score(entry) > 0][:k]


class ToolSearchTool:
    """``tool_search`` (P3): discover available capabilities by description."""

    name = "tool_search"
    description = "Search available tools and skills by a natural-language query."
    writes = False

    def __init__(self, catalog: Sequence[CatalogEntry]) -> None:
        self._catalog = list(catalog)

    def input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        }

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        hits = tool_search(str(args.get("query", "")), self._catalog)
        return ToolResult(ok=True, output="\n".join(f"{h.name}: {h.description}" for h in hits))
