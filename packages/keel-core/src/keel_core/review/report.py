"""Immutable, content-addressed review report artifacts (JSON + Markdown) (WS-R).

Reuses the coding :class:`~keel_core.coding.protocols.ArtifactStore` seam so a review report
is stored exactly like any other project artifact: content-addressed by SHA-256, retained,
and reaped/purged by the same lifecycle. The run's ``result_ref`` points at the JSON report's
content hash. Reports carry only reviewed diff content and findings — never a token, a raw
provider log, or a system prompt.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

from keel_core.coding.models import ArtifactRetention, CodingRunId, ProjectId
from keel_core.coding.protocols import ArtifactStore

from .models import ReviewReport, snippet_hash

REPORT_JSON_NAME = "review-report.json"
REPORT_MARKDOWN_NAME = "review-report.md"
_REVIEW_ARTIFACT_KIND = "review_report"


def render_json_bytes(report: ReviewReport) -> bytes:
    """Canonical (sorted-key) JSON encoding of a report — stable content address."""
    return (json.dumps(report.to_dict(), sort_keys=True, indent=2) + "\n").encode("utf-8")


def render_markdown(report: ReviewReport, summary: str) -> str:
    """Human-facing Markdown rendering of a review report."""
    lines: list[str] = []
    lines.append(f"# Code Review — {report.project_id}")
    lines.append("")
    lines.append(f"- **Review**: `{report.review_id}`")
    lines.append(f"- **Run**: `{report.run_id}`")
    lines.append(f"- **Source**: {report.source.value}")
    lines.append(f"- **Base**: `{report.base_sha}`")
    lines.append(f"- **Head**: `{report.head_sha}`")
    lines.append(f"- **Model**: `{report.model}`")
    lines.append(f"- **Status**: {report.status.value}")
    lines.append(
        f"- **Findings**: {len(report.findings)} "
        f"({', '.join(f'{k}={v}' for k, v in report.severity_counts.items() if v)})"
        if any(report.severity_counts.values())
        else f"- **Findings**: {len(report.findings)}"
    )
    lines.append(
        f"- **Tokens**: prompt={report.prompt_tokens} completion={report.completion_tokens} "
        f"cost_usd={report.cost_usd:.6f}"
    )
    if report.truncated:
        lines.append("- **Note**: the diff was truncated to the configured size limit.")
    lines.append("")
    if summary:
        lines.append("## Summary")
        lines.append("")
        lines.append(summary)
        lines.append("")
    lines.append("## Findings")
    lines.append("")
    if not report.findings:
        lines.append("_No issues were identified in the reviewed change set._")
        lines.append("")
    for index, finding in enumerate(report.findings, start=1):
        verified = "verified" if finding.verified else "unverified"
        lines.append(
            f"### {index}. {finding.title} "
            f"({finding.severity.value}/{finding.confidence.value}, {verified})"
        )
        lines.append("")
        lines.append(f"- **File**: `{finding.file_path}`")
        span = (
            f"{finding.line_start}"
            if finding.line_start == finding.line_end
            else f"{finding.line_start}–{finding.line_end}"
        )
        lines.append(f"- **Lines**: {span}")
        if finding.verification:
            lines.append(f"- **Evidence**: {finding.verification}")
        lines.append("")
        lines.append(finding.explanation)
        lines.append("")
        lines.append("```")
        lines.append(finding.snippet)
        lines.append("```")
        lines.append("")
        lines.append(f"**Recommendation**: {finding.recommendation}")
        lines.append("")
    if report.limitations:
        lines.append("## Limitations")
        lines.append("")
        for limitation in report.limitations:
            lines.append(f"- {limitation}")
        lines.append("")
    lines.append("---")
    lines.append(
        "_This is an automated, read-only review. It makes no code changes and posts no "
        "comments. A human must validate every finding before acting on it._"
    )
    lines.append("")
    return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class StoredReport:
    report: ReviewReport
    json_sha256: str
    markdown_sha256: str
    json_bytes: int
    markdown_bytes: int


class ReviewArtifactWriter:
    """Render + persist a review report as immutable, content-addressed artifacts."""

    def __init__(self, store: ArtifactStore) -> None:
        self._store = store

    def store(
        self,
        report: ReviewReport,
        *,
        project_id: str,
        coding_run_id: str,
        summary: str,
        retained_until: datetime | None = None,
    ) -> StoredReport:
        pid = ProjectId(project_id)
        rid = CodingRunId(coding_run_id)
        markdown = render_markdown(report, summary)
        markdown_bytes = markdown.encode("utf-8")
        markdown_digest = snippet_hash(markdown)
        # Embed the markdown artifact hash into the JSON body (a body cannot hold its own hash).
        final_report = report.with_markdown_hash(markdown_digest)
        json_bytes = render_json_bytes(final_report)
        metadata = {
            "kind": _REVIEW_ARTIFACT_KIND,
            "review_id": final_report.review_id,
            "source": final_report.source.value,
            "head_sha": final_report.head_sha,
            "base_sha": final_report.base_sha,
        }
        markdown_record = self._store.put(
            pid,
            rid,
            markdown_bytes,
            name=REPORT_MARKDOWN_NAME,
            media_type="text/markdown; charset=utf-8",
            retention=ArtifactRetention.retained,
            retained_until=retained_until,
            metadata={**metadata, "artifact": "markdown"},
        )
        json_record = self._store.put(
            pid,
            rid,
            json_bytes,
            name=REPORT_JSON_NAME,
            media_type="application/json",
            retention=ArtifactRetention.retained,
            retained_until=retained_until,
            metadata={**metadata, "artifact": "json", "markdown_sha256": markdown_digest},
        )
        return StoredReport(
            report=final_report,
            json_sha256=json_record.content_hash,
            markdown_sha256=markdown_record.content_hash,
            json_bytes=json_record.size_bytes,
            markdown_bytes=markdown_record.size_bytes,
        )


__all__ = [
    "REPORT_JSON_NAME",
    "REPORT_MARKDOWN_NAME",
    "ReviewArtifactWriter",
    "StoredReport",
    "render_json_bytes",
    "render_markdown",
]
