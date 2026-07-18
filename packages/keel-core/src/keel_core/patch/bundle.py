"""Immutable, content-addressed proposal-bundle artifacts (WS-PP).

A controlled patch proposal's heavy evidence — the exact unified ``base..head`` diff, per-command
test logs, and the canonical manifest tying it all together — is persisted as immutable,
content-addressed artifacts (SHA-256), never as a mutable DB blob. The durable ``patch_proposals``
row only stores the *hashes*:

* each test log is stored and referenced by its ``log_sha256``;
* the unified diff is stored and referenced by ``diff_sha256``;
* the canonical manifest JSON (:class:`~keel_core.patch.models.PatchBundleManifest`) is stored and
  its content hash becomes the proposal's ``bundle_sha256`` — the value a human approval binds to.

Re-storing identical bytes is idempotent (same hash), so a retried generation that reproduces the
same tree yields the same bundle hash. Reads verify the content hash on the way out (the artifact
store already re-hashes), so a tampered artifact fails closed.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta

from keel_core.coding.models import ArtifactRetention, CodingRunId, ProjectId
from keel_core.coding.protocols import ArtifactStore

from .errors import PatchValidationError
from .models import PatchBundleManifest, TestResult

_BUNDLE_ARTIFACT_KIND = "patch_bundle"
MANIFEST_NAME = "bundle.json"
DIFF_NAME = "proposal.diff"
_TEST_LOG_NAME = "test.log"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def render_manifest_bytes(manifest: PatchBundleManifest) -> bytes:
    """Canonical, sorted-key JSON so an identical proposal is byte-identical (stable hash)."""
    return json.dumps(manifest.to_dict(), sort_keys=True, separators=(",", ":")).encode("utf-8")


@dataclass(frozen=True, slots=True)
class StoredBundle:
    manifest: PatchBundleManifest
    bundle_sha256: str
    diff_sha256: str
    manifest_bytes: int
    diff_bytes: int


class PatchBundleWriter:
    """Render + persist a proposal bundle as immutable, content-addressed artifacts."""

    def __init__(self, store: ArtifactStore, *, retention_days: int = 90) -> None:
        self._store = store
        self._retention_days = retention_days

    def store_test_log(
        self, *, project_id: str, coding_run_id: str, command: str, log: bytes, now: datetime
    ) -> str:
        """Persist one test log and return its content hash (referenced from the manifest)."""
        record = self._store.put(
            ProjectId(project_id),
            CodingRunId(coding_run_id),
            log,
            name=_TEST_LOG_NAME,
            media_type="text/plain; charset=utf-8",
            retention=ArtifactRetention.retained,
            retained_until=now + timedelta(days=self._retention_days),
            metadata={"kind": _BUNDLE_ARTIFACT_KIND, "artifact": "test_log"},
        )
        return record.content_hash

    def store(
        self,
        manifest: PatchBundleManifest,
        *,
        diff: bytes,
        project_handle: str,
        coding_run_id: str,
        now: datetime | None = None,
    ) -> StoredBundle:
        created = now or manifest.created_at
        pid = ProjectId(project_handle)
        rid = CodingRunId(coding_run_id)
        diff_hash = sha256_hex(diff)
        if manifest.diff_sha256 != diff_hash:
            raise PatchValidationError("manifest diff_sha256 does not match the diff bytes")
        retained_until = created + timedelta(days=self._retention_days)
        metadata = {
            "kind": _BUNDLE_ARTIFACT_KIND,
            "proposal_id": manifest.proposal_id,
            "base_sha": manifest.base_sha,
            "head_sha": manifest.head_sha,
        }
        diff_record = self._store.put(
            pid,
            rid,
            diff,
            name=DIFF_NAME,
            media_type="text/x-diff; charset=utf-8",
            retention=ArtifactRetention.retained,
            retained_until=retained_until,
            metadata={**metadata, "artifact": "diff"},
        )
        manifest_bytes = render_manifest_bytes(manifest)
        manifest_record = self._store.put(
            pid,
            rid,
            manifest_bytes,
            name=MANIFEST_NAME,
            media_type="application/json",
            retention=ArtifactRetention.retained,
            retained_until=retained_until,
            metadata={**metadata, "artifact": "manifest", "diff_sha256": diff_record.content_hash},
        )
        return StoredBundle(
            manifest=manifest,
            bundle_sha256=manifest_record.content_hash,
            diff_sha256=diff_record.content_hash,
            manifest_bytes=manifest_record.size_bytes,
            diff_bytes=diff_record.size_bytes,
        )


class PatchBundleReader:
    """Read + verify immutable proposal-bundle artifacts by content hash (fail closed)."""

    def __init__(self, store: ArtifactStore) -> None:
        self._store = store

    def read_manifest(
        self, *, project_id: str, coding_run_id: str, bundle_sha256: str
    ) -> PatchBundleManifest:
        raw = self._store.read(ProjectId(project_id), CodingRunId(coding_run_id), bundle_sha256)
        if sha256_hex(raw) != bundle_sha256:
            raise PatchValidationError("bundle manifest content hash mismatch")
        manifest = PatchBundleManifest.from_dict(json.loads(raw.decode("utf-8")))
        return manifest

    def read_diff(self, *, project_id: str, coding_run_id: str, diff_sha256: str) -> bytes:
        raw = self._store.read(ProjectId(project_id), CodingRunId(coding_run_id), diff_sha256)
        if sha256_hex(raw) != diff_sha256:
            raise PatchValidationError("bundle diff content hash mismatch")
        return raw


def aggregate_test_status(tests: tuple[TestResult, ...]) -> str:
    """Aggregate individual command outcomes into a single :class:`TestStatus` value."""
    if not tests:
        return "skipped"
    if all(t.passed for t in tests):
        return "passed"
    return "failed"


__all__ = [
    "DIFF_NAME",
    "MANIFEST_NAME",
    "PatchBundleReader",
    "PatchBundleWriter",
    "StoredBundle",
    "aggregate_test_status",
    "render_manifest_bytes",
    "sha256_hex",
]
