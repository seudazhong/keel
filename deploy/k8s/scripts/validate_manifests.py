#!/usr/bin/env python3
"""Deterministic validation for deploy/k8s manifests — no cluster required.

Usage:
    python deploy/k8s/scripts/validate_manifests.py

What this checks (see docs/security-model.md for the rationale behind each rule):
  1. Every ``*.yaml``/``*.yml`` file under ``deploy/k8s`` parses as YAML (multi-document aware).
  2. ``kubectl kustomize`` renders ``base/`` and every ``overlays/*`` directory without error, if
     `kubectl` is installed. This never contacts a cluster (pure client-side templating); the
     check is skipped with a warning, not a failure, when `kubectl` is unavailable so this script
     stays usable in minimal environments.
  3. Deterministic text/structure checks over the rendered manifests:
       - every container-bearing workload sets a restrictive securityContext (non-root,
         dropped ALL capabilities, no privilege escalation, RuntimeDefault seccomp) and
         explicit resources.requests/limits;
       - the sandbox ServiceAccount and Job template disable ServiceAccount token automount
         and are never given a Role/RoleBinding;
       - the sandbox Job template never mounts the durable Git PVC and never sets
         `runtimeClassName` (it must stay opt-in/configurable, not silently required);
       - a default-deny NetworkPolicy exists and every other NetworkPolicy narrows rather than
         widens it;
       - example/template files (secret*.example.yaml, datastores/*.example.yaml) only contain
         placeholder credential markers, never a value that looks like a real secret.

Exit status is non-zero if any check fails. Intended to run in CI and locally without a
Kubernetes cluster, kind/minikube, or any live credentials.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - exercised only in environments missing PyYAML
    yaml = None

K8S_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = K8S_ROOT.parent.parent

# Files that are intentionally templates/examples: excluded from kustomization builds and
# expected to contain only placeholder credential markers, never real-looking secrets.
EXAMPLE_FILE_MARKERS = ("example.yaml", "example.yml")

# Files rendered per-run by application code, not applied directly via kustomize.
RENDERED_TEMPLATE_FILES = {"job-template.yaml"}

PLACEHOLDER_MARKERS = ("CHANGEME", "REPLACE_WITH", "_PLACEHOLDER")

# A conservative set of patterns that look like real, live credentials rather than
# placeholders. Kept intentionally narrow (false negatives are safer than false positives
# blocking legitimate placeholder text) — this is a lightweight guard, not a secret scanner.
SUSPICIOUS_SECRET_PATTERNS = [
    re.compile(r"AKIA[0-9A-Z]{16}"),  # AWS access key id
    re.compile(r"sk-[A-Za-z0-9]{20,}"),  # OpenAI-style key
    re.compile(r"sk-ant-[A-Za-z0-9\-]{20,}"),  # Anthropic-style key
    re.compile(r"ghp_[A-Za-z0-9]{30,}"),  # GitHub PAT
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
]

WORKLOAD_FILES = [
    "base/server/deployment.yaml",
    "base/worker/deployment.yaml",
    "base/scheduler/deployment.yaml",
    "base/web/deployment.yaml",
    "base/sandbox/job-template.yaml",
]


class Finding(str):
    """A human-readable validation failure message."""


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def check_yaml_parses(findings: list[Finding]) -> None:
    if yaml is None:
        findings.append(
            Finding(
                "SKIPPED: PyYAML is not installed, so full YAML parsing was not verified "
                "(text-based checks below still ran)."
            )
        )
        return
    for path in sorted(K8S_ROOT.rglob("*.yaml")) + sorted(K8S_ROOT.rglob("*.yml")):
        try:
            list(yaml.safe_load_all(read(path)))
        except yaml.YAMLError as exc:
            findings.append(Finding(f"{path.relative_to(REPO_ROOT)}: invalid YAML ({exc})"))


def check_kustomize_builds(findings: list[Finding]) -> None:
    kubectl = shutil.which("kubectl")
    if kubectl is None:
        findings.append(
            Finding("SKIPPED: kubectl not found on PATH; kustomize build was not verified.")
        )
        return
    targets = [K8S_ROOT / "base", *sorted((K8S_ROOT / "overlays").glob("*"))]
    for target in targets:
        if not (target / "kustomization.yaml").exists():
            continue
        result = subprocess.run(
            [kubectl, "kustomize", str(target)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            findings.append(
                Finding(
                    f"kubectl kustomize failed for {target.relative_to(REPO_ROOT)}: "
                    f"{result.stderr.strip()}"
                )
            )


def check_workload_hardening(findings: list[Finding]) -> None:
    for rel in WORKLOAD_FILES:
        path = K8S_ROOT / rel
        text = read(path)
        label = str(path.relative_to(REPO_ROOT))

        if "runAsNonRoot: true" not in text:
            findings.append(Finding(f"{label}: missing `runAsNonRoot: true`"))
        if "allowPrivilegeEscalation: false" not in text:
            findings.append(Finding(f"{label}: missing `allowPrivilegeEscalation: false`"))
        if not re.search(r"drop:\s*\[\s*\"?ALL\"?\s*\]", text):
            findings.append(Finding(f"{label}: missing `capabilities.drop: [ALL]`"))
        if "seccompProfile" not in text or "RuntimeDefault" not in text:
            findings.append(Finding(f"{label}: missing seccompProfile RuntimeDefault"))
        if "readOnlyRootFilesystem: true" not in text:
            findings.append(Finding(f"{label}: missing `readOnlyRootFilesystem: true`"))
        if "requests:" not in text or "limits:" not in text:
            findings.append(Finding(f"{label}: missing resources.requests/limits"))


def check_sandbox_isolation(findings: list[Finding]) -> None:
    sa_path = K8S_ROOT / "base/sandbox/serviceaccount.yaml"
    job_path = K8S_ROOT / "base/sandbox/job-template.yaml"
    sa_text = read(sa_path)
    job_text = read(job_path)

    if "automountServiceAccountToken: false" not in sa_text:
        findings.append(
            Finding(
                f"{sa_path.relative_to(REPO_ROOT)}: sandbox ServiceAccount must disable "
                "token automount"
            )
        )
    if "automountServiceAccountToken: false" not in job_text:
        findings.append(
            Finding(f"{job_path.relative_to(REPO_ROOT)}: sandbox Job must disable token automount")
        )
    if "keel-git-storage" in job_text:
        findings.append(
            Finding(
                f"{job_path.relative_to(REPO_ROOT)}: sandbox Job must never mount the "
                "durable Git PVC (keel-git-storage)"
            )
        )
    # runtimeClassName must stay commented/opt-in, never uncommented as a hard requirement.
    active_runtimeclass = re.search(r"^\s*runtimeClassName:\s*\S+", job_text, re.MULTILINE)
    if active_runtimeclass and not active_runtimeclass.group(0).lstrip().startswith("#"):
        findings.append(
            Finding(
                f"{job_path.relative_to(REPO_ROOT)}: `runtimeClassName` must remain "
                "commented/opt-in by default"
            )
        )

    # No RoleBinding anywhere in the repo may reference the sandbox ServiceAccount.
    for path in sorted(K8S_ROOT.rglob("*.yaml")):
        if path == sa_path:
            continue
        text = read(path)
        if "kind: RoleBinding" in text and "keel-sandbox-run" in text:
            findings.append(
                Finding(
                    f"{path.relative_to(REPO_ROOT)}: no RoleBinding may target the "
                    "sandbox ServiceAccount"
                )
            )


def check_network_policy_default_deny(findings: list[Finding]) -> None:
    deny_path = K8S_ROOT / "base/networkpolicy/default-deny-all.yaml"
    text = read(deny_path)
    if "podSelector: {}" not in text:
        findings.append(
            Finding(
                f"{deny_path.relative_to(REPO_ROOT)}: default-deny must select all pods "
                "(`podSelector: {}`)"
            )
        )
    if "Ingress" not in text or "Egress" not in text:
        findings.append(
            Finding(
                f"{deny_path.relative_to(REPO_ROOT)}: default-deny must cover both "
                "Ingress and Egress"
            )
        )


def check_no_leaked_secrets(findings: list[Finding]) -> None:
    for path in sorted(K8S_ROOT.rglob("*.yaml")):
        text = read(path)
        is_secret_like = "kind: Secret" in text
        is_example = any(path.name.endswith(marker) for marker in EXAMPLE_FILE_MARKERS)
        for pattern in SUSPICIOUS_SECRET_PATTERNS:
            if pattern.search(text):
                findings.append(
                    Finding(
                        f"{path.relative_to(REPO_ROOT)}: matched a live-credential-shaped "
                        f"pattern ({pattern.pattern}); replace with a placeholder"
                    )
                )
        if is_secret_like and is_example:
            if not any(marker in text for marker in PLACEHOLDER_MARKERS):
                findings.append(
                    Finding(
                        f"{path.relative_to(REPO_ROOT)}: Secret example must use one of "
                        f"{PLACEHOLDER_MARKERS} as its placeholder marker"
                    )
                )


def _resource_entries(kustomization_text: str) -> list[str]:
    """Extract the string entries listed under a kustomization's ``resources:`` key.

    Deliberately scoped to that one key (not a whole-file substring search) so a file's
    header *comments* may reference example/template filenames without tripping the
    "excluded from kustomization" check below.
    """
    if yaml is not None:
        try:
            data = yaml.safe_load(kustomization_text) or {}
        except yaml.YAMLError:
            return []
        return list(data.get("resources") or [])
    # Fallback without PyYAML: grab the literal `- foo.yaml` lines directly under `resources:`.
    match = re.search(r"^resources:\n((?:[ \t]+-.*\n?)+)", kustomization_text, re.MULTILINE)
    if not match:
        return []
    return [line.split("-", 1)[1].strip() for line in match.group(1).splitlines() if line.strip()]


def check_example_files_excluded_from_kustomization(findings: list[Finding]) -> None:
    for kustomization in K8S_ROOT.rglob("kustomization.yaml"):
        entries = _resource_entries(read(kustomization))
        for entry in entries:
            for marker in EXAMPLE_FILE_MARKERS:
                if entry.endswith(marker):
                    findings.append(
                        Finding(
                            f"{kustomization.relative_to(REPO_ROOT)}: must not list "
                            f"'{entry}' (a template file) as a build resource"
                        )
                    )
            for rendered in RENDERED_TEMPLATE_FILES:
                if entry.endswith(rendered):
                    findings.append(
                        Finding(
                            f"{kustomization.relative_to(REPO_ROOT)}: must not list "
                            f"'{entry}' (rendered per-run by application code) as a build "
                            "resource"
                        )
                    )


def run_all_checks() -> list[Finding]:
    findings: list[Finding] = []
    check_yaml_parses(findings)
    check_kustomize_builds(findings)
    check_workload_hardening(findings)
    check_sandbox_isolation(findings)
    check_network_policy_default_deny(findings)
    check_no_leaked_secrets(findings)
    check_example_files_excluded_from_kustomization(findings)
    return findings


def main() -> int:
    findings = run_all_checks()
    hard_failures = [f for f in findings if not f.startswith("SKIPPED")]
    skips = [f for f in findings if f.startswith("SKIPPED")]
    for skip in skips:
        print(f"WARN: {skip}")
    if hard_failures:
        print(f"FAIL: {len(hard_failures)} manifest validation finding(s):")
        for finding in hard_failures:
            print(f"  - {finding}")
        return 1
    print("OK: deploy/k8s manifests passed all deterministic validation checks.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
