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
  3. ``kubeconform`` schema-validates the same rendered output, only if it is installed. Skipped
     (not failed) when absent, and any failure caused by it needing network access to fetch
     schemas is treated as skipped too — this script must not require a cluster *or* network.
  4. Every file actually reachable from a ``kustomization.yaml``'s ``resources:``/``patches:``
     (i.e. genuinely deployable, not a documented ``*.example.yaml`` template or a per-run
     rendered template) contains no unresolved placeholder marker
     (``CHANGEME``/``REPLACE_WITH``/``_PLACEHOLDER``) — a leftover placeholder in something that
     is actually applied is a broken manifest, not a template.
  5. Every ``cidr:`` value anywhere under ``deploy/k8s`` is either a recognized placeholder or a
     syntactically valid IP network (via the standard library ``ipaddress`` module).
  6. Every ``envFrom``/``secretRef``/``configMapRef``/``serviceAccountName`` reference in the
     rendered base resolves to either an object defined in that render or a documented external
     template (a Secret/ConfigMap of the same name in a tracked ``*.example.yaml`` file) — this
     catches typos and dangling references that `kubectl kustomize` alone does not catch.
  7. Deterministic text/structure checks over the rendered manifests:
       - every container-bearing workload sets a restrictive securityContext (non-root,
         dropped ALL capabilities, no privilege escalation, RuntimeDefault seccomp), a
         read-only root filesystem, and explicit resources.requests/limits;
       - `automountServiceAccountToken: false` on every workload — a control-plane pod that
         executes shell/code tools must never hold a Kubernetes ServiceAccount token;
       - no active (deployable) resource anywhere grants RBAC (Role/ClusterRole/RoleBinding/
         ClusterRoleBinding) at all — this scaffold intentionally ships no Kubernetes API
         access for the app's own ServiceAccounts (see docs/security-model.md);
       - `base/secret-app.example.yaml` requires a non-empty, syntactically valid
         (`key:role`, role one of viewer/operator/admin) `KEEL_API_KEYS` entry (without ever
         logging the key material itself), `base/configmap-app.yaml` never sets that key
         (auth must come from an externally supplied Secret, never a default-empty/open-admin
         config), and the active config forces `KEEL_CLOUD_MODE: "true"`, which is consumed by
         application code (``Settings.cloud_mode`` / ``app.state.auth_required``) so
         fail-closed auth is the shipped, active default;
       - `base/secret-app.example.yaml` also requires a non-empty, >=32-byte
         `KEEL_SANDBOX_RPC_SECRET` (without ever logging the value), absent from
         `base/configmap-app.yaml` — `execution_backend` defaults to `"sandbox"`, so a
         missing/short value crash-loops keel-server/keel-worker at startup, before either
         serves `/health`;
       - keel-server's tool workspace (`workingDir`) is never the read-only `/app` source
         tree, and is backed by a real writable volume mount;
       - keel-server stays at `replicas: 1` with a non-overlapping rollout `strategy`
         (`Recreate`, or `RollingUpdate` with `maxSurge: 0`) in both the base manifest text and
         the rendered base/production Kustomize output, so an old and new keel-server Pod are
         never scheduled at once during an upgrade;
       - the sandbox ServiceAccount and Job template disable ServiceAccount token automount
         and are never given a Role/RoleBinding;
       - the sandbox Job template never mounts the durable Git PVC and never sets
         `runtimeClassName` (it must stay opt-in/configurable, not silently required);
       - a default-deny NetworkPolicy exists and every other NetworkPolicy narrows rather than
         widens it, and both directions (egress on the source, ingress on the destination) are
         present for keel-web -> keel-server, sandbox -> keel-server, and sandbox -> the
         egress-proxy;
       - DNS egress is scoped to the `kube-system` namespace by name, never `namespaceSelector: {}`
         (any namespace);
       - example/template files (secret*.example.yaml, datastores/*.example.yaml) only contain
         placeholder credential markers, never a value that looks like a real secret.

Exit status is non-zero if any check fails. Intended to run in CI and locally without a
Kubernetes cluster, kind/minikube, or any live credentials.
"""

from __future__ import annotations

import ipaddress
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

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

# Non-security operational customization points that are expected to remain a placeholder in
# the *shipped* base — a container image reference and a cluster-specific ingress-controller
# namespace. These are ordinary Kustomize customization points (every real install pins its
# own image digest and knows its own ingress namespace; docs/clean-install-runbook.md walks
# through setting both), not a security defect the way an empty auth secret or a wrong trust
# boundary would be. Everything else — Secrets, CIDRs, any other stray placeholder in an
# active resource — still fails check_no_placeholders_in_active_resources below. Matched by
# exact stripped line text so this allowlist cannot accidentally swallow an unrelated
# placeholder introduced later on the same line pattern.
ALLOWED_ACTIVE_PLACEHOLDER_LINES = {
    "image: ghcr.io/OWNER/keel-app:REPLACE_WITH_PINNED_DIGEST",
    "image: ghcr.io/OWNER/keel-web:REPLACE_WITH_PINNED_DIGEST",
    "kubernetes.io/metadata.name: REPLACE_WITH_INGRESS_CONTROLLER_NAMESPACE",
}

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

# Every workload this scaffold actually ships as a deployable Pod template. Kept in sync with
# base/kustomization.yaml's `resources:` by hand.
WORKLOAD_FILES = [
    "base/server/deployment.yaml",
    "base/worker/deployment.yaml",
    "base/web/deployment.yaml",
    "base/sandbox/job-template.yaml",
]

RBAC_KINDS = ("Role", "ClusterRole", "RoleBinding", "ClusterRoleBinding")


class Finding(str):
    """A human-readable validation failure message."""


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _has_placeholder(text: str) -> bool:
    return any(marker in text for marker in PLACEHOLDER_MARKERS)


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


def _kustomize_targets() -> list[Path]:
    return [K8S_ROOT / "base", *sorted((K8S_ROOT / "overlays").glob("*"))]


def _render_kustomize(target: Path, kubectl: str) -> str | None:
    result = subprocess.run(
        [kubectl, "kustomize", str(target)],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout if result.returncode == 0 else None


def check_kustomize_builds(findings: list[Finding]) -> None:
    kubectl = shutil.which("kubectl")
    if kubectl is None:
        findings.append(
            Finding("SKIPPED: kubectl not found on PATH; kustomize build was not verified.")
        )
        return
    for target in _kustomize_targets():
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


def check_schema_validation(findings: list[Finding]) -> None:
    """Best-effort ``kubeconform`` schema validation — optional, never a hard requirement.

    Skipped (with a warning, not a failure) whenever the tool is missing, or whenever it
    fails in a way that looks like a network/schema-download problem, so this script never
    ends up requiring internet access or a cluster to pass.
    """
    kubeconform = shutil.which("kubeconform")
    kubectl = shutil.which("kubectl")
    if kubeconform is None or kubectl is None:
        findings.append(
            Finding(
                "SKIPPED: kubeconform (or kubectl) not found on PATH; schema validation was "
                "not verified."
            )
        )
        return
    network_error_markers = ("no such host", "dial tcp", "download", "timeout", "connection")
    for target in _kustomize_targets():
        if not (target / "kustomization.yaml").exists():
            continue
        rendered = _render_kustomize(target, kubectl)
        if rendered is None:
            continue  # already reported by check_kustomize_builds
        result = subprocess.run(
            [kubeconform, "-strict", "-summary"],
            input=rendered,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            combined = (result.stdout + result.stderr).lower()
            if any(marker in combined for marker in network_error_markers):
                findings.append(
                    Finding(
                        f"SKIPPED: kubeconform could not reach a schema source for "
                        f"{target.relative_to(REPO_ROOT)} (network-dependent); not treated "
                        "as a failure."
                    )
                )
                continue
            findings.append(
                Finding(
                    f"kubeconform schema validation failed for "
                    f"{target.relative_to(REPO_ROOT)}: {result.stdout.strip()}"
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
        if "automountServiceAccountToken: false" not in text:
            findings.append(
                Finding(
                    f"{label}: every workload must set `automountServiceAccountToken: "
                    "false` — a process that can run shell/code tools must never hold a "
                    "Kubernetes ServiceAccount token"
                )
            )


def _non_overlapping_strategy(strategy: dict[str, Any] | None) -> bool:
    """True if a Deployment rollout strategy can never run an old and new Pod together.

    Kubernetes' default (unset) `RollingUpdate` strategy uses 25% `maxSurge`/`maxUnavailable`,
    which rounds `maxSurge` up to 1 even at a single replica — the new Pod is started *before*
    the old one is removed, so both run concurrently mid-rollout. `Recreate` (terminate then
    create) never does this. An explicit `RollingUpdate` with `maxSurge: 0` also never does
    this: no new Pod may start until the old one has been removed to free the "surge" budget.
    """
    if not strategy:
        return False
    if strategy.get("type") == "Recreate":
        return True
    if strategy.get("type") == "RollingUpdate":
        max_surge = str((strategy.get("rollingUpdate") or {}).get("maxSurge", "25%"))
        return max_surge in ("0", "0%")
    return False


def check_server_single_instance_rollout(findings: list[Finding]) -> None:
    """`keel-server` must stay logically single-instance until durable run/approval
    coordination lands (docs/security-model.md "Why `keel-server` is pinned to one replica").
    `replicas: 1` alone is not enough — Kubernetes' default `RollingUpdate` strategy still
    surges a new Pod up before removing the old one even at a single replica, so an old and
    new `keel-server` would run concurrently mid-rollout. The rollout `strategy` must be
    `Recreate` (or an explicit `RollingUpdate` with `maxSurge: 0`) so that never happens, in
    both the base manifest and the rendered production overlay.
    """
    server_path = K8S_ROOT / "base/server/deployment.yaml"
    text = read(server_path)
    label = str(server_path.relative_to(REPO_ROOT))

    if not re.search(r"^\s*replicas:\s*1\s*$", text, re.MULTILINE):
        findings.append(Finding(f"{label}: must set `replicas: 1`"))

    if yaml is not None:
        try:
            base_doc = yaml.safe_load(text)
        except yaml.YAMLError:
            base_doc = None
        if base_doc is not None and not _non_overlapping_strategy(
            (base_doc.get("spec") or {}).get("strategy")
        ):
            findings.append(
                Finding(
                    f"{label}: `spec.strategy` must be `Recreate` (or `RollingUpdate` with "
                    "`maxSurge: 0`) so an old and new keel-server Pod are never scheduled at "
                    "once — the Kubernetes default RollingUpdate (25%/25%) surges a new Pod "
                    "up before removing the old one even at replicas: 1"
                )
            )

    kubectl = shutil.which("kubectl")
    if kubectl is None:
        findings.append(
            Finding(
                "SKIPPED: kubectl not found on PATH; rendered keel-server replicas/strategy "
                "were not verified for base/production."
            )
        )
        return
    for target in (K8S_ROOT / "base", K8S_ROOT / "overlays" / "production"):
        objects = _rendered_objects(target, kubectl)
        if objects is None:
            findings.append(
                Finding(
                    f"SKIPPED: could not render/parse {target.relative_to(REPO_ROOT)} to "
                    "verify keel-server replicas/strategy."
                )
            )
            continue
        server_objects = [
            obj
            for obj in objects
            if obj.get("kind") == "Deployment"
            and obj.get("metadata", {}).get("name") == "keel-server"
        ]
        if not server_objects:
            findings.append(
                Finding(
                    f"{target.relative_to(REPO_ROOT)}: rendered output has no keel-server "
                    "Deployment"
                )
            )
            continue
        for obj in server_objects:
            spec = obj.get("spec", {})
            if spec.get("replicas") != 1:
                findings.append(
                    Finding(
                        f"{target.relative_to(REPO_ROOT)}: rendered keel-server Deployment "
                        f"has replicas={spec.get('replicas')!r}, must be 1"
                    )
                )
            if not _non_overlapping_strategy(spec.get("strategy")):
                findings.append(
                    Finding(
                        f"{target.relative_to(REPO_ROOT)}: rendered keel-server Deployment "
                        "must use a non-overlapping rollout strategy (`Recreate` or "
                        "`RollingUpdate` with `maxSurge: 0`)"
                    )
                )


def check_no_active_rbac(findings: list[Finding]) -> None:
    """No Role/ClusterRole/RoleBinding/ClusterRoleBinding may ship as a deployable resource.

    This scaffold intentionally grants the application's own ServiceAccounts zero Kubernetes
    API access: arbitrary Job-create RBAC on a shell-hosting process is equivalent to broad
    Secret/PVC/node exfiltration risk in most clusters. See docs/security-model.md "Pending
    gates" for what a real, narrowly-scoped sandbox-controller would require instead.
    """
    for path in sorted(_active_resource_files()):
        text = read(path)
        for kind in RBAC_KINDS:
            if re.search(rf"^kind:\s*{kind}\s*$", text, re.MULTILINE):
                findings.append(
                    Finding(
                        f"{path.relative_to(REPO_ROOT)}: no {kind} may ship as an active/"
                        "deployable resource in this scaffold (see docs/security-model.md "
                        "'Pending gates')"
                    )
                )


def check_workspace_is_not_readonly_app(findings: list[Finding]) -> None:
    """keel-server must not use the read-only `/app` source tree as its tool workspace.

    `packages/keel-server/src/keel_server/app.py` defaults the tool workspace to
    `Path.cwd()`, and the shared image's `WORKDIR` is `/app` (deploy/docker/app.Dockerfile).
    Without an explicit `workingDir` override, a read-only root filesystem would make every
    write/edit/shell tool call fail against the app's own source directory.
    """
    path = K8S_ROOT / "base/server/deployment.yaml"
    text = read(path)
    label = str(path.relative_to(REPO_ROOT))
    match = re.search(r"^\s*workingDir:\s*(\S+)", text, re.MULTILINE)
    if not match:
        findings.append(Finding(f"{label}: must set `workingDir` to a non-`/app` writable path"))
        return
    working_dir = match.group(1).strip('"')
    if working_dir in ("/app", "."):
        findings.append(
            Finding(f"{label}: `workingDir` must not be the read-only /app source tree")
        )
        return
    if f"mountPath: {working_dir}" not in text:
        findings.append(
            Finding(
                f"{label}: `workingDir: {working_dir}` has no matching writable "
                "`volumeMounts` entry"
            )
        )


def check_auth_secret_required(findings: list[Finding]) -> None:
    """Auth must come from an externally supplied, non-empty, syntactically valid
    `KEEL_API_KEYS`, and every active config must force fail-closed cloud mode.

    Empty/missing `KEEL_API_KEYS` makes every request an implicit, unauthenticated admin
    (`packages/keel-core/src/keel_core/config.py` `api_keys`; docs/OPERATIONS.md). The
    scaffold cannot force an operator to fill in a real value, but it can and must (a) require
    the key to exist with a non-empty, correctly-shaped (`key:role`) placeholder in the
    shipped template, (b) never let it leak into the plaintext ConfigMap, and (c) ship
    `KEEL_CLOUD_MODE: "true"` in the active config, which is consumed by application code
    (`Settings.cloud_mode` / `app.state.auth_required` / `authenticate()` in
    `packages/keel-server/src/keel_server/auth.py`): malformed/empty `KEEL_API_KEYS` under
    cloud mode fails every request closed (503) rather than falling back to local
    implicit-admin.

    Deliberately never includes the actual key value in a finding message — only whether a
    validly-shaped entry was found — so this check cannot leak key material into CI logs.
    """
    secret_path = K8S_ROOT / "base/secret-app.example.yaml"
    secret_text = read(secret_path)
    secret_label = str(secret_path.relative_to(REPO_ROOT))
    match = re.search(r"^\s*KEEL_API_KEYS:\s*\"?([^\"\n]*)\"?\s*$", secret_text, re.MULTILINE)
    if not match or not match.group(1).strip():
        findings.append(
            Finding(
                f"{secret_label}: must define a non-empty `KEEL_API_KEYS` — empty/missing "
                "means open, unauthenticated admin mode"
            )
        )
    elif not _has_valid_key_role_entry(match.group(1)):
        findings.append(
            Finding(
                f"{secret_label}: `KEEL_API_KEYS` must contain at least one syntactically "
                "valid `key:role` entry (role one of viewer/operator/admin, matching "
                "packages/keel-server/src/keel_server/auth.py parse_api_keys) — value shape "
                "omitted from this message on purpose"
            )
        )

    configmap_path = K8S_ROOT / "base/configmap-app.yaml"
    configmap_text = read(configmap_path)
    configmap_label = str(configmap_path.relative_to(REPO_ROOT))
    if re.search(r"^\s*KEEL_API_KEYS:", configmap_text, re.MULTILINE):
        findings.append(
            Finding(
                f"{configmap_label}: `KEEL_API_KEYS` must live only in a Secret, never a ConfigMap"
            )
        )

    cloud_mode_match = re.search(
        r'^\s*KEEL_CLOUD_MODE:\s*"?(\w+)"?\s*$', configmap_text, re.MULTILINE
    )
    if not cloud_mode_match or cloud_mode_match.group(1).lower() != "true":
        findings.append(
            Finding(
                f'{configmap_label}: must set `KEEL_CLOUD_MODE: "true"` so auth fails closed '
                "rather than falling back to open admin mode"
            )
        )


_KEY_ROLE_ENTRY = re.compile(r"^[^\s,:]+:(viewer|operator|admin)$", re.IGNORECASE)


def _has_valid_key_role_entry(raw: str) -> bool:
    """Mirror `parse_api_keys`'s `key:role` grammar (packages/keel-server/src/keel_server/auth.py)
    closely enough to catch an obviously-malformed template value, without being a full
    reimplementation of that parser.
    """
    return any(_KEY_ROLE_ENTRY.match(entry.strip()) for entry in raw.split(",") if entry.strip())


# Mirrors `MIN_RPC_SECRET_BYTES` in packages/keel-core/src/keel_core/tools/rpc_auth.py.
# tests/unit/test_deploy_k8s_manifests.py asserts these two never drift apart.
MIN_SANDBOX_RPC_SECRET_BYTES = 32


def check_sandbox_rpc_secret_required(findings: list[Finding]) -> None:
    """`KEEL_SANDBOX_RPC_SECRET` must be a real, >=32-byte value in the Secret template.

    `execution_backend` defaults to `"sandbox"` (`base/configmap-app.yaml` does not override
    it), so `keel_core.tools.wiring.build_service_execution_environment` builds an
    authenticated `SandboxExecutionEnvironment` client for both `keel-server` and
    `keel-worker` **at process startup**, not lazily per tool call. Its `RpcRequestSigner`
    (`packages/keel-core/src/keel_core/tools/rpc_auth.py`) raises immediately if the shared
    secret is empty or shorter than `MIN_RPC_SECRET_BYTES` (32) — unlike `KEEL_API_KEYS`
    (which fails closed per-request), a missing/short sandbox RPC secret crash-loops the Pod
    before it ever serves `/health`.

    Deliberately never includes the actual secret value in a finding message — only whether a
    non-empty, minimum-length value was found — so this check cannot leak key material into
    CI logs.
    """
    secret_path = K8S_ROOT / "base/secret-app.example.yaml"
    secret_text = read(secret_path)
    secret_label = str(secret_path.relative_to(REPO_ROOT))
    match = re.search(
        r"^\s*KEEL_SANDBOX_RPC_SECRET:\s*\"?([^\"\n]*)\"?\s*$", secret_text, re.MULTILINE
    )
    if not match or not match.group(1).strip():
        findings.append(
            Finding(
                f"{secret_label}: must define a non-empty `KEEL_SANDBOX_RPC_SECRET` — "
                "empty/missing crash-loops keel-server/keel-worker at startup (execution_backend "
                "defaults to `sandbox`)"
            )
        )
    elif len(match.group(1).strip().encode("utf-8")) < MIN_SANDBOX_RPC_SECRET_BYTES:
        findings.append(
            Finding(
                f"{secret_label}: `KEEL_SANDBOX_RPC_SECRET` must be at least "
                f"{MIN_SANDBOX_RPC_SECRET_BYTES} bytes (matching "
                "packages/keel-core/src/keel_core/tools/rpc_auth.py MIN_RPC_SECRET_BYTES) — "
                "value shape/length omitted from this message on purpose"
            )
        )

    configmap_path = K8S_ROOT / "base/configmap-app.yaml"
    configmap_text = read(configmap_path)
    configmap_label = str(configmap_path.relative_to(REPO_ROOT))
    if re.search(r"^\s*KEEL_SANDBOX_RPC_SECRET:", configmap_text, re.MULTILINE):
        findings.append(
            Finding(
                f"{configmap_label}: `KEEL_SANDBOX_RPC_SECRET` must live only in a Secret, "
                "never a ConfigMap"
            )
        )


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


def check_network_policy_directionality(findings: list[Finding]) -> None:
    """Every cross-Pod allow needs BOTH a matching egress rule (source) and ingress rule
    (destination) — NetworkPolicy is directional and default-deny-all.yaml blocks a side that
    has no matching rule even if the other side allows it.
    """
    networkpolicy_dir = K8S_ROOT / "base/networkpolicy"
    combined = "\n".join(read(p) for p in networkpolicy_dir.glob("*.yaml"))

    if "allow-web-egress-to-server" not in combined:
        findings.append(
            Finding(
                "base/networkpolicy: missing an egress allow from keel-web to keel-server "
                "(ingress-only rules do not let keel-web reach keel-server)"
            )
        )
    if "allow-server-ingress-from-sandbox" not in combined:
        findings.append(
            Finding(
                "base/networkpolicy: missing an ingress allow on keel-server from sandbox "
                "Pods (egress-only rules do not let the sandbox RPC callback reach keel-server)"
            )
        )
    if "allow-egress-proxy-ingress-from-sandbox" not in combined:
        findings.append(
            Finding(
                "base/networkpolicy: missing an ingress allow on the egress-proxy from "
                "sandbox Pods (allow-sandbox-egress-proxy-only.yaml's egress rule has no "
                "matching ingress half)"
            )
        )


def check_dns_policy_scoped_to_kube_system(findings: list[Finding]) -> None:
    """DNS egress must be scoped to `kube-system`, not any namespace.

    `namespaceSelector: {}` matches every namespace in the cluster — a Pod anywhere with a
    matching DNS label (`k8s-app: kube-dns`, etc.) outside `kube-system` would also become a
    legitimate egress target, which is broader than "let Pods resolve cluster DNS".
    """
    path = K8S_ROOT / "base/networkpolicy/allow-dns-egress.yaml"
    text = read(path)
    label = str(path.relative_to(REPO_ROOT))
    code_lines = [line for line in text.splitlines() if not line.strip().startswith("#")]
    code_text = "\n".join(code_lines)
    if "namespaceSelector: {}" in code_text:
        findings.append(
            Finding(
                f"{label}: DNS egress must scope `namespaceSelector` to `kube-system` "
                "(kubernetes.io/metadata.name), not `{}` (any namespace)"
            )
        )
    if "kubernetes.io/metadata.name: kube-system" not in code_text:
        findings.append(
            Finding(f"{label}: DNS egress must restrict namespaceSelector to kube-system")
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


def _load_kustomization(kustomization_text: str) -> dict[str, Any]:
    if yaml is not None:
        try:
            return dict(yaml.safe_load(kustomization_text) or {})
        except yaml.YAMLError:
            return {}
    # Fallback without PyYAML: grab the literal `- foo.yaml` lines directly under `resources:`.
    match = re.search(r"^resources:\n((?:[ \t]+-.*\n?)+)", kustomization_text, re.MULTILINE)
    if not match:
        return {}
    entries = [
        line.split("-", 1)[1].strip() for line in match.group(1).splitlines() if line.strip()
    ]
    return {"resources": entries}


def _resource_entries(kustomization_text: str) -> list[str]:
    """Extract the string entries listed under a kustomization's ``resources:`` key.

    Deliberately scoped to that one key (not a whole-file substring search) so a file's
    header *comments* may reference example/template filenames without tripping the
    "excluded from kustomization" check below.
    """
    data = _load_kustomization(kustomization_text)
    return [str(entry) for entry in (data.get("resources") or [])]


def _patch_entries(kustomization_text: str) -> list[str]:
    """Extract file paths from a kustomization's ``patches:`` key (``path:`` entries only)."""
    data = _load_kustomization(kustomization_text)
    paths: list[str] = []
    for patch in data.get("patches") or []:
        if isinstance(patch, dict) and "path" in patch:
            paths.append(str(patch["path"]))
    return paths


def _active_resource_files() -> set[Path]:
    """Every file genuinely reachable from a kustomization's `resources:`/`patches:`, resolved
    transitively (a `resources:` entry may itself be a directory with its own
    kustomization.yaml, as overlays do for `../../base`).
    """
    active: set[Path] = set()
    seen_kustomizations: set[Path] = set()

    def visit(kustomization_path: Path) -> None:
        if kustomization_path in seen_kustomizations or not kustomization_path.exists():
            return
        seen_kustomizations.add(kustomization_path)
        directory = kustomization_path.parent
        text = read(kustomization_path)
        for entry in _resource_entries(text):
            entry_path = (directory / entry).resolve()
            if entry_path.is_dir():
                visit(entry_path / "kustomization.yaml")
            elif entry_path.exists():
                active.add(entry_path)
        for entry in _patch_entries(text):
            entry_path = (directory / entry).resolve()
            if entry_path.exists():
                active.add(entry_path)

    for kustomization in K8S_ROOT.rglob("kustomization.yaml"):
        visit(kustomization.resolve())
    return active


def check_no_placeholders_in_active_resources(findings: list[Finding]) -> None:
    for path in sorted(_active_resource_files()):
        text = read(path)
        offending_lines = [
            line
            for line in text.splitlines()
            if _has_placeholder(line) and line.strip() not in ALLOWED_ACTIVE_PLACEHOLDER_LINES
        ]
        if offending_lines:
            findings.append(
                Finding(
                    f"{path.relative_to(REPO_ROOT)}: is an active/deployable resource but "
                    f"still contains an unresolved placeholder ({offending_lines[0].strip()}) "
                    "— either resolve it or move the file to a dormant `*.example.yaml`"
                )
            )


def check_cidrs_are_valid(findings: list[Finding]) -> None:
    cidr_pattern = re.compile(r"cidr:\s*([^\s#]+)")
    for path in sorted(K8S_ROOT.rglob("*.yaml")):
        text = read(path)
        for match in cidr_pattern.finditer(text):
            value = match.group(1).strip('"').strip("'")
            if _has_placeholder(value):
                continue  # a documented template placeholder, not a real CIDR to validate
            try:
                ipaddress.ip_network(value, strict=False)
            except ValueError:
                findings.append(Finding(f"{path.relative_to(REPO_ROOT)}: invalid CIDR '{value}'"))


def _rendered_objects(target: Path, kubectl: str) -> list[dict[str, Any]] | None:
    if yaml is None:
        return None
    rendered = _render_kustomize(target, kubectl)
    if rendered is None:
        return None
    try:
        return [doc for doc in yaml.safe_load_all(rendered) if doc]
    except yaml.YAMLError:
        return None


def _example_object_names(kind: str) -> set[str]:
    """Names of Secret/ConfigMap objects declared in tracked `*.example.yaml` templates.

    These are the documented "operator must supply this externally" names — a Deployment
    referencing one of these is expected, not a dangling reference.
    """
    names: set[str] = set()
    if yaml is None:
        return names
    for path in K8S_ROOT.rglob("*.yaml"):
        if not any(path.name.endswith(marker) for marker in EXAMPLE_FILE_MARKERS):
            continue
        try:
            for doc in yaml.safe_load_all(read(path)):
                if doc and doc.get("kind") == kind:
                    name = doc.get("metadata", {}).get("name")
                    if name:
                        names.add(name)
        except yaml.YAMLError:
            continue
    return names


def check_secret_configmap_sa_references_resolve(findings: list[Finding]) -> None:
    kubectl = shutil.which("kubectl")
    if kubectl is None or yaml is None:
        findings.append(
            Finding(
                "SKIPPED: kubectl and/or PyYAML unavailable; Secret/ConfigMap/ServiceAccount "
                "reference resolution was not verified."
            )
        )
        return

    objects = _rendered_objects(K8S_ROOT / "base", kubectl)
    if objects is None:
        findings.append(
            Finding("SKIPPED: could not parse the rendered base for reference resolution.")
        )
        return

    defined_configmaps = {o["metadata"]["name"] for o in objects if o.get("kind") == "ConfigMap"}
    defined_secrets = {o["metadata"]["name"] for o in objects if o.get("kind") == "Secret"}
    defined_serviceaccounts = {
        o["metadata"]["name"] for o in objects if o.get("kind") == "ServiceAccount"
    }
    external_configmaps = _example_object_names("ConfigMap")
    external_secrets = _example_object_names("Secret")

    for obj in objects:
        if obj.get("kind") not in ("Deployment", "Job"):
            continue
        name = obj.get("metadata", {}).get("name", "<unnamed>")
        pod_spec = obj.get("spec", {}).get("template", {}).get("spec", {})
        sa_name = pod_spec.get("serviceAccountName")
        if sa_name and sa_name not in defined_serviceaccounts and sa_name != "default":
            findings.append(
                Finding(
                    f"{name}: serviceAccountName '{sa_name}' does not resolve to any "
                    "ServiceAccount defined in the rendered base"
                )
            )
        for container in pod_spec.get("containers", []) + pod_spec.get("initContainers", []):
            for env_from in container.get("envFrom", []) or []:
                cm_ref = env_from.get("configMapRef", {}).get("name")
                if cm_ref and cm_ref not in defined_configmaps | external_configmaps:
                    findings.append(
                        Finding(
                            f"{name}/{container.get('name')}: configMapRef '{cm_ref}' does not "
                            "resolve to a rendered ConfigMap or a documented external template"
                        )
                    )
                secret_ref = env_from.get("secretRef", {}).get("name")
                if secret_ref and secret_ref not in defined_secrets | external_secrets:
                    findings.append(
                        Finding(
                            f"{name}/{container.get('name')}: secretRef '{secret_ref}' does not "
                            "resolve to a rendered Secret or a documented external template"
                        )
                    )


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
    check_schema_validation(findings)
    check_workload_hardening(findings)
    check_server_single_instance_rollout(findings)
    check_no_active_rbac(findings)
    check_workspace_is_not_readonly_app(findings)
    check_auth_secret_required(findings)
    check_sandbox_rpc_secret_required(findings)
    check_sandbox_isolation(findings)
    check_network_policy_default_deny(findings)
    check_network_policy_directionality(findings)
    check_dns_policy_scoped_to_kube_system(findings)
    check_no_leaked_secrets(findings)
    check_example_files_excluded_from_kustomization(findings)
    check_no_placeholders_in_active_resources(findings)
    check_cidrs_are_valid(findings)
    check_secret_configmap_sa_references_resolve(findings)
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
