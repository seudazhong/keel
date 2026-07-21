"""Validation tests for the deploy/k8s manifest scaffold.

These tests exercise the checks in ``deploy/k8s/scripts/validate_manifests.py`` — a
deterministic, cluster-free validator for the Kubernetes manifests under ``deploy/k8s``. No
live Kubernetes cluster, kind/minikube, or credentials are required or contacted; the only
external tool involved is ``kubectl kustomize`` (pure client-side templating), and that check
is skipped gracefully when ``kubectl`` is not on PATH.
"""

from __future__ import annotations

import importlib.util
import re
import sys
import types
from pathlib import Path

import pytest

from keel_core.tools.rpc_auth import MIN_RPC_SECRET_BYTES
from keel_server.auth import parse_api_keys

REPO_ROOT = Path(__file__).resolve().parents[2]
VALIDATOR_PATH = REPO_ROOT / "deploy" / "k8s" / "scripts" / "validate_manifests.py"


def _load_validator() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("validate_manifests", VALIDATOR_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def validator() -> types.ModuleType:
    return _load_validator()


def test_manifest_scaffold_exists() -> None:
    assert VALIDATOR_PATH.exists(), "deploy/k8s/scripts/validate_manifests.py is missing"
    assert (REPO_ROOT / "deploy" / "k8s" / "base" / "kustomization.yaml").exists()


def test_deploy_k8s_manifests_pass_all_checks(validator: types.ModuleType) -> None:
    findings = validator.run_all_checks()
    hard_failures = [f for f in findings if not f.startswith("SKIPPED")]
    assert not hard_failures, "\n".join(hard_failures)


def test_default_deny_network_policy_present() -> None:
    deny = (
        REPO_ROOT / "deploy" / "k8s" / "base" / "networkpolicy" / "default-deny-all.yaml"
    ).read_text(encoding="utf-8")
    assert "podSelector: {}" in deny
    assert "Ingress" in deny and "Egress" in deny


def test_sandbox_job_template_has_no_service_account_token(
    validator: types.ModuleType,
) -> None:
    job_template = (
        REPO_ROOT / "deploy" / "k8s" / "base" / "sandbox" / "job-template.yaml"
    ).read_text(encoding="utf-8")
    assert "automountServiceAccountToken: false" in job_template
    assert "keel-git-storage" not in job_template, (
        "the sandbox Job template must never mount the durable Git PVC"
    )


def test_example_and_rendered_templates_excluded_from_kustomize_build(
    validator: types.ModuleType,
) -> None:
    kustomization_text = (REPO_ROOT / "deploy" / "k8s" / "base" / "kustomization.yaml").read_text(
        encoding="utf-8"
    )
    entries = validator._resource_entries(kustomization_text)
    assert not any(entry.endswith("example.yaml") for entry in entries)
    assert not any(entry.endswith("job-template.yaml") for entry in entries)


def test_no_rbac_ships_as_an_active_resource(validator: types.ModuleType) -> None:
    """Arbitrary Job-create RBAC on the app's own ServiceAccount was a review finding: it is
    equivalent to broad Secret/PVC/node exfiltration risk. This scaffold must ship none.
    """
    for path in validator._active_resource_files():
        text = path.read_text(encoding="utf-8")
        for kind in ("Role", "ClusterRole", "RoleBinding", "ClusterRoleBinding"):
            assert f"kind: {kind}" not in text, f"{path} must not define a {kind}"
    assert not (
        REPO_ROOT / "deploy" / "k8s" / "base" / "serviceaccount-control-plane.yaml"
    ).exists()


def test_server_never_mounts_a_service_account_token(validator: types.ModuleType) -> None:
    server = (REPO_ROOT / "deploy" / "k8s" / "base" / "server" / "deployment.yaml").read_text(
        encoding="utf-8"
    )
    assert "automountServiceAccountToken: false" in server
    assert "serviceAccountName:" not in server


def test_server_workspace_is_writable_and_not_app_source(validator: types.ModuleType) -> None:
    server = (REPO_ROOT / "deploy" / "k8s" / "base" / "server" / "deployment.yaml").read_text(
        encoding="utf-8"
    )
    assert "workingDir: /workspace" in server
    assert "mountPath: /workspace" in server


def test_api_keys_required_in_secret_and_absent_from_configmap() -> None:
    secret = (REPO_ROOT / "deploy" / "k8s" / "base" / "secret-app.example.yaml").read_text(
        encoding="utf-8"
    )
    configmap = (REPO_ROOT / "deploy" / "k8s" / "base" / "configmap-app.yaml").read_text(
        encoding="utf-8"
    )
    assert "KEEL_API_KEYS:" in secret
    assert 'KEEL_API_KEYS: ""' not in secret
    assert not any(line.strip().startswith("KEEL_API_KEYS:") for line in configmap.splitlines()), (
        "KEEL_API_KEYS must live only in the Secret, never the ConfigMap's `data:`"
    )


def test_sandbox_rpc_secret_required_in_secret_and_absent_from_configmap(
    validator: types.ModuleType,
) -> None:
    """`execution_backend` defaults to `"sandbox"`
    (`packages/keel-core/src/keel_core/config.py`), so `keel-server`/`keel-worker` build an
    authenticated `SandboxExecutionEnvironment` client at startup — a missing or too-short
    `KEEL_SANDBOX_RPC_SECRET` crash-loops the Pod before it serves `/health`, unlike
    `KEEL_API_KEYS`/`KEEL_CLOUD_MODE`, which fail closed per-request.
    """
    secret = (REPO_ROOT / "deploy" / "k8s" / "base" / "secret-app.example.yaml").read_text(
        encoding="utf-8"
    )
    configmap = (REPO_ROOT / "deploy" / "k8s" / "base" / "configmap-app.yaml").read_text(
        encoding="utf-8"
    )
    assert "KEEL_SANDBOX_RPC_SECRET:" in secret
    assert 'KEEL_SANDBOX_RPC_SECRET: ""' not in secret
    assert not any(
        line.strip().startswith("KEEL_SANDBOX_RPC_SECRET:") for line in configmap.splitlines()
    ), "KEEL_SANDBOX_RPC_SECRET must live only in the Secret, never the ConfigMap's `data:`"

    match = re.search(r'^\s*KEEL_SANDBOX_RPC_SECRET:\s*"([^"\n]*)"\s*$', secret, re.MULTILINE)
    assert match is not None
    assert len(match.group(1).encode("utf-8")) >= validator.MIN_SANDBOX_RPC_SECRET_BYTES

    findings: list[str] = []
    validator.check_sandbox_rpc_secret_required(findings)
    hard_failures = [f for f in findings if not f.startswith("SKIPPED")]
    assert not hard_failures, "\n".join(hard_failures)


def test_sandbox_rpc_secret_minimum_length_matches_rpc_auth(
    validator: types.ModuleType,
) -> None:
    """The validator's `MIN_SANDBOX_RPC_SECRET_BYTES` must match the real
    `MIN_RPC_SECRET_BYTES` enforced by `keel_core.tools.rpc_auth.RpcRequestSigner` — otherwise
    the validator could pass a placeholder shape the real client would still reject (or vice
    versa) at server/worker startup.
    """
    assert validator.MIN_SANDBOX_RPC_SECRET_BYTES == MIN_RPC_SECRET_BYTES


def test_sandbox_rpc_secret_check_never_leaks_secret_material(
    validator: types.ModuleType,
) -> None:
    findings: list[str] = []
    validator.check_sandbox_rpc_secret_required(findings)
    secret = (REPO_ROOT / "deploy" / "k8s" / "base" / "secret-app.example.yaml").read_text(
        encoding="utf-8"
    )
    match = re.search(r'^\s*KEEL_SANDBOX_RPC_SECRET:\s*"([^"\n]*)"\s*$', secret, re.MULTILINE)
    assert match is not None
    for finding in findings:
        assert match.group(1) not in finding


def test_scheduler_and_git_pvc_are_dormant_not_active() -> None:
    k8s_base = REPO_ROOT / "deploy" / "k8s" / "base"
    assert not (k8s_base / "scheduler" / "deployment.yaml").exists()
    assert (k8s_base / "scheduler" / "deployment.example.yaml").exists()
    assert not (k8s_base / "datastores" / "git-pvc.yaml").exists()
    assert (k8s_base / "datastores" / "git-pvc.example.yaml").exists()


def test_networkpolicy_directional_rules_present() -> None:
    networkpolicy_dir = REPO_ROOT / "deploy" / "k8s" / "base" / "networkpolicy"
    combined = "\n".join(p.read_text(encoding="utf-8") for p in networkpolicy_dir.glob("*.yaml"))
    assert "allow-web-egress-to-server" in combined
    assert "allow-server-ingress-from-sandbox" in combined
    assert "allow-egress-proxy-ingress-from-sandbox" in combined


def test_cidr_and_reference_checks_are_clean(validator: types.ModuleType) -> None:
    findings: list[str] = []
    validator.check_cidrs_are_valid(findings)
    validator.check_no_placeholders_in_active_resources(findings)
    hard_failures = [f for f in findings if not f.startswith("SKIPPED")]
    assert not hard_failures, "\n".join(hard_failures)


def test_server_pinned_to_single_replica_in_base_and_production() -> None:
    server = (REPO_ROOT / "deploy" / "k8s" / "base" / "server" / "deployment.yaml").read_text(
        encoding="utf-8"
    )
    assert "replicas: 1" in server
    assert "strategy:" in server
    assert "type: Recreate" in server

    production_patch = (
        REPO_ROOT
        / "deploy"
        / "k8s"
        / "overlays"
        / "production"
        / "patch-server-single-replica.yaml"
    ).read_text(encoding="utf-8")
    assert "value: 1" in production_patch

    production_kustomization = (
        REPO_ROOT / "deploy" / "k8s" / "overlays" / "production" / "kustomization.yaml"
    ).read_text(encoding="utf-8")
    assert "patch-server-single-replica.yaml" in production_kustomization


def test_server_rollout_strategy_never_overlaps_old_and_new_pods(
    validator: types.ModuleType,
) -> None:
    """`replicas: 1` alone does not prevent overlap: Kubernetes' default `RollingUpdate`
    strategy still surges a new Pod up before removing the old one even at a single replica.
    This must be caught in the base manifest text and in the rendered base/production
    Kustomize output alike.
    """
    findings: list[str] = []
    validator.check_server_single_instance_rollout(findings)
    hard_failures = [f for f in findings if not f.startswith("SKIPPED")]
    assert not hard_failures, "\n".join(hard_failures)

    assert validator._non_overlapping_strategy({"type": "Recreate"})
    assert validator._non_overlapping_strategy(
        {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 0, "maxUnavailable": 1}}
    )
    assert not validator._non_overlapping_strategy(None)
    assert not validator._non_overlapping_strategy({"type": "RollingUpdate"})
    assert not validator._non_overlapping_strategy(
        {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": "25%"}}
    )


def test_cloud_mode_forced_true_and_api_keys_have_valid_role(
    validator: types.ModuleType,
) -> None:
    findings: list[str] = []
    validator.check_auth_secret_required(findings)
    hard_failures = [f for f in findings if not f.startswith("SKIPPED")]
    assert not hard_failures, "\n".join(hard_failures)

    configmap = (REPO_ROOT / "deploy" / "k8s" / "base" / "configmap-app.yaml").read_text(
        encoding="utf-8"
    )
    assert 'KEEL_CLOUD_MODE: "true"' in configmap


def test_dns_egress_scoped_to_kube_system() -> None:
    dns_policy = (
        REPO_ROOT / "deploy" / "k8s" / "base" / "networkpolicy" / "allow-dns-egress.yaml"
    ).read_text(encoding="utf-8")
    code_lines = [line for line in dns_policy.splitlines() if not line.strip().startswith("#")]
    code_text = "\n".join(code_lines)
    assert "namespaceSelector: {}" not in code_text
    assert "kubernetes.io/metadata.name: kube-system" in code_text


def test_api_key_format_check_never_leaks_key_material(validator: types.ModuleType) -> None:
    findings: list[str] = []
    validator._has_valid_key_role_entry("supersecretvalue123:admin")
    validator.check_auth_secret_required(findings)
    for finding in findings:
        assert "supersecretvalue123" not in finding


def test_validator_key_shape_check_matches_real_parse_api_keys(
    validator: types.ModuleType,
) -> None:
    """The validator's syntactic `key:role` shape check
    (``validate_manifests._has_valid_key_role_entry``) is a lightweight mirror of the real
    parser, ``keel_server.auth.parse_api_keys`` — it must accept/reject the same raw
    `KEEL_API_KEYS` shapes so the two cannot silently drift apart (the validator passing a
    template shape the real server would treat as entirely empty, or vice versa). This uses
    only synthetic placeholder values and asserts solely on the boolean outcome, so no key
    material is ever included in a test failure message.
    """
    accepted_and_rejected_shapes = [
        "adm:admin",
        "adm:admin:global",
        "svc:operator:org=org-1:agent=agt-1",
        "op:operator,vw:viewer",
        "adm:admin, op:operator ,vw:VIEWER,bad:notarole,, norole",
        "",
        "   ",
        "noColonAtAll",
        "onlyrole:",
        ":admin",
        "key:notarole",
        ",,,",
        "a:b:role",
        "key1:admin,key2:bogus,key3:operator",
        " KEY:ADMIN ",
    ]
    for raw in accepted_and_rejected_shapes:
        validator_accepts = validator._has_valid_key_role_entry(raw)
        parser_accepts = bool(parse_api_keys(raw))
        assert validator_accepts == parser_accepts, (
            "validator._has_valid_key_role_entry and keel_server.auth.parse_api_keys "
            "disagree on whether a KEEL_API_KEYS shape is accepted "
            f"(validator={validator_accepts}, parser={parser_accepts})"
        )


def test_cloud_capable_api_key_shape_requires_global_or_org_agent_binding(
    validator: types.ModuleType,
) -> None:
    assert validator._has_cloud_capable_key_entry("adm:admin:global")
    assert validator._has_cloud_capable_key_entry("svc:operator:org=org-1:agent=agt-1")
    assert validator._has_cloud_capable_key_entry("svc:operator:ORG=org-1:AGENT=agt-1")
    assert not validator._has_cloud_capable_key_entry("adm:admin")
    assert not validator._has_cloud_capable_key_entry("svc:operator:org=org-1")
    assert not validator._has_cloud_capable_key_entry("svc:operator:agent=agt-1")
    assert not validator._has_cloud_capable_key_entry("svc:viewer:global")
