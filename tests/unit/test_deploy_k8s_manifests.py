"""Validation tests for the deploy/k8s manifest scaffold.

These tests exercise the checks in ``deploy/k8s/scripts/validate_manifests.py`` — a
deterministic, cluster-free validator for the Kubernetes manifests under ``deploy/k8s``. No
live Kubernetes cluster, kind/minikube, or credentials are required or contacted; the only
external tool involved is ``kubectl kustomize`` (pure client-side templating), and that check
is skipped gracefully when ``kubectl`` is not on PATH.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

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
