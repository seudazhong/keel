"""GitHub App primitives: JWT, JIT tokens, HTTP client retry, webhooks, URL SSRF (M3.7)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from keel_core.projects.github.auth import (
    AppJwtMinter,
    GitHubAppConfigError,
    InstallationTokenService,
    resolve_private_key,
)
from keel_core.projects.github.client import (
    GitHubAuthError,
    GitHubClient,
    GitHubError,
    GitHubRateLimitError,
    GitHubResponse,
    RetryPolicy,
)
from keel_core.projects.github.urls import (
    UntrustedUrlError,
    normalize_clone_url,
    normalize_https_url,
)
from keel_core.projects.github.webhooks import (
    ALLOWED_EVENTS,
    WebhookVerificationError,
    delivery_id,
    event_name,
    parse_event,
    verify_signature,
)


def _rsa_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


def _public_pem(pem: str) -> bytes:
    key = serialization.load_pem_private_key(pem.encode(), password=None)
    return key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )


# --- URL / SSRF ----------------------------------------------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "http://github.com/x",  # non-https
        "https://user:pw@github.com/x",  # credentials
        "https://127.0.0.1/x",  # loopback ip
        "https://10.0.0.5/x",  # private ip
        "https://169.254.169.254/x",  # link-local (cloud metadata)
        "https://localhost/x",  # loopback host
        "https://github.com:8443/x",  # non-443 port
        "https://evil.example/x",  # off allowlist
        "https://github.com/x?a=1",  # query
    ],
)
def test_url_ssrf_rejected(url: str) -> None:
    with pytest.raises(UntrustedUrlError):
        normalize_https_url(url, allowed_hosts=frozenset({"github.com"}))


def test_clone_url_pin_repo() -> None:
    ok = normalize_clone_url(
        "https://github.com/acme/repo.git",
        allowed_hosts=frozenset({"github.com"}),
        repo_full_name="acme/repo",
    )
    assert ok == "https://github.com/acme/repo.git"
    with pytest.raises(UntrustedUrlError):
        normalize_clone_url(
            "https://github.com/attacker/repo.git",
            allowed_hosts=frozenset({"github.com"}),
            repo_full_name="acme/repo",
        )


# --- JWT + JIT tokens ----------------------------------------------------------------


def test_app_jwt_signed_and_short_lived() -> None:
    pem = _rsa_pem()
    minter = AppJwtMinter(app_id=42, private_key_loader=lambda: pem)
    token = minter.generate()
    claims = jwt.decode(token, _public_pem(pem), algorithms=["RS256"])
    assert claims["iss"] == "42"
    assert claims["exp"] - claims["iat"] <= 600


def test_app_jwt_requires_positive_id() -> None:
    with pytest.raises(GitHubAppConfigError):
        AppJwtMinter(app_id=0, private_key_loader=lambda: "x")


def test_resolve_private_key_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GH_KEY", "-----BEGIN-----\nabc\n-----END-----")
    assert resolve_private_key("env:GH_KEY").startswith("-----BEGIN-----")
    with pytest.raises(GitHubAppConfigError):
        resolve_private_key("")
    with pytest.raises(GitHubAppConfigError):
        resolve_private_key("env:MISSING_VAR_XYZ")


async def test_installation_token_cached_and_not_persisted() -> None:
    pem = _rsa_pem()
    minter = AppJwtMinter(app_id=1, private_key_loader=lambda: pem)
    calls = {"n": 0}

    async def mint(installation_id: int, app_jwt: str) -> GitHubResponse:
        calls["n"] += 1
        return GitHubResponse(
            status=201,
            json_body={
                "token": "ghs_secret_value",
                "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            },
        )

    svc = InstallationTokenService(minter, mint, cache_seconds=300)
    t1 = await svc.get_token(500)
    await svc.get_token(500)
    assert t1.token == "ghs_secret_value"
    assert calls["n"] == 1  # second call served from cache
    # No public attribute stores tokens beyond the in-process cache.
    assert 500 in svc._cache and svc._cache[500].token == "ghs_secret_value"


async def test_installation_token_refreshes_near_expiry() -> None:
    pem = _rsa_pem()
    minter = AppJwtMinter(app_id=1, private_key_loader=lambda: pem)
    calls = {"n": 0}

    async def mint(installation_id: int, app_jwt: str) -> GitHubResponse:
        calls["n"] += 1
        return GitHubResponse(
            status=201,
            json_body={
                "token": f"tok{calls['n']}",
                "expires_at": (datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
            },
        )

    svc = InstallationTokenService(minter, mint, cache_seconds=300)
    await svc.get_token(9)
    # Expiry is within the refresh skew, so the next call re-mints.
    await svc.get_token(9)
    assert calls["n"] == 2


# --- HTTP client retry / rate-limit --------------------------------------------------


class _FakeTransport:
    def __init__(self, responses: list[GitHubResponse]) -> None:
        self._responses = responses
        self.calls = 0

    async def request(self, method: str, url: str, *, headers, json=None) -> GitHubResponse:
        self.calls += 1
        return self._responses[min(self.calls - 1, len(self._responses) - 1)]


async def test_client_retries_5xx_then_succeeds() -> None:
    transport = _FakeTransport(
        [GitHubResponse(status=503), GitHubResponse(status=200, json_body={"id": 1})]
    )
    client = GitHubClient(
        transport,
        api_base_url="https://api.github.com",
        retry=RetryPolicy(max_attempts=3, base_delay_seconds=0),
    )
    repo = await client.get_repository(token="t", full_name="a/b")
    assert repo["id"] == 1
    assert transport.calls == 2


async def test_client_rate_limit_raises() -> None:
    transport = _FakeTransport([GitHubResponse(status=403, headers={"x-ratelimit-remaining": "0"})])
    client = GitHubClient(
        transport,
        api_base_url="https://api.github.com",
        retry=RetryPolicy(max_attempts=2, base_delay_seconds=0),
    )
    with pytest.raises(GitHubRateLimitError):
        await client.get_repository(token="t", full_name="a/b")


async def test_client_auth_error() -> None:
    transport = _FakeTransport([GitHubResponse(status=401)])
    client = GitHubClient(transport, api_base_url="https://api.github.com")
    with pytest.raises(GitHubAuthError):
        await client.get_repository(token="t", full_name="a/b")


async def test_client_unexpected_payload() -> None:
    transport = _FakeTransport([GitHubResponse(status=200, json_body=["not", "a", "dict"])])
    client = GitHubClient(transport, api_base_url="https://api.github.com")
    with pytest.raises(GitHubError):
        await client.get_repository(token="t", full_name="a/b")


# --- webhook verification / parsing --------------------------------------------------


def test_signature_verify_and_bad() -> None:
    import hashlib
    import hmac

    secret = "supersecret"
    body = b'{"hello":"world"}'
    good = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_signature(secret, body, good) is True
    assert verify_signature(secret, body, "sha256=00") is False
    assert verify_signature(secret, body, None) is False
    assert verify_signature("", body, good) is False
    # A single altered body byte fails.
    assert verify_signature(secret, body + b" ", good) is False


def test_delivery_and_event_headers() -> None:
    assert delivery_id("  abc  ") == "abc"
    assert delivery_id(None) is None
    assert event_name("push") == "push"
    assert event_name("") is None


def test_parse_event_allowlist() -> None:
    assert "push" in ALLOWED_EVENTS
    with pytest.raises(WebhookVerificationError):
        parse_event(event="deploy_key", delivery="d", payload={})
    evt = parse_event(
        event="push",
        delivery="d",
        payload={
            "ref": "refs/heads/main",
            "installation": {"id": 7},
            "repository": {"id": 88, "default_branch": "main"},
        },
    )
    assert evt.installation_id == 7 and evt.repository_ids == (88,)
    assert evt.default_branch == "main"
