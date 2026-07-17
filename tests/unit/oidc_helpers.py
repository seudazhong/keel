"""Local RSA/EC keypair + JWK + token helpers for OIDC verifier tests (no network)."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa


@dataclass
class SigningKey:
    kid: str
    alg: str
    private_pem: bytes
    jwk: dict[str, Any]


def make_rsa_key(kid: str = "rsa-1", alg: str = "RS256") -> SigningKey:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    jwk.update({"kid": kid, "alg": alg, "use": "sig"})
    return SigningKey(kid=kid, alg=alg, private_pem=pem, jwk=jwk)


def make_ec_key(kid: str = "ec-1", alg: str = "ES256") -> SigningKey:
    key = ec.generate_private_key(ec.SECP256R1())
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    jwk = json.loads(jwt.algorithms.ECAlgorithm.to_jwk(key.public_key()))
    jwk.update({"kid": kid, "alg": alg, "use": "sig"})
    return SigningKey(kid=kid, alg=alg, private_pem=pem, jwk=jwk)


def sign_token(
    key: SigningKey,
    *,
    issuer: str = "https://issuer.example",
    audience: str = "keel",
    subject: str = "subject-123",
    email: str | None = "user@example.com",
    email_verified: bool = True,
    lifetime: int = 300,
    now: int | None = None,
    **overrides: Any,
) -> str:
    issued = now if now is not None else int(time.time())
    claims: dict[str, Any] = {
        "iss": issuer,
        "sub": subject,
        "aud": audience,
        "iat": issued,
        "exp": issued + lifetime,
    }
    if email is not None:
        claims["email"] = email
        claims["email_verified"] = email_verified
    claims.update(overrides)
    claims = {k: v for k, v in claims.items() if v is not _OMIT}
    return jwt.encode(claims, key.private_pem, algorithm=key.alg, headers={"kid": key.kid})


_OMIT = object()
OMIT = _OMIT
