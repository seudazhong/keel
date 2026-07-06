"""Envelope encryption for secrets at rest (DESIGN-REVIEW G9/G18).

Connector OAuth tokens are stored encrypted, keyed by ``(scope_id, connector_id)``.
:class:`EnvelopeCipher` wraps Fernet (AES-128-CBC + HMAC); the Fernet key is derived
from the process ``secret_key`` so operators supply an ordinary passphrase. With no
key the cipher **fails closed** — the token store refuses to read or write.
"""

from __future__ import annotations

import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken

from keel_core.config import Settings, get_settings
from keel_core.errors import KeelError


class SecretsError(KeelError):
    """Raised when encryption/decryption is unavailable or fails."""


def _derive_fernet_key(secret: str) -> bytes:
    """Derive a urlsafe-base64 32-byte Fernet key from an arbitrary secret."""
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


class EnvelopeCipher:
    """Symmetric authenticated encryption for small secrets (tokens)."""

    def __init__(self, secret: str) -> None:
        if not secret:
            raise SecretsError("no secret_key configured — token encryption is unavailable")
        self._fernet = Fernet(_derive_fernet_key(secret))

    def encrypt(self, plaintext: str) -> str:
        """Return a ciphertext token (urlsafe text) for ``plaintext``."""
        return self._fernet.encrypt(plaintext.encode("utf-8")).decode("ascii")

    def decrypt(self, ciphertext: str) -> str:
        """Return the plaintext for a ciphertext token; raise on tamper/wrong key."""
        try:
            return self._fernet.decrypt(ciphertext.encode("ascii")).decode("utf-8")
        except InvalidToken as exc:
            raise SecretsError("token decryption failed (wrong key or tampered)") from exc


def cipher_from_settings(settings: Settings | None = None) -> EnvelopeCipher:
    """Build an :class:`EnvelopeCipher` from settings; fail closed if no key."""
    settings = settings or get_settings()
    return EnvelopeCipher(settings.secret_key)
