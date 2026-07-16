"""Envelope encryption for secrets at rest (DESIGN-REVIEW G9/G18, M3.3 rotation).

Connector OAuth tokens are stored encrypted, keyed by ``(scope_id, connector_id)``.
:class:`EnvelopeCipher` wraps Fernet (AES-128-CBC + HMAC); the Fernet key is derived
from the process ``secret_key`` so operators supply an ordinary passphrase. With no
key the cipher **fails closed** — the token store refuses to read or write.

:class:`KeyRing` layers **key versioning** on top: every stored ciphertext records the
``key_id`` that encrypted it, new writes use the *active* key, and decrypt selects the
matching key by id. This makes key rotation safe — add a new active key, keep the old
one listed until every row is re-encrypted (see ``PostgresTokenStore.reencrypt_stale``).
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass

from cryptography.fernet import Fernet, InvalidToken

from keel_core.config import Settings, get_settings
from keel_core.errors import KeelError

# The key id the legacy single ``secret_key`` (and pre-M3.3 rows) map to.
LEGACY_KEY_ID = "v1"


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
    """Build an :class:`EnvelopeCipher` from settings; fail closed if no key.

    Retained for callers that only need the active key. Prefer :func:`keyring_from_settings`
    for stores that must decrypt historical (rotated) ciphertext.
    """
    ring = keyring_from_settings(settings)
    return ring.active_cipher()


@dataclass(frozen=True)
class EncryptedSecret:
    """A ciphertext plus the id of the key that encrypted it (for versioned decrypt)."""

    key_id: str
    ciphertext: str


class KeyRing:
    """A versioned set of envelope keys: one *active* key for writes, many for reads.

    Each key is registered under a stable ``key_id``. :meth:`encrypt` always uses the
    active key and returns the id alongside the ciphertext so it can be persisted;
    :meth:`decrypt` looks the key up by id. A ring with an unknown key id fails closed
    rather than guessing — the operator must keep every historical key listed.
    """

    def __init__(self, keys: dict[str, str], active_id: str) -> None:
        if not keys:
            raise SecretsError("no secret keys configured — token encryption is unavailable")
        if active_id not in keys:
            raise SecretsError(f"active key id {active_id!r} is not among configured secret keys")
        self._ciphers = {kid: EnvelopeCipher(secret) for kid, secret in keys.items()}
        self._active_id = active_id

    @property
    def active_id(self) -> str:
        return self._active_id

    @property
    def key_ids(self) -> frozenset[str]:
        return frozenset(self._ciphers)

    @classmethod
    def from_cipher(cls, cipher: EnvelopeCipher, key_id: str = LEGACY_KEY_ID) -> KeyRing:
        """Wrap a single :class:`EnvelopeCipher` as a one-key ring (backwards compat)."""
        ring = cls.__new__(cls)
        ring._ciphers = {key_id: cipher}
        ring._active_id = key_id
        return ring

    def _cipher(self, key_id: str) -> EnvelopeCipher:
        cipher = self._ciphers.get(key_id)
        if cipher is None:
            raise SecretsError(f"no secret key registered for key id {key_id!r}")
        return cipher

    def active_cipher(self) -> EnvelopeCipher:
        return self._cipher(self._active_id)

    def encrypt(self, plaintext: str) -> EncryptedSecret:
        """Encrypt with the active key; return ``(active_id, ciphertext)``."""
        return EncryptedSecret(self._active_id, self.active_cipher().encrypt(plaintext))

    def decrypt(self, key_id: str, ciphertext: str) -> str:
        """Decrypt ciphertext that was written under ``key_id``."""
        return self._cipher(key_id).decrypt(ciphertext)

    def needs_rotation(self, key_id: str) -> bool:
        """True if a row encrypted under ``key_id`` is not on the active key."""
        return key_id != self._active_id


def _parse_secret_keys(raw: str) -> dict[str, str]:
    """Parse ``key_id:secret[,key_id:secret...]`` into ``{key_id: secret}``.

    Blank entries and entries missing an id or secret are skipped so a typo can't
    silently drop a key or grant an empty one.
    """
    keys: dict[str, str] = {}
    for entry in raw.split(","):
        key_id, sep, secret = entry.strip().partition(":")
        if not sep:
            continue
        key_id = key_id.strip()
        secret = secret.strip()
        if key_id and secret:
            keys[key_id] = secret
    return keys


def keyring_from_settings(settings: Settings | None = None) -> KeyRing:
    """Build a :class:`KeyRing` from settings; fail closed when no key is configured.

    Precedence: the versioned ``secret_keys`` map (with ``secret_key_active_id``) wins;
    otherwise the legacy single ``secret_key`` is registered under :data:`LEGACY_KEY_ID`.
    A legacy ``secret_key``, when present, is always registered too so rows written by
    the pre-rotation code path still decrypt after an operator adds ``secret_keys``.
    """
    settings = settings or get_settings()
    keys = _parse_secret_keys(settings.secret_keys)
    if settings.secret_key and LEGACY_KEY_ID not in keys:
        keys[LEGACY_KEY_ID] = settings.secret_key
    if not keys:
        raise SecretsError("no secret_key configured — token encryption is unavailable")
    active_id = settings.secret_key_active_id.strip()
    if not active_id:
        # Default to the legacy id when it exists, else the sole configured key.
        active_id = LEGACY_KEY_ID if LEGACY_KEY_ID in keys else next(iter(keys))
    return KeyRing(keys, active_id)
