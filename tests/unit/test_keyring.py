"""KeyRing (versioned envelope) tests: encrypt/decrypt, rotation, fail-closed."""

from __future__ import annotations

import pytest

from keel_core.config import Settings
from keel_core.secrets import (
    LEGACY_KEY_ID,
    EnvelopeCipher,
    KeyRing,
    SecretsError,
    keyring_from_settings,
)


def test_keyring_encrypt_tags_active_key_and_round_trips() -> None:
    ring = KeyRing({"v1": "old", "v2": "new"}, active_id="v2")
    enc = ring.encrypt("secret")
    assert enc.key_id == "v2"
    assert ring.decrypt(enc.key_id, enc.ciphertext) == "secret"


def test_keyring_decrypts_older_key_after_rotation() -> None:
    # A ciphertext written under v1 still decrypts once v2 becomes active.
    old = KeyRing({"v1": "old"}, active_id="v1")
    written = old.encrypt("token")
    assert written.key_id == "v1"

    rotated = KeyRing({"v1": "old", "v2": "new"}, active_id="v2")
    assert rotated.decrypt("v1", written.ciphertext) == "token"
    assert rotated.needs_rotation("v1") is True
    assert rotated.needs_rotation("v2") is False


def test_keyring_unknown_key_fails_closed() -> None:
    ring = KeyRing({"v2": "new"}, active_id="v2")
    with pytest.raises(SecretsError):
        ring.decrypt("v1", "anything")


def test_keyring_active_must_be_configured() -> None:
    with pytest.raises(SecretsError):
        KeyRing({"v1": "x"}, active_id="v2")


def test_keyring_from_cipher_uses_legacy_id() -> None:
    ring = KeyRing.from_cipher(EnvelopeCipher("k"))
    enc = ring.encrypt("s")
    assert enc.key_id == LEGACY_KEY_ID
    assert ring.decrypt(LEGACY_KEY_ID, enc.ciphertext) == "s"


def test_keyring_from_settings_prefers_versioned_and_keeps_legacy() -> None:
    settings = Settings(secret_key="legacy", secret_keys="v2:new", secret_key_active_id="v2")
    ring = keyring_from_settings(settings)
    assert ring.active_id == "v2"
    # Legacy secret_key stays registered under v1 so old rows still decrypt.
    assert ring.key_ids == frozenset({"v1", "v2"})


def test_keyring_from_settings_defaults_active_to_legacy() -> None:
    ring = keyring_from_settings(Settings(secret_key="only"))
    assert ring.active_id == LEGACY_KEY_ID


def test_keyring_from_settings_fails_closed_without_keys() -> None:
    with pytest.raises(SecretsError):
        keyring_from_settings(Settings(secret_key="", secret_keys=""))
