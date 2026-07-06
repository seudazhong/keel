"""Envelope cipher tests: round-trip, fail-closed, tamper detection."""

from __future__ import annotations

import pytest

from keel_core.secrets import EnvelopeCipher, SecretsError


def test_encrypt_decrypt_round_trip() -> None:
    cipher = EnvelopeCipher("a-passphrase")
    token = cipher.encrypt("ya29.secret-oauth-token")
    assert token != "ya29.secret-oauth-token"  # ciphertext, not plaintext
    assert cipher.decrypt(token) == "ya29.secret-oauth-token"


def test_empty_key_fails_closed() -> None:
    with pytest.raises(SecretsError):
        EnvelopeCipher("")


def test_wrong_key_cannot_decrypt() -> None:
    token = EnvelopeCipher("key-one").encrypt("secret")
    with pytest.raises(SecretsError):
        EnvelopeCipher("key-two").decrypt(token)


def test_tampered_ciphertext_is_rejected() -> None:
    cipher = EnvelopeCipher("k")
    token = cipher.encrypt("secret")
    with pytest.raises(SecretsError):
        cipher.decrypt(token[:-2] + "xy")
