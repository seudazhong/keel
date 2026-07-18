"""Feishu webhook signature and AES-CBC helpers."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7


def event_signature(timestamp: str, nonce: str, encrypt_key: str, body: bytes) -> str:
    material = timestamp.encode() + nonce.encode() + encrypt_key.encode() + body
    return hashlib.sha256(material).hexdigest()


def verify_event_signature(
    timestamp: str,
    nonce: str,
    encrypt_key: str,
    body: bytes,
    signature: str | None,
) -> bool:
    if not signature:
        return False
    return hmac.compare_digest(
        event_signature(timestamp, nonce, encrypt_key, body),
        signature.strip().lower(),
    )


def decrypt_event(encrypt_key: str, encrypted: str) -> bytes:
    key = hashlib.sha256(encrypt_key.encode()).digest()
    try:
        ciphertext = base64.b64decode(encrypted, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("Feishu encrypted event is not valid base64") from exc
    if not ciphertext or len(ciphertext) % 16:
        raise ValueError("Feishu encrypted event has an invalid length")
    decryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).decryptor()
    padded = decryptor.update(ciphertext) + decryptor.finalize()
    unpadder = PKCS7(128).unpadder()
    try:
        return unpadder.update(padded) + unpadder.finalize()
    except ValueError as exc:
        raise ValueError("Feishu encrypted event padding is invalid") from exc


def encrypt_event_for_test(encrypt_key: str, plaintext: bytes) -> str:
    key = hashlib.sha256(encrypt_key.encode()).digest()
    padder = PKCS7(128).padder()
    padded = padder.update(plaintext) + padder.finalize()
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode()


__all__ = [
    "decrypt_event",
    "encrypt_event_for_test",
    "event_signature",
    "verify_event_signature",
]
