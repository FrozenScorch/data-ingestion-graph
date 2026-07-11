"""Authenticated encryption for connection configuration stored in JSONB."""

import base64
import hashlib
import json
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from app.config import settings


_ENCRYPTED_KEY = "$encrypted"


def _fernet() -> Fernet:
    # Development remains usable without extra setup, while non-development
    # startup requires an independent key through Settings.validate_security.
    material = settings.connection_encryption_key or settings.jwt_secret_key
    key = base64.urlsafe_b64encode(hashlib.sha256(material.encode("utf-8")).digest())
    return Fernet(key)


def encrypt_connection_config(config: dict[str, Any] | None) -> dict[str, str]:
    if config and set(config) == {_ENCRYPTED_KEY}:
        return {_ENCRYPTED_KEY: str(config[_ENCRYPTED_KEY])}
    serialized = json.dumps(config or {}, sort_keys=True, separators=(",", ":")).encode()
    return {_ENCRYPTED_KEY: _fernet().encrypt(serialized).decode("ascii")}


def decrypt_connection_config(config: dict[str, Any] | None) -> dict[str, Any]:
    if not config:
        return {}
    token = config.get(_ENCRYPTED_KEY)
    if token is None:
        # Backward compatibility for records created before encryption was added.
        return dict(config)
    try:
        plaintext = _fernet().decrypt(str(token).encode("ascii"))
    except InvalidToken as exc:
        raise ValueError("Saved connection credentials could not be decrypted") from exc
    decoded = json.loads(plaintext)
    if not isinstance(decoded, dict):
        raise ValueError("Saved connection configuration is invalid")
    return decoded


def is_encrypted_connection_config(config: dict[str, Any] | None) -> bool:
    return bool(config and set(config) == {_ENCRYPTED_KEY})
