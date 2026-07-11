"""Secret references keep credentials out of serialized pipeline definitions."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from ingestion_graph.errors import ConfigurationError


@dataclass(frozen=True, slots=True)
class SecretRef:
    key: str
    provider: str = "env"

    def __repr__(self) -> str:
        return f"SecretRef(provider={self.provider!r}, key={self.key!r})"


class SecretValue(str):
    def __repr__(self) -> str:
        return "SecretValue('********')"


class SecretProvider(Protocol):
    name: str

    def resolve(self, reference: SecretRef) -> SecretValue: ...


class EnvSecretProvider:
    name = "env"

    def __init__(self, environ: Mapping[str, str] | None = None) -> None:
        self._environ = environ if environ is not None else os.environ

    def resolve(self, reference: SecretRef) -> SecretValue:
        if reference.provider != self.name:
            raise ConfigurationError(
                f"Secret provider {reference.provider!r} cannot be resolved by {self.name!r}"
            )
        value = self._environ.get(reference.key)
        if not value:
            raise ConfigurationError(f"Required secret {reference.key!r} is not configured")
        return SecretValue(value)
