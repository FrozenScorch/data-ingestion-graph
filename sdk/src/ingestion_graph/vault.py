"""Read approved Vault KV v2 fields through a caller-authenticated client."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from ingestion_graph.errors import ConfigurationError
from ingestion_graph.secrets import SecretRef, SecretValue


class VaultKV2Reader(Protocol):
    """The subset of ``hvac.Client.secrets.kv.v2.read_secret_version`` used here."""

    def __call__(
        self,
        *,
        path: str,
        mount_point: str,
        version: int | None,
        raise_on_deleted_version: bool,
    ) -> Mapping[str, object]: ...


def _validate_path(value: str) -> None:
    # Keep inputs literal relative paths, independent of client URL encoding.
    if (
        not isinstance(value, str)
        or re.fullmatch(r"[A-Za-z0-9_./-]+", value) is None
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise ConfigurationError("Vault paths must contain literal, nonempty relative segments")


@dataclass(frozen=True, slots=True)
class VaultSecretBinding:
    """One approved KV v2 field; ``path`` is relative to ``mount_point``.

    ``version=None`` reads the latest version on each resolution. Binding an
    explicit positive version opts out of following secret rotation.
    """

    path: str
    field: str
    mount_point: str = "secret"
    version: int | None = None

    def __post_init__(self) -> None:
        _validate_path(self.path)
        _validate_path(self.mount_point)
        if not isinstance(self.field, str) or not self.field:
            raise ConfigurationError("Vault secret fields must be nonempty strings")
        if self.version is not None and (type(self.version) is not int or self.version <= 0):
            raise ConfigurationError("Vault secret versions must be positive integers")


class VaultSecretProvider:
    """Resolve aliases from a fixed binding map, without persisting secret values.

    The host must authenticate/authorize the job before selecting its client and
    bindings. Vault policies and worker isolation remain the security boundary;
    this adapter is not a sandbox or a tenant authorization service. The caller
    owns authentication, TLS, timeouts, token renewal and client cleanup.
    """

    name = "vault"

    def __init__(
        self,
        read_secret_version: VaultKV2Reader,
        bindings: Mapping[str, VaultSecretBinding],
    ) -> None:
        if not callable(read_secret_version):
            raise ConfigurationError("Vault requires a KV v2 reader")
        if not isinstance(bindings, Mapping) or any(
            not isinstance(alias, str) or not alias or not isinstance(binding, VaultSecretBinding)
            for alias, binding in bindings.items()
        ):
            raise ConfigurationError("Vault requires named secret bindings")
        self._read_secret_version = read_secret_version
        # Do not let subsequent edits to a host's configuration dict widen scope.
        self._bindings = dict(bindings)

    def resolve(self, reference: SecretRef) -> SecretValue:
        if (
            not isinstance(reference, SecretRef)
            or reference.provider != self.name
            or not isinstance(reference.key, str)
            or reference.key not in self._bindings
        ):
            raise ConfigurationError("Vault secret reference is not configured")
        binding = self._bindings[reference.key]
        try:
            response = self._read_secret_version(
                path=binding.path,
                mount_point=binding.mount_point,
                version=binding.version,
                raise_on_deleted_version=True,
            )
        except Exception:
            # Client exceptions may include tokens, URLs or response bodies. Raise
            # outside the handler to avoid retaining the original exception chain.
            pass
        else:
            if isinstance(response, Mapping):
                data = response.get("data")
                if isinstance(data, Mapping):
                    fields = data.get("data")
                    if isinstance(fields, Mapping):
                        value = fields.get(binding.field)
                        if isinstance(value, str) and value:
                            return SecretValue(value)
        # No cache or fallback: revoked/deleted/unavailable secrets fail closed.
        raise ConfigurationError("Vault secret could not be resolved")
