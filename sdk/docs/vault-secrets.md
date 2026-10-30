# Vault secret bindings

`VaultSecretProvider` reads string fields from HashiCorp Vault **KV v2** using
the SDK's existing `SecretRef` / `SecretProvider` contract. A pipeline carries
an alias; the trusted host binds that alias to an approved mount, path and field.
The provider does not persist resolved credentials or put them into the graph.
Secrets still enter worker memory and the connector that uses them.

## Use an existing authenticated client

The SDK has no mandatory Vault client dependency. If your environment already
provides an approved `hvac` installation, pass its bound KV v2 read method:

```python
from ingestion_graph import SecretRef, VaultSecretBinding, VaultSecretProvider
from ingestion_graph.sources import PostgresSource

# vault_client is an authenticated hvac.Client supplied by the trusted host.
# The host selects it AFTER authorizing the user/job for the project.
secrets = VaultSecretProvider(
    vault_client.secrets.kv.v2.read_secret_version,
    {
        "source-password": VaultSecretBinding(
            mount_point="kv",
            path="teams/research/source-postgres",
            field="password",
        ),
    },
)
source = PostgresSource(
    "database.internal", 5432, "research", "ingestion_reader",
    SecretRef("source-password", provider="vault"),
    secret_provider=secrets,
    query="SELECT id, updated_at, title FROM documents",
    stream="documents",
    primary_key=("id",),
    cursor_field="updated_at",
)
```

The host configures the client's approved HTTPS address, certificate verification
(including any enterprise CA), namespace where applicable, bounded network
timeouts and redirect policy. It also owns authentication, token renewal,
revocation handling and client cleanup. A tenant's Vault administrator must grant
that identity access. Workload JWT authentication can avoid a stored bootstrap
password when the infrastructure supports it; AppRole authentication normally
requires secure delivery of a SecretID. This adapter does not implement either
authentication flow, and a Vault URL alone grants no access.

A compatible reader can be supplied instead of `hvac`. `VaultKV2Reader` describes
the keyword arguments and response shape: `path`, `mount_point`, `version` and
`raise_on_deleted_version=True`, returning a mapping with `data.data` fields.
The reader must raise on transport, authentication, permission or deleted-version
errors. Do not implement a reader that silently returns stale credentials.

## Bindings and rotation

- `SecretRef.key` is an exact alias lookup, never a caller-selected Vault URL or
  path. Unknown aliases and other provider names fail before invoking the reader.
- Bindings are frozen and the provider copies the alias map. Subsequent edits to
  the original configuration map do not change an existing provider's scope.
- Mount and secret paths are relative. This adapter accepts ASCII letters,
  digits, `_`, `-`, `.` and `/`, with no empty, `.` or `..` segments. URLs,
  percent-encoded segments, query strings and fragments are rejected. `path`
  excludes the mount and Vault's API `/data/` segment.
- Each resolution reads again. `version=None` follows the latest KV version;
  a positive integer pins a specific version. There is no secret cache, fallback,
  automatic retry, or write/delete/list operation.
- Missing, empty or non-string fields fail with `ConfigurationError`. Client
  failures use a generic error without retaining the client's exception chain.
  The provider emits no logs. Configure client logging and application error
  reporting to avoid capturing headers, response bodies or frame locals.
- `SecretValue` redacts `repr`; `str(value)` is plaintext so drivers can use it.
  Never serialize resolved values into pipeline configs, state or logs.
- Rotation affects the next resolution. An established database connection may
  continue using its original credentials until it reconnects. Token lifecycle
  and reconnection policy belong to the host/connector.

## Project ownership and contributed connectors

Create a provider per authorized project/job using that project's client identity
and approved connection bindings. Never derive this selection solely from a
`tenant_id`, Vault role or path submitted in a graph. Keep connection endpoints
and database resource grants under the same authorization checks: a graph must
not redirect an approved credential to an arbitrary host.

Vault policies must limit access independently of this Python alias map. For
the example above, the relevant KV v2 read policy path is
`kv/data/teams/research/source-postgres`. Vault grants access to the whole secret
at that path, not an individual JSON field. Put fields with different access
requirements in separate Vault secrets. The reader receives the complete secret
response even though the provider returns only its bound field.

This is an adapter for trusted connectors, not a sandbox. Installed Python
plugins run with their process's authority and can inspect memory or bypass SDK
interfaces. Untrusted tenant code requires separate execution isolation with
scoped identities, storage and network policy. Database permissions must also
restrict the project's actual tables, collections, keys and administrative
operations; Vault access does not enforce those permissions.

This contribution does not add Studio project memberships, a Vault configuration
UI, KV v1, dynamic database credentials, lease renewal/revocation, or Windows
integrated authentication. It leaves the dependency-free core and existing
`SecretRef` format unchanged. Downstream applications must deliberately upgrade
their reviewed SDK commit before using the new provider.

## Verification

Run `python -m pytest tests/test_vault_secrets.py -q` from `sdk/`. These offline
tests exercise alias isolation, rotation, revocation/failure behavior, malformed
responses, error redaction and use by the existing PostgreSQL connection adapter.
They require no Vault instance, live credentials or database. They do not verify
a deployment's Vault policies, identity configuration or network reachability.

Protocol references:

- [Vault KV v2 HTTP API](https://developer.hashicorp.com/vault/api-docs/secret/kv/kv-v2)
- [hvac KV v2 reader](https://python-hvac.org/en/stable/usage/secrets_engines/kv_v2.html)
- [Vault policies](https://developer.hashicorp.com/vault/docs/concepts/policies)
- [Vault JWT authentication](https://developer.hashicorp.com/vault/docs/auth/jwt)
- [Vault AppRole authentication](https://developer.hashicorp.com/vault/docs/auth/approle)
