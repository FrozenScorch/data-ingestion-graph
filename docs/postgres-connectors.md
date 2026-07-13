# PostgreSQL SDK connectors

Install the optional driver and keep the password behind a secret reference:

```bash
pip install 'ingestion-graph[postgres]'
```

```python
from ingestion_graph import Pipeline, SecretRef
from ingestion_graph.destinations import PostgresDestination
from ingestion_graph.sources import PostgresSource

source = PostgresSource(
    "db.internal",
    5432,
    "application",
    "reader",
    SecretRef("SOURCE_DATABASE_PASSWORD"),
    query="SELECT id, updated_at, name FROM public.customers",
    stream="customers",
    primary_key=("id",),
    cursor_field="updated_at",
)
destination = PostgresDestination(
    "warehouse.internal",
    5432,
    "warehouse",
    "writer",
    SecretRef("DESTINATION_DATABASE_PASSWORD"),
    target="public.customers",
    mode="upsert",
    key_fields=("id",),
)
result = await Pipeline("customers-sync", source, destination).run()
```

Incremental mode requires a non-null, monotonic cursor plus a non-null primary
key. Together they must form a unique lexicographic order. A primary key without
a cursor runs recurring keyset full-refresh cycles and resets only after a clean
end of the query. Rows inserted behind an active full-refresh checkpoint are
picked up by the next cycle. A query without a primary key is intentionally a
bounded, non-resumable preview and requires `max_records`.

Tagged checkpoints preserve keyset scalar types and bind to the non-secret host,
port, database, username, query, stream, keyset, cursor, and checkpoint format.
State from one database therefore cannot silently skip rows in another. Snapshot
envelopes include their full-refresh cycle in event identity, so `A -> B -> A`
across three cycles is applied three times while retrying one cycle remains
idempotent.

The destination validates and quotes every configured identifier. Upsert keys
must match a real unique or primary constraint. It creates a same-schema replay
ledger named `_ingestion_graph_versions` by default, so the writer role needs
permission to create and use that table or an administrator must pre-provision
it. The ledger retains applied event identities; plan retention for high-volume
targets. Exact envelope replays are skipped. A later logical reversion must carry
a new cursor, checksum, or metadata identity. Without a monotonic event identity,
cross-pipeline conflicts are last-committed-wins.

The destination resolves each target to its PostgreSQL table OID and canonical
schema inside the transaction. Qualified and unqualified aliases share one
advisory lock and replay-ledger scope. Upsert readiness accepts only valid,
immediate, non-partial, non-expression unique indexes that PostgreSQL can infer for
`ON CONFLICT`; included columns are not mistaken for conflict keys.

Timestamps, dates, times, intervals, UUIDs, decimals, bytes, and arrays containing
those native values remain JSON-safe in envelopes through reversible type hints.
A PostgreSQL destination restores them before binding values through `asyncpg`.

DELETE envelopes use the configured `key_fields` and require their values in
`envelope.metadata["key"]`; metadata never controls column names. `replace()` is
the one-shot atomic replacement API: target truncation without `CASCADE`, scoped
ledger cleanup, and all inserts share one transaction, including empty snapshots.
`reset()` is destructive maintenance shorthand for an empty atomic replacement.
The embedded `Pipeline` uses ordinary checkpoint-safe `write()` and never resets
a destination implicitly.
