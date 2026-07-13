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

The destination validates and quotes every configured identifier. Upsert keys
must match a real unique or primary constraint. It creates a same-schema replay
ledger named `_ingestion_graph_versions` by default, so the writer role needs
permission to create and use that table or an administrator must pre-provision
it. The ledger retains applied event identities; plan retention for high-volume
targets. Exact envelope replays are skipped. A later logical reversion must carry
a new cursor, checksum, or metadata identity. Without a monotonic event identity,
cross-pipeline conflicts are last-committed-wins.

DELETE envelopes use the configured `key_fields` and require their values in
`envelope.metadata["key"]`; metadata never controls column names. `replace()` is
the one-shot atomic replacement API: target truncation without `CASCADE`, scoped
ledger cleanup, and all inserts share one transaction, including empty snapshots.
`reset()` is destructive maintenance shorthand for an empty atomic replacement.
The embedded `Pipeline` uses ordinary checkpoint-safe `write()` and never resets
a destination implicitly.
