# ingestion-graph SDK

`ingestion-graph` is the independently installable, local-first data plane used
by Enterprise Data Ingestion Graph Studio and by any other Python project.

It owns connector contracts, canonical envelopes, resumable checkpoints,
idempotent destinations, artifacts, secret references, and queryable current
views. Ordered transform chains make mapping, filtering, and normalization
reusable across embedded projects. The SDK has no dependency on FastAPI,
Svelte, PostgreSQL, Redis, or Studio.

## Install

From any Python project today, install the SDK directly from its independently
packaged GitHub subdirectory:

```shell
python -m pip install "ingestion-graph[discord] @ git+https://github.com/FrozenScorch/data-ingestion-graph.git@main#subdirectory=sdk"
```

For reproducible deployments, replace `@main` with a reviewed commit SHA or
release tag. The package is not published to PyPI yet. From a checkout of this
monorepo:

```shell
python -m pip install -e "./sdk[discord]"
```

For PDF, Word, and Excel parsing, install the document extra:

```shell
python -m pip install "ingestion-graph[documents] @ git+https://github.com/FrozenScorch/data-ingestion-graph.git@main#subdirectory=sdk"
```

```python
from ingestion_graph import LocalDocumentsSource, Pipeline
from ingestion_graph.destinations import SQLiteCollection

await Pipeline(
    "my-documents",
    LocalDocumentsSource(["~/Documents", "./mail"], recursive=True),
    SQLiteCollection("./data/documents.db"),
).run()
```

`LocalDocumentsSource` treats each configured file or directory root as a
stream and ingests PDF, Word (`.docx`), Excel (`.xlsx`), CSV, email (`.eml`),
HTML, Markdown, and text. Immutable snapshots ensure checkpoints describe the
exact parsed bytes. Per-file SHA-256, element counts, and parser fingerprints
resume within unchanged documents, reprocess configuration changes, and emit
tombstones when a document shrinks or a configured file or directory child
disappears. With symlink following disabled (the default), snapshots use
root-anchored safe opens and reject path/reparse changes; followed directory
cycles are deduplicated. Use explicit `stream_names` when identities must remain
stable after moving a root between machines. File and expanded Office-archive
size limits are enforced while copying and parsing.

## Test an ingestion pipeline

```shell
ingestion-graph ingest-jsonl data/people.jsonl --collection .ingestion/people.db
ingestion-graph query "Ada Lovelace" --collection .ingestion/people.db
ingestion-graph query --collection .ingestion/people.db --limit 20
```

The source resumes from byte-offset checkpoints and detects changes to data
already read. `SQLiteCollection` applies UPSERT and DELETE operations to a
durable FTS5-backed current view.

## Embedded API

```python
from collections.abc import Sequence

from ingestion_graph import Envelope, Pipeline, QueryRequest, SQLiteStateStore, Transform
from ingestion_graph.destinations import SQLiteCollection
from ingestion_graph.sources import JsonlSource

collection = SQLiteCollection(".ingestion/people.db")

class ActiveRecords(Transform):
    async def apply(self, records: Sequence[Envelope]) -> Sequence[Envelope]:
        return [record for record in records if record.metadata.get("active", True)]

await Pipeline(
    "people",
    JsonlSource("data/people.jsonl"),
    collection,
    transforms=[ActiveRecords()],
    state_store=SQLiteStateStore(".ingestion/state.db"),
).run()

for hit in await collection.query(QueryRequest("Ada", limit=5)):
    print(hit.envelope.payload, hit.score)
```

## Protocol guarantees

- Sources emit typed envelopes and explicit state messages.
- State advances only after the destination durably writes and flushes a page.
- Transforms run in order on checkpoint-bounded batches before destination writes.
- A transform may map, filter, or expand records but cannot move them across streams.
- Transform changes do not invalidate saved source state automatically; use a new
  pipeline name or reset its state when existing records must be reprocessed.
- Resumable destinations must declare idempotency.
- UPSERT and DELETE operations share stable source/stream/record identity.
- Large payloads can be represented by content-addressed `BlobRef` values.
- Serialized pipeline definitions contain `SecretRef`, not resolved credentials.
- Connectors use standard Python entry-point groups.

## Connector manifests

Built-in sources and destinations publish configuration and capability metadata
without requiring credentials, paths, or network clients:

```python
from ingestion_graph.destinations import SQLiteCollection
from ingestion_graph.sources import DiscordSource

source_manifest = DiscordSource.manifest()
destination_manifest = SQLiteCollection.manifest()
print(source_manifest.name, destination_manifest.config_schema)
```

Plugin hosts can call `load_connector_manifest(kind, name)` for either `sources`
or `destinations` before creating a connector. Older third-party connectors remain
valid runtime plugins, but are reported as not manifest-aware instead of being
instantiated just to inspect their metadata. Duplicate installed entry-point names
fail closed so connector selection is deterministic.

## Connector conformance kit

External connector packages can reuse the dependency-free conformance checks
from ordinary async tests, `unittest`, or pytest:

```python
from ingestion_graph import inspect_destination_replay, inspect_source_messages

# Capture a deterministic source page with a fake client, then check identity,
# stream, capability, schema-message, and checkpoint ordering invariants.
source_report = inspect_source_messages(source, stream, captured_messages)
source_report.raise_for_errors()

# Use a disposable destination: this performs a write, flush, and exact replay.
destination_report = await inspect_destination_replay(destination, records)
destination_report.raise_for_errors()
```

The destination must be disposable and initially empty; an UPSERT case expects
all supplied records to be newly written before the zero-write replay. For a
DELETE case, first populate the rows to remove and pass a positive
`expected_first_write` count so a no-op delete cannot be certified.

`inspect_manifest` checks an already loaded `ConnectorSpec`, while
`inspect_installed_manifest` also exercises entry-point loading.
`inspect_secret_redaction` checks caller-supplied secret values against the
representations a connector test chooses (such as repr, errors, logs, state,
and provenance); the SDK does not guess which fields are confidential.

The kit intentionally checks protocol invariants rather than remote systems.
It does not infer cursor monotonicity, test live pagination or rate limits, or
require one physical tombstone strategy. Connector packages should inject fake
clients and clocks for those cases, and keep separate opt-in integration tests
for real services.

## Development

```shell
cd sdk
python -m pip install -e ".[dev]"
python -m ruff check src tests
python -m ruff format --check src tests
python -m mypy
python -m pytest tests -q
python -m build
```
