# ingestion-graph SDK

`ingestion-graph` is the independently installable, local-first data plane used
by Enterprise Data Ingestion Graph Studio and by any other Python project.

It owns connector contracts, canonical envelopes, resumable checkpoints,
idempotent destinations, artifacts, secret references, and queryable current
views. It has no dependency on FastAPI, Svelte, PostgreSQL, Redis, or Studio.

## Install

From any Python project today, install the SDK directly from its independently
packaged GitHub subdirectory. Pin a commit for reproducible projects:

```shell
python -m pip install "ingestion-graph[discord] @ git+https://github.com/FrozenScorch/data-ingestion-graph.git@44d7a11df3152ab54dbf7040e4654254c1ea1723#subdirectory=sdk"
```

Track `@main` only when intentionally following SDK development. The package is
not published to PyPI yet. From a checkout of this monorepo:

```shell
python -m pip install -e "./sdk[discord]"
```

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
from ingestion_graph import Pipeline, QueryRequest, SQLiteStateStore
from ingestion_graph.destinations import SQLiteCollection
from ingestion_graph.sources import JsonlSource

collection = SQLiteCollection(".ingestion/people.db")
await Pipeline(
    "people",
    JsonlSource("data/people.jsonl"),
    collection,
    state_store=SQLiteStateStore(".ingestion/state.db"),
).run()

for hit in await collection.query(QueryRequest("Ada", limit=5)):
    print(hit.envelope.payload, hit.score)
```

## Protocol guarantees

- Sources emit typed envelopes and explicit state messages.
- State advances only after the destination durably writes and flushes a page.
- Resumable destinations must declare idempotency.
- UPSERT and DELETE operations share stable source/stream/record identity.
- Large payloads can be represented by content-addressed `BlobRef` values.
- Serialized pipeline definitions contain `SecretRef`, not resolved credentials.
- Connectors use standard Python entry-point groups.

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
