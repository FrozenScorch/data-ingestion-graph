# Generic REST source

Install the optional HTTP dependency with the SDK:

```shell
python -m pip install "ingestion-graph[rest] @ git+https://github.com/FrozenScorch/data-ingestion-graph.git@main#subdirectory=sdk"
```

`RestSource` reads JSON objects from a bounded sequence of HTTPS `GET` requests.
The endpoint URL cannot contain credentials. Authentication is resolved from a
`SecretRef` only when a request is sent, so the resolved value is never placed in
the manifest, connector representation, envelope metadata, provenance, or saved
source state.

```python
from ingestion_graph import Pipeline, SecretRef, SQLiteStateStore
from ingestion_graph.destinations import JsonlDestination
from ingestion_graph.sources import RestSource

source = RestSource(
    "https://api.example.com",
    "/v1/widgets",
    stream="widgets",
    records_path="data.items",
    primary_key=("tenant_id", "id"),
    pagination="cursor",
    next_cursor_path="data.next_cursor",
    cursor_param="after",
    auth_type="bearer",
    secret=SecretRef("WIDGET_API_TOKEN"),
    max_pages=100,
    max_records=50_000,
)

await Pipeline(
    "widgets",
    source,
    JsonlDestination("data/widgets.jsonl"),
    state_store=SQLiteStateStore(".ingestion/state.db"),
).run()
```

For API-key authentication, set `auth_type="api_key"` and choose the header with
`api_key_header` (for example, `X-API-Key`). Query-string credentials are rejected.
Plain HTTP requires `allow_http=True` and is still restricted to a loopback host,
which keeps that escape hatch suitable only for local tests.

## Extraction, identity, and discovery

`records_path`, `next_cursor_path`, and primary-key fields use dotted JSON object
paths. Use an empty `records_path` to read a root JSON array. Every primary-key
component must exist and be a non-null JSON scalar, and its JSON type must remain
stable across pages. The typed primary-key tuple produces a deterministic envelope
ID; the connector emits `SNAPSHOT` records and does not infer deletes.

`discover()` samples the first response page and infers a JSON schema without
producing or committing a checkpoint. Optional fields are not marked required.
Malformed response shapes, duplicate keys within a page, and primary-key type drift
fail closed.

## Pagination and resume behavior

- `pagination="cursor"` reads the next token at `next_cursor_path` and sends it in
  `cursor_param`.
- `pagination="link"` follows the RFC `Link` header target whose `rel` contains
  `next`. Relative and same-origin absolute links are supported.
- `pagination="none"` reads one page.

Each response page ends in a `StateMessage`. A page-boundary checkpoint names the
exact next request. If `max_records` stops inside a page, the checkpoint instead
stores the current request, the first uncommitted record offset, and a hash of the
whole page (including its next-page target). Resume refetches the page and verifies
that hash before skipping committed records. If the API changed the page, the run
fails instead of silently dropping or duplicating data. Stable envelope IDs also
make replay after a failure before checkpoint persistence safe for idempotent
destinations.

At clean end-of-list the connector saves a new full-refresh cycle and points back
to the configured endpoint. The cycle is included in envelope metadata, so a
legitimate `A -> B -> A` reversion remains distinct while retrying records within
one cycle remains idempotent. `max_pages` and `max_records` are per-run safety
bounds; a non-final bound leaves a continuation checkpoint for the next run.

Cross-origin next links are rejected by default. `allow_cross_origin_next=True`
permits HTTPS traversal, but authentication headers are deliberately withheld from
the other origin. Redirect responses are not followed automatically.
