# Enterprise Studio and SDK architecture

## Boundaries

`ingestion_graph` is the public data plane. It owns envelopes, source and
destination contracts, transactional cursor advancement, artifact references,
secret references, and plugin discovery. It must not import from `backend.app`.

`backend/app` and `frontend` are Enterprise Data Ingestion Graph Studio. Studio
owns authentication, encrypted saved connections, predefined graphs, visual DAG
editing, execution history, and bounded previews. It installs the SDK from
`sdk/pyproject.toml`; the repository root is intentionally not a Python package.

Studio nodes declare whether they are `studio` implementations or thin
`sdk-adapter` nodes. Discord and Queryable Test Store already delegate to public
SDK components. Remaining native parsing and database nodes can migrate without
changing the Studio graph format.

The Connection Center is the sole UI for connector credentials. The backend
publishes typed connection forms, encrypts secrets at rest, and node schemas bind
to saved connections through `connection-ref` fields. Legacy graphs are migrated
by selecting a saved connection in the node editor; the next version drops config
fields that are no longer part of the registered node contract. Runtime access to
the Studio control-plane database is never used as an implicit fallback.

Predefined pipelines are immutable Studio catalog entries. They reference live
node contracts and are validated at startup for missing nodes, ports, required
inputs, and incompatible data types. They never duplicate connector code.

## Checkpoint transaction

For every source page, the runtime performs:

1. Receive records followed by a `StateMessage`.
2. Write the records to an idempotent destination.
3. Flush the destination durably.
4. Save the state message atomically.

If execution stops between steps 2 and 4, replay may resend the page. Stable
record IDs and an idempotent destination make that replay safe. A source ending
after records without a state message is a protocol error.

## Connector conformance requirements

Every connector should be tested for configuration validation, authentication,
discovery, pagination, mid-page failure, page resume, rate limits, duplicates,
schema changes, permission failures, and secret leakage. Connectors must declare
unsupported capabilities such as delete capture rather than implying them.

## Migration sequence

1. Wrap existing file parsing and database nodes behind SDK sources/transforms.
2. Replace graph tokens/passwords with `SecretRef` values and a server-side
   secret provider.
3. Store large node outputs as artifact references instead of JSONB.
4. Adapt the DAG executor to consume SDK messages.
5. Generate Studio node metadata from connector specs.

The first control-plane adapter is now `sdk_query_store`: it converts bounded
legacy node output into canonical SDK envelopes and materializes a per-run query
collection. See [Ingest and query architecture](ingest-and-query.md) for the query
contract and the boundary with LLM orchestration frameworks.

Per-run query collections have a configurable retention window
(`QUERY_ARTIFACT_TTL_HOURS`, seven days by default). Studio prunes expired SQLite
databases and sidecars at startup, and
the query API deletes and rejects an expired artifact with HTTP 410.
