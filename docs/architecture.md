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
`sdk-adapter` nodes. Discord, Document Source, and Queryable Test Store delegate
to public SDK components. Document Source accepts only owner-scoped managed upload
artifact IDs; absolute paths are resolved inside the control plane and removed
from emitted metadata and provenance. Stable `upload-<artifact-id>` stream names
preserve SDK record identity across runs. Remaining native database nodes can
migrate without changing the Studio graph format.

Manifest-aware SDK sources expose constructor-free `ConnectorSpec` metadata.
Manifest-backed Studio adapters project those schemas into control-plane-safe fields,
explicitly replacing raw secrets with saved connections today; managed-artifact path
projection follows the same boundary as document adapters adopt manifests. Registry
startup materializes every node contract and fails if an SDK field is neither projected
nor intentionally omitted, so connector upgrades cannot silently drift away from the
visual node contract.

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

The Studio document adapter binds the SDK `StateStore` contract to committed
PostgreSQL rows keyed by owner, graph, node, source, and stream. Source writes go
to a separate run-scoped candidate table containing the base revision/state and a
save-or-delete intent. The adapter buffers all state messages until every stream
completes. The source POST_EXEC checkpoint commits its successful bounded output
and candidates together, but committed SDK reads do not see those candidates.

After every graph node reports success, the durable worker fences its live job
lease, takes the run advisory fence and sorted affected source scopes, then locks
the run, rejects stale base revisions, and
promotes all candidates in the same transaction that marks the run completed.
Downstream failure, cancellation, lease loss, or a crash before that transaction
leaves committed source state unchanged. Failed runs retain candidates so
failed-node retry can restore the source POST_EXEC output and promote them after
its downstream nodes succeed, without rerunning the source. Cancellation is
terminal and deletes that run's candidates atomically. Starting a new full run
locks prior failed jobs and then their runs for that owner/graph. A queued or leased
retry wins and keeps its run/candidates; otherwise the old run becomes terminally
`superseded` and its candidates are deleted before the new run is created. A
concurrent retry then sees either `pending` or `superseded`, never an unlocked
intermediate state. Paused runs retain candidates; their jobs are not claimable
until resume atomically invalidates the prior lease and requeues the job. Revisions and retained delete
tombstones prevent an older concurrent run from recreating or regressing state.

This makes each Document Source run an incremental delta: unchanged uploads emit
nothing, changed files emit stable upserts and deletes, and deselecting a prior
artifact emits tombstones. Promotion relies on each downstream node's existing
success contract. It does not make a generic Studio destination durable: a
destination provides flush-before-acknowledgement only when it reports success
after its own durable write/flush. The per-run Queryable Test Store remains a
delta inspector, not a persistent current view across runs.

## Connector conformance requirements

Every connector should be tested for configuration validation, authentication,
discovery, pagination, mid-page failure, page resume, rate limits, duplicates,
schema changes, permission failures, and secret leakage. Connectors must declare
unsupported capabilities such as delete capture rather than implying them.

## Migration sequence

1. Wrap remaining database nodes behind SDK sources/transforms.
2. Replace graph tokens/passwords with `SecretRef` values and a server-side
   secret provider.
3. Store large node outputs as artifact references instead of JSONB.
4. Adapt the DAG executor to consume SDK messages.
5. Generate Studio node metadata from connector specs.

The `sdk_document_source` adapter exposes canonical document delta envelopes, and
`sdk_query_store` materializes bounded node output into a per-run query collection.
See [Ingest and query architecture](ingest-and-query.md) for the query contract and
the boundary with LLM orchestration frameworks.

Per-run query collections have a configurable retention window
(`QUERY_ARTIFACT_TTL_HOURS`, seven days by default). Studio prunes expired SQLite
databases and sidecars at startup, and
the query API deletes and rejects an expired artifact with HTTP 410.
