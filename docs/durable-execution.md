# Durable Studio execution

Studio dispatches every manual run, replay, and failed-node retry through the
`run_jobs` database table. API requests commit the run and its queue record in
one transaction. Workers claim jobs with PostgreSQL row locks, renew expiring
leases, and fence heartbeats/completion by worker identity. A process crash
therefore leaves recoverable work instead of losing a FastAPI background task.
Cron/interval schedules and accepted signed webhooks use the same atomic Run plus
RunJob boundary, pinned to the immutable graph version selected when the trigger
was saved.

At startup, the worker also queues legacy pending or running runs that have an
immutable graph version but no job record. Completed and cancelled runs are
never re-executed when an expired lease is reclaimed.

SDK source adapters stage run-scoped state candidates at source POST_EXEC. The
worker promotes them only after all graph nodes succeed. A long connector read
holds only the deterministic run and source-scope transaction advisory fences;
its lease checks do not lock the job row, so heartbeats can renew concurrently.
Candidate staging then locks the job row followed by the forced-refresh run row
for the short POST_EXEC transaction. Completion takes the run advisory fence,
sorted source scopes, job row, and run row. Thus every mixed database row-lock
path remains job-then-run while advisory fences preserve a stable candidate
snapshot. The lease is rechecked after waits and immediately before commits.
Direct execution uses the same advisory fences without requiring a job lease.
A downstream failure keeps candidates for same-run failed-node retry; a crash or
lease loss cannot expose them as committed source state. Cancellation atomically
deletes its candidates because it is terminal. Paused runs retain candidates,
while a new full run locks prior failed jobs and then their runs for the same owner and
graph. Queued or leased retries survive; inactive failures become terminally
`superseded` and lose their candidates atomically before the new run is created.

Queue claims join the run and accept only `pending` or `running` work. Pausing
serializes the job row before the run row; a cooperative worker requeues rather
than completes that job. Resume changes `paused` to `running` and resets the
single job to `queued` in the same transaction, clearing the old lease so a stale
worker cannot stage state or acknowledge failure/completion.

## Runtime settings

```dotenv
RUN_WORKER_ENABLED=true
RUN_WORKER_CONCURRENCY=1
RUN_WORKER_POLL_SECONDS=1
RUN_WORKER_LEASE_SECONDS=60
RUN_WORKER_HEARTBEAT_SECONDS=15
```

The heartbeat interval must be less than half the lease duration. Increase the
lease for connectors that can block the Python event loop for long periods.
Multiple Studio processes may safely claim from the same PostgreSQL database;
each process starts its configured number of worker slots.

## Schedule and webhook dispatch

Each API process may run a trigger scheduler. Due enabled schedule rows are
claimed in bounded batches with `FOR UPDATE SKIP LOCKED`, so multiple processes
can poll the same database without dispatching the same occurrence. Interval
and five-field cron schedules calculate the first instant strictly after the
poll time; downtime therefore skips missed backlog instead of producing a run
storm. Each dispatch creates a pending Run and queued RunJob, records the last
run, and advances `next_run_at` in one transaction. A savepoint isolates an
invalid trigger from the rest of its claimed batch. Scheduler state and its last
poll/error are exposed by `/health`.

Webhook triggers return a 256-bit URL-safe secret only on creation or rotation.
The database stores that value through the same Fernet-backed encrypted JSONB
primitive used for saved connection credentials. Senders POST a JSON object to
`/api/webhooks/{trigger_id}` with:

```text
X-Ingestion-Timestamp: <Unix seconds>
X-Ingestion-Delivery: <stable delivery ID>
X-Ingestion-Signature: sha256=<hex HMAC-SHA256>
```

The signature input is the exact bytes `timestamp + "." + raw_body`. Studio
checks clock skew and the configured body limit before accepting the delivery,
then locks the trigger for the short replay/rate-limit transaction. A unique
trigger/delivery constraint and the trigger row lock guarantee that concurrent
copies create one delivery ledger row, one Run, and one RunJob. The JSON object
is stored on the Run but excluded from normal Run responses; execution supplies
it only as `webhook_payload` to a root `webhook_source` node, including failed-node
retry. Other nodes receive it only through ordinary graph edges.

```dotenv
TRIGGER_SCHEDULER_ENABLED=true
TRIGGER_SCHEDULER_POLL_SECONDS=5
TRIGGER_SCHEDULER_BATCH_SIZE=50
WEBHOOK_MAX_BYTES=1048576
WEBHOOK_TIMESTAMP_SKEW_SECONDS=300
WEBHOOK_DELIVERY_RETENTION_HOURS=168
WEBHOOK_PRUNE_INTERVAL_SECONDS=3600
```

The scheduler prunes webhook delivery ledger rows after the retention window.
Once a row is pruned, that old delivery ID is no longer a replay guard; senders
must keep retry attempts inside the configured window.

Run failure recording locks and validates the worker's current job lease before
using a forced-refresh run-row lock. A late node failure
can change only `running` to `failed`, so it cannot overwrite a concurrently
committed cancellation, pause, completion, or supersession.

## Delivery guarantee

Recovery is **at least once**. A worker can perform an external side effect and
die before committing its checkpoint and job completion. Connector authors
must use stable source keys, destination upserts, idempotency keys, or
transactional writes. Non-idempotent destinations should not be used for
automatically recovered production runs until they implement one of those
guards.

Source-state promotion observes the node success boundary; it does not strengthen
a destination that returns success before its data is durable. Destination nodes
participating in flush-before-acknowledgement must finish their durable write or
flush before returning success.

## Verification

The backend suite includes a real PostgreSQL contention test that proves only
one worker claims a queued row, an expired lease can be reclaimed, and a stale
worker cannot heartbeat or finish the job. CI provides PostgreSQL through a
service container and sets `TEST_DATABASE_URL` for that test. Trigger contention
tests additionally prove that two schedulers dispatch one due occurrence and
two concurrent copies of a webhook delivery create only one Run/RunJob pair.

Compose gates API startup on the one-shot `ingestion-migrate` service. Legacy
unversioned legacy databases run idempotent ordered migrations; versioned databases apply ordered
Alembic upgrades. Zero-downtime multi-replica upgrade/downgrade policy and
automated backup/restore validation remain enterprise-readiness items.
