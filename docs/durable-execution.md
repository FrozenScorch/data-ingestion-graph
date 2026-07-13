# Durable Studio execution

Studio dispatches every manual run, replay, and failed-node retry through the
`run_jobs` database table. API requests commit the run and its queue record in
one transaction. Workers claim jobs with PostgreSQL row locks, renew expiring
leases, and fence heartbeats/completion by worker identity. A process crash
therefore leaves recoverable work instead of losing a FastAPI background task.

At startup, the worker also queues legacy pending or running runs that have an
immutable graph version but no job record. Completed and cancelled runs are
never re-executed when an expired lease is reclaimed.

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

## Delivery guarantee

Recovery is **at least once**. A worker can perform an external side effect and
die before committing its checkpoint and job completion. Connector authors
must use stable source keys, destination upserts, idempotency keys, or
transactional writes. Non-idempotent destinations should not be used for
automatically recovered production runs until they implement one of those
guards.

## Verification

The backend suite includes a real PostgreSQL contention test that proves only
one worker claims a queued row, an expired lease can be reclaimed, and a stale
worker cannot heartbeat or finish the job. CI provides PostgreSQL through a
service container and sets `TEST_DATABASE_URL` for that test.

Database schema migrations are still a deployment-readiness item: existing
Studio startup creates missing tables, but production upgrade/downgrade policy
and migration gates must be completed before zero-downtime releases.
