# Studio and SDK readiness roadmap

Baseline: document-source branch after PR #33, audited 2026-07-12.

## Executive assessment

The SDK is usable today from unrelated Python projects. The Studio is a useful
manual, local visual ingestion product, but it is not yet a safe LAN appliance or
a continuous anything-to-anything synchronization platform.

| Outcome | Readiness | Current reality |
| --- | ---: | --- |
| Reusable Python SDK | 78% | Installable, typed, resumable core with Discord, JSONL, and local document sources plus JSONL/SQLite destinations |
| Local single-user visual ingestion | 60% | Manual batch graphs, transforms, PostgreSQL, server files, Discord preview, query inspection |
| Trusted-LAN Studio | 25–30% | Experimental deployment only; upload, networking, auth coverage, and worker durability are incomplete |
| Enterprise multi-user Studio | 10–15% | Tenant isolation, service auth, SSO, durable workers, HA, backups, and observability are release gates |
| Anywhere-to-anywhere continuous sync | 15–20% | The SDK protocol is credible, but connector breadth and sync modes are narrow |

These percentages measure delivered capability, not code volume.

## What works now

### Reusable SDK

- Independent `sdk/pyproject.toml`, no Studio dependency, Python 3.11–3.13.
- Canonical UPSERT/DELETE envelopes, stable identities, secret and artifact references.
- Flush-before-checkpoint pipeline semantics and SQLite-backed durable source state.
- Real resumable `JsonlSource`, paginated/incremental `DiscordSource`, and
  checkpoint-safe `LocalDocumentsSource` for PDF, Word, Excel/CSV, email, HTML,
  Markdown, and text.
- Idempotent JSONL changelog and transactional SQLite FTS5 current-view destinations.
- Plugin discovery, CLI ingestion/query commands, strict typing, wheel build, and CI.
- Verified installation from GitHub's `sdk/` subdirectory in a fresh virtual environment.

### Studio

- Svelte visual DAG editor backed by a dynamic node registry and typed ports.
- Graph versions, manual execution, retries/replay, node checkpoints, DLQ, lineage, and run inspection.
- Encrypted owner-scoped PostgreSQL and Discord Connection Center.
- Discord, PostgreSQL, and documents starter graphs.
- PDF, DOCX, CSV, text/Markdown/JSON/XML/HTML parsing; chunking and AI transforms.
- PostgreSQL writer, pgvector writer, and expiring per-run SDK query collections.

## Material gaps

### Sources and destinations

Real SDK coverage is currently three source families and two local destinations. Studio
adds PostgreSQL, server-side files, SEC EDGAR, transforms, PostgreSQL/pgvector,
and HTTP actions, but several displayed capabilities are incomplete:

- Browser uploads and File Source selection are owner-scoped and usable; folder watch,
  object storage, quotas, malware scanning, and multi-replica shared storage remain.
- Local Excel/XLSX and RFC email-file ingestion are now supported by the SDK;
  OCR, mailbox APIs, Slack/Teams, Drive/SharePoint/S3,
  SQL Server/MySQL/Oracle/MongoDB, queues, generic paginated REST, or database CDC.
- GitHub Source is a stub; Webhook Source has no receiver route.
- The generic HTTP node does not yet stream upstream records as a destination.
- SDK plugins do not automatically become typed Studio nodes or Connection Center forms.
- Destination delete propagation, reconciliation, schema drift, and source-key upserts are inconsistent.

### SDK release engineering

- The working external install is a pinned GitHub VCS dependency; no PyPI package,
  git tag, GitHub Release, changelog, or release workflow exists yet.
- Package metadata declares proprietary licensing, but the repository does not contain
  explicit license terms. A deliberate licensing decision is required before third-party distribution.
- Installed-wheel typing tests cover the core consumer path but not custom connectors,
  secret providers, plugin loading, or every public submodule.
- The transform plugin contract and ordered, checkpoint-safe transform chain are implemented;
  plugin conformance and versioned transform-state migration remain.

### Sync semantics

- Studio runs are manual, stateless batch/preview jobs. SDK source state is not persisted
  per graph/node across Studio runs.
- `schedule` and `webhook` are labels, not implemented trigger services.
- Work executes inside FastAPI background tasks, without a durable queue, leases,
  heartbeat, restart recovery, or per-stream concurrency control.
- Missing modes: scheduled polling, snapshot-to-incremental handoff, CDC, streaming,
  bidirectional conflict resolution, partitioned backfill, and reconciliation.

### UX and LAN readiness

- Folder selection/watch, connector discovery/preview, schema mapping,
  run freshness, and schedule management need first-class UX.
- Complete owner checks are required across every execution, WebSocket, DLQ, and lineage path.
- LAN deployment needs non-default secrets, private service networks, TLS/reverse proxy,
  secure headers/rate limits, LAN-aware CORS/origin, and working WebSocket proxying.
- File access must be confined to server-owned roots; outbound HTTP needs an SSRF policy.
- Health reporting, Alembic migrations, backup/restore, structured metrics/tracing,
  retention, quotas, and disaster-recovery tests are incomplete.
- Enterprise use additionally needs organizations/projects, scoped API keys or service
  accounts, SSO/OIDC, audit logs, RBAC/ACLs, HA workers, and shared event delivery.

## Distance and delivery sequence

Estimates are for focused implementation by one strong engineer/agent workflow and
assume connectors can use stable upstream APIs. They are planning ranges, not promises.

### Milestone 0 — safe local/LAN foundation (1–3 weeks)

1. Fix authorization coverage for runs, controls, WebSockets, DLQ, and lineage.
2. Done: browser upload, server-owned owner isolation, file picker, and documents template.
3. Add production Compose profiles: generated secrets, private networks, reverse proxy/TLS,
   WebSockets, LAN origins, health correctness, and migrations.
4. Lock down outbound HTTP and add a scoped agent/service authentication path.

Exit: a trusted user can safely upload documents and run graphs from another LAN device.

### Milestone 1 — real recurring sync (3–6 additional weeks)

1. Execute SDK `Pipeline` adapters with per-graph/per-node durable source state.
2. Add durable queued workers, run leases, heartbeat/recovery, and concurrency policies.
3. Implement cron/interval schedules and authenticated webhook triggers with UI.
4. Add freshness, cursor, lag, and reconciliation status to each stream.

Exit: PostgreSQL/files/Discord can run repeatedly without rereading everything or losing work on restart.

### Milestone 2 — connector platform (6–12 additional weeks)

1. Generate Studio nodes and connection forms from SDK connector manifests.
2. Add connector conformance tests for discovery, pagination, resume, rate limits,
   duplicates, deletes, schema changes, and secret leakage.
3. Ship high-value packs: filesystem/object storage, email/productivity,
   databases/warehouses, collaboration/messaging, and generic REST/webhook.
4. Add mapping, normalization, snapshot+delta, delete propagation, and backfill UX.

Exit: new connectors plug into both embedded SDK projects and Studio without duplicating runtime logic.

### Milestone 3 — enterprise operations (2–4 additional months)

Add tenant/project isolation, SSO and service accounts, audit trails, quotas, HA,
backups/PITR, metrics/tracing/alerts, policy controls, and upgrade/DR validation.

Exit: multiple teams can operate audited, recoverable syncs on a LAN or private network.

## Honest horizon

- A safe, useful personal LAN ingestion appliance: approximately **4–8 focused weeks**.
- A broad personal connector platform covering the common sources above: **3–5 months**.
- A credible enterprise multi-tenant sync product: **6–12+ months**.
- Literal “any source to any sync type” is an ecosystem, not a finite feature. The scalable
  target is a conformance-tested plugin platform where new connectors are cheap and safe.

## Highest-leverage next features

1. Generate a safe Studio adapter for `LocalDocumentsSource`, using owner-scoped
   uploads and LAN folder roots without exposing arbitrary server paths.
2. Add database and object-storage source/destination packs plus connector
   conformance tests shared by SDK and Studio.
3. Merge the reviewed durable-run worker, then persist SDK source state per
   graph/node only after downstream writes are durable.
