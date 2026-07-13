# Studio and SDK readiness roadmap

Baseline: `main` after PR #40 plus this trigger milestone, audited 2026-07-13.

## Executive assessment

The SDK is usable today from unrelated Python projects. The Studio is now a useful
single-host local/LAN visual ingestion appliance, but it is not yet a hardened
multi-user service or a continuous anything-to-anything synchronization platform.

| Outcome | Readiness | Current reality |
| --- | ---: | --- |
| Reusable Python SDK | 82% | Installable, typed, resumable core with constructor-free manifests, three real source families, and two local destinations |
| Local single-user visual ingestion | 78% | Visual manual and recurring graphs, durable workers, transforms, PostgreSQL, server files, Discord preview, query inspection, and one-command deployment |
| Trusted-LAN Studio | 60% | Private networking, generated secrets, TLS, exact origins, migrations, durable execution, schedules, and signed webhooks exist; service auth, edge rate limits, and backups remain |
| Enterprise multi-user Studio | 15% | Tenant isolation, service auth, SSO, HA, backups, and observability are release gates |
| Anywhere-to-anywhere continuous sync | 32% | The SDK protocol, downstream acknowledgement, durable execution, and recurring triggers are credible, but connector breadth and sync modes remain narrow |

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
- Version-pinned interval/cron schedules and replay-protected signed webhook triggers.
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
- GitHub Source is a stub.
- The generic HTTP node does not yet stream upstream records as a destination.
- Constructor-free SDK manifests and strict Studio schema projections are implemented
  for built-ins, with Discord as the first manifest-backed node. Generic executable
  node generation and Connection Center form projection remain.
- Destination delete propagation, reconciliation, schema drift, and source-key upserts are inconsistent.

### SDK release engineering

- The working external install is a pinned GitHub VCS dependency; no PyPI package,
  git tag, GitHub Release, changelog, or release workflow exists yet.
- Package metadata declares proprietary licensing, but the repository does not contain
  explicit license terms. A deliberate licensing decision is required before third-party distribution.
- Installed-wheel typing tests cover the core consumer path and public conformance
  reporting, but not complete custom connectors, secret providers, or every public submodule.
- The transform plugin contract and ordered, checkpoint-safe transform chain are implemented;
  plugin conformance and versioned transform-state migration remain.

### Sync semantics

- Studio's SDK Document Source resumes and reconciles per graph/node, but Discord
  and native sources do not yet share the same durable state bridge.
- Document state is staged durably with source POST_EXEC output and promoted only
  when every downstream node succeeds. Failed-node retry reuses that run-scoped
  candidate, and concurrent stale runs cannot regress the committed checkpoint.
  Cancellation deletes terminal candidates. New full runs atomically supersede
  inactive failures, while queued/leased retries and paused runs retain candidates.
  This boundary depends on destinations returning success only after their own
  durable write/flush; the starter query collection remains a per-run delta view.
- Owner-scoped interval/cron schedules and signed webhook triggers now pin saved
  graph versions and dispatch through the durable Run/RunJob transaction. The
  webhook ledger provides bounded replay and per-trigger rate protection.
- Durable PostgreSQL jobs, leases, heartbeat, and expired-job recovery are implemented.
  The worker still runs inside the API deployment and lacks an independently scaled
  worker profile and per-stream concurrency policy.
- Missing modes: snapshot-to-incremental handoff, CDC, streaming,
  bidirectional conflict resolution, partitioned backfill, and reconciliation.

### UX and LAN readiness

- Folder selection/watch, connector discovery/preview, schema mapping, and
  richer run freshness/reconciliation views still need first-class UX. Basic
  schedule/webhook management is available from the graph toolbar.
- Complete owner checks are required across every execution, WebSocket, DLQ, and lineage path.
- The single-host LAN appliance now generates non-default secrets, uses private service
  networks, offers HTTP or private-CA TLS, enforces exact HTTP/WebSocket origins, adds
  security headers, and gates API startup on schema migration. Edge rate limits remain.
- File access must be confined to server-owned roots; outbound HTTP needs an SSRF policy.
- Deeper dependency health, backup/restore, structured metrics/tracing, retention,
  quotas, and disaster-recovery tests are incomplete.
- Enterprise use additionally needs organizations/projects, scoped API keys or service
  accounts, SSO/OIDC, audit logs, RBAC/ACLs, HA workers, and shared event delivery.

## Distance and delivery sequence

Estimates are for focused implementation by one strong engineer/agent workflow and
assume connectors can use stable upstream APIs. They are planning ranges, not promises.

### Milestone 0 — safe local/LAN foundation (1–3 weeks)

1. Done: owner authorization coverage for runs, controls, WebSockets, DLQ, and lineage.
2. Done: browser upload, server-owned owner isolation, file picker, and documents template.
3. Done: one-command Compose appliance with generated secrets, private networks,
   reverse proxy/private TLS, WebSockets, exact LAN origins, health, and migration gate.
4. Lock down outbound HTTP and add a scoped agent/service authentication path.

Exit: a trusted user can safely upload documents and run graphs from another LAN device.

### Milestone 1 — real recurring sync (3–6 additional weeks)

1. Done for managed documents: per-graph/per-node PostgreSQL state advances only
   after full graph success, with durable same-run retry candidates. Extend the
   bridge to other sources and audit every destination's durable-success contract.
2. Done in-process: durable queued workers, run leases, heartbeat, and recovery.
   Add per-stream concurrency policies and a separately deployed worker profile.
3. Done: cron/interval schedules and HMAC-authenticated webhook triggers with UI.
4. Add freshness, cursor, lag, and reconciliation status to each stream.

Exit: PostgreSQL/files/Discord can run repeatedly without rereading everything or losing work on restart.

### Milestone 2 — connector platform (6–12 additional weeks)

1. Extend manifest-backed Studio node and connection-form generation to every connector.
2. Done for portable SDK invariants: the reusable conformance kit checks manifests,
   source identity/checkpoints/capabilities, destination flush/replay/delete cases,
   and explicit secret representations. Connector packs must add injected pagination,
   resume, rate-limit, permission, and schema-change scenarios.
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

1. Generate Studio nodes and Connection Center forms from every SDK connector
   manifest, keeping execution in the reusable SDK.
2. Add database and object-storage source/destination packs and require the shared
   conformance kit plus connector-specific injected failure scenarios.
3. Add service accounts/scoped API keys, outbound HTTP policy, and freshness,
   cursor, lag, and reconciliation views.
