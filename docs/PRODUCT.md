# Ingestion Graph Studio: product overview

## The problem

AI and data teams repeatedly rebuild the same ingestion plumbing inside each project: source authentication, pagination, checkpointing, retries, transforms, destinations, and recovery. The first demo works; the operational burden appears later, when a failed job must be explained or replayed without duplicating data.

Ingestion Graph Studio turns that repeated work into one visible, reusable boundary.

## Who it is for

- Platform and data engineers standardizing ingestion across teams.
- AI application teams that need trustworthy materialized inputs before retrieval or agents.
- Operators running private or regulated data workflows on infrastructure they control.
- Technical leaders evaluating whether a shared ingestion contract can replace project-specific loaders.

## Product promise

Design, run, inspect, recover, and query ingestion pipelines without hiding execution state or coupling every project to the Studio UI.

The intended outcomes are less duplicated connector work, faster recovery from failed runs, and a clearer handoff between source owners, pipeline builders, and downstream applications. These are product goals, not published customer metrics.

## Why it is different

- **SDK and Studio stay separate.** The reusable `ingestion-graph` SDK can be installed without the visual control plane.
- **Durability is part of the contract.** Checkpoints, leases, retries, and dead-letter recovery are designed in. Execution is at-least-once, so duplicate safety still depends on each destination's upsert, idempotency-key, or transactional behavior.
- **The run is inspectable.** Operators can see graph definitions, node state, events, credentials boundaries, and materialized outputs.
- **Private deployment is normal.** The appliance is designed for localhost, LAN, or WireGuard operation with explicit egress policy.

## The product experience

1. Choose a predefined pipeline or build a graph on the canvas.
2. Configure source, transform, and destination nodes through reusable contracts.
3. Validate the graph and start a durable run.
4. Inspect node progress, events, failures, and checkpoints.
5. Recover or replay with the destination-specific idempotency guarantees understood.
6. Query the materialized output from the same control plane.

## Proof available today

- Independent Python SDK and connector protocol.
- Visual Svelte Studio and FastAPI control plane.
- REST and PostgreSQL source paths.
- Replay, checkpoint, leased-worker, and egress-policy tests for the implemented paths.
- Documented trusted-LAN deployment and readiness roadmap.

## How to evaluate it

Start with the repository quickstart, then run one pipeline through success and one intentional failure. The useful proof is not a screenshot; it is whether the same run can be inspected and recovered, and whether the chosen destination's upsert, idempotency-key, or transaction semantics prevent duplicate side effects under at-least-once delivery.

### Evaluation questions

- Can a connector be reused outside Studio?
- Can an operator identify where a run stopped, and does the selected destination make replay idempotent?
- Are secrets, server paths, and outbound destinations kept inside explicit boundaries?
- Can downstream software query a stable materialized result?
- Are current connector coverage and readiness limits clear enough for the intended workload?

## Deployment and trust

The current posture is self-hosted and private-first. Use the [LAN deployment guide](lan-deployment.md) for secrets, TLS, migrations, network exposure, and egress controls. Review the [architecture](architecture.md) and [durable execution model](durable-execution.md) before treating Studio as a production control plane.

## Current limits

- Connector coverage is growing and is not comparable to a mature connector marketplace.
- Execution is at-least-once; replay is not universally duplicate-free, and durable-state coverage varies by connector path.
- Hosted multi-tenant operation is not the current product boundary.
- Studio API documentation is less complete than the SDK and architecture guides.
- Performance and recovery should be validated against the buyer's real data volumes.

## Roadmap and evidence

- [Readiness roadmap](readiness-roadmap.md)
- [Architecture](architecture.md)
- [Ingest and query workflow](ingest-and-query.md)
- [SDK documentation](../sdk/README.md)
- [Canonical repository](https://github.com/FrozenScorch/data-ingestion-graph)
