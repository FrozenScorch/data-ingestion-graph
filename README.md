# Enterprise Data Ingestion Graph Studio

Enterprise Data Ingestion Graph Studio is the visual product for designing,
testing, running, and querying enterprise ingestion pipelines. The FastAPI
backend and Svelte canvas are a control plane; they explicitly install and
import the independently reusable [`ingestion-graph` SDK](sdk/README.md).

## Repository boundary

```text
sdk/       independently buildable Python SDK and connector protocol
backend/   Studio API, graph execution, credentials, and SDK node adapters
frontend/  visual graph builder, predefined pipelines, run inspection, query UI
```

Dependency direction is one-way:

```text
Enterprise Studio -> ingestion_graph SDK
ingestion_graph SDK -X-> Enterprise Studio
```

Use the SDK from another project without installing Studio:

```shell
python -m pip install "ingestion-graph[discord] @ git+https://github.com/FrozenScorch/data-ingestion-graph.git@44d7a11df3152ab54dbf7040e4654254c1ea1723#subdirectory=sdk"
```

SDK-backed nodes are marked **SDK** in the node palette. Studio-native nodes can
be migrated behind SDK sources, transforms, and destinations without changing
saved graph definitions.

## Start Studio locally or on a LAN

Generate production-mode secrets for the exact hostname or IPv4 address that
browsers will open, then start the complete visual appliance. Docker is built
from the repository root because the backend installs `./sdk` as a normal package
dependency.

```shell
python scripts/init_lan_env.py --host localhost
docker compose up --build -d
```

The command prints the Studio URL and one-time initial admin password. Only the
Caddy edge port is published; PostgreSQL, Redis, the API, and the frontend stay
on private Compose networks. Add `--tls` for Caddy's private LAN CA and follow
the trust instructions in [docs/lan-deployment.md](docs/lan-deployment.md).

Open Studio, upload private document inputs from **Files**, create a graph from a
predefined pipeline or the blank canvas, configure saved connections, run it,
and inspect/query its outputs. File Source nodes store opaque file IDs only;
server paths stay owner-scoped and server-controlled.

## Develop Studio

Run these commands from the repository root so `./sdk` resolves correctly:

```shell
python -m pip install -r backend/requirements.txt
python -m pytest backend/tests -q

cd frontend
npm ci
npm run check
npm run build
```

SDK development and publishing instructions live in [sdk/README.md](sdk/README.md).
The boundaries are detailed in [docs/architecture.md](docs/architecture.md) and
[docs/ingest-and-query.md](docs/ingest-and-query.md). Current product readiness,
gaps, and the path to local/LAN synchronization are tracked in
[docs/readiness-roadmap.md](docs/readiness-roadmap.md). Studio's leased worker,
recovery behavior, and connector idempotency requirements are documented in
[docs/durable-execution.md](docs/durable-execution.md).
