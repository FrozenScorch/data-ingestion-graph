# Local and LAN Studio deployment

The default Compose project is a single-host appliance for a trusted workstation
or private LAN. Only Caddy publishes host ports. PostgreSQL and Redis use an
internal bridge network; the API and Svelte frontend are reachable only through
the same-origin edge proxy. Caddy routes REST, health, and authenticated WebSocket
traffic to the API and all other traffic to Studio.

## First start

Generate an ignored `.env` with independent random database, Redis, JWT,
connection-encryption, and initial-admin credentials. Use the hostname or IPv4
address that browsers will actually open:

```shell
python scripts/init_lan_env.py --host 192.168.1.50
docker compose up --build -d
docker compose ps
```

Open the printed URL, normally `http://192.168.1.50:8040`. Store the printed
admin password before continuing: it seeds the first account and later `.env`
changes do not replace that account's password.

The origin is exact by design. If the host name, IP address, or port changes,
regenerate `.env` with `--force` and restart the project. Reconfiguration preserves
all existing secrets and custom settings; it changes only the public endpoint/TLS
fields. Restrict the host firewall to your trusted LAN or set
`STUDIO_BIND=127.0.0.1` for local-only use.

## Outbound connector policy

Studio's HTTP Request node and saved PostgreSQL/Discord connection tests apply a
fail-closed outbound policy before creating a network client or socket. The default
`EGRESS_POLICY_MODE=public` accepts HTTP(S) and PostgreSQL targets only when every
resolved address is public. It rejects URL credentials, loopback, private,
link-local, multicast, unspecified, reserved, mixed public/private DNS answers,
and known cloud-metadata destinations. HTTP and PostgreSQL connections use the
validated address directly; HTTPS retains the original hostname for TLS SNI and
certificate validation. Redirects are disabled in the client and GET redirects
are followed only after same-origin revalidation, up to `EGRESS_MAX_REDIRECTS`.

Private LAN connectors require an explicit comma-separated exact-host or CIDR
allowlist in `.env`, for example:

```dotenv
EGRESS_ALLOWED_HOSTS=warehouse.home.arpa,nas.home.arpa
EGRESS_ALLOWED_CIDRS=10.40.0.0/24,fd12:3456:789a::/64
```

Use `EGRESS_POLICY_MODE=allowlist-only` when every outbound destination, including
public services, must be explicitly listed. Exact host entries are normalized for
IDNA and a trailing DNS dot. CIDRs may enable private, loopback, or link-local
addresses intentionally, but cloud-metadata addresses and mixed public/private DNS
answers remain blocked. Treat additions as privileged configuration and keep ranges
as narrow as possible.

The address pin covers these policy-integrated paths and removes the DNS lookup gap
between validation and connection. It cannot make an upstream HTTP proxy trustworthy;
the pinned client therefore ignores proxy environment variables. Other Studio-native
connectors must adopt the same service before they can claim this protection.

## Private LAN TLS

Caddy can issue a certificate from its private authority:

```shell
python scripts/init_lan_env.py --host ingestion.home.arpa --tls --force
docker compose up --build -d
docker compose cp ingestion-proxy:/data/caddy/pki/authorities/local/root.crt ./ingestion-caddy-root.crt
```

Resolve that name to the appliance through local DNS (or a hosts entry), install
`ingestion-caddy-root.crt` as a trusted root on each client device, and open the
printed `https://...:8443` URL. Protect the exported root certificate. Publicly
trusted internet TLS should terminate at an organization-managed reverse proxy;
this profile is intentionally for a private LAN.

## Schema and data lifecycle

`ingestion-migrate` must finish before the API can start. A completely empty database
is materialized to the current model and stamped at the Alembic head. An existing
unversioned Studio database is based before migration `0001` and runs the ordered,
legacy-safe migrations; versioned databases run ordinary `alembic upgrade head`.
Failure stops API startup instead of serving traffic against a partial schema.
Before schema migration, `ingestion-postgres-credentials` verifies the generated
database password and, only for an exact older Compose installation, transitions the
legacy `ingestion_password` role to that generated secret on the private data network.
After migration, `ingestion-connection-credentials` re-encrypts saved connection rows
that used a recognized public legacy encryption key. Unknown keys fail closed instead
of starting Studio with unreadable credentials.

The Compose file intentionally has no fixed project name, so an existing clone keeps
its directory-derived PostgreSQL and Redis volume identity across this upgrade.
On first start, `ingestion-storage-init` copies earlier `./data/uploads` and
`./data/temp` bind data into writable named volumes without overwriting existing volume
files. A durable marker prevents later restarts from resurrecting deleted legacy files,
and any import error blocks API startup. PostgreSQL, Redis, managed uploads, temporary
files, and Caddy authority state then persist in project-scoped named volumes.
Run `docker compose down` before changing the checkout directory or Compose project
name. Normal `docker compose down` preserves data. Do not use `down -v` unless permanent
volume deletion is intended. Keep the legacy `./data/uploads` directory until its
contents have been verified in Studio. Back up PostgreSQL and uploads before upgrades;
automated backup/restore and disaster-recovery validation remain enterprise gaps.

## Operational boundary

This is a trusted-LAN, single-host deployment—not an internet-facing multi-tenant
service. It provides production-mode secret validation, owner-scoped credentials,
private service networking, exact browser/WebSocket origin enforcement, security
headers, health checks, durable workers, owner-scoped schedules, and signed webhook
ingress with application-level replay/rate protection. It does not yet provide SSO, scoped
service accounts, HA workers, centralized audit/metrics, automated backups, or
edge rate limiting.
