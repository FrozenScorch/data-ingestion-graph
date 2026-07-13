"""Validate the rendered Compose topology for the local/LAN Studio appliance."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def verify(config: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    services = config.get("services", {})
    expected = {
        "ingestion-postgres",
        "ingestion-postgres-credentials",
        "ingestion-redis",
        "ingestion-storage-init",
        "ingestion-migrate",
        "ingestion-api",
        "ingestion-frontend",
        "ingestion-proxy",
    }
    if set(services) != expected:
        errors.append(f"unexpected service set: {sorted(services)}")
        return errors

    published = {name for name, service in services.items() if service.get("ports")}
    if published != {"ingestion-proxy"}:
        errors.append(f"only ingestion-proxy may publish ports, got {sorted(published)}")
    if any(service.get("network_mode") == "host" for service in services.values()):
        errors.append("host networking is forbidden")

    networks = config.get("networks", {})
    data_network = networks.get("data") or next(
        (value for value in networks.values() if value.get("name", "").endswith("_data")),
        None,
    )
    if not data_network or data_network.get("internal") is not True:
        errors.append("the data network must be internal")

    api = services["ingestion-api"]
    api_environment = api.get("environment", {})
    if (
        api_environment.get("APP_ENV") != "production"
        or api_environment.get("APP_DEBUG") != "false"
    ):
        errors.append("the appliance API must run with production security validation")
    if "ingestion-postgres:5432" not in api_environment.get("DATABASE_URL", ""):
        errors.append("the API must use private Compose DNS for PostgreSQL")
    if ":ingestion_password@" in api_environment.get("DATABASE_URL", ""):
        errors.append("the API must not use the exact legacy PostgreSQL password")
    if (
        services["ingestion-postgres"].get("environment", {}).get("POSTGRES_PASSWORD")
        == "ingestion_password"
    ):
        errors.append("PostgreSQL must initialize with a generated password")
    if "ingestion-redis:6379" not in api_environment.get("REDIS_URL", ""):
        errors.append("the API must use private Compose DNS for Redis")
    migrate_dependency = api.get("depends_on", {}).get("ingestion-migrate", {})
    if migrate_dependency.get("condition") != "service_completed_successfully":
        errors.append("API startup must be gated on successful schema migration")
    storage_dependency = api.get("depends_on", {}).get("ingestion-storage-init", {})
    if storage_dependency.get("condition") != "service_completed_successfully":
        errors.append("API startup must be gated on successful storage initialization")

    migration = services["ingestion-migrate"]
    credential_dependency = migration.get("depends_on", {}).get(
        "ingestion-postgres-credentials", {}
    )
    if credential_dependency.get("condition") != "service_completed_successfully":
        errors.append("schema migration must follow PostgreSQL credential transition")

    frontend = services["ingestion-frontend"]
    if frontend.get("profiles"):
        errors.append("the visual frontend must start in the default Compose project")
    if frontend.get("environment", {}).get("API_HOST") != "http://ingestion-api:8040":
        errors.append("the private frontend proxy must address the API through Compose DNS")

    rendered = json.dumps(config, sort_keys=True)
    for insecure in (
        "redis_secret_change_me",
        "change-this-secret-in-production",
        '"ADMIN_PASSWORD":"admin123"',
    ):
        if insecure in rendered:
            errors.append(f"rendered Compose contains insecure default {insecure!r}")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path, help="JSON from `docker compose config --format json`")
    args = parser.parse_args()
    errors = verify(json.loads(args.config.read_text(encoding="utf-8")))
    if errors:
        raise SystemExit("Invalid LAN Compose configuration:\n- " + "\n- ".join(errors))
    print("LAN Compose configuration is valid")


if __name__ == "__main__":
    main()
