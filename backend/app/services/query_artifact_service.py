"""Lifecycle policy for per-run SDK query artifacts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from app.config import settings


def query_artifact_path(run_id: str, *, base_dir: str | Path | None = None) -> Path:
    return Path(base_dir or settings.temp_dir) / "query" / f"{run_id}.db"


def artifact_expires_at(path: Path) -> datetime:
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    return modified + timedelta(hours=settings.query_artifact_ttl_hours)


def is_query_artifact_expired(path: Path, *, now: datetime | None = None) -> bool:
    if not path.is_file():
        return False
    current = now or datetime.now(UTC)
    return artifact_expires_at(path) <= current


def delete_query_artifact(path: Path) -> None:
    """Delete a SQLite database and any journal sidecars."""
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        candidate.unlink(missing_ok=True)


def prune_query_artifacts(
    *, now: datetime | None = None, base_dir: str | Path | None = None
) -> int:
    query_dir = Path(base_dir or settings.temp_dir) / "query"
    if not query_dir.is_dir():
        return 0
    removed = 0
    for path in query_dir.glob("*.db"):
        if is_query_artifact_expired(path, now=now):
            try:
                delete_query_artifact(path)
            except OSError:
                continue
            else:
                removed += 1
    return removed
