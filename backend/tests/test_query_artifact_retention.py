"""Query artifact retention and sidecar cleanup tests."""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import app.services.query_artifact_service as artifact_service
from app.config import settings
from app.services.query_artifact_service import (
    artifact_expires_at,
    is_query_artifact_expired,
    prune_query_artifacts,
    query_artifact_path,
)


def _set_modified(path: Path, value: datetime) -> None:
    timestamp = value.timestamp()
    os.utime(path, (timestamp, timestamp))


def test_prune_removes_expired_database_and_sidecars(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "temp_dir", str(tmp_path))
    monkeypatch.setattr(settings, "query_artifact_ttl_hours", 24)
    now = datetime.now(UTC)
    expired = query_artifact_path("expired")
    expired.parent.mkdir(parents=True)
    paths = (expired, Path(f"{expired}-wal"), Path(f"{expired}-shm"))
    for path in paths:
        path.write_bytes(b"data")
        _set_modified(path, now - timedelta(hours=25))
    fresh = query_artifact_path("fresh")
    fresh.write_bytes(b"data")

    assert prune_query_artifacts(now=now) == 1
    assert all(not path.exists() for path in paths)
    assert fresh.exists()


def test_expiry_metadata_uses_configured_ttl(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "query_artifact_ttl_hours", 2)
    path = tmp_path / "run.db"
    path.write_bytes(b"data")
    modified = datetime.now(UTC) - timedelta(hours=1)
    _set_modified(path, modified)

    assert not is_query_artifact_expired(path, now=modified + timedelta(minutes=119))
    assert is_query_artifact_expired(path, now=modified + timedelta(hours=2))
    assert artifact_expires_at(path) == modified + timedelta(hours=2)


def test_prune_skips_locked_artifact_without_failing(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "temp_dir", str(tmp_path))
    monkeypatch.setattr(settings, "query_artifact_ttl_hours", 1)
    path = query_artifact_path("locked")
    path.parent.mkdir(parents=True)
    path.write_bytes(b"data")
    now = datetime.now(UTC)
    _set_modified(path, now - timedelta(hours=2))

    def locked(_: Path) -> None:
        raise PermissionError("file is in use")

    monkeypatch.setattr(artifact_service, "delete_query_artifact", locked)
    assert prune_query_artifacts(now=now) == 0
    assert path.exists()
