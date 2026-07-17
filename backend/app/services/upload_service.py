"""Owner-scoped storage for Studio-managed ingestion files."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from app.config import settings
from fastapi import HTTPException, UploadFile, status

ALLOWED_EXTENSIONS = {
    ".csv",
    ".doc",
    ".docx",
    ".eml",
    ".htm",
    ".html",
    ".json",
    ".md",
    ".pdf",
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".tif",
    ".tiff",
    ".txt",
    ".xml",
    ".xlsx",
}
CHUNK_SIZE = 1024 * 1024
STAGING_MAX_AGE_SECONDS = 60 * 60
QUOTA_LOCK_WAIT_SECONDS = 30


def _storage_root() -> Path:
    root = Path(settings.upload_dir).absolute()
    current = Path(root.anchor)
    for part in root.parts[1:]:
        current /= part
        if current.exists() and current.is_symlink():
            raise RuntimeError("Upload storage must not contain symlinked directories")
    root.mkdir(parents=True, exist_ok=True)
    return root


def owner_root(owner_id: UUID) -> Path:
    root = _storage_root()
    child = root / str(UUID(str(owner_id)))
    child.relative_to(root)
    if child.exists() and child.is_symlink():
        raise RuntimeError("Owner upload directory must not be a symlink")
    child.mkdir(parents=True, exist_ok=True, mode=0o700)
    return child


def _validate_filename(filename: str | None) -> str:
    if not filename or len(filename) > 200:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid filename")
    if filename in {".", ".."} or any(ord(char) < 32 for char in filename):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid filename")
    if "/" in filename or "\\" in filename or Path(filename).is_absolute():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Filename must not contain a path")
    if Path(filename).suffix.lower() not in ALLOWED_EXTENSIONS:
        allowed = ", ".join(sorted(ALLOWED_EXTENSIONS))
        raise HTTPException(
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, f"Unsupported file type. Allowed: {allowed}"
        )
    return filename


def _artifact_dir(owner_id: UUID, artifact_id: UUID) -> Path:
    root = owner_root(owner_id)
    path = root / str(UUID(str(artifact_id)))
    path.relative_to(root)
    return path


@asynccontextmanager
async def _owner_quota_lock(owner_id: UUID):
    """Serialize quota accounting with a crash-safe OS advisory lock."""
    from filelock import FileLock, Timeout

    lock_path = owner_root(owner_id) / ".quota.lock"
    if lock_path.is_symlink():
        raise RuntimeError("Upload quota lock must not be a symlink")
    # Acquisition/release run in worker threads to avoid blocking the event loop;
    # shared context lets either worker release the same OS lock safely.
    lock = FileLock(lock_path, thread_local=False)
    try:
        await asyncio.to_thread(lock.acquire, timeout=QUOTA_LOCK_WAIT_SECONDS)
    except Timeout:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Upload storage is busy; retry shortly",
        ) from None
    try:
        yield
    finally:
        await asyncio.to_thread(lock.release)


async def save_upload(owner_id: UUID, upload: UploadFile) -> dict:
    name = _validate_filename(upload.filename)
    artifact_id = uuid4()
    owner = owner_root(owner_id)
    staging = owner / f".staging-{artifact_id}"
    directory = _artifact_dir(owner_id, artifact_id)
    max_bytes = settings.max_upload_size_mb * 1024 * 1024
    owner_limit = settings.max_upload_storage_mb * 1024 * 1024
    try:
        async with _owner_quota_lock(owner_id):
            staging.mkdir(mode=0o700)
            file_path = staging / name
            existing_bytes = sum(item["size"] for item in list_uploads(owner_id))
            size = 0
            with file_path.open("xb") as target:
                while chunk := await upload.read(CHUNK_SIZE):
                    size += len(chunk)
                    if size > max_bytes:
                        raise HTTPException(
                            status.HTTP_413_CONTENT_TOO_LARGE,
                            f"File exceeds the {settings.max_upload_size_mb} MB limit",
                        )
                    if existing_bytes + size > owner_limit:
                        raise HTTPException(
                            status.HTTP_413_CONTENT_TOO_LARGE,
                            f"Owner storage exceeds the {settings.max_upload_storage_mb} MB quota",
                        )
                    target.write(chunk)
            created_at = datetime.now(UTC).isoformat()
            metadata = {
                "id": str(artifact_id),
                "name": name,
                "size": size,
                "content_type": upload.content_type or "application/octet-stream",
                "created_at": created_at,
            }
            (staging / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
            os.replace(staging, directory)
            return metadata
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        await upload.close()


def _read_artifact(owner_id: UUID, artifact_id: UUID) -> tuple[dict, Path] | None:
    directory = _artifact_dir(owner_id, artifact_id)
    metadata_path = directory / "metadata.json"
    if (
        not directory.is_dir()
        or directory.is_symlink()
        or not metadata_path.is_file()
        or metadata_path.is_symlink()
    ):
        return None
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if metadata.get("id") != str(artifact_id):
            return None
        name = _validate_filename(metadata.get("name"))
        file_path = directory / name
        file_path.relative_to(directory)
        if not file_path.is_file() or file_path.is_symlink():
            return None
        metadata["size"] = file_path.stat().st_size
        return metadata, file_path
    except (OSError, ValueError, json.JSONDecodeError, HTTPException):
        return None


def list_uploads(owner_id: UUID) -> list[dict]:
    result = []
    owner = owner_root(owner_id)
    for directory in owner.iterdir():
        if directory.name.startswith(".staging-"):
            try:
                if (
                    directory.is_dir()
                    and not directory.is_symlink()
                    and time.time() - directory.stat().st_mtime > STAGING_MAX_AGE_SECONDS
                ):
                    shutil.rmtree(directory)
            except OSError:
                pass
            continue
        try:
            artifact_id = UUID(directory.name)
        except ValueError:
            continue
        item = _read_artifact(owner_id, artifact_id)
        if item:
            result.append(item[0])
    return sorted(result, key=lambda item: item["created_at"], reverse=True)


def resolve_uploads(owner_id: UUID, artifact_ids: list[str]) -> list[Path]:
    try:
        ids = [UUID(value) for value in artifact_ids]
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid upload reference") from exc
    paths = []
    for artifact_id in ids:
        item = _read_artifact(owner_id, artifact_id)
        if item is None:
            raise ValueError("Upload not found or does not belong to graph owner")
        paths.append(item[1])
    return paths


def upload_reconciliation_path(owner_id: UUID, artifact_id: UUID) -> Path:
    """Return a guaranteed-missing path inside an owner-scoped artifact directory."""
    directory = _artifact_dir(owner_id, artifact_id)
    path = directory / ".studio-reconciliation" / "missing.txt"
    path.relative_to(owner_root(owner_id))
    if path.exists():
        raise RuntimeError("Managed upload reconciliation path is unexpectedly occupied")
    return path


def delete_upload(owner_id: UUID, artifact_id: UUID) -> bool:
    item = _read_artifact(owner_id, artifact_id)
    if item is None:
        return False
    directory = _artifact_dir(owner_id, artifact_id)
    shutil.rmtree(directory)
    return True
