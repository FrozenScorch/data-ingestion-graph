"""Owner-scoped storage for Studio-managed ingestion files."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from app.config import settings
from fastapi import HTTPException, UploadFile, status

ALLOWED_EXTENSIONS = {
    ".csv",
    ".doc",
    ".docx",
    ".htm",
    ".html",
    ".json",
    ".md",
    ".pdf",
    ".txt",
    ".xml",
}
CHUNK_SIZE = 1024 * 1024


def _storage_root() -> Path:
    root = Path(settings.upload_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def owner_root(owner_id: UUID) -> Path:
    root = _storage_root()
    child = (root / str(UUID(str(owner_id)))).resolve()
    child.relative_to(root)
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
    path = (root / str(UUID(str(artifact_id)))).resolve()
    path.relative_to(root)
    return path


async def save_upload(owner_id: UUID, upload: UploadFile) -> dict:
    name = _validate_filename(upload.filename)
    artifact_id = uuid4()
    directory = _artifact_dir(owner_id, artifact_id)
    directory.mkdir(mode=0o700)
    file_path = directory / name
    max_bytes = settings.max_upload_size_mb * 1024 * 1024
    size = 0
    try:
        with file_path.open("xb") as target:
            while chunk := await upload.read(CHUNK_SIZE):
                size += len(chunk)
                if size > max_bytes:
                    raise HTTPException(
                        status.HTTP_413_CONTENT_TOO_LARGE,
                        f"File exceeds the {settings.max_upload_size_mb} MB limit",
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
        (directory / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
        return metadata
    except Exception:
        shutil.rmtree(directory, ignore_errors=True)
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
        file_path = (directory / name).resolve()
        file_path.relative_to(directory.resolve())
        if not file_path.is_file() or file_path.is_symlink():
            return None
        metadata["size"] = file_path.stat().st_size
        return metadata, file_path
    except (OSError, ValueError, json.JSONDecodeError, HTTPException):
        return None


def list_uploads(owner_id: UUID) -> list[dict]:
    result = []
    for directory in owner_root(owner_id).iterdir():
        try:
            artifact_id = UUID(directory.name)
        except ValueError:
            continue
        item = _read_artifact(owner_id, artifact_id)
        if item:
            result.append(item[0])
    return sorted(result, key=lambda item: item["created_at"], reverse=True)


def resolve_uploads(owner_id: UUID, artifact_ids: list[str] | None = None) -> list[Path]:
    if artifact_ids:
        try:
            ids = [UUID(value) for value in artifact_ids]
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid upload reference") from exc
    else:
        ids = [UUID(item["id"]) for item in list_uploads(owner_id)]
    paths = []
    for artifact_id in ids:
        item = _read_artifact(owner_id, artifact_id)
        if item is None:
            raise ValueError("Upload not found or does not belong to graph owner")
        paths.append(item[1])
    return paths


def delete_upload(owner_id: UUID, artifact_id: UUID) -> bool:
    item = _read_artifact(owner_id, artifact_id)
    if item is None:
        return False
    directory = _artifact_dir(owner_id, artifact_id)
    shutil.rmtree(directory)
    return True
