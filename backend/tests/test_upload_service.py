import asyncio
import multiprocessing
import os
import time
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from app.api.files import delete_file, router
from app.config import settings
from app.middleware.auth import get_current_user
from app.nodes.base import NodeContext
from app.nodes.file_source import FileSourceNode
from app.services import upload_service
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.testclient import TestClient


@pytest.fixture
def upload_root(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "upload_dir", str(tmp_path))
    return tmp_path


def make_upload(name: str, content: bytes) -> UploadFile:
    return UploadFile(filename=name, file=BytesIO(content))


def _hold_quota_lock(upload_dir: str, owner_id: str, ready) -> None:
    """Spawn-safe helper: hold the real OS lock until this process is terminated."""
    settings.upload_dir = upload_dir

    async def hold() -> None:
        async with upload_service._owner_quota_lock(UUID(owner_id)):
            ready.set()
            await asyncio.sleep(60)

    asyncio.run(hold())


@pytest.mark.asyncio
async def test_uploads_are_owner_scoped_and_resolvable(upload_root):
    owner_a, owner_b = uuid4(), uuid4()
    saved = await upload_service.save_upload(owner_a, make_upload("notes.txt", b"hello"))

    assert [item["id"] for item in upload_service.list_uploads(owner_a)] == [saved["id"]]
    assert upload_service.list_uploads(owner_b) == []
    assert upload_service.resolve_uploads(owner_a, [saved["id"]])[0].read_bytes() == b"hello"
    with pytest.raises(ValueError, match="does not belong"):
        upload_service.resolve_uploads(owner_b, [saved["id"]])
    assert upload_service.delete_upload(owner_b, saved["id"]) is False


@pytest.mark.asyncio
async def test_invalid_and_oversized_uploads_leave_no_files(upload_root, monkeypatch):
    owner = uuid4()
    with pytest.raises(HTTPException) as invalid:
        await upload_service.save_upload(owner, make_upload("../secret.txt", b"no"))
    assert invalid.value.status_code == 400

    monkeypatch.setattr(settings, "max_upload_size_mb", 0)
    with pytest.raises(HTTPException) as oversized:
        await upload_service.save_upload(owner, make_upload("large.txt", b"x"))
    assert oversized.value.status_code == 413
    assert upload_service.list_uploads(owner) == []
    remaining = list(upload_service.owner_root(owner).iterdir())
    assert {path.name for path in remaining} <= {".quota.lock"}
    assert not any(path.is_dir() or path.name.startswith(".staging-") for path in remaining)


@pytest.mark.asyncio
async def test_file_source_reads_only_selected_owner_uploads(upload_root):
    owner = uuid4()
    first = await upload_service.save_upload(owner, make_upload("first.txt", b"one"))
    await upload_service.save_upload(owner, make_upload("second.txt", b"two"))
    context = NodeContext(
        run_id="run",
        node_id="files",
        config={"source_type": "upload", "artifact_ids": [first["id"]]},
        state={"owner_id": str(owner)},
        working_dir=str(upload_root),
    )

    result = await FileSourceNode().execute(context)
    assert result.success is True
    assert [item["name"] for item in result.output_data["file_list"]] == ["first.txt"]


@pytest.mark.asyncio
async def test_empty_selection_never_expands_to_current_or_future_uploads(upload_root):
    owner = uuid4()
    await upload_service.save_upload(owner, make_upload("first.txt", b"one"))
    context = NodeContext(
        run_id="run",
        node_id="files",
        config={"source_type": "upload", "artifact_ids": []},
        state={"owner_id": str(owner)},
        working_dir=str(upload_root),
    )
    assert (await FileSourceNode().execute(context)).output_data["file_list"] == []
    await upload_service.save_upload(owner, make_upload("future.txt", b"future"))
    assert (await FileSourceNode().execute(context)).output_data["file_list"] == []


@pytest.mark.asyncio
async def test_file_source_rejects_server_paths(tmp_path):
    file_path = tmp_path / "secret.txt"
    file_path.write_text("secret", encoding="utf-8")
    context = NodeContext(
        run_id="run",
        node_id="files",
        config={"source_type": "path", "file_path": str(file_path)},
        working_dir=str(tmp_path),
    )
    result = await FileSourceNode().execute(context)
    assert result.success is False
    assert "managed uploads only" in (result.error_message or "")


def test_multipart_api_upload_and_list(upload_root):
    owner = uuid4()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_current_user] = lambda: {
        "user_id": owner,
        "username": "owner",
        "role": "user",
    }

    with TestClient(app) as client:
        response = client.post(
            "/api/files",
            files=[("files", ("browser.txt", b"from browser", "text/plain"))],
        )
        assert response.status_code == 201
        uploaded = response.json()[0]
        listing = client.get("/api/files")
        assert listing.status_code == 200
        assert listing.json()["files"][0]["id"] == uploaded["id"]


def test_multipart_api_rolls_back_earlier_files_on_error(upload_root):
    owner = uuid4()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_current_user] = lambda: {
        "user_id": owner,
        "username": "owner",
        "role": "user",
    }

    with TestClient(app) as client:
        response = client.post(
            "/api/files",
            files=[
                ("files", ("valid.txt", b"valid", "text/plain")),
                ("files", ("blocked.exe", b"blocked", "application/octet-stream")),
            ],
        )
        assert response.status_code == 415
        assert client.get("/api/files").json()["files"] == []


def test_multipart_api_enforces_request_file_count(upload_root, monkeypatch):
    monkeypatch.setattr(settings, "max_upload_files_per_request", 1)
    owner = uuid4()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_current_user] = lambda: {
        "user_id": owner,
        "username": "owner",
        "role": "user",
    }
    with TestClient(app) as client:
        response = client.post(
            "/api/files",
            files=[
                ("files", ("one.txt", b"1", "text/plain")),
                ("files", ("two.txt", b"2", "text/plain")),
            ],
        )
        assert response.status_code == 413
        assert client.get("/api/files").json()["files"] == []


def test_multipart_api_enforces_aggregate_request_size(upload_root, monkeypatch):
    monkeypatch.setattr(settings, "max_upload_request_mb", 1)
    owner = uuid4()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_current_user] = lambda: {
        "user_id": owner,
        "username": "owner",
        "role": "user",
    }
    with TestClient(app) as client:
        response = client.post(
            "/api/files",
            files=[
                ("files", ("one.txt", b"1" * 600_000, "text/plain")),
                ("files", ("two.txt", b"2" * 600_000, "text/plain")),
            ],
        )
        assert response.status_code == 413
        assert client.get("/api/files").json()["files"] == []


def test_symlinked_owner_root_is_rejected(upload_root):
    owner = uuid4()
    target = upload_root / "target"
    target.mkdir()
    link = upload_root / str(owner)
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("Symlink creation is unavailable on this platform")
    with pytest.raises(RuntimeError, match="symlink"):
        upload_service.owner_root(owner)


@pytest.mark.asyncio
async def test_owner_storage_quota_is_enforced(upload_root, monkeypatch):
    monkeypatch.setattr(settings, "max_upload_storage_mb", 1)
    owner = uuid4()
    await upload_service.save_upload(owner, make_upload("full.txt", b"x" * (1024 * 1024)))
    with pytest.raises(HTTPException) as exc:
        await upload_service.save_upload(owner, make_upload("extra.txt", b"x"))
    assert exc.value.status_code == 413
    assert [item["name"] for item in upload_service.list_uploads(owner)] == ["full.txt"]


@pytest.mark.asyncio
async def test_concurrent_uploads_cannot_race_owner_quota(upload_root, monkeypatch):
    monkeypatch.setattr(settings, "max_upload_storage_mb", 1)
    owner = uuid4()
    results = await asyncio.gather(
        upload_service.save_upload(owner, make_upload("one.txt", b"1" * 700_000)),
        upload_service.save_upload(owner, make_upload("two.txt", b"2" * 700_000)),
        return_exceptions=True,
    )
    assert sum(isinstance(item, dict) for item in results) == 1
    errors = [item for item in results if isinstance(item, HTTPException)]
    assert len(errors) == 1 and errors[0].status_code == 413
    assert sum(item["size"] for item in upload_service.list_uploads(owner)) == 700_000


@pytest.mark.asyncio
async def test_quota_lock_blocks_other_process_and_recovers_after_crash(upload_root, monkeypatch):
    owner = uuid4()
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    process = context.Process(
        target=_hold_quota_lock,
        args=(str(upload_root), str(owner), ready),
    )
    process.start()
    try:
        # Windows spawn re-imports the full application in the child and can
        # exceed ten seconds on Defender/OneDrive-backed workspaces.
        startup_timeout = 30 if os.name == "nt" else 10
        assert ready.wait(startup_timeout), "child process did not acquire quota lock"
        monkeypatch.setattr(upload_service, "QUOTA_LOCK_WAIT_SECONDS", 0.2)
        with pytest.raises(HTTPException) as blocked:
            async with upload_service._owner_quota_lock(owner):
                pass
        assert blocked.value.status_code == 503
    finally:
        process.terminate()
        process.join(10)
    assert not process.is_alive()

    # Kernel releases the advisory lock when the holder dies; no stale-lock
    # deletion or lease takeover is needed.
    async with upload_service._owner_quota_lock(owner):
        pass


@pytest.mark.asyncio
async def test_delete_preserves_files_referenced_by_historical_version(upload_root):
    owner = uuid4()
    saved = await upload_service.save_upload(owner, make_upload("history.txt", b"history"))
    result = MagicMock()
    result.scalars.return_value = [
        {"files": {"artifact_ids": [saved["id"]]}},
        {"files": {"artifact_ids": []}},
    ]
    db = AsyncMock()
    db.execute = AsyncMock(return_value=result)
    with pytest.raises(HTTPException) as exc:
        await delete_file(saved["id"], db=db, current_user={"user_id": owner})
    assert exc.value.status_code == 409
    assert upload_service.list_uploads(owner)[0]["id"] == saved["id"]


def test_symlinked_artifact_directory_is_never_listed(upload_root):
    owner = uuid4()
    artifact_id = uuid4()
    owner_dir = upload_service.owner_root(owner)
    target = upload_root / "external-artifact"
    target.mkdir()
    link = owner_dir / str(artifact_id)
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("Symlink creation is unavailable on this platform")
    assert upload_service.list_uploads(owner) == []
    assert upload_service.delete_upload(owner, artifact_id) is False


@pytest.mark.asyncio
async def test_symlinked_payload_is_never_listed_or_deleted(upload_root):
    owner = uuid4()
    saved = await upload_service.save_upload(owner, make_upload("payload.txt", b"safe"))
    directory = upload_service.owner_root(owner) / saved["id"]
    payload = directory / "payload.txt"
    outside = upload_root / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    payload.unlink()
    try:
        payload.symlink_to(outside)
    except OSError:
        pytest.skip("Symlink creation is unavailable on this platform")
    assert upload_service.list_uploads(owner) == []
    assert upload_service.delete_upload(owner, saved["id"]) is False
    assert outside.read_text(encoding="utf-8") == "outside"


def test_stale_staging_directories_are_cleaned(upload_root):
    owner = uuid4()
    staging = upload_service.owner_root(owner) / f".staging-{uuid4()}"
    staging.mkdir()
    (staging / "partial.txt").write_text("partial", encoding="utf-8")
    old = time.time() - upload_service.STAGING_MAX_AGE_SECONDS - 10
    os.utime(staging, (old, old))
    assert upload_service.list_uploads(owner) == []
    assert not staging.exists()
