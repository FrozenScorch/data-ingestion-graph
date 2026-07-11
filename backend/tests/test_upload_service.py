from io import BytesIO
from uuid import uuid4

import pytest
from app.api.files import router
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
    assert list(upload_service.owner_root(owner).iterdir()) == []


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
    assert "Absolute" in (result.error_message or "")


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
