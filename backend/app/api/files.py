"""Authenticated browser-file management for ingestion graphs."""

from uuid import UUID

from app.config import settings
from app.db.session import get_session
from app.middleware.auth import get_current_user
from app.models.graph import Graph, GraphVersion
from app.services import upload_service
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/api/files", tags=["files"])


class FileResponse(BaseModel):
    id: UUID
    name: str
    size: int
    content_type: str
    created_at: str


class FileListResponse(BaseModel):
    files: list[FileResponse]
    total: int


@router.post("", response_model=list[FileResponse], status_code=status.HTTP_201_CREATED)
async def upload_files(
    files: list[UploadFile] = File(...),
    current_user: dict = Depends(get_current_user),
):
    if not files:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Select at least one file")
    if len(files) > settings.max_upload_files_per_request:
        raise HTTPException(
            status.HTTP_413_CONTENT_TOO_LARGE,
            f"At most {settings.max_upload_files_per_request} files may be uploaded at once",
        )
    saved = []
    try:
        for upload in files:
            saved.append(await upload_service.save_upload(current_user["user_id"], upload))
            if sum(item["size"] for item in saved) > settings.max_upload_request_mb * 1024 * 1024:
                raise HTTPException(
                    status.HTTP_413_CONTENT_TOO_LARGE,
                    f"Upload request exceeds {settings.max_upload_request_mb} MB",
                )
    except Exception:
        # Keep a multipart request atomic: a later invalid/oversized file must
        # not leave earlier files behind while the client receives only an error.
        for item in saved:
            upload_service.delete_upload(current_user["user_id"], item["id"])
        raise
    return saved


@router.get("", response_model=FileListResponse)
async def list_files(current_user: dict = Depends(get_current_user)):
    files = upload_service.list_uploads(current_user["user_id"])
    return {"files": files, "total": len(files)}


@router.delete("/{file_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_file(
    file_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    result = await db.execute(
        select(GraphVersion.node_configs)
        .join(Graph, Graph.id == GraphVersion.graph_id)
        .where(Graph.owner_id == current_user["user_id"])
    )
    for configs in result.scalars():
        if any(
            str(file_id) in (config.get("artifact_ids") or [])
            for config in (configs or {}).values()
            if isinstance(config, dict)
        ):
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "File is referenced by a saved graph version; remove it from the graph first",
            )
    if not upload_service.delete_upload(current_user["user_id"], file_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "File not found")
