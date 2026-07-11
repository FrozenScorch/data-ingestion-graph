"""Authenticated browser-file management for ingestion graphs."""

from uuid import UUID

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
    saved = []
    for upload in files:
        saved.append(await upload_service.save_upload(current_user["user_id"], upload))
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
        select(GraphVersion.graph_id, GraphVersion.version_number, GraphVersion.node_configs)
        .join(Graph, Graph.id == GraphVersion.graph_id)
        .where(Graph.owner_id == current_user["user_id"])
        .order_by(GraphVersion.graph_id, GraphVersion.version_number.desc())
    )
    latest_configs = {}
    for graph_id, _, configs in result.all():
        latest_configs.setdefault(graph_id, configs)
    for configs in latest_configs.values():
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
