"""
Connection API routes: CRUD and test for saved database connections.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.middleware.auth import get_current_user
from app.services import connection_service
from app.services.connection_crypto import decrypt_connection_config
from app.schemas.graph import (
    ConnectionCreate,
    ConnectionUpdate,
    ConnectionResponse,
)


router = APIRouter(prefix="/api/connections", tags=["connections"])


class ConnectionTestRequest(BaseModel):
    """Request body for testing a connection with config."""

    config: dict = Field(..., description="Connection configuration to test")
    type: str = Field(..., description="Connection type (e.g., postgres)")


class ConnectionTestResponse(BaseModel):
    """Response for connection test."""

    success: bool
    message: str


class ConnectionListResponse(BaseModel):
    """Response for listing connections."""

    connections: list[ConnectionResponse]
    total: int


@router.post("", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(
    request: ConnectionCreate,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Create a new database connection."""
    try:
        connection = await connection_service.create_connection(
            db,
            user_id=current_user["user_id"],
            name=request.name,
            type=request.type,
            config=request.config,
        )
        return connection
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )


@router.get("", response_model=ConnectionListResponse)
async def list_connections(
    type: str | None = Query(None, description="Filter by connection type"),
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """List all connections for the current user, optionally filtered by type."""
    connections = await connection_service.get_connections(
        db,
        user_id=current_user["user_id"],
        type=type,
    )
    return ConnectionListResponse(
        connections=connections,
        total=len(connections),
    )


@router.get("/{connection_id}", response_model=ConnectionResponse)
async def get_connection(
    connection_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Get a connection by ID."""
    connection = await connection_service.get_connection(db, connection_id)
    if connection is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found",
        )
    if current_user["role"] != "admin" and str(connection.user_id) != str(current_user["user_id"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to access this connection",
        )
    return connection


@router.put("/{connection_id}", response_model=ConnectionResponse)
async def update_connection(
    connection_id: UUID,
    request: ConnectionUpdate,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Update a connection's name and/or config."""
    connection = await connection_service.update_connection(
        db,
        connection_id=connection_id,
        user_id=current_user["user_id"],
        name=request.name,
        config=request.config,
    )
    if connection is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found",
        )
    return connection


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Delete a connection."""
    deleted = await connection_service.delete_connection(
        db,
        connection_id=connection_id,
        user_id=current_user["user_id"],
    )
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found",
        )


@router.post("/{connection_id}/test", response_model=ConnectionTestResponse)
async def test_connection(
    connection_id: UUID,
    db: AsyncSession = Depends(get_session),
    current_user: dict = Depends(get_current_user),
):
    """Test an existing saved connection."""
    connection = await connection_service.get_connection(db, connection_id)
    if connection is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Connection not found",
        )
    if current_user["role"] != "admin" and str(connection.user_id) != str(current_user["user_id"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to access this connection",
        )

    result = await connection_service.test_connection(
        config=decrypt_connection_config(connection.config),
        type=connection.type,
    )

    # Update is_valid flag based on test result
    connection.is_valid = result["success"]
    await db.commit()

    return ConnectionTestResponse(**result)
