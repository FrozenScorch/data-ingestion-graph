"""
WebSocket manager for execution run progress events.
"""

import json
import logging
from contextlib import suppress
from typing import Any
from uuid import UUID

from app.db.session import AsyncSessionLocal
from app.models.execution import Run
from app.models.graph import Graph
from app.services.auth_service import decode_token, get_user_by_id
from fastapi import WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def authorize_run_subscription(db: AsyncSession, *, run_id: str, user_id: UUID) -> bool:
    """Return whether an active user may observe a run's live output."""
    try:
        parsed_run_id = UUID(run_id)
    except ValueError:
        return False
    user = await get_user_by_id(db, user_id)
    if user is None or not user.is_active:
        return False
    if user.role == "admin":
        return True
    result = await db.execute(
        select(Graph.owner_id).join(Run, Run.graph_id == Graph.id).where(Run.id == parsed_run_id)
    )
    return result.scalar_one_or_none() == user_id


class ExecutionWebSocketManager:
    """
    Manages WebSocket connections for live run progress events.
    """

    def __init__(self):
        # run_id -> list of WebSocket connections
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, run_id: str):
        """Accept a WebSocket connection and register it for a run."""
        await websocket.accept()
        run_id_str = str(run_id)
        if run_id_str not in self.active_connections:
            self.active_connections[run_id_str] = []
        self.active_connections[run_id_str].append(websocket)
        logger.info(
            f"WebSocket connected for run {run_id_str}. "
            f"Total connections: {len(self.active_connections[run_id_str])}"
        )

    def disconnect(self, websocket: WebSocket, run_id: str):
        """Remove a WebSocket connection."""
        run_id_str = str(run_id)
        if run_id_str in self.active_connections:
            with suppress(ValueError):
                self.active_connections[run_id_str].remove(websocket)
            if not self.active_connections[run_id_str]:
                del self.active_connections[run_id_str]

    async def broadcast(self, run_id: UUID, event_type: str, data: dict[str, Any]):
        """Send an event to all WebSocket connections watching a run."""
        run_id_str = str(run_id)
        message = json.dumps(
            {
                "type": event_type,
                "run_id": run_id_str,
                "data": data,
            }
        )
        connections = self.active_connections.get(run_id_str, [])
        dead_connections = []

        for ws in connections:
            try:
                await ws.send_text(message)
            except Exception:
                dead_connections.append(ws)

        # Clean up dead connections
        for ws in dead_connections:
            self.disconnect(ws, run_id_str)

    async def handle_connection(self, websocket: WebSocket, run_id: str):
        """
        Handle a WebSocket connection lifecycle.
        Validates JWT token from query parameter before accepting the connection.
        Keeps connection alive until client disconnects.
        """
        # Validate JWT token from query parameter
        token = websocket.query_params.get("token")
        if not token:
            await websocket.close(code=1008, reason="Missing authentication token")
            return

        try:
            payload = decode_token(token)
        except Exception:
            await websocket.close(code=1008, reason="Invalid or expired token")
            return

        if payload.get("type") != "access":
            await websocket.close(code=1008, reason="Invalid token type")
            return

        try:
            user_id = UUID(payload["sub"])
        except (KeyError, TypeError, ValueError):
            await websocket.close(code=1008, reason="Invalid token subject")
            return

        async with AsyncSessionLocal() as db:
            if not await authorize_run_subscription(db, run_id=run_id, user_id=user_id):
                await websocket.close(code=1008, reason="Run not found")
                return

        await self.connect(websocket, run_id)
        try:
            while True:
                # Wait for messages from client (ping/pong or commands)
                data = await websocket.receive_text()
                # Handle any client messages here if needed
                logger.debug(f"Received WS message for run {run_id}: {data}")
        except WebSocketDisconnect:
            self.disconnect(websocket, run_id)
            logger.info(f"WebSocket disconnected for run {run_id}")


# Global WebSocket manager instance
ws_manager = ExecutionWebSocketManager()
