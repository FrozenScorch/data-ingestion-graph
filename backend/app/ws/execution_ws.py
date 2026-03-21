"""
WebSocket manager for execution run progress events.
"""
import asyncio
import json
import logging
from typing import Any
from uuid import UUID

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


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
        logger.info(f"WebSocket connected for run {run_id_str}. "
                     f"Total connections: {len(self.active_connections[run_id_str])}")

    def disconnect(self, websocket: WebSocket, run_id: str):
        """Remove a WebSocket connection."""
        run_id_str = str(run_id)
        if run_id_str in self.active_connections:
            try:
                self.active_connections[run_id_str].remove(websocket)
            except ValueError:
                pass
            if not self.active_connections[run_id_str]:
                del self.active_connections[run_id_str]

    async def broadcast(self, run_id: UUID, event_type: str, data: dict[str, Any]):
        """Send an event to all WebSocket connections watching a run."""
        run_id_str = str(run_id)
        message = json.dumps({
            "type": event_type,
            "run_id": run_id_str,
            "data": data,
        })
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
        Keeps connection alive until client disconnects.
        """
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
