# backend/services/websocket_manager.py
import asyncio
import json
import logging
from collections import defaultdict
from typing import Dict, List, Any
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

class WebSocketManager:
    def __init__(self):
        # Maps job_id to a list of active WebSocket connections
        self.active_connections: Dict[str, List[WebSocket]] = defaultdict(list)

    async def connect(self, websocket: WebSocket, job_id: str):
        """Register a new WebSocket connection for a specific job."""
        await websocket.accept()
        self.active_connections[job_id].append(websocket)
        logger.info(f"WebSocket connected for job_id: {job_id}")

    def disconnect(self, websocket: WebSocket, job_id: str):
        """Disconnect a WebSocket."""
        try:
            if websocket in self.active_connections[job_id]:
                self.active_connections[job_id].remove(websocket)
            if not self.active_connections[job_id]:
                del self.active_connections[job_id]
            logger.info(f"WebSocket disconnected for job_id: {job_id}")
        except (ValueError, KeyError):
            logger.warning(f"WebSocket already disconnected or job_id {job_id} not found.")

    async def broadcast_to_job(self, job_id: str, update: Dict[str, Any]):
        """Send a JSON update to all WebSockets connected to a specific job."""
        if job_id not in self.active_connections:
            return

        message = json.dumps(update)
        disconnected_sockets = []

        # Use asyncio.gather for concurrent sends
        async def send_message(websocket: WebSocket):
            try:
                await websocket.send_text(message)
            except (WebSocketDisconnect, RuntimeError):
                disconnected_sockets.append(websocket)

        tasks = [send_message(ws) for ws in self.active_connections[job_id]]
        await asyncio.gather(*tasks)

        # Clean up disconnected sockets
        for websocket in disconnected_sockets:
            self.disconnect(websocket, job_id)

    async def send_status_update(
        self,
        job_id: str,
        status: str,
        message: str,
        result: Any = None,
        error: Any = None
    ):
        """
        Helper function to format and broadcast a status update.
        This is the primary method used by the nodes.
        """
        update = {
            "type": "status_update",
            "job_id": job_id,
            "status": status,
            "message": message,
            "data": result,
            "error": str(error) if error else None
        }
        await self.broadcast_to_job(job_id, update)

    async def safe_send(self, *, state: Dict[str, Any] | None = None, job_id: str | None = None, status: str, message: str, result: Any = None, error: Any = None, fallback_job_id: str | None = None):
        """
        Safely send a status update using either an explicit job_id or a provided state dict.

        Priority for job id selection:
        1. Explicit job_id argument
        2. state['job_id'] if present and truthy
        3. fallback_job_id if provided

        If no job id can be determined this will log a warning and return without raising.
        """
        # Determine which job_id to use
        job_to_use = None
        if job_id:
            job_to_use = job_id
        elif state and state.get('job_id'):
            job_to_use = state.get('job_id')
        elif fallback_job_id:
            job_to_use = fallback_job_id

        if not job_to_use:
            # Helpful debug output showing keys we received
            state_keys = list(state.keys()) if isinstance(state, dict) else None
            # Lower severity to DEBUG to avoid spamming logs when job_id is not available
            logger.debug(f"Could not send WebSocket update: job_id missing. state_keys={state_keys}, explicit_job_id={job_id!r}, fallback={fallback_job_id!r}")
            return

        update = {
            "type": "status_update",
            "job_id": job_to_use,
            "status": status,
            "message": message,
            "data": result,
            "error": str(error) if error else None
        }

        await self.broadcast_to_job(job_to_use, update)