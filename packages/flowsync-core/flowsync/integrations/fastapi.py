import logging
import asyncio
from fastapi import FastAPI
from starlette.websockets import WebSocket, WebSocketDisconnect
from flowsync.hub import FlowSyncHub
from flowsync.dashboard import create_dashboard_router

logger = logging.getLogger("flowsync.integrations.fastapi")

class FastAPIWebSocketAdapter:
    """
    Adapter that wraps a FastAPI/Starlette WebSocket connection
    and exposes the methods/properties expected by FlowSyncHub.
    """
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket

    @property
    def remote_address(self):
        if self.websocket.client:
            return (self.websocket.client.host, self.websocket.client.port)
        return ("127.0.0.1", 0)

    async def send(self, message: str) -> None:
        await self.websocket.send_text(message)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        try:
            await self.websocket.close(code=code, reason=reason)
        except Exception:
            pass

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        try:
            return await self.websocket.receive_text()
        except (WebSocketDisconnect, RuntimeError):
            raise StopAsyncIteration


def mount_flowsync(
    app: FastAPI,
    hub: FlowSyncHub,
    path: str = "/flowsync",
    dashboard_path: str = "/dashboard"
) -> None:
    """
    Mounts FlowSync Hub WebSocket connection handler and Diagnostics Dashboard
    directly onto a FastAPI application.
    """
    # Initialize stats start time if not already running standalone
    import time
    if hub._start_time == 0.0:
        hub._start_time = time.time()

    @app.on_event("startup")
    async def startup_flowsync_hub():
        if hub._stats_task is None or hub._stats_task.done():
            hub._stats_task = hub._spawn(hub._stats_tracker())

    @app.on_event("shutdown")
    async def cleanup_flowsync_hub():
        if hub._stats_task and not hub._stats_task.done():
            hub._stats_task.cancel()
            try:
                await hub._stats_task
            except asyncio.CancelledError:
                pass

    # 1. Mount WebSocket Route
    @app.websocket(path)
    async def flowsync_ws_endpoint(websocket: WebSocket):
        await websocket.accept()
        adapter = FastAPIWebSocketAdapter(websocket)
        try:
            await hub._handle_connection(adapter)
        except Exception as e:
            logger.error(f"Error handling FlowSync connection: {e}", exc_info=True)

    # 2. Mount Diagnostics Dashboard
    router = create_dashboard_router(hub)
    app.include_router(router, prefix=dashboard_path)
    logger.info(f"Mounted FlowSync WebSocket at {path} and Dashboard at {dashboard_path}")
