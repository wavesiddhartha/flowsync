import json
import logging
import asyncio
from typing import Callable, Any, Optional, Set

logger = logging.getLogger("flowsync.integrations.postgres")

class PostgresSyncAdapter:
    """
    Adapter that listens to PostgreSQL LISTEN/NOTIFY events
    and forwards database change notifications directly to a FlowSync stream.
    """
    def __init__(
        self,
        dsn: str,
        pg_channel: str,
        hub,
        stream_name: str,
        payload_mapper: Optional[Callable[[Any], Any]] = None
    ):
        self.dsn = dsn
        self.pg_channel = pg_channel
        self.hub = hub
        self.stream_name = stream_name
        self.payload_mapper = payload_mapper or (lambda p: p)
        self._listener_task: Optional[asyncio.Task] = None
        self._running = False
        self._background_tasks: Set[asyncio.Task] = set()

    async def start(self) -> None:
        try:
            import asyncpg
        except ImportError:
            logger.error("asyncpg is required to use the PostgresSyncAdapter. Please run 'pip install asyncpg'.")
            raise ImportError("asyncpg package not found.")

        self._running = True
        self._listener_task = asyncio.create_task(self._listen_loop())
        logger.info(f"PostgresSyncAdapter listening on channel '{self.pg_channel}' -> stream '{self.stream_name}'")

    async def stop(self) -> None:
        self._running = False
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        
        # Cancel any active background tasks
        for task in list(self._background_tasks):
            task.cancel()
            
        logger.info("PostgresSyncAdapter listener stopped.")

    def _task_error_handler(self, task: asyncio.Task) -> None:
        try:
            exc = task.exception()
            if exc:
                logger.error(f"Postgres notify task failed: {exc}", exc_info=exc)
        except asyncio.CancelledError:
            pass

    async def _listen_loop(self) -> None:
        import asyncpg
        while self._running:
            conn = None
            try:
                conn = await asyncpg.connect(self.dsn)
                
                # Listener handler
                def callback(connection, pid, channel, payload):
                    task = asyncio.create_task(self._handle_notify(payload))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                    task.add_done_callback(self._task_error_handler)
                
                await conn.add_listener(self.pg_channel, callback)
                
                # Keep connection alive while adapter is running
                while self._running:
                    await asyncio.sleep(5.0)
                    if conn.is_closed():
                        logger.warning("Postgres connection closed. Reconnecting...")
                        break
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in PG connection for channel '{self.pg_channel}': {e}. Reconnecting in 5s...")
                await asyncio.sleep(5.0)
            finally:
                if conn:
                    try:
                        await conn.close()
                    except Exception:
                        pass

    async def _handle_notify(self, payload: str) -> None:
        try:
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                data = payload
                
            mapped_value = self.payload_mapper(data)
            
            s = self.hub.stream(self.stream_name)
            await s.push(mapped_value, {"node_id": "postgres-sync"})
        except Exception as e:
            logger.error(f"Error forwarding PG notification to FlowSync: {e}")
