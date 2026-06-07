import aiosqlite
import asyncio
import json
import os
import re
import time
from typing import Any, List, Dict, Optional, Tuple

class OfflineQueue:
    """
    Stores push() calls locally when the node is offline.
    Uses SQLite so data survives even if the process restarts.
    """
    MAX_QUEUE_SIZE = 100_000

    def __init__(self, node_id: str, db_path: Optional[str] = None):
        """
        Initializes the OfflineQueue.

        Args:
            node_id: Unique identifier for the client node.
            db_path: Optional path to the SQLite database file. Defaults to ~/.flowsync/{node_id}_queue.db.
        """
        # Sanitization to prevent path traversal via node_id
        self.node_id = node_id
        safe_node_id = re.sub(r'[^a-zA-Z0-9_-]', '_', node_id)
        
        if db_path is None:
            # Default to ~/.flowsync/{safe_node_id}_queue.db with secure 0o700 permissions
            home_dir = os.path.expanduser("~/.flowsync")
            os.makedirs(home_dir, mode=0o700, exist_ok=True)
            self.db_path = os.path.join(home_dir, f"{safe_node_id}_queue.db")
        else:
            # Ensure the parent directory of custom path exists
            dir_name = os.path.dirname(db_path)
            if dir_name:
                os.makedirs(dir_name, mode=0o700, exist_ok=True)
            self.db_path = db_path

        self._initialized = False
        self._lock = asyncio.Lock()

    async def _init_db(self) -> None:
        """Helper to dynamically initialize connection and create table."""
        if self._initialized:
            return
        
        async with self._lock:
            if self._initialized:
                return
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS offline_queue (
                        id        INTEGER PRIMARY KEY AUTOINCREMENT,
                        stream    TEXT NOT NULL,
                        value     TEXT NOT NULL,        -- JSON-serialized
                        timestamp REAL NOT NULL,
                        created_at REAL NOT NULL
                    )
                    """
                )
                await db.commit()
            self._initialized = True

    async def enqueue(self, stream: str, value: Any, timestamp: float) -> None:
        """
        Save a pending change to local SQLite.

        Args:
            stream: The name of the stream.
            value: The value being pushed (will be serialized to JSON).
            timestamp: The timestamp of the push.
        """
        await self._init_db()
        
        # Enforce size limit to prevent Disk Exhaustion DoS
        current_count = await self.count()
        if current_count >= self.MAX_QUEUE_SIZE:
            async with aiosqlite.connect(self.db_path) as db:
                # Evict oldest 1000 items
                await db.execute(
                    "DELETE FROM offline_queue WHERE id IN (SELECT id FROM offline_queue ORDER BY timestamp ASC LIMIT 1000)"
                )
                await db.commit()

        value_json = json.dumps(value)
        created_at = time.time()
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO offline_queue (stream, value, timestamp, created_at) VALUES (?, ?, ?, ?)",
                (stream, value_json, timestamp, created_at)
            )
            await db.commit()

    async def drain(self) -> List[Dict[str, Any]]:
        """
        Return all pending changes ordered by timestamp.
        Included for backwards compatibility.
        """
        changes, _ = await self.drain_with_max_id()
        return changes

    async def drain_with_max_id(self) -> Tuple[List[Dict[str, Any]], int]:
        """
        Return all pending changes ordered by timestamp, alongside the maximum record ID retrieved.
        This allows safe non-destructive clearing of processed changes.
        """
        await self._init_db()
        results = []
        max_id = 0
        
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT id, stream, value, timestamp FROM offline_queue ORDER BY timestamp ASC"
            ) as cursor:
                async for row in cursor:
                    row_id, stream, value_json, timestamp = row
                    if row_id > max_id:
                        max_id = row_id
                    try:
                        value = json.loads(value_json)
                    except json.JSONDecodeError:
                        value = value_json
                    results.append({
                        "stream": stream,
                        "value": value,
                        "timestamp": timestamp
                    })
        return results, max_id

    async def clear(self) -> None:
        """Delete all pending changes after successful sync."""
        await self._init_db()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM offline_queue")
            await db.commit()

    async def clear_up_to(self, max_id: int) -> None:
        """Delete pending changes with ID up to and including max_id."""
        if max_id <= 0:
            return
        await self._init_db()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM offline_queue WHERE id <= ?", (max_id,))
            await db.commit()

    async def count(self) -> int:
        """How many changes are waiting to be sent."""
        await self._init_db()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(*) FROM offline_queue") as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
