import json
import logging
import asyncio
import re
from urllib.parse import urlparse
import redis.asyncio as aioredis
from typing import Any, Dict, Optional

logger = logging.getLogger("flowsync.storage.redis")

class RedisStorage:
    """
    Adapter for Redis-backed state persistence and multi-hub Pub/Sub routing.
    Allows horizontal scalability of multiple FlowSyncHub instances.
    """
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.client: Optional[aioredis.Redis] = None
        self.pubsub: Optional[aioredis.client.PubSub] = None
        self._listener_task: Optional[asyncio.Task] = None
        self._hub = None

    def _safe_stream_key(self, stream_name: str) -> str:
        """Sanitize stream names to prevent Redis namespace injection."""
        safe_name = re.sub(r'[^a-zA-Z0-9_:.\-/]', '_', stream_name)
        return f"flowsync:stream:{safe_name}"

    async def connect(self) -> None:
        self.client = aioredis.from_url(self.redis_url, decode_responses=True)
        # Verify connection health immediately on startup
        await self.client.ping()
        
        # Mask credentials in log message
        parsed = urlparse(self.redis_url)
        if parsed.password:
            netloc = f"{parsed.username or ''}:***@{parsed.hostname}:{parsed.port}"
        else:
            netloc = parsed.netloc
        safe_url = parsed._replace(netloc=netloc).geturl()
        logger.info(f"Connected to Redis at {safe_url}")

    async def disconnect(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        if self.pubsub:
            await self.pubsub.close()
        if self.client:
            await self.client.close()
            logger.info("Disconnected from Redis.")

    async def get_stream_state(self, stream_name: str) -> Optional[Dict[str, Any]]:
        if not self.client:
            return None
        try:
            key = self._safe_stream_key(stream_name)
            data = await self.client.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Error fetching state for stream {stream_name}: {e}")
        return None

    async def save_stream_state(self, stream_name: str, value: Any, ts: float, node_id: str, metadata: dict) -> None:
        if not self.client:
            return
        try:
            state = {
                "value": value,
                "ts": ts,
                "node_id": node_id,
                "metadata": metadata
            }
            key = self._safe_stream_key(stream_name)
            # Apply a 7-day TTL to prevent unbounded Redis storage growth
            await self.client.set(key, json.dumps(state), ex=86400 * 7)
        except Exception as e:
            logger.error(f"Error saving state for stream {stream_name}: {e}")

    async def publish_update(self, stream_name: str, value: Any, ts: float, node_id: str, metadata: dict) -> None:
        if not self.client:
            return
        try:
            msg = {
                "stream": stream_name,
                "value": value,
                "ts": ts,
                "node_id": node_id,
                "metadata": metadata
            }
            await self.client.publish("flowsync:updates", json.dumps(msg))
        except Exception as e:
            logger.error(f"Error publishing update for stream {stream_name}: {e}")

    async def save_and_publish(self, stream_name: str, value: Any, ts: float, node_id: str, metadata: dict) -> None:
        """Atomically saves stream state and publishes update using a pipeline."""
        if not self.client:
            return
        try:
            state = {
                "value": value,
                "ts": ts,
                "node_id": node_id,
                "metadata": metadata
            }
            msg = {
                "stream": stream_name,
                "value": value,
                "ts": ts,
                "node_id": node_id,
                "metadata": metadata
            }
            key = self._safe_stream_key(stream_name)
            async with self.client.pipeline(transaction=True) as pipe:
                pipe.set(key, json.dumps(state), ex=86400 * 7)
                pipe.publish("flowsync:updates", json.dumps(msg))
                await pipe.execute()
        except Exception as e:
            logger.error(f"Error in pipelined save_and_publish for stream {stream_name}: {e}")

    async def start_listening(self, hub) -> None:
        self._hub = hub
        self.pubsub = self.client.pubsub()
        await self.pubsub.subscribe("flowsync:updates")
        self._listener_task = asyncio.create_task(self._listen_loop())
        logger.info("Started listening for Redis pub/sub updates.")

    async def _listen_loop(self) -> None:
        while True:
            try:
                if self.pubsub is None:
                    self.pubsub = self.client.pubsub()
                    await self.pubsub.subscribe("flowsync:updates")
                    logger.info("Redis pub/sub listener resubscribed.")

                async for message in self.pubsub.listen():
                    if message["type"] == "message":
                        data_str = message["data"]
                        try:
                            data = json.loads(data_str)
                        except json.JSONDecodeError:
                            continue
                        
                        stream_name = data.get("stream")
                        value = data.get("value")
                        ts = data.get("ts", 0.0)
                        node_id = data.get("node_id", "redis")
                        metadata = data.get("metadata", {})
                        
                        if not self._hub or not stream_name:
                            continue
                            
                        # Tag metadata to prevent local pub/sub loops
                        metadata["source"] = "redis"
                        
                        # Fetch or create the stream in a thread-safe / asyncio-safe manner
                        s = self._hub._streams.get(stream_name)
                        if s is None:
                            s_access = metadata.get("access")
                            s = self._hub.stream(stream_name, access=s_access)
                        
                        await s.push(value, {"node_id": node_id, "ts": ts, **metadata})
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Redis pub/sub listen loop: {e}. Reconnecting in 5s...", exc_info=True)
                if self.pubsub:
                    try:
                        await self.pubsub.close()
                    except Exception:
                        pass
                self.pubsub = None
                await asyncio.sleep(5.0)
