import time
import asyncio
from collections import OrderedDict
from typing import Dict, Tuple, Optional

class TokenBucket:
    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    def update_limits(self, capacity: float, refill_rate: float) -> None:
        """Update capacity and refill rate while preserving accumulated tokens (clamped)."""
        if self.capacity != capacity or self.refill_rate != refill_rate:
            self.capacity = capacity
            self.refill_rate = refill_rate
            self.tokens = min(self.tokens, capacity)

    async def consume(self, amount: float = 1.0) -> bool:
        async with self.lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self.last_update)
            self.last_update = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            if self.tokens >= amount:
                self.tokens -= amount
                return True
            return False

    def rollback(self, amount: float = 1.0) -> None:
        """Rollback a consumption attempt if a downstream limit check failed."""
        self.tokens = min(self.capacity, self.tokens + amount)


class FlowSyncRateLimiter:
    MAX_BUCKETS = 50_000

    def __init__(self, default_capacity: float = 100.0, default_refill_rate: float = 10.0):
        self.default_capacity = default_capacity
        self.default_refill_rate = default_refill_rate
        self.buckets: OrderedDict[str, TokenBucket] = OrderedDict()
        self.lock = asyncio.Lock()

    async def _get_bucket(self, key: str, capacity: Optional[float] = None, refill_rate: Optional[float] = None) -> TokenBucket:
        async with self.lock:
            cap = capacity if capacity is not None else self.default_capacity
            ref = refill_rate if refill_rate is not None else self.default_refill_rate
            
            if key in self.buckets:
                # Move to end to track LRU usage
                self.buckets.move_to_end(key)
                bucket = self.buckets[key]
                bucket.update_limits(cap, ref)
                return bucket
            
            # Limit memory footprint
            if len(self.buckets) >= self.MAX_BUCKETS:
                self.buckets.popitem(last=False)  # Evict oldest active bucket
                
            bucket = TokenBucket(cap, ref)
            self.buckets[key] = bucket
            return bucket

    async def check_limit(
        self,
        node_id: str,
        stream_name: Optional[str] = None,
        node_limit: Optional[Tuple[float, float]] = None,
        stream_limit: Optional[Tuple[float, float]] = None,
        global_limit: Optional[Tuple[float, float]] = None,
    ) -> bool:
        """
        Checks rate limits. Returns True if request is allowed, False if rate-limited.
        Rolls back tokens from earlier checked buckets if a later check fails.
        """
        consumed_buckets = []

        # 1. Global limit
        if global_limit:
            cap, ref = global_limit
            global_bucket = await self._get_bucket("global", cap, ref)
            if not await global_bucket.consume(1.0):
                return False
            consumed_buckets.append(global_bucket)

        # 2. Node limit
        if node_limit:
            cap, ref = node_limit
            node_bucket = await self._get_bucket(f"node:{node_id}", cap, ref)
            if not await node_bucket.consume(1.0):
                # Rollback global
                for bucket in consumed_buckets:
                    bucket.rollback(1.0)
                return False
            consumed_buckets.append(node_bucket)

        # 3. Stream limit
        if stream_name and stream_limit:
            cap, ref = stream_limit
            stream_bucket = await self._get_bucket(f"stream:{stream_name}", cap, ref)
            if not await stream_bucket.consume(1.0):
                # Rollback global and node
                for bucket in consumed_buckets:
                    bucket.rollback(1.0)
                return False

        return True
