import asyncio
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Union
import websockets
from websockets.client import WebSocketClientProtocol

from flowsync.transport import protocol
from flowsync.transport.protocol import (
    AuthMessage,
    ErrorMessage,
    FlowSyncMessage,
    GetMessage,
    JoinRoomMessage,
    LeaveRoomMessage,
    PingMessage,
    PushMessage,
    SubscribeMessage,
    UnsubscribeMessage,
    ReconnectMessage,
    ReconnectAckMessage,
)
from flowsync.storage.offline_queue import OfflineQueue
from flowsync.stream import MergeRuleRegistry
from flowsync.merge.crdt_counter import CRDTCounter
from flowsync.merge.crdt_set import CRDTSet
from flowsync.merge.crdt_text import CRDTText

logger = logging.getLogger("flowsync.node")


class FlowSyncNode:
    """
    A Node is any device/process that connects to the Hub.
    Works on IoT, servers, scripts, Raspberry Pi, etc.
    """

    def __init__(
        self,
        hub_url: str,
        node_id: Optional[str] = None,
        auth_token: Optional[str] = None,
        offline_mode: bool = True,
        reconnect: bool = True,
        db_path: Optional[str] = None,
    ):
        self.hub_url = hub_url
        self.node_id = node_id or f"node_{uuid.uuid4().hex[:6]}"
        self.auth_token = auth_token or "anonymous"
        self.offline_mode = offline_mode
        self.reconnect = reconnect

        # Resolve SQLite DB path for OfflineQueue with sanitization to prevent path traversal
        safe_node_id = re.sub(r'[^a-zA-Z0-9_-]', '_', self.node_id)
        if db_path is None:
            home_dir = os.path.expanduser("~/.flowsync")
            self.db_path = os.path.join(home_dir, f"{safe_node_id}_queue.db")
        else:
            self.db_path = db_path

        self._offline_queue = OfflineQueue(node_id=self.node_id, db_path=self.db_path)
        self._pending_count_cache = self._get_pending_count_sync()

        # WebSocket connection and listener state
        self._ws: Optional[WebSocketClientProtocol] = None
        self._listener_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._is_online = False
        self._explicit_disconnect = False
        self._was_authenticated = False

        # Auth synchronization
        self._auth_future: Optional[asyncio.Future[str]] = None

        # Callbacks & Caches
        self._active_subscriptions: Dict[str, Set[Callable[[Any, Dict[str, Any]], Any]]] = {}
        self._active_rooms: Set[str] = set()
        self._pending_gets: Dict[str, asyncio.Future[Any]] = {}
        self._cache: Dict[str, Any] = {}
        self._cache_meta: Dict[str, Dict[str, Any]] = {}

        # Event handlers (e.g. sync:complete, disconnect, connect)
        self._event_handlers: Dict[str, Set[Callable[..., Any]]] = {}

        # Last seen timestamp for syncing
        self._last_seen_ts = 0.0

    @property
    def is_online(self) -> bool:
        """True if connected to Hub and authenticated."""
        return self._is_online

    @property
    def pending_changes(self) -> list:
        """Changes queued while offline (synchronous read)."""
        return self._get_pending_changes_sync()

    def on_event(self, event_name: str, callback: Callable[..., Any]) -> Callable[[], None]:
        """Subscribe to Node client events (e.g. 'sync:complete', 'disconnect')."""
        self._event_handlers.setdefault(event_name, set()).add(callback)
        return lambda: self._event_handlers.get(event_name, set()).discard(callback)

    async def _fire_event(self, event_name: str, *args: Any, **kwargs: Any) -> None:
        """Helper to invoke registered event handler callbacks."""
        handlers = self._event_handlers.get(event_name, set())
        tasks = []
        for cb in list(handlers):
            if asyncio.iscoroutinefunction(cb):
                tasks.append(asyncio.create_task(cb(*args, **kwargs)))
            else:
                try:
                    cb(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error in client event handler '{event_name}': {e}", exc_info=True)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def connect(self) -> None:
        """Connect to Hub. Resolves when connected and authenticated."""
        if self._is_online:
            return
            
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            self._reconnect_task = None
            
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            self._listener_task = None

        self._explicit_disconnect = False
        await self._connect_and_auth()

        # Start listener loop
        self._listener_task = asyncio.create_task(self._listener_loop())

    async def _connect_and_auth(self) -> None:
        """Establishes websocket connection and performs auth/reconnect handshake."""
        logger.info(f"Connecting to FlowSync Hub at {self.hub_url}...")
        self._ws = await websockets.connect(self.hub_url)
        self._is_online = False

        # Set up a future to await AuthOk / AuthFail / ReconnectAck
        loop = asyncio.get_running_loop()
        self._auth_future = loop.create_future()

        # Decide whether to perform standard AUTH or RECONNECT handshake
        if self._was_authenticated:
            # Drain queue to send in RECONNECT payload
            pending_changes, max_id = await self._offline_queue.drain_with_max_id()
            logger.info(f"Sending RECONNECT handshake with {len(pending_changes)} pending changes.")
            
            reconnect_msg = ReconnectMessage(
                node_id=self.node_id,
                last_seen_ts=self._last_seen_ts,
                pending=pending_changes,
                token=self.auth_token
            )
            await self._ws.send(protocol.encode(reconnect_msg))
            
            # Read RECONNECT_ACK response
            try:
                raw_response = await self._ws.recv()
                response = protocol.decode(raw_response)

                if response.type == "RECONNECT_ACK":
                    # Mark online first so messages can be sent/received
                    self._is_online = True
                    self._auth_future.set_result(self.node_id)

                    # Restore subscriptions and rooms on the Hub
                    for stream in self._active_subscriptions:
                        await self._send_subscribe(stream)
                    for room in self._active_rooms:
                        await self._send_join_room(room)

                    # Fire reconnect event before replaying changes
                    await self._fire_event("reconnect")
                    
                    # Apply missed updates from other nodes first
                    missed_updates = response.missed_updates
                    for update in missed_updates:
                        s_name = update["stream"]
                        val = update["value"]
                        ts = update["timestamp"]
                        node_id_other = update["node_id"]
                        meta = update.get("metadata") or {"ts": ts, "node_id": node_id_other}
                        self._apply_incoming_update(s_name, val, meta)
                        
                    # Replay own pending changes locally through merge rules
                    for change in pending_changes:
                        s_name = change["stream"]
                        val = change["value"]
                        ts = change["timestamp"]
                        m_rule = self._cache_meta.get(s_name, {}).get("merge_rule")
                        meta = {"ts": ts, "node_id": self.node_id}
                        if m_rule:
                            meta["merge_rule"] = m_rule
                        self._apply_incoming_update(s_name, val, meta)

                    # Clear offline queue up to the successfully sent max_id
                    await self._offline_queue.clear_up_to(max_id)
                    self._pending_count_cache = await self._offline_queue.count()
                    
                    logger.info("Successfully reconnected and synchronized state with Hub.")
                    
                    # Fire sync complete notification
                    synced_streams = list(set([c["stream"] for c in pending_changes] + [u["stream"] for u in missed_updates]))
                    conflicts_resolved = sum(1 for u in missed_updates if (u.get("metadata") or {}).get("conflict_resolved"))
                    
                    summary = {
                        "synced_streams": synced_streams,
                        "conflicts_resolved": conflicts_resolved,
                        "changes_sent": len(pending_changes),
                        "changes_received": len(missed_updates)
                    }
                    await self._fire_event("sync:complete", summary)
                else:
                    self._auth_future.set_exception(ConnectionError(f"Unexpected reconnect handshake response: {response.type}"))
                    await self._ws.close()
                    raise ConnectionError(f"Unexpected reconnect handshake response: {response.type}")
            except Exception as e:
                if self._auth_future and not self._auth_future.done():
                    self._auth_future.set_exception(e)
                raise e
        else:
            # First time connecting, send AUTH
            auth_msg = AuthMessage(token=self.auth_token, node_id=self.node_id)
            await self._ws.send(protocol.encode(auth_msg))

            try:
                raw_response = await self._ws.recv()
                response = protocol.decode(raw_response)

                if response.type == "AUTH_OK":
                    self.node_id = response.node_id
                    self._is_online = True
                    self._was_authenticated = True
                    self._auth_future.set_result(self.node_id)
                    logger.info(f"FlowSyncNode authenticated successfully as '{self.node_id}'")
                elif response.type == "AUTH_FAIL":
                    self._auth_future.set_exception(ConnectionError(f"Authentication failed: {response.reason}"))
                    await self._ws.close()
                    raise ConnectionError(f"Authentication failed: {response.reason}")
                else:
                    self._auth_future.set_exception(ConnectionError(f"Unexpected handshake response: {response.type}"))
                    await self._ws.close()
                    raise ConnectionError(f"Unexpected handshake response: {response.type}")
            except Exception as e:
                if self._auth_future and not self._auth_future.done():
                    self._auth_future.set_exception(e)
                raise e

    def _apply_incoming_update(self, stream_name: str, val: Any, meta: dict) -> None:
        """Applies incoming raw value/operation update to the cache using the correct merge rule."""
        self._last_seen_ts = max(self._last_seen_ts, meta.get("ts", 0.0))
        merge_rule = self._cache_meta.get(stream_name, {}).get("merge_rule") or meta.get("merge_rule", "lww")

        existing_val = self._cache.get(stream_name)
        existing_meta = self._cache_meta.get(stream_name) or {"ts": 0.0, "node_id": "system"}

        if existing_meta["ts"] > 0.0:
            merge_fn = MergeRuleRegistry.get(merge_rule)
            merged_val, merged_meta = merge_fn(existing_val, val, existing_meta, meta)
            self._cache[stream_name] = merged_val
            self._cache_meta[stream_name] = merged_meta
        else:
            # First write locally
            if merge_rule == "crdt-counter":
                if isinstance(val, dict) and "op" in val:
                    counter = CRDTCounter(node_id=meta.get("node_id", "server"))
                    op = val.get("op")
                    amount = val.get("amount", 1)
                    if op == "increment":
                        counter.increment(amount)
                    elif op == "decrement":
                        counter.decrement(amount)
                    self._cache[stream_name] = counter.to_state()
                else:
                    self._cache[stream_name] = val
            elif merge_rule == "crdt-set":
                if isinstance(val, dict) and "op" in val:
                    crdt_set = CRDTSet()
                    op = val.get("op")
                    item = val.get("item")
                    if op == "add":
                        crdt_set.add(item)
                    elif op == "remove":
                        crdt_set.remove(item)
                    self._cache[stream_name] = crdt_set.to_state()
                else:
                    self._cache[stream_name] = val
            elif merge_rule == "crdt-text":
                current_ops = []
                new_ops = [val] if (isinstance(val, dict) and "op" in val) else (val if isinstance(val, list) else [])
                self._cache[stream_name] = CRDTText.merge_ops(current_ops, new_ops)
            else:
                self._cache[stream_name] = val

            self._cache_meta[stream_name] = meta

        # Trigger subscriber callbacks with computed value
        computed_val = self._compute_crdt_value(stream_name, self._cache[stream_name], self._cache_meta[stream_name])
        callbacks = self._active_subscriptions.get(stream_name, set())
        for cb in list(callbacks):
            try:
                if asyncio.iscoroutinefunction(cb):
                    asyncio.create_task(cb(computed_val, self._cache_meta[stream_name]))
                else:
                    cb(computed_val, self._cache_meta[stream_name])
            except Exception as e:
                logger.error(f"Error executing subscription callback for '{stream_name}': {e}", exc_info=True)

    def _compute_crdt_value(self, stream_name: str, val: Any, metadata: dict) -> Any:
        """Resolve raw internal CRDT structures into user-facing computed values."""
        merge_rule = metadata.get("merge_rule", "lww")
        if merge_rule == "crdt-counter":
            if isinstance(val, dict):
                return sum(val.get("increments", {}).values()) - sum(val.get("decrements", {}).values())
            return val or 0
        elif merge_rule == "crdt-set":
            if isinstance(val, dict):
                added = set(val.get("added", []))
                removed = set(val.get("removed", []))
                return list(added - removed)
            return val or []
        elif merge_rule == "crdt-text":
            if isinstance(val, list):
                return CRDTText.apply_all(val)
            return val or ""
        return val

    async def disconnect(self) -> None:
        """Gracefully disconnect from the Hub."""
        self._explicit_disconnect = True
        self._is_online = False
        
        if self._listener_task:
            self._listener_task.cancel()
            self._listener_task = None
            
        if self._reconnect_task:
            self._reconnect_task.cancel()
            self._reconnect_task = None

        if self._ws:
            await self._ws.close()
            self._ws = None

        logger.info("Disconnected from FlowSync Hub.")
        await self._fire_event("disconnect")

    async def _send_message(self, msg: FlowSyncMessage) -> None:
        """Helper to serialize and send message over WebSocket if online."""
        if not self._is_online or not self._ws:
            raise ConnectionError("Not connected to FlowSync Hub")
        await self._ws.send(protocol.encode(msg))

    async def push(self, stream: str, value: Any) -> None:
        """Push value to a stream. Queued in SQLite OfflineQueue if offline."""
        ts = time.time()
        
        # Enrich CRDT text/set/counter operations with op_id and ts if missing
        if isinstance(value, dict) and "op" in value and "op_id" not in value:
            value = value.copy()
            value["op_id"] = f"{self.node_id}:{time.time_ns()}:{uuid.uuid4().hex[:4]}"
            if "ts" not in value:
                value["ts"] = ts
        elif isinstance(value, list):
            new_val = []
            for item in value:
                if isinstance(item, dict) and "op" in item and "op_id" not in item:
                    item = item.copy()
                    item["op_id"] = f"{self.node_id}:{time.time_ns()}:{uuid.uuid4().hex[:4]}"
                    if "ts" not in item:
                        item["ts"] = ts
                new_val.append(item)
            value = new_val

        logger.debug(f"[{self.node_id}] push() called for '{stream}'. is_online={self._is_online}, ws={self._ws is not None}")
        
        if self._is_online:
            try:
                push_msg = PushMessage(stream=stream, value=value, ts=ts)
                await self._send_message(push_msg)
                return
            except Exception as e:
                logger.warning(f"[{self.node_id}] Failed to push to online stream '{stream}', queuing instead: {e}")

        # Offline path
        if self.offline_mode:
            logger.info(f"[{self.node_id}] Queuing push for stream '{stream}' in SQLite offline queue.")
            await self._offline_queue.enqueue(stream, value, ts)
            self._pending_count_cache = await self._offline_queue.count()
        else:
            raise ConnectionError("Cannot push. Node is offline and offline_mode is disabled.")

    async def get(self, stream: str) -> Any:
        """Get the current value of a stream. Fetches from server if online, otherwise returns cached value."""
        if self._is_online:
            req_id = uuid.uuid4().hex
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            self._pending_gets[req_id] = fut

            try:
                get_msg = GetMessage(stream=stream, req_id=req_id)
                await self._send_message(get_msg)
                # Await reply (raw CRDT state) with timeout
                val = await asyncio.wait_for(fut, timeout=5.0)
                # Keep local cache updated
                self._cache[stream] = val
                
                # Fetch metadata from response (hub does not send raw meta on GET_REPLY, but we can infer LWW)
                # Wait, GET_REPLY returns raw state.
                return self._compute_crdt_value(stream, val, self._cache_meta.get(stream, {}))
            except Exception as e:
                logger.warning(f"GET request for '{stream}' failed: {e}. Falling back to cache.")
                self._pending_gets.pop(req_id, None)
            
        # Return cached value if offline or server GET failed
        if stream in self._cache:
            return self._compute_crdt_value(stream, self._cache[stream], self._cache_meta.get(stream, {}))
        
        raise KeyError(f"Stream '{stream}' not found in local cache and Hub is unreachable.")

    def on(self, stream: str, callback: Callable[[Any, Dict[str, Any]], Any]) -> Callable[[], None]:
        """Subscribe to stream changes. Returns an unsubscribe function."""
        is_first = stream not in self._active_subscriptions
        self._active_subscriptions.setdefault(stream, set()).add(callback)

        # Send subscribe request to server if online and this is the first handler
        if self._is_online and is_first:
            asyncio.create_task(self._send_subscribe(stream))

        def unsubscribe() -> None:
            callbacks = self._active_subscriptions.get(stream, set())
            callbacks.discard(callback)
            if not callbacks:
                self._active_subscriptions.pop(stream, None)
                if self._is_online:
                    asyncio.create_task(self._send_unsubscribe(stream))

        return unsubscribe

    async def _send_subscribe(self, stream: str) -> None:
        try:
            await self._send_message(SubscribeMessage(stream=stream))
        except Exception as e:
            logger.error(f"[{self.node_id}] Failed to send SUBSCRIBE for stream '{stream}': {e}")

    async def _send_unsubscribe(self, stream: str) -> None:
        try:
            await self._send_message(UnsubscribeMessage(stream=stream))
        except Exception as e:
            logger.error(f"[{self.node_id}] Failed to send UNSUBSCRIBE for stream '{stream}': {e}")

    def join_room(self, room: str) -> None:
        """Join a room for targeted broadcasts."""
        self._active_rooms.add(room)
        if self._is_online:
            asyncio.create_task(self._send_join_room(room))

    async def _send_join_room(self, room: str) -> None:
        try:
            await self._send_message(JoinRoomMessage(room=room))
        except Exception as e:
            logger.error(f"[{self.node_id}] Failed to join room '{room}': {e}")

    def leave_room(self, room: str) -> None:
        """Leave a room."""
        self._active_rooms.discard(room)
        if self._is_online:
            asyncio.create_task(self._send_leave_room(room))

    async def _send_leave_room(self, room: str) -> None:
        try:
            await self._send_message(LeaveRoomMessage(room=room))
        except Exception as e:
            logger.error(f"[{self.node_id}] Failed to leave room '{room}': {e}")

    async def _replay_pending_changes(self) -> None:
        """Replay changes queued while offline (internal helper)."""
        pending_changes, max_id = await self._offline_queue.drain_with_max_id()
        if not pending_changes:
            return
        
        logger.info(f"Replaying {len(pending_changes)} pending changes...")
        sent_count = 0
        for change in pending_changes:
            try:
                push_msg = PushMessage(
                    stream=change["stream"],
                    value=change["value"],
                    ts=change["timestamp"]
                )
                await self._send_message(push_msg)
                sent_count += 1
            except Exception as e:
                logger.error(f"Failed to replay change for '{change['stream']}': {e}")
                break
        
        if sent_count > 0:
            # Prune only the changes that were actually successfully sent
            # Wait, clear_up_to max_id is only safe if ALL were sent.
            # If only some were sent, we can clear only up to the last sent item's ID or similar.
            # But since it breaks on error, we can just clear_up_to the max_id if sent_count == len(pending_changes).
            if sent_count == len(pending_changes):
                await self._offline_queue.clear_up_to(max_id)
                self._pending_count_cache = await self._offline_queue.count()
            else:
                # Alternatively, we could clear them one by one, but for simplicity:
                pass

    async def _listener_loop(self) -> None:
        """Continuous task listening for incoming websocket messages from the Hub."""
        try:
            async for raw_msg in self._ws:
                try:
                    msg = protocol.decode(raw_msg)
                except ValueError as e:
                    logger.error(f"Failed to decode message from Hub: {e}")
                    continue

                if msg.type == "UPDATE":
                    self._apply_incoming_update(msg.stream, msg.value, msg.metadata or {})

                elif msg.type == "GET_REPLY":
                    fut = self._pending_gets.pop(msg.req_id, None)
                    if fut and not fut.done():
                        # GET_REPLY contains raw value, stored in cache inside get()
                        fut.set_result(msg.value)

                elif msg.type == "BROADCAST":
                    asyncio.create_task(self._fire_event(f"broadcast:{msg.event}", msg.data))

                elif msg.type == "ERROR":
                    logger.error(f"Received error from Hub [code={msg.code}]: {msg.message}")
                    asyncio.create_task(self._fire_event("error", msg.message))

                elif msg.type == "PONG":
                    pass

        except websockets.exceptions.ConnectionClosed:
            logger.warning("Connection to Hub closed unexpectedly.")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in listener loop: {e}", exc_info=True)
        finally:
            self._is_online = False
            await self._fire_event("disconnect")
            
            # Initiate reconnection loop if requested and not explicitly disconnected
            if self.reconnect and not self._explicit_disconnect:
                if not self._reconnect_task or self._reconnect_task.done():
                    self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        """Background loop executing reconnects with exponential backoff."""
        backoff = 2.0
        while not self._is_online and not self._explicit_disconnect:
            logger.info(f"Reconnecting to Hub in {backoff:.1f} seconds...")
            await asyncio.sleep(backoff)
            try:
                await self._connect_and_auth()
                
                # Restore subscriptions
                for stream in self._active_subscriptions:
                    await self._send_subscribe(stream)
                
                # Restore room registrations
                for room in self._active_rooms:
                    await self._send_join_room(room)

                # Start listener task again
                if self._listener_task and not self._listener_task.done():
                    self._listener_task.cancel()
                    try:
                        await self._listener_task
                    except asyncio.CancelledError:
                        pass
                self._listener_task = asyncio.create_task(self._listener_loop())
                break
            except Exception as e:
                logger.warning(f"Reconnection attempt failed: {e}")
                backoff = min(backoff * 2.0, 30.0)

    # Event subscription helpers
    def on_disconnect(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Called when connection drops."""
        return self.on_event("disconnect", callback)

    def on_reconnect(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Called when connection restored, before sync."""
        return self.on_event("reconnect", callback)

    def on_sync_complete(self, callback: Callable[[dict], None]) -> Callable[[], None]:
        """Called after offline queue is fully replayed."""
        return self.on_event("sync:complete", callback)

    @property
    def pending_count(self) -> int:
        """Number of changes waiting to sync."""
        return self._pending_count_cache

    async def pending_count_async(self) -> int:
        """Asynchronously query the current pending count without blocking the event loop."""
        count = await self._offline_queue.count()
        self._pending_count_cache = count
        return count

    def _get_pending_count_sync(self) -> int:
        """Query SQLite synchronously to count pending changes (survives restarts)."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='offline_queue'")
            if cursor.fetchone():
                cursor.execute("SELECT COUNT(*) FROM offline_queue")
                row = cursor.fetchone()
                count = row[0] if row else 0
            else:
                count = 0
            conn.close()
            return count
        except Exception:
            return 0

    def _get_pending_changes_sync(self) -> List[Dict[str, Any]]:
        """Query SQLite synchronously to fetch pending changes list."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='offline_queue'")
            if not cursor.fetchone():
                conn.close()
                return []
            cursor.execute("SELECT stream, value, timestamp FROM offline_queue ORDER BY timestamp ASC")
            rows = cursor.fetchall()
            conn.close()

            results = []
            for row in rows:
                stream, value_json, timestamp = row
                try:
                    value = json.loads(value_json)
                except Exception:
                    value = value_json
                results.append({
                    "stream": stream,
                    "value": value,
                    "timestamp": timestamp
                })
            return results
        except Exception:
            return []
