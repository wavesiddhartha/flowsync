import asyncio
import logging
import time
import uuid
import re
import weakref
from typing import Any, Callable, Dict, List, Literal, Optional, Set, Union
import websockets
from websockets.server import WebSocketServerProtocol

from flowsync.stream import Stream
from flowsync.transport import protocol
from flowsync.auth import AuthResult
from flowsync.rate_limiter import FlowSyncRateLimiter
from flowsync.storage.redis import RedisStorage
from flowsync.transport.protocol import (
    AuthFailMessage,
    AuthOkMessage,
    BroadcastMessage,
    ErrorMessage,
    FlowSyncMessage,
    GetReplyMessage,
    PongMessage,
    UpdateMessage,
    ReconnectMessage,
    ReconnectAckMessage,
)

logger = logging.getLogger("flowsync.hub")


class FlowSyncHub:
    """
    The central server. Manages all Streams and Node connections.
    Can run standalone or be embedded in other ASGI applications.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8765,
        auth_fn: Optional[Callable[[str], Any]] = None,  # async fn(token) -> node_id | None
        max_nodes: int = 10_000,
        log_level: str = "INFO",
        rate_limit: Optional[Dict[str, Any]] = None,
        redis_url: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.auth_fn = auth_fn
        self.max_nodes = max_nodes
        self.rate_limit = rate_limit or {}
        self.redis_url = redis_url
        self._redis = RedisStorage(redis_url) if redis_url else None

        # Configure logging
        logging.basicConfig(level=getattr(logging, log_level.upper(), logging.INFO))

        # Core state
        self._streams: Dict[str, Stream] = {}
        self._nodes: Dict[str, WebSocketServerProtocol] = {}
        self._nodes_auth: Dict[str, Any] = {}
        self._subscriptions: Dict[str, Dict[str, Callable[[], None]]] = {}  # node_id -> {stream_name: unsub_fn}
        self._rooms: Dict[str, Set[str]] = {}  # room_name -> {node_id}
        self._rate_limiter = FlowSyncRateLimiter()
        self._ws_locks = weakref.WeakKeyDictionary()

        # Lifecycle events
        self._stop_event = asyncio.Event()
        self._server: Optional[websockets.server.WebSocketServer] = None
        self._start_time = 0.0

        # Stats
        self._msg_counter_current = 0
        self._msg_per_sec = 0
        self._total_messages = 0
        self._stats_task: Optional[asyncio.Task] = None
        self._background_tasks: Set[asyncio.Task] = set()

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        task.add_done_callback(self._task_error_handler)
        return task

    def _task_error_handler(self, task: asyncio.Task) -> None:
        try:
            exc = task.exception()
            if exc:
                logger.error(f"Background task failed: {exc}", exc_info=exc)
        except asyncio.CancelledError:
            pass

    def stream(self, name: str, **kwargs: Any) -> Stream:
        """Get or create a Stream."""
        # Sanitize/validate stream name to prevent traversal or command injection attacks
        if not re.match(r'^[a-zA-Z0-9_:.\-/]{1,256}$', name):
            raise ValueError(f"Invalid stream name: '{name}'. Must be 1-256 chars (alphanumeric, :, ., -, _, /).")

        if name not in self._streams:
            # Enforce max stream count
            MAX_STREAMS = 10_000
            if len(self._streams) >= MAX_STREAMS:
                raise ValueError(f"Maximum stream limit ({MAX_STREAMS}) reached. Cannot create stream '{name}'.")

            logger.info(f"Creating stream: '{name}'")
            s = Stream(name=name, **kwargs)
            self._streams[name] = s
            
            # Setup Redis persistence and pub/sub if configured
            if self._redis:
                try:
                    loop = asyncio.get_running_loop()
                    if loop.is_running():
                        # Callback to push local changes to Redis pub/sub
                        async def redis_update_callback(value, meta):
                            if meta.get("source") != "redis":
                                pub_meta = {**meta, "source": "redis", "access": s.access}
                                # Use atomic pipeline if available
                                if hasattr(self._redis, 'save_and_publish'):
                                    await self._redis.save_and_publish(name, value, meta.get("ts", 0.0), meta.get("node_id", "system"), pub_meta)
                                else:
                                    await self._redis.save_stream_state(name, value, meta.get("ts", 0.0), meta.get("node_id", "system"), pub_meta)
                                    await self._redis.publish_update(name, value, meta.get("ts", 0.0), meta.get("node_id", "system"), pub_meta)
                        
                        # VULN-50 FIX: Subscribe synchronously before yielding control during load_and_sub
                        s.subscribe(redis_update_callback)

                        async def load_and_sub():
                            state = await self._redis.get_stream_state(name)
                            if state:
                                if state.get("ts", 0.0) > s._ts:
                                    await s.push(state["value"], {
                                        "node_id": state.get("node_id", "system"),
                                        "ts": state.get("ts", 0.0),
                                        "source": "redis",
                                        **(state.get("metadata") or {})
                                    })
                            
                        self._spawn(load_and_sub())
                except RuntimeError:
                    pass
        return self._streams[name]

    async def start(self) -> None:
        """Start the hub. Blocks until stopped."""
        self._start_time = time.time()
        self._stats_task = self._spawn(self._stats_tracker())
        
        if self._redis:
            await self._redis.connect()
            await self._redis.start_listening(self)
        
        async with websockets.serve(self._handle_connection, self.host, self.port) as server:
            self._server = server
            # Retrieve dynamic bound port if port 0 was passed
            self.bound_port = server.sockets[0].getsockname()[1]
            logger.info(f"FlowSyncHub listening on ws://{self.host}:{self.bound_port}")
            await self._stop_event.wait()
            
        logger.info("FlowSyncHub stopping WebSocket server...")
        if self._stats_task:
            self._stats_task.cancel()
        
        if self._redis:
            await self._redis.disconnect()
            
        logger.info("FlowSyncHub stopped.")

    def stop(self) -> None:
        """Stop the hub WebSocket server."""
        self._stop_event.set()
        if self._redis:
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(self._redis.disconnect())
            except RuntimeError:
                pass

    async def broadcast(self, event: str, data: Any) -> None:
        """Broadcast a custom event to ALL authenticated nodes."""
        msg = BroadcastMessage(event=event, data=data)
        encoded = protocol.encode(msg)
        
        coros = [self._send_raw(node, encoded) for node in self._nodes.values()]
        if coros:
            await asyncio.gather(*coros, return_exceptions=True)

    async def broadcast_to_room(self, room: str, event: str, data: Any) -> None:
        """Broadcast a custom event to a specific room/namespace."""
        node_ids = self._rooms.get(room, set())
        if not node_ids:
            return

        msg = BroadcastMessage(event=event, data=data)
        encoded = protocol.encode(msg)

        coros = []
        for node_id in node_ids:
            node = self._nodes.get(node_id)
            if node:
                coros.append(self._send_raw(node, encoded))

        if coros:
            await asyncio.gather(*coros, return_exceptions=True)

    def stats(self) -> Dict[str, Any]:
        """Return live stats: node count, stream count, msg/s."""
        conflicts = sum(getattr(s, "conflicts_resolved", 0) for s in self._streams.values())
        
        streams_stats = []
        for name, s in self._streams.items():
            streams_stats.append({
                "name": name,
                "subscribers": len(s._subscribers),
                "last_updated": f"{int((time.time() - s._ts) * 1000)}ms ago" if s._ts > 0 else "never"
            })

        return {
            "nodes": len(self._nodes),
            "streams_count": len(self._streams),
            "messages_per_second": self._msg_per_sec,
            "total_messages": self._total_messages,
            "uptime_seconds": int(time.time() - self._start_time) if self._start_time > 0 else 0,
            "conflicts_resolved": conflicts,
            "streams": streams_stats,
        }

    async def _stats_tracker(self) -> None:
        """Background loop updating msg/s statistics."""
        try:
            while True:
                await asyncio.sleep(1.0)
                self._msg_per_sec = self._msg_counter_current
                self._msg_counter_current = 0
                await self.broadcast_to_room("monitor", "stats", self.stats())
        except asyncio.CancelledError:
            pass

    async def _send_raw(self, websocket: WebSocketServerProtocol, encoded: str) -> None:
        """Send a raw encoded string over a WebSocket, serialized using a connection lock."""
        lock = self._ws_locks.get(websocket)
        if lock is None:
            lock = self._ws_locks.setdefault(websocket, asyncio.Lock())
        async with lock:
            try:
                await websocket.send(encoded)
                self._msg_counter_current += 1
                self._total_messages += 1
            except websockets.exceptions.ConnectionClosed:
                pass

    async def _send_message(self, websocket: WebSocketServerProtocol, msg: FlowSyncMessage) -> None:
        """Encode and send a message over a WebSocket, tracking stats."""
        await self._send_raw(websocket, protocol.encode(msg))

    async def _send_error(self, websocket: WebSocketServerProtocol, code: int, message: str) -> None:
        """Send an ErrorMessage to the client."""
        await self._send_message(websocket, ErrorMessage(code=code, message=message))

    def _mask_addr(self, addr: Any) -> str:
        """Mask client IP addresses in logs for GDPR compliance."""
        if not addr or not isinstance(addr, tuple):
            return str(addr)
        ip = addr[0]
        if ":" in ip:  # IPv6
            parts = ip.split(":")
            return f"{':'.join(parts[:3])}:***:{addr[1]}"
        else:  # IPv4
            parts = ip.split(".")
            if len(parts) == 4:
                return f"{parts[0]}.{parts[1]}.***.***:{addr[1]}"
        return f"***:{addr[1]}"

    def _check_access(self, node_id: str, stream: Stream, action: Literal["read", "write"]) -> bool:
        """Verify whether node_id is authorized to perform action on stream."""
        auth = self._nodes_auth.get(node_id)
        
        # 1. If we have AuthResult, check its permissions first
        if auth and hasattr(auth, "has_permission"):
            if not auth.has_permission(action, stream.name):
                return False
                
        # 2. Check stream's own access restrictions
        allowed_roles = stream.access.get(action, ["*"])
        if "*" in allowed_roles:
            return True
            
        # If stream has restricted roles and user is anonymous / not authenticated, deny
        if not auth:
            return False
            
        # If stream has restricted roles, check user roles/identity
        if hasattr(auth, "roles"):
            if "admin" in auth.roles:
                return True
            if node_id in allowed_roles:
                return True
            for role in auth.roles:
                if role in allowed_roles:
                    return True
            return False
            
        return False

    async def _handle_connection(self, websocket: WebSocketServerProtocol) -> None:
        """Manages the lifecycle and message routing for a single client connection."""
        node_id: Optional[str] = None
        authenticated = False
        addr = websocket.remote_address
        logger.info(f"New connection from {self._mask_addr(addr)}")
        auth_timeout_task = None

        async def enforce_auth_timeout():
            try:
                await asyncio.sleep(10.0)
                if not authenticated:
                    logger.warning(f"Connection from {self._mask_addr(addr)} timed out waiting for AUTH/RECONNECT.")
                    await self._send_error(websocket, 408, "Auth timeout")
                    await websocket.close(code=4008, reason="Auth timeout")
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error in auth timeout task: {e}")

        try:
            # Enforce max connection count
            if len(self._nodes) >= self.max_nodes:
                logger.warning("Max node connection count reached. Rejecting connection.")
                await self._send_error(websocket, 429, "Too many connections")
                await websocket.close(code=4029, reason="Max nodes limit reached")
                return

            auth_timeout_task = asyncio.create_task(enforce_auth_timeout())

            # Wait for AUTH/RECONNECT message
            async for raw_msg in websocket:
                self._msg_counter_current += 1
                self._total_messages += 1
                
                try:
                    msg = protocol.decode(raw_msg)
                except ValueError as e:
                    logger.warning(f"Malformed message from {self._mask_addr(addr)}: {e}")
                    await self._send_error(websocket, 400, "Malformed message")
                    continue

                # Enforce Rate Limiting
                if self.rate_limit:
                    global_lim = self.rate_limit.get("global")
                    node_lim = self.rate_limit.get("node")
                    stream_lim = self.rate_limit.get("stream")
                    
                    stream_name = getattr(msg, "stream", None)
                    lim_node_id = node_id or f"anon:{addr[0] if addr else 'unknown'}"
                    
                    allowed = await self._rate_limiter.check_limit(
                        node_id=lim_node_id,
                        stream_name=stream_name,
                        global_limit=global_lim,
                        node_limit=node_lim,
                        stream_limit=stream_lim,
                    )
                    if not allowed:
                        await self._send_error(websocket, 429, "Rate limit exceeded")
                        continue

                # Enforce authentication
                if not authenticated:
                    if msg.type not in ("AUTH", "RECONNECT"):
                        logger.warning(f"First message from {self._mask_addr(addr)} must be AUTH or RECONNECT. Got {msg.type}.")
                        await self._send_error(websocket, 401, "Authentication required")
                        await websocket.close(code=4001, reason="Authentication required")
                        return

                    if msg.type == "AUTH":
                        # Authenticating
                        token = msg.token
                        req_node_id = msg.node_id

                        if self.auth_fn:
                            try:
                                # Invoke user auth function
                                auth_result = await self.auth_fn(token)
                                if auth_result is None:
                                    logger.warning(f"Authentication failed for token from {self._mask_addr(addr)}")
                                    await self._send_message(websocket, AuthFailMessage(reason="Invalid token"))
                                    await websocket.close(code=4003, reason="Authentication failed")
                                    return
                                if hasattr(auth_result, "node_id"):
                                    node_id = auth_result.node_id
                                else:
                                    node_id = str(auth_result)
                            except Exception as e:
                                logger.error(f"Error executing auth_fn: {e}")
                                await self._send_message(websocket, AuthFailMessage(reason="Internal auth error"))
                                await websocket.close(code=4500, reason="Internal auth error")
                                return
                        else:
                            # Auto-authenticate
                            node_id = req_node_id or f"node_{uuid.uuid4().hex[:6]}"
                            auth_result = AuthResult(node_id=node_id)

                        # If node already connected, close old session
                        if node_id in self._nodes:
                            logger.warning(f"Node '{node_id}' already connected. Disconnecting old session.")
                            old_ws = self._nodes[node_id]
                            await old_ws.close(code=4000, reason="Connected from another location")
                        
                        self._nodes[node_id] = websocket
                        self._nodes_auth[node_id] = auth_result
                        self._subscriptions[node_id] = {}
                        authenticated = True
                        if auth_timeout_task:
                            auth_timeout_task.cancel()
                        logger.info(f"Node '{node_id}' successfully authenticated")
                        await self._send_message(websocket, AuthOkMessage(node_id=node_id))
                        continue

                    elif msg.type == "RECONNECT":
                        node_id = msg.node_id
                        
                        # Validate token if authentication is enabled
                        if self.auth_fn:
                            token = getattr(msg, "token", None)
                            if not token:
                                logger.warning(f"RECONNECT from {self._mask_addr(addr)} missing token.")
                                await self._send_message(websocket, AuthFailMessage(reason="Token required for reconnect"))
                                await websocket.close(code=4001, reason="Token required")
                                return
                            try:
                                auth_result = await self.auth_fn(token)
                                if auth_result is None:
                                    logger.warning(f"Authentication failed for reconnect token from {self._mask_addr(addr)}")
                                    await self._send_message(websocket, AuthFailMessage(reason="Invalid reconnect token"))
                                    await websocket.close(code=4003, reason="Authentication failed")
                                    return
                                
                                # Validate node_id match
                                auth_node_id = auth_result.node_id if hasattr(auth_result, "node_id") else str(auth_result)
                                if auth_node_id != node_id:
                                    logger.warning(f"Reconnect identity mismatch: token identity '{auth_node_id}' != claimed node_id '{node_id}'")
                                    await self._send_message(websocket, AuthFailMessage(reason="Node identity mismatch"))
                                    await websocket.close(code=4003, reason="Identity mismatch")
                                    return
                            except Exception as e:
                                logger.error(f"Error executing auth_fn on reconnect: {e}")
                                await self._send_message(websocket, AuthFailMessage(reason="Internal auth error"))
                                await websocket.close(code=4500, reason="Internal auth error")
                                return
                        else:
                            auth_result = AuthResult(node_id=node_id)
                        
                        # Verify we don't have duplicate connections
                        if node_id in self._nodes:
                            logger.warning(f"Node '{node_id}' already connected. Disconnecting old session.")
                            old_ws = self._nodes[node_id]
                            await old_ws.close(code=4000, reason="Connected from another location")
                            
                        self._nodes[node_id] = websocket
                        self._nodes_auth[node_id] = auth_result
                        self._subscriptions[node_id] = {}
                        authenticated = True
                        if auth_timeout_task:
                            auth_timeout_task.cancel()
                        logger.info(f"Node '{node_id}' successfully authenticated via RECONNECT")

                        # Apply client's pending pushes
                        MAX_PENDING = 1000
                        for change in msg.pending[:MAX_PENDING]:
                            s_name = change.get("stream")
                            if not s_name or not isinstance(s_name, str):
                                continue
                            val = change.get("value")
                            ts = change.get("timestamp")
                            if not isinstance(ts, (int, float)):
                                ts = time.time()
                                
                            if not re.match(r'^[a-zA-Z0-9_:.\-/]{1,256}$', s_name):
                                continue
                            
                            try:
                                s = self.stream(s_name)
                                if self._check_access(node_id, s, "write"):
                                    await s.push(val, {"node_id": node_id, "ts": ts})
                            except Exception as e:
                                logger.error(f"Error replaying pending change for '{s_name}': {e}")
                                
                        # Gather missed updates since last_seen_ts
                        missed_updates = []
                        for s in self._streams.values():
                            if self._check_access(node_id, s, "read"):
                                s_history = await s.history(limit=s.history_limit)
                                for entry in s_history:
                                    if entry["ts"] > msg.last_seen_ts and entry["node_id"] != node_id:
                                        missed_updates.append({
                                            "stream": s.name,
                                            "value": entry["value"],
                                            "timestamp": entry["ts"],
                                            "node_id": entry["node_id"],
                                            "metadata": {**(entry.get("metadata") or {}), "merge_rule": s.merge_rule}
                                        })
                                        
                        # Sort missed updates by timestamp
                        missed_updates.sort(key=lambda x: x["timestamp"])
                        
                        # Send ACK reply
                        await self._send_message(websocket, ReconnectAckMessage(missed_updates=missed_updates))
                        continue

                # Process post-AUTH messages
                if msg.type == "PING":
                    await self._send_message(websocket, PongMessage())

                elif msg.type == "PUSH":
                    # Validate stream name format
                    if not re.match(r'^[a-zA-Z0-9_:.\-/]{1,256}$', msg.stream):
                        await self._send_error(websocket, 400, "Invalid stream name")
                        continue
                    try:
                        s = self.stream(msg.stream)
                    except ValueError as ve:
                        await self._send_error(websocket, 400, str(ve))
                        continue

                    if not self._check_access(node_id, s, "write"):
                        await self._send_error(websocket, 403, "Access denied")
                        continue
                    
                    # Push value into Stream. Stream manages LWW comparison and notifies subscribers.
                    # We inject node_id and timestamp from wire message
                    await s.push(msg.value, {"node_id": node_id, "ts": msg.ts, **(msg.metadata or {})})

                elif msg.type == "GET":
                    if not re.match(r'^[a-zA-Z0-9_:.\-/]{1,256}$', msg.stream):
                        await self._send_error(websocket, 400, "Invalid stream name")
                        continue
                    try:
                        s = self.stream(msg.stream)
                    except ValueError as ve:
                        await self._send_error(websocket, 400, str(ve))
                        continue

                    if not self._check_access(node_id, s, "read"):
                        await self._send_error(websocket, 403, "Access denied")
                        continue
                    
                    val = await s.get_raw()
                    await self._send_message(
                        websocket,
                        GetReplyMessage(
                            stream=msg.stream,
                            value=val,
                            req_id=msg.req_id,
                            ts=s._ts,
                            node_id=s._node_id,
                        ),
                    )

                elif msg.type == "SUBSCRIBE":
                    if not re.match(r'^[a-zA-Z0-9_:.\-/]{1,256}$', msg.stream):
                        await self._send_error(websocket, 400, "Invalid stream name")
                        continue
                    try:
                        s = self.stream(msg.stream)
                    except ValueError as ve:
                        await self._send_error(websocket, 400, str(ve))
                        continue

                    if not self._check_access(node_id, s, "read"):
                        await self._send_error(websocket, 403, "Access denied")
                        continue
                    
                    # Prevent duplicate subscription callbacks for the same stream
                    if msg.stream in self._subscriptions.get(node_id, {}):
                        continue

                    # Send immediate current state
                    current_val = await s.get_raw()
                    if current_val is not None or s._ts > 0:
                        await self._send_message(
                            websocket,
                            UpdateMessage(
                                stream=s.name,
                                value=current_val,
                                ts=s._ts,
                                node_id=s._node_id,
                                metadata={**(s._metadata or {}), "merge_rule": s.merge_rule},
                            ),
                        )

                    # Create connection specific callback
                    async def make_update_callback(ws: WebSocketServerProtocol, stream_name: str):
                        async def on_update(val: Any, meta: Dict[str, Any]):
                            # Safeguard: only send if the WebSocket is actually still open
                            if getattr(ws, 'open', True):
                                await self._send_message(
                                    ws,
                                    UpdateMessage(
                                        stream=stream_name,
                                        value=val,
                                        ts=meta.get("ts", time.time()),
                                        node_id=meta.get("node_id", "server"),
                                        metadata=meta,
                                    ),
                                )
                        return on_update

                    cb = await make_update_callback(websocket, s.name)
                    unsub = s.subscribe(cb)
                    self._subscriptions[node_id][s.name] = unsub

                elif msg.type == "UNSUBSCRIBE":
                    unsub = self._subscriptions.get(node_id, {}).pop(msg.stream, None)
                    if unsub:
                        unsub()

                elif msg.type == "JOIN_ROOM":
                    # Validate room name format
                    if not re.match(r'^[a-zA-Z0-9_:.\-/]{1,256}$', msg.room):
                        await self._send_error(websocket, 400, "Invalid room name")
                        continue
                    # Restrict access to the diagnostics 'monitor' room to admin roles only
                    if msg.room == "monitor":
                        auth = self._nodes_auth.get(node_id)
                        if not (auth and hasattr(auth, "roles") and "admin" in auth.roles):
                            logger.warning(f"Node '{node_id}' rejected from monitor room: insufficient permissions")
                            await self._send_error(websocket, 403, "Access denied")
                            continue
                    
                    # Limit total room count
                    MAX_ROOMS = 1000
                    if msg.room not in self._rooms and len(self._rooms) >= MAX_ROOMS:
                        await self._send_error(websocket, 429, "Too many rooms on Hub")
                        continue

                    self._rooms.setdefault(msg.room, set()).add(node_id)
                    logger.debug(f"Node '{node_id}' joined room '{msg.room}'")

                elif msg.type == "LEAVE_ROOM":
                    self._rooms.get(msg.room, set()).discard(node_id)
                    logger.debug(f"Node '{node_id}' left room '{msg.room}'")

                else:
                    await self._send_error(websocket, 400, "Unsupported message type")

        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Connection closed for {self._mask_addr(addr)} (authenticated={authenticated}, node_id='{node_id}')")
        finally:
            if auth_timeout_task:
                auth_timeout_task.cancel()
            # Clean up node connections, subscriptions, and rooms
            if node_id:
                # Ensure we only prune state if this websocket is the one currently registered for the node_id
                if self._nodes.get(node_id) is websocket:
                    self._nodes.pop(node_id, None)
                    self._nodes_auth.pop(node_id, None)
                    
                    # Unsubscribe from all active stream hooks
                    node_subs = self._subscriptions.pop(node_id, {})
                    for stream_name, unsub in node_subs.items():
                        try:
                            unsub()
                        except Exception as e:
                            logger.error(f"Error unsubscribing node '{node_id}' from stream '{stream_name}': {e}")
                    
                    # Leave all rooms
                    for room_name, members in list(self._rooms.items()):
                        members.discard(node_id)
                        if not members:
                            self._rooms.pop(room_name, None)
