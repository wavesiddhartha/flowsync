import asyncio
import jwt
import pytest
import time
from flowsync import FlowSyncHub, FlowSyncNode
from flowsync.auth import AuthResult, jwt_auth

SECRET = "super-secret-key"

@pytest.mark.asyncio
async def test_jwt_authentication_flow():
    # 1. Start Hub with jwt_auth helper
    hub = FlowSyncHub(host="127.0.0.1", port=0, auth_fn=jwt_auth(SECRET))
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    # 2. Try to connect with invalid token
    node_invalid = FlowSyncNode(url, node_id="bad-boy", auth_token="invalid-token")
    with pytest.raises(Exception):
        await node_invalid.connect()

    # 3. Connect with valid token
    token = jwt.encode({"sub": "alice", "roles": ["user"]}, SECRET, algorithm="HS256")
    node_valid = FlowSyncNode(url, node_id="alice", auth_token=token)
    await node_valid.connect()
    assert node_valid.is_online

    # Cleanup
    await node_valid.disconnect()
    hub.stop()
    await hub_task


@pytest.mark.asyncio
async def test_wildcard_permissions():
    # Verify AuthResult wildcard matching logic directly
    auth = AuthResult(
        node_id="test-node",
        roles=["user"],
        permissions=["read:game:*", "write:game:score"]
    )

    # Allowed reads
    assert auth.can_read("game:score") is True
    assert auth.can_read("game:lobby") is True
    
    # Denied reads
    assert auth.can_read("admin:logs") is False

    # Allowed writes
    assert auth.can_write("game:score") is True

    # Denied writes
    assert auth.can_write("game:lobby") is False


@pytest.mark.asyncio
async def test_rate_limiting():
    # Start Hub with low rate limits: capacity of 2, refill of 1 token per second
    rate_limit = {
        "global": (2.0, 1.0)
    }
    hub = FlowSyncHub(host="127.0.0.1", port=0, rate_limit=rate_limit)
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    # Connect Node
    node = FlowSyncNode(url, node_id="client-rate")
    await node.connect()

    # Setup a stream
    hub.stream("chat:room")

    # Send 2 updates (must succeed)
    await node.push("chat:room", "msg 1")
    await node.push("chat:room", "msg 2")

    # The 3rd update should trigger rate limiting and fail or receive a 429 error
    # Let's listen to errors on node
    errors = []
    node.on_event("error", lambda err: errors.append(err))
    
    await node.push("chat:room", "msg 3")
    await asyncio.sleep(0.2)

    # Assert: rate limiting was triggered
    assert len(errors) > 0
    assert any("Rate limit exceeded" in str(e) for e in errors)

    # Cleanup
    await node.disconnect()
    hub.stop()
    await hub_task


@pytest.mark.asyncio
async def test_anonymous_access_to_restricted_stream():
    # 1. Start Hub
    hub = FlowSyncHub(host="127.0.0.1", port=0)
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    # 2. Setup restricted stream (only admins allowed to write)
    hub.stream("secret-stream", access={"read": ["*"], "write": ["admin"]})

    # 3. Connect anonymous node
    node = FlowSyncNode(url, node_id="anon-client", auth_token="anonymous")
    await node.connect()

    # 4. Try to write (must fail with access denied error)
    errors = []
    node.on_event("error", lambda err: errors.append(err))
    await node.push("secret-stream", "hacked")
    await asyncio.sleep(0.2)

    assert len(errors) > 0
    assert any("Access denied" in str(e) for e in errors)

    # Cleanup
    await node.disconnect()
    hub.stop()
    await hub_task


def test_payload_size_limit():
    from flowsync.transport import protocol
    
    # 1. Test acceptable small payload
    small_payload = '{"type": "PING"}'
    decoded = protocol.decode(small_payload)
    assert decoded.type == "PING"

    # 2. Test large payload exceeding 10MB
    large_payload = '{"type": "PING", "padding": "' + ("x" * 11 * 1024 * 1024) + '"}'
    with pytest.raises(ValueError, match="Payload size exceeds maximum limit of 10MB"):
        protocol.decode(large_payload)


def test_cli_prototype_pollution_sanitizer():
    from flowsync.cli import sanitize_json

    dangerous_input = {
        "user": "alice",
        "__proto__": {"admin": True},
        "details": {
            "constructor": "malicious",
            "age": 30,
            "prototype": "dangerous"
        },
        "list": [
            {"__proto__": "polluted"},
            "clean"
        ]
    }

    sanitized = sanitize_json(dangerous_input)

    assert "user" in sanitized
    assert sanitized["user"] == "alice"
    assert "__proto__" not in sanitized
    assert "constructor" not in sanitized["details"]
    assert "prototype" not in sanitized["details"]
    assert sanitized["details"]["age"] == 30
    assert "__proto__" not in sanitized["list"][0]
    assert sanitized["list"][1] == "clean"


@pytest.mark.asyncio
async def test_async_pending_count():
    # Verify that the async pending count method works without blocking
    node = FlowSyncNode("ws://127.0.0.1:0", node_id="test-count-node", offline_mode=True)
    await node._offline_queue.clear()
    
    # Push 3 items offline
    await node.push("s1", "v1")
    await node.push("s2", "v2")
    await node.push("s3", "v3")

    async_count = await node.pending_count_async()
    assert async_count == 3
    assert node.pending_count == 3

    await node._offline_queue.clear()

