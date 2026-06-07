import asyncio
import pytest
import time
from flowsync import FlowSyncHub, FlowSyncNode

@pytest.mark.asyncio
async def test_hub_and_node_sync():
    # 1. Start Hub on dynamic port
    hub = FlowSyncHub(host="127.0.0.1", port=0, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    # Wait for server to bind
    await asyncio.sleep(0.1)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    # 2. Connect client nodes
    node_a = FlowSyncNode(hub_url=url, node_id="node-a")
    node_b = FlowSyncNode(hub_url=url, node_id="node-b")
    await node_a.connect()
    await node_b.connect()

    assert node_a.is_online
    assert node_b.is_online

    # 3. Test push and subscribe sync
    received = []
    def on_change(val, meta):
        received.append(val)
        
    node_b.on("test-stream", on_change)
    await asyncio.sleep(0.1)  # allow subscription message to be processed

    await node_a.push("test-stream", "hello-world")
    await asyncio.sleep(0.1)  # allow push and update messages to propagate

    assert "hello-world" in received

    # 4. Test GET
    val = await node_b.get("test-stream")
    assert val == "hello-world"

    # 5. Test Room Broadcast
    broadcasts = []
    def on_bc(data):
        broadcasts.append(data)
        
    node_b.on_event("broadcast:alert", on_bc)
    node_b.join_room("lobby")
    await asyncio.sleep(0.1)

    await hub.broadcast_to_room("lobby", "alert", {"text": "fire!"})
    await asyncio.sleep(0.1)
    
    assert len(broadcasts) == 1
    assert broadcasts[0] == {"text": "fire!"}

    # 6. Test Stats
    stats = hub.stats()
    assert stats["nodes"] == 2
    assert stats["streams_count"] == 1

    # 7. Disconnect and clean up
    await node_a.disconnect()
    await node_b.disconnect()
    hub.stop()
    await hub_task


@pytest.mark.asyncio
async def test_auth_fn():
    async def my_auth(token):
        if token == "secret":
            return "vip-node"
        return None

    hub = FlowSyncHub(host="127.0.0.1", port=0, auth_fn=my_auth, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    # Connect with invalid token -> should fail
    node_wrong = FlowSyncNode(hub_url=url, node_id="node-1", auth_token="wrong")
    with pytest.raises(Exception):
        await node_wrong.connect()

    # Connect with correct token -> should succeed
    node_correct = FlowSyncNode(hub_url=url, node_id="node-1", auth_token="secret")
    await node_correct.connect()
    
    assert node_correct.is_online
    assert node_correct.node_id == "vip-node"

    await node_correct.disconnect()
    hub.stop()
    await hub_task


@pytest.mark.asyncio
async def test_stream_authorization():
    hub = FlowSyncHub(host="127.0.0.1", port=0, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    # Define a stream with restricted write access to only "admin-node"
    hub.stream("admin:settings", access={"read": ["*"], "write": ["admin-node"]})

    node_guest = FlowSyncNode(hub_url=url, node_id="guest-node")
    node_admin = FlowSyncNode(hub_url=url, node_id="admin-node")
    await node_guest.connect()
    await node_admin.connect()

    # Admin pushes successfully
    await node_admin.push("admin:settings", {"theme": "dark"})
    await asyncio.sleep(0.1)
    
    val = await node_admin.get("admin:settings")
    assert val == {"theme": "dark"}

    # Guest pushes -> should be ignored because guest does not have write access
    await node_guest.push("admin:settings", {"theme": "light"})
    await asyncio.sleep(0.1)

    # Value should remain unchanged (still "dark")
    val_after = await node_admin.get("admin:settings")
    assert val_after == {"theme": "dark"}

    await node_guest.disconnect()
    await node_admin.disconnect()
    hub.stop()
    await hub_task
