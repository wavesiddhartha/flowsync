import asyncio
import os
import pytest
import time
import json
import jwt
from flowsync import FlowSyncHub, FlowSyncNode
from flowsync.auth import AuthResult, jwt_auth
from flowsync.merge.crdt_set import CRDTSet
from flowsync.merge.crdt_text import CRDTText
from flowsync.transport import protocol

SECRET = "super-secret-key"

def clean_db(node_id):
    db_path = os.path.expanduser(f"~/.flowsync/{node_id}_queue.db")
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            os.remove(db_path + "-shm")
            os.remove(db_path + "-wal")
        except OSError:
            pass

@pytest.mark.asyncio
async def test_concurrent_push_crdt_set_multi_user():
    # 1. Start Hub
    hub = FlowSyncHub(host="127.0.0.1", port=0, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    # Setup CRDT Set stream
    hub.stream("shared:set", merge_rule="crdt-set", initial={"added": [], "removed": []})

    # 2. Connect 3 nodes concurrently
    node_ids = ["alice", "bob", "charlie"]
    nodes = []
    for node_id in node_ids:
        clean_db(node_id)
        node = FlowSyncNode(url, node_id=node_id)
        await node.connect()
        nodes.append(node)

    # 3. Concurrently push to the CRDT set in a loop
    async def push_loop(node):
        for i in range(10):
            # In CRDT Set, push uses the operation format: {"op": "add", "item": ...}
            await node.push("shared:set", {"op": "add", "item": f"{node.node_id}-{i}"})
            await asyncio.sleep(0.01)

    # Run pushing concurrently
    await asyncio.gather(*(push_loop(node) for node in nodes))
    await asyncio.sleep(0.3)  # Wait for sync propagation

    # 4. Read final state from each node and assert convergence
    val_alice = await nodes[0].get("shared:set")
    val_bob = await nodes[1].get("shared:set")
    val_charlie = await nodes[2].get("shared:set")

    # Assert they converged to the same elements
    assert val_alice == val_bob == val_charlie
    assert len(val_alice["added"]) == 30  # 3 nodes * 10 items each

    # Cleanup
    for node in nodes:
        await node.disconnect()
    hub.stop()
    await hub_task
    for node_id in node_ids:
        clean_db(node_id)


@pytest.mark.asyncio
async def test_stream_sql_injection_and_path_traversal():
    # Attempt SQL injection and path traversal payloads on stream name and node ID
    injection_node_id = "node_id_inject' OR '1'='1'-- /..\\../"
    injection_stream = "stream' OR '1'='1'-- /..\\../"

    clean_db(injection_node_id)

    node = FlowSyncNode("ws://127.0.0.1:0", node_id=injection_node_id, offline_mode=True)
    
    # 1. Verify path traversal in node database resolution was sanitized
    # safe_node_id will replace special chars with underscores
    assert "../" not in node.db_path
    assert ".." not in os.path.basename(node.db_path)

    # 2. Verify pushing with special characters to SQLite queue
    # SQLite parameters should escape single quotes safely
    await node.push(injection_stream, {"key": "value"})
    assert node.pending_count == 1

    # Verify we can drain it back
    drained = await node._offline_queue.drain()
    assert len(drained) == 1
    assert drained[0]["stream"] == injection_stream
    assert drained[0]["value"] == {"key": "value"}

    # Clean up
    await node._offline_queue.clear()
    clean_db(injection_node_id)


@pytest.mark.asyncio
async def test_reconnect_expired_or_invalid_token():
    hub = FlowSyncHub(host="127.0.0.1", port=0, auth_fn=jwt_auth(SECRET))
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    # Create node with valid token
    token = jwt.encode({"sub": "alice", "roles": ["user"]}, SECRET, algorithm="HS256")
    node_id = "alice-expired-test"
    clean_db(node_id)

    node = FlowSyncNode(url, node_id=node_id, auth_token=token)
    await node.connect()
    assert node.is_online

    # Disconnect node
    await node.disconnect()
    assert not node.is_online

    # Alter auth function on Hub to reject this user dynamically
    def invalid_auth(token):
        return AuthResult(node_id="", roles=[], permissions=[])  # Deny all
    hub.auth_fn = invalid_auth

    # Try to reconnect. Since the token is now rejected, connect should fail
    with pytest.raises(Exception):
        await node.connect()

    assert not node.is_online

    # Cleanup
    hub.stop()
    await hub_task
    clean_db(node_id)


@pytest.mark.asyncio
async def test_crdt_text_complex_interleaving_and_compaction():
    hub = FlowSyncHub(host="127.0.0.1", port=0, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    hub.stream("shared:text", merge_rule="crdt-text", initial="")

    # Connect Alice, Bob, Charlie
    node_a_id = "alice-text"
    node_b_id = "bob-text"
    node_c_id = "charlie-text"
    clean_db(node_a_id)
    clean_db(node_b_id)
    clean_db(node_c_id)

    node_a = FlowSyncNode(url, node_id=node_a_id)
    node_b = FlowSyncNode(url, node_id=node_b_id)
    node_c = FlowSyncNode(url, node_id=node_c_id)

    await node_a.connect()
    await node_b.connect()
    await node_c.connect()

    # Subscribe all nodes to state changes
    node_a.on("shared:text", lambda v, m: None)
    node_b.on("shared:text", lambda v, m: None)
    node_c.on("shared:text", lambda v, m: None)
    await asyncio.sleep(0.1)

    # Bob and Charlie concurrently edit offline
    await node_b.disconnect()
    await node_c.disconnect()

    # Alice inserts "A" at pos 0 online
    await node_a.push("shared:text", {"op": "insert", "pos": 0, "char": "A"})
    await asyncio.sleep(0.1)

    # Bob concurrently inserts "B" at pos 0 offline
    await node_b.push("shared:text", {"op": "insert", "pos": 0, "char": "B"})
    
    # Charlie concurrently deletes pos 0 offline
    await node_c.push("shared:text", {"op": "delete", "pos": 0, "len": 1})

    # Reconnect Bob and Charlie concurrently
    await asyncio.gather(node_b.connect(), node_c.connect())
    await asyncio.sleep(0.3)  # Wait for sync

    # Verify convergence
    val_a = await node_a.get("shared:text")
    val_b = await node_b.get("shared:text")
    val_c = await node_c.get("shared:text")

    # They should all converge on the same string
    assert val_a == val_b == val_c

    # Cleanup
    await node_a.disconnect()
    await node_b.disconnect()
    await node_c.disconnect()
    hub.stop()
    await hub_task
    clean_db(node_a_id)
    clean_db(node_b_id)
    clean_db(node_c_id)
