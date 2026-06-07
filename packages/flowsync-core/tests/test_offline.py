import asyncio
import os
import pytest
import time
from unittest.mock import patch

from flowsync import FlowSyncHub, FlowSyncNode
from flowsync.merge.crdt_counter import CRDTCounter
from flowsync.merge.crdt_set import CRDTSet
from flowsync.merge.crdt_text import CRDTText

# Helper to clean up SQLite files
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
async def test_basic_offline_queue():
    node_id = "test-node-1"
    clean_db(node_id)
    
    node = FlowSyncNode("ws://localhost:8765", node_id=node_id, offline_mode=True)
    # The node is disconnected (offline)
    assert not node.is_online
    
    # Push 5 times while offline
    for i in range(5):
        await node.push("game:score", 10 + i)
        
    # Assert: pending count is 5
    assert node.pending_count == 5
    assert len(node.pending_changes) == 5
    
    # Clean up
    clean_db(node_id)

@pytest.mark.asyncio
async def test_reconnect_and_sync():
    hub = FlowSyncHub(host="127.0.0.1", port=0, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    # Setup stream with "append" merge rule
    hub.stream("logs:events", merge_rule="append", initial=[])

    # Connect Node A and B
    node_a_id = "node-a-sync"
    node_b_id = "node-b-sync"
    clean_db(node_a_id)
    clean_db(node_b_id)

    node_a = FlowSyncNode(url, node_id=node_a_id)
    node_b = FlowSyncNode(url, node_id=node_b_id)
    await node_a.connect()
    await node_b.connect()

    # Bob (Node B) goes offline
    await node_b.disconnect()
    
    # Alice (Node A) pushes "hello" while B is offline
    await node_a.push("logs:events", "hello")
    await asyncio.sleep(0.1)

    # Bob (Node B) pushes "world" while offline (goes to SQLite queue)
    await node_b.push("logs:events", "world")
    assert node_b.pending_count == 1

    # Bob comes back online (reconnects)
    await node_b.connect()
    await asyncio.sleep(0.2)  # wait for synchronization

    # Verify both nodes have the same merged value ["hello", "world"]
    val_a = await node_a.get("logs:events")
    val_b = await node_b.get("logs:events")
    
    assert val_a == ["hello", "world"]
    assert val_b == ["hello", "world"]

    # Clean up
    await node_a.disconnect()
    await node_b.disconnect()
    hub.stop()
    await hub_task
    clean_db(node_a_id)
    clean_db(node_b_id)

def test_crdt_counter_merge():
    # Counter A: increment(5), increment(3)
    counter_a = CRDTCounter(node_id="node-A", initial=0)
    counter_a.increment(5)
    counter_a.increment(3)

    # Counter B: increment(2), decrement(1)
    counter_b = CRDTCounter(node_id="node-B", initial=0)
    counter_b.increment(2)
    counter_b.decrement(1)

    # Merge A and B
    merged_state = CRDTCounter.merge(counter_a.to_state(), counter_b.to_state())
    
    # Reconstruct merged counter
    merged = CRDTCounter.from_state(merged_state, "node-A")
    assert merged.value == 9

def test_crdt_set_merge():
    # Set A: add("alice"), add("bob")
    set_a = CRDTSet()
    set_a.add("alice")
    set_a.add("bob")

    # Set B: add("alice"), remove("alice"), add("charlie")
    set_b = CRDTSet()
    set_b.add("alice")
    set_b.remove("alice")
    set_b.add("charlie")

    # Merge A and B
    merged_state = CRDTSet.merge(set_a.to_state(), set_b.to_state())
    merged = CRDTSet.from_state(merged_state)

    assert "alice" not in merged.items  # remove wins
    assert "bob" in merged.items
    assert "charlie" in merged.items

def test_crdt_text_concurrent_insert():
    # Node A inserts "Hello" at pos 0
    text_a = CRDTText()
    op1 = text_a.insert(pos=0, char="Hello", node_id="node-a")

    # Node B inserts " World" at pos 0 concurrently
    # Mock similar timestamps
    text_b = CRDTText()
    op2 = text_b.insert(pos=0, char=" World", node_id="node-b")
    
    # Force identical timestamps to test tie-breaking
    ts = time.time()
    op1["ts"] = ts
    op2["ts"] = ts

    # Merge both operation lists
    merged_ops = CRDTText.merge_ops([op1], [op2])
    final_text = CRDTText.apply_all(merged_ops)

    # Result should be either "Hello World" or " WorldHello" consistently
    assert final_text in ("Hello World", " WorldHello")

@pytest.mark.asyncio
async def test_queue_survives_restart():
    node_id = "test-node-restart"
    clean_db(node_id)

    # 1. Instantiate node and push 3 changes offline
    node = FlowSyncNode("ws://localhost:8765", node_id=node_id, offline_mode=True)
    await node.push("game:score", 100)
    await node.push("game:score", 200)
    await node.push("game:score", 300)
    
    assert node.pending_count == 3

    # 2. Re-instantiate FlowSyncNode representing a process restart
    restarted_node = FlowSyncNode("ws://localhost:8765", node_id=node_id, offline_mode=True)
    
    # Assert: pending count is still 3
    assert restarted_node.pending_count == 3
    
    # Clean up
    clean_db(node_id)

@pytest.mark.asyncio
async def test_exponential_backoff():
    attempts = 0
    async def mock_connect_and_auth(self):
        nonlocal attempts
        attempts += 1
        if attempts <= 3:
            raise ConnectionError("Hub down")
        self._is_online = True
        self._auth_future.set_result(self.node_id)

    sleep_calls = []
    original_sleep = asyncio.sleep
    async def mock_sleep(delay):
        sleep_calls.append(delay)
        await original_sleep(0.001)

    node = FlowSyncNode("ws://localhost:9999", node_id="test-backoff", reconnect=True)
    node._was_authenticated = True

    with patch.object(FlowSyncNode, "_connect_and_auth", mock_connect_and_auth), \
         patch("asyncio.sleep", mock_sleep):
        
        # Trigger reconnection loop
        await node._reconnect_loop()
        
        assert attempts == 4
        assert len(sleep_calls) >= 3
        # Should backoff exponentially starting at 2.0, 4.0, 8.0
        assert sleep_calls[0] == 2.0
        assert sleep_calls[1] == 4.0
        assert sleep_calls[2] == 8.0


@pytest.mark.asyncio
async def test_crdt_text_offline_sync():
    hub = FlowSyncHub(host="127.0.0.1", port=0, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    # Setup stream with "crdt-text" merge rule
    hub.stream("doc:text", merge_rule="crdt-text", initial="")

    # Connect Node A and B
    node_a_id = "node-a-text"
    node_b_id = "node-b-text"
    clean_db(node_a_id)
    clean_db(node_b_id)

    node_a = FlowSyncNode(url, node_id=node_a_id)
    node_b = FlowSyncNode(url, node_id=node_b_id)
    await node_a.connect()
    await node_b.connect()

    # Subscribe to trigger state caching
    node_a.on("doc:text", lambda v, m: None)
    node_b.on("doc:text", lambda v, m: None)
    await asyncio.sleep(0.1)

    # Bob (Node B) goes offline
    await node_b.disconnect()
    
    # Alice (Node A) pushes "Hello " while B is offline
    await node_a.push("doc:text", {"op": "insert", "pos": 0, "char": "Hello "})
    await asyncio.sleep(0.1)

    # Bob (Node B) pushes "World" while offline
    await node_b.push("doc:text", {"op": "insert", "pos": 0, "char": "World"})
    assert node_b.pending_count == 1

    # Bob comes back online (reconnects)
    await node_b.connect()
    await asyncio.sleep(0.2)  # wait for synchronization

    # Verify both nodes have the same merged value "Hello World"
    val_a = await node_a.get("doc:text")
    val_b = await node_b.get("doc:text")
    
    assert val_a == "Hello World"
    assert val_b == "Hello World"

    # Clean up
    await node_a.disconnect()
    await node_b.disconnect()
    hub.stop()
    await hub_task
    clean_db(node_a_id)
    clean_db(node_b_id)
