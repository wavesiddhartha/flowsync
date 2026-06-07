import asyncio
import pytest
import time
import random
import os
from flowsync import FlowSyncHub, FlowSyncNode

@pytest.mark.asyncio
async def test_stress_concurrent_pushes():
    """Test 1000+ concurrent pushes on a stream from multiple tasks."""
    hub = FlowSyncHub(host="127.0.0.1", port=0, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    url = f"ws://127.0.0.1:{hub.bound_port}"

    # Connect 5 pusher nodes
    pushers = []
    for i in range(5):
        node = FlowSyncNode(hub_url=url, node_id=f"pusher-{i}")
        await node.connect()
        pushers.append(node)

    # Pre-register a stream
    hub.stream("stress:lww", merge_rule="lww")

    # Queue 1000 pushes (200 per node)
    async def push_loop(node, count):
        for idx in range(count):
            await node.push("stress:lww", f"value-{node.node_id}-{idx}")
            # Yield control occasionally
            if idx % 10 == 0:
                await asyncio.sleep(0.001)

    tasks = [asyncio.create_task(push_loop(node, 200)) for node in pushers]
    await asyncio.gather(*tasks)
    
    # Wait for all updates to be processed
    await asyncio.sleep(0.5)

    # Verify state is valid
    val = await pushers[0].get("stress:lww")
    assert val.startswith("value-pusher-")

    # Teardown
    for node in pushers:
        await node.disconnect()
    hub.stop()
    await hub_task


@pytest.mark.asyncio
async def test_stress_crdt_counter():
    """Test 1000+ concurrent increments/decrements on a CRDT counter stream."""
    hub = FlowSyncHub(host="127.0.0.1", port=0, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    url = f"ws://127.0.0.1:{hub.bound_port}"

    # Pre-register collaborative counter
    hub.stream("stress:counter", merge_rule="crdt-counter")

    # Connect 10 nodes
    nodes = []
    for i in range(10):
        node = FlowSyncNode(hub_url=url, node_id=f"counter-node-{i}")
        await node.connect()
        nodes.append(node)

    # Subscribe node 0 to track the total sum
    sum_tracker = []
    def on_change(val, meta):
        sum_tracker.append(val)
    nodes[0].on("stress:counter", on_change)
    await asyncio.sleep(0.1)

    # 100 increments per node = 1000 total operations
    async def increment_loop(node):
        for _ in range(100):
            await node.push("stress:counter", {"op": "increment", "amount": 1})
            # Stagger slightly to force concurrent merging
            await asyncio.sleep(0.001)

    tasks = [asyncio.create_task(increment_loop(n)) for n in nodes]
    await asyncio.gather(*tasks)
    
    await asyncio.sleep(0.5)

    # Verify value is exactly 1000
    final_val = await nodes[0].get("stress:counter")
    assert final_val == 1000

    # Cleanup
    for n in nodes:
        await n.disconnect()
    hub.stop()
    await hub_task


@pytest.mark.asyncio
async def test_stress_offline_sqlite_queue():
    """Stress testing the SQLite offline transaction queue with 1000+ buffered writes."""
    db_path = "stress_offline_test.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    hub = FlowSyncHub(host="127.0.0.1", port=0, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    url = f"ws://127.0.0.1:{hub.bound_port}"

    hub.stream("stress:offline", merge_rule="lww")

    # Create a node with a persistent SQLite database
    node = FlowSyncNode(hub_url=url, node_id="offline-stress-node", db_path=db_path)
    await node.connect()
    
    # Simulate network failure by disconnecting the node from socket layer, but keep offline queue
    await node.disconnect()
    assert not node.is_online

    # Buffer 1000 operations locally in SQLite while offline
    logger_tasks = []
    for i in range(1000):
        logger_tasks.append(node.push("stress:offline", f"offline-val-{i}"))
    
    await asyncio.gather(*logger_tasks)

    # Check that the pending count matches 1000
    count = await node.pending_count_async()
    assert count == 1000

    # Reconnect node to target URL
    await node.connect()
    assert node.is_online

    # Give it a second to replay all 1000 events over WebSocket
    await asyncio.sleep(1.5)

    # SQLite queue should now be empty (processed)
    count_after = await node.pending_count_async()
    assert count_after == 0

    # Verify latest value matches the final write
    latest_val = await node.get("stress:offline")
    assert latest_val == "offline-val-999"

    # Cleanup
    await node.disconnect()
    hub.stop()
    await hub_task
    
    # Remove temp db files
    for suffix in ["", "-shm", "-wal"]:
        path = db_path + suffix
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass


@pytest.mark.asyncio
async def test_stress_rate_limiting():
    """Verify rate limiter thread-safety and functionality under a burst of 1000+ queries."""
    hub = FlowSyncHub(host="127.0.0.1", port=0, rate_limit={"global": (200.0, 200.0)}, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.1)
    url = f"ws://127.0.0.1:{hub.bound_port}"

    hub.stream("stress:limited", merge_rule="lww")

    node = FlowSyncNode(hub_url=url, node_id="bursty-node")
    await node.connect()

    # Subscribe node
    received = []
    node.on("stress:limited", lambda v, m: received.append(v))
    await asyncio.sleep(0.1)

    # Fire 1000 pushes rapidly in parallel
    pushed = []
    async def push_one(idx):
        try:
            await node.push("stress:limited", f"burst-{idx}")
            pushed.append(idx)
        except Exception:
            pass # rate limiter rejected connection closure or socket write failure

    tasks = [asyncio.create_task(push_one(i)) for i in range(1000)]
    await asyncio.gather(*tasks)

    # Give some buffer for final processing
    await asyncio.sleep(0.5)

    # The global rate limit is set to 200/s, so out of 1000 requests, a significant portion must be rejected
    # (since the bucket size will be depleted immediately).
    # Verify that not all 1000 messages successfully completed/propagated due to rate limiting
    assert len(received) < 1000

    # Cleanup
    await node.disconnect()
    hub.stop()
    await hub_task
