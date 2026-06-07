import asyncio
import pytest
import time
from flowsync.stream import Stream

@pytest.mark.asyncio
async def test_stream_initialization():
    s = Stream(name="test:state", initial=42, history_limit=5)
    assert s.name == "test:state"
    assert await s.get() == 42
    h = await s.history()
    assert len(h) == 1
    assert h[0]["value"] == 42
    assert h[0]["node_id"] == "system"

@pytest.mark.asyncio
async def test_stream_push_lww():
    s = Stream(name="test:state", initial=10, merge_rule="lww")
    
    # Push with a higher timestamp should succeed
    now = time.time()
    await s.push(20, {"ts": now, "node_id": "node-1"})
    assert await s.get() == 20
    
    # Push with a lower timestamp should be ignored (LWW conflict resolution)
    await s.push(15, {"ts": now - 10, "node_id": "node-2"})
    assert await s.get() == 20
    
    # Push with equal timestamp but lexicographically lower node_id should be ignored
    await s.push(30, {"ts": now, "node_id": "node-0"})
    assert await s.get() == 20

    # Push with equal timestamp but lexicographically higher node_id should win
    await s.push(40, {"ts": now, "node_id": "node-2"})
    assert await s.get() == 40

@pytest.mark.asyncio
async def test_stream_subscription():
    s = Stream(name="test:state", initial=0)
    received = []
    
    def on_change(val, meta):
        received.append((val, meta["node_id"]))

    unsub = s.subscribe(on_change)
    
    await s.push(5, {"node_id": "node-a"})
    await s.push(10, {"node_id": "node-b"})
    
    # Unsubscribe
    unsub()
    await s.push(15, {"node_id": "node-c"})
    
    # Wait briefly for callbacks
    await asyncio.sleep(0.01)
    
    assert len(received) == 2
    assert received[0] == (5, "node-a")
    assert received[1] == (10, "node-b")

@pytest.mark.asyncio
async def test_stream_history_limit():
    s = Stream(name="test:state", initial=0, history_limit=3)
    
    await s.push(1, {"ts": 1.0})
    await s.push(2, {"ts": 2.0})
    await s.push(3, {"ts": 3.0})
    await s.push(4, {"ts": 4.0})
    
    h = await s.history(limit=10)
    assert len(h) == 3
    assert [entry["value"] for entry in h] == [2, 3, 4]

@pytest.mark.asyncio
async def test_stream_delete():
    s = Stream(name="test:state", initial=100)
    
    deleted_called = False
    def on_update(val, meta):
        nonlocal deleted_called
        if meta.get("deleted"):
            deleted_called = True
            
    s.subscribe(on_update)
    await s.delete()
    
    assert await s.get() is None
    assert deleted_called
    assert len(await s.history()) == 0
