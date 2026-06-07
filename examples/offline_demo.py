import asyncio
import os
import sys
import logging

# Configure clean logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

from flowsync import FlowSyncHub, FlowSyncNode

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

async def main():
    # Clean up any database leftovers
    clean_db("alice")
    clean_db("bob")

    # Start hub on port 8765
    hub = FlowSyncHub(host="127.0.0.1", port=8765, log_level="INFO")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.5)

    # Create a collaborative text stream
    doc = hub.stream("doc:text", merge_rule="crdt-text", initial="")

    # Create two nodes
    node_a = FlowSyncNode("ws://127.0.0.1:8765", node_id="alice")
    node_b = FlowSyncNode("ws://127.0.0.1:8765", node_id="bob")

    await node_a.connect()
    await node_b.connect()

    # Subscribe both to see updates
    node_a.on("doc:text", lambda v, m: print(f"Alice sees: '{v}'"))
    node_b.on("doc:text", lambda v, m: print(f"Bob sees:   '{v}'"))

    # Allow subscriptions to propagate
    await asyncio.sleep(0.2)
    print(f"Alice online status: {node_a.is_online}")
    print(f"Bob online status: {node_b.is_online}")

    # Simulate Bob going offline
    await node_b.disconnect()
    print("\n--- Bob is OFFLINE ---")
    print(f"Alice online status: {node_a.is_online}")
    print(f"Bob online status: {node_b.is_online}")

    # Alice types while Bob is offline
    await node_a.push("doc:text", {"op": "insert", "pos": 0, "char": "Hello "})
    print("Alice typed: 'Hello '")

    # Bob types while offline (goes to SQLite queue)
    await node_b.push("doc:text", {"op": "insert", "pos": 0, "char": "World"})
    print("Bob typed: 'World' (queued, offline)")
    print(f"Bob's pending queue: {node_b.pending_count} changes")

    # Bob comes back online
    await asyncio.sleep(1.0)
    print("\n--- Bob is BACK ONLINE ---")
    await node_b.connect()

    # Wait for sync
    await asyncio.sleep(1.0)

    # Both should see same merged result
    final = await node_a.get("doc:text")
    print(f"\nFinal merged text: '{final}'")
    
    # Graceful teardown
    await node_a.disconnect()
    await node_b.disconnect()
    hub.stop()
    await hub_task
    
    clean_db("alice")
    clean_db("bob")

    if final == "Hello World":
        print("✅ No data lost. Conflict resolved automatically.")
    else:
        print(f"❌ State diverged. Got: '{final}'")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
