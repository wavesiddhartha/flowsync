import asyncio
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../packages/flowsync-core")))

from flowsync import FlowSyncHub, FlowSyncNode

async def main():
    print("==================================================")
    print("FLOWSYNC COLLABORATIVE TEXT EDITOR SIMULATION")
    print("==================================================")

    # 1. Start Hub
    hub = FlowSyncHub(host="127.0.0.1", port=8765, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.5)

    # Initialize CRDT Text stream
    hub.stream("doc:shared", merge_rule="crdt-text", initial=[])

    # 2. Connect Alice and Bob
    alice = FlowSyncNode("ws://127.0.0.1:8765", node_id="alice", offline_mode=True)
    bob = FlowSyncNode("ws://127.0.0.1:8765", node_id="bob", offline_mode=True)
    await alice.connect()
    await bob.connect()

    # Initial state
    await alice.push("doc:shared", {"op": "insert", "pos": 0, "char": "FlowSync"})
    await asyncio.sleep(0.2)
    
    val = await alice.get("doc:shared")
    print(f"Initial document state: '{val}'")
    print("--------------------------------------------------")

    # 3. Simulate Internet outage (Disconnect Bob)
    print("Internet goes down. Bob goes offline...")
    await bob.disconnect()

    # Alice inserts " is fast" at the end of "FlowSync" (index 8)
    print("Alice inserts ' is fast' -> 'FlowSync is fast'")
    await alice.push("doc:shared", {"op": "insert", "pos": 8, "char": " is fast"})
    
    # Bob concurrently inserts "Awesome " at the beginning (index 0)
    print("Bob (offline) inserts 'Awesome ' -> 'Awesome FlowSync'")
    await bob.push("doc:shared", {"op": "insert", "pos": 0, "char": "Awesome "})

    print(f"Alice locally sees: '{await alice.get('doc:shared')}'")
    print(f"Bob locally sees:   '{await bob.get('doc:shared')}' (changes queued)")
    print("--------------------------------------------------")

    # 4. Bob reconnects. FlowSync syncs the offline change queues
    print("Bob reconnects to the network...")
    await bob.connect()
    
    # Wait for sync
    await asyncio.sleep(1.0)

    # Both clients must now see the merged text: "Awesome FlowSync is fast"
    final_alice = await alice.get("doc:shared")
    final_bob = await bob.get("doc:shared")

    print(f"Alice resolved document: '{final_alice}'")
    print(f"Bob resolved document:   '{final_bob}'")
    print("--------------------------------------------------")

    if final_alice == "Awesome FlowSync is fast":
        print("✅ Success: Conflict resolved automatically via Sequence CRDT!")
    else:
        print("❌ Error: Divergence detected.")

    # Teardown
    await alice.disconnect()
    await bob.disconnect()
    hub.stop()
    await hub_task

if __name__ == "__main__":
    asyncio.run(main())
