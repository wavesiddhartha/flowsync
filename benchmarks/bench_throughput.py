import asyncio
import time
import logging
import sys
from flowsync import FlowSyncHub, FlowSyncNode

# Configure clean logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("flowsync.benchmark")

async def run_benchmark():
    print("==================================================")
    print("FLOWSYNC PERFORMANCE BENCHMARK SUITE")
    print("==================================================")

    # 1. Initialize and start the Hub on a dynamic port
    hub = FlowSyncHub(host="127.0.0.1", port=0, log_level="WARNING")
    hub_task = asyncio.create_task(hub.start())
    await asyncio.sleep(0.2)
    port = hub.bound_port
    url = f"ws://127.0.0.1:{port}"

    # Setup the benchmark stream
    hub.stream("bench:stream")

    # 2. Connect Pusher and Listener Nodes
    pusher = FlowSyncNode(url, node_id="pusher")
    listener = FlowSyncNode(url, node_id="listener")
    await pusher.connect()
    await listener.connect()

    # Track message propagation
    received_count = 0
    latencies = []
    completion_event = asyncio.Event()
    num_messages = 10000

    def on_update(value, metadata):
        nonlocal received_count
        received_count += 1
        
        # Calculate latency
        sent_ts = metadata.get("ts", 0.0)
        recv_ts = time.time()
        latencies.append(recv_ts - sent_ts)
        
        if received_count >= num_messages:
            completion_event.set()

    listener.on("bench:stream", on_update)
    await asyncio.sleep(0.1) # Wait for subscription propagation

    # 3. Start Pushing
    print(f"Pushing {num_messages} messages as fast as possible...")
    start_time = time.time()

    # We send pushes in batches to maximize concurrency and avoid blocking
    batch_size = 500
    for i in range(0, num_messages, batch_size):
        tasks = []
        for j in range(i, min(i + batch_size, num_messages)):
            tasks.append(pusher.push("bench:stream", f"val-{j}"))
        await asyncio.gather(*tasks)

    # Wait for all messages to be received by listener
    try:
        await asyncio.wait_for(completion_event.wait(), timeout=15.0)
    except asyncio.TimeoutError:
        print("Timeout waiting for message receipt.")

    end_time = time.time()
    total_time = end_time - start_time
    throughput = received_count / total_time
    avg_latency_ms = (sum(latencies) / len(latencies)) * 1000 if latencies else 0

    print("--------------------------------------------------")
    print("RESULTS:")
    print(f"  Total Messages Sent:      {num_messages}")
    print(f"  Total Messages Received:  {received_count}")
    print(f"  Total Duration:           {total_time:.4f} seconds")
    print(f"  Throughput:               {throughput:.2f} msg/sec")
    print(f"  Average Latency:          {avg_latency_ms:.2f} ms")
    print("--------------------------------------------------")

    # Cleanup
    await pusher.disconnect()
    await listener.disconnect()
    hub.stop()
    await hub_task

    # Validate throughput and latency constraints
    if throughput >= 10000:
        print("✅ Throughput benchmark PASSED (> 10,000 msg/sec)")
    else:
        print("⚠️ Throughput was below 10,000 msg/sec on this machine/runtime.")

    if avg_latency_ms <= 10.0:
        print("✅ Latency benchmark PASSED (sub-10ms)")
    else:
        print("⚠️ Average latency was above 10ms on this machine/runtime.")
    print("==================================================")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
