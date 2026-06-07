import asyncio
import sys
import os
import random
import math
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../packages/flowsync-core")))

from flowsync import FlowSyncNode

async def main():
    print("==================================================")
    print("FLOWSYNC SIMULATED IOT CPU SENSOR")
    print("==================================================")

    url = "ws://localhost:8765"
    node_id = "iot-sensor-cpu"
    node = FlowSyncNode(url, node_id=node_id)
    
    try:
        await node.connect()
    except Exception as e:
        print(f"Could not connect to FlowSync Hub at {url}. Make sure the hub is running! ({e})")
        return

    print("Connected to Hub successfully. Starting sensor stream...")
    print("Reporting CPU metrics to stream 'metrics:cpu' every 0.5s.")
    print("Press Ctrl+C to stop.\n")

    t = 0
    try:
        while True:
            # Generate a realistic-looking wavy CPU load
            base_load = 30 + 15 * math.sin(t * 0.1)
            spike = 30 if random.random() > 0.9 else 0  # 10% chance of random spike
            noise = random.uniform(-5, 5)
            
            cpu_usage = max(0.0, min(100.0, base_load + spike + noise))
            
            print(f"\rReporting CPU usage: {cpu_usage:.1f}%", end="")
            sys.stdout.flush()

            # Push metric to stream
            await node.push("metrics:cpu", cpu_usage)
            
            t += 1
            await asyncio.sleep(0.5)
    except KeyboardInterrupt:
        print("\nSensor reporting stopped by user.")
    finally:
        await node.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
