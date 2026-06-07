import asyncio
import logging
import sys

# Configure clean logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("flowsync.demo")

from flowsync import FlowSyncHub, FlowSyncNode

async def main():
    logger.info("=== Starting FlowSync Core Demo ===")
    
    # 1. Initialize and start the Hub on port 8765
    hub = FlowSyncHub(host="127.0.0.1", port=8765, log_level="INFO")
    hub_task = asyncio.create_task(hub.start())
    
    # Give the Hub server half a second to initialize and start listening
    await asyncio.sleep(0.5)
    
    # 2. Connect Node A (Pusher)
    node_a = FlowSyncNode(hub_url="ws://127.0.0.1:8765", node_id="node-a-sensor")
    await node_a.connect()
    logger.info("Node A (Sensor) connected.")
    
    # 3. Connect Node B (Receiver / Logger)
    node_b = FlowSyncNode(hub_url="ws://127.0.0.1:8765", node_id="node-b-viewer")
    await node_b.connect()
    logger.info("Node B (Viewer) connected.")
    
    # 4. Subscribe Node B to "sensor:temperature" stream
    received_updates = []
    
    def on_temp_change(value, metadata):
        received_updates.append(value)
        logger.info(
            f"📥 Node B received update for 'sensor:temperature' -> Value: {value} "
            f"(Pushed by: '{metadata.get('node_id')}', ts: {metadata.get('ts')})"
        )

    # Node B listens to stream
    node_b.on("sensor:temperature", on_temp_change)
    logger.info("Node B subscribed to 'sensor:temperature'")
    
    # 5. Node A pushes a value every second for 5 seconds
    logger.info("Node A starting pushes...")
    for i in range(1, 6):
        temp_value = 20.0 + (i * 1.5)
        logger.info(f"📤 Node A pushing temperature: {temp_value}°C")
        await node_a.push("sensor:temperature", temp_value)
        await asyncio.sleep(1.0)
        
    # Give a tiny buffer for any remaining updates to arrive
    await asyncio.sleep(0.5)
    
    # 6. Print stats from the Hub
    logger.info("\n=== Hub Statistics ===")
    import json
    logger.info(json.dumps(hub.stats(), indent=2))
    
    # 7. Graceful teardown
    logger.info("\n=== Tearing Down Demo ===")
    await node_a.disconnect()
    await node_b.disconnect()
    hub.stop()
    await hub_task
    
    logger.info("Demo execution completed successfully.")
    assert len(received_updates) == 5, f"Expected 5 updates, but received {len(received_updates)}"
    logger.info("Validation passed! Node B successfully received all 5 sync updates in real-time.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Demo interrupted by user.")
