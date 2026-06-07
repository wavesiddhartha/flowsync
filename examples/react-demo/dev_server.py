import asyncio
import logging
import sys
from flowsync import FlowSyncHub

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("flowsync.dev_server")

async def main():
    logger.info("Starting FlowSync Dev Server for React Demo...")
    
    # Initialize Hub on port 8765
    hub = FlowSyncHub(host="127.0.0.1", port=8765, log_level="INFO")
    
    # Pre-register streams with correct CRDT merge rules
    hub.stream("counter:value", merge_rule="crdt-counter", initial={"increments": {}, "decrements": {}})
    hub.stream("text:doc", merge_rule="crdt-text", initial=[])
    hub.stream("set:guests", merge_rule="crdt-set", initial={"added": [], "removed": []})
    
    logger.info("Registered streams:")
    logger.info("  - 'counter:value' (crdt-counter)")
    logger.info("  - 'text:doc' (crdt-text)")
    logger.info("  - 'set:guests' (crdt-set)")
    
    # Start and wait
    try:
        await hub.start()
    except KeyboardInterrupt:
        logger.info("Dev server interrupted.")
    finally:
        hub.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process exited.")
