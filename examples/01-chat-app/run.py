import asyncio
import sys
import os
import aioconsole  # Using standard async input loop helper

# Add flowsync-core path to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../packages/flowsync-core")))

from flowsync import FlowSyncNode

async def main():
    print("==================================================")
    click_prompt = "Starting FlowSync Chat Client..."
    print(click_prompt)
    print("==================================================")
    
    # 1. Connect client node
    url = "ws://localhost:8765"
    username = input("Enter your username: ").strip() or "Anonymous"
    
    node = FlowSyncNode(url, node_id=username)
    try:
        await node.connect()
    except Exception as e:
        print(f"Error: Could not connect to FlowSync Hub at {url}. Make sure the hub server is running first! ({e})")
        return

    print(f"\nConnected to Hub! Subscribing to 'chat:messages'...")

    # 2. Callback for stream updates
    def on_chat_update(value, metadata):
        # Clear current prompt line and print list of messages
        sys.stdout.write("\r\033[K")  # Clear line
        print("\n--- Chat History ---")
        if isinstance(value, list):
            for msg in value[-15:]:  # Show last 15 messages
                if isinstance(msg, dict):
                    print(f"[{msg.get('user', 'system')}]: {msg.get('text', '')}")
                else:
                    print(f" {msg}")
        else:
            print(f" {value}")
        print("--------------------")
        sys.stdout.write(f"[{username}] Enter message: ")
        sys.stdout.flush()

    node.on("chat:messages", on_chat_update)

    # 3. Message typing loop
    print("Type your message and press Enter. Type 'exit' or press Ctrl+C to quit.\n")
    try:
        while True:
            # Read line asynchronously
            user_input = await asyncio.get_event_loop().run_in_executor(None, input, f"[{username}] Enter message: ")
            if user_input.strip().lower() == "exit":
                break
            if user_input.strip():
                # Push msg using append rule
                await node.push("chat:messages", {"user": username, "text": user_input})
                await asyncio.sleep(0.1) # Wait a tiny bit for the update to fire locally
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        await node.disconnect()
        print("\nDisconnected from chat.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram exited.")
