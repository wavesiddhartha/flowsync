import click
import asyncio
import sys
import os
import json
from flowsync.hub import FlowSyncHub
from flowsync.node import FlowSyncNode

@click.group()
def main():
    """FlowSync Command Line Interface."""
    pass


@main.command()
@click.option("--host", default="127.0.0.1", help="Host to bind to.")
@click.option("--port", default=8765, help="Port to bind to.")
@click.option("--redis", default=None, help="Redis URL for clustering.")
def dev(host, port, redis):
    """Spawn a local FlowSync development Hub."""
    click.echo(f"Starting FlowSync Hub on ws://{host}:{port}")
    if redis:
        click.echo(f"Clustering enabled via Redis URL: {redis}")
    
    hub = FlowSyncHub(host=host, port=port, redis_url=redis)
    
    # Pre-register streams for convenience
    hub.stream("counter:value", merge_rule="crdt-counter", initial={"increments": {}, "decrements": {}})
    hub.stream("text:doc", merge_rule="crdt-text", initial=[])
    hub.stream("set:guests", merge_rule="crdt-set", initial={"added": [], "removed": []})

    try:
        asyncio.run(hub.start())
    except KeyboardInterrupt:
        click.echo("\nHub stopped.")


def sanitize_json(data):
    """Recursively strip prototype pollution keys from data to protect JS clients."""
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            if k in ("__proto__", "constructor", "prototype"):
                continue
            cleaned[k] = sanitize_json(v)
        return cleaned
    elif isinstance(data, list):
        return [sanitize_json(x) for x in data]
    return data


@main.command()
@click.argument("stream")
@click.argument("value")
@click.option("--host", default="127.0.0.1", help="Hub host.")
@click.option("--port", default=8765, help="Hub port.")
def push(stream, value, host, port):
    """Push a value to a FlowSync stream."""
    url = f"ws://{host}:{port}"
    
    # Check if value is JSON
    try:
        parsed_val = json.loads(value)
        parsed_val = sanitize_json(parsed_val)
    except Exception:
        parsed_val = value

    async def run():
        node = FlowSyncNode(url, node_id="cli-pusher")
        await node.connect()
        await node.push(stream, parsed_val)
        await asyncio.sleep(0.1)  # allow event loop to flush message
        await node.disconnect()
        click.echo(f"Successfully pushed value to '{stream}'.")

    asyncio.run(run())


@main.command()
@click.argument("stream")
@click.option("--host", default="127.0.0.1", help="Hub host.")
@click.option("--port", default=8765, help="Hub port.")
def get(stream, host, port):
    """Fetch the current value of a stream."""
    url = f"ws://{host}:{port}"

    async def run():
        node = FlowSyncNode(url, node_id="cli-getter")
        await node.connect()
        try:
            val = await node.get(stream)
            click.echo(f"Value for '{stream}': {val}")
        except Exception as e:
            click.echo(f"Error getting stream: {e}", err=True)
        await node.disconnect()

    asyncio.run(run())


@main.command()
@click.option("--host", default="127.0.0.1", help="Hub host.")
@click.option("--port", default=8765, help="Hub port.")
def monitor(host, port):
    """Real-time terminal dashboard monitoring active hubs."""
    url = f"ws://{host}:{port}"
    click.echo(f"Connecting monitor to {url}...")
    click.echo("Press Ctrl+C to exit.\n")
    click.echo("---------------------------------------------------------------------------------")
    click.echo("  Nodes   |  Streams  |  Throughput  |  Total Messages  |  Conflicts Resolved  ")
    click.echo("---------------------------------------------------------------------------------")

    def display_stats(stats):
        # Clear line and print stats
        sys.stdout.write("\r")
        sys.stdout.write(
            f"   {stats.get('nodes', 0):<7} | "
            f"{stats.get('streams_count', 0):<9} | "
            f"{stats.get('messages_per_second', 0):<4} msg/s  | "
            f"{stats.get('total_messages', 0):<16} | "
            f"{stats.get('conflicts_resolved', 0):<19}"
        )
        sys.stdout.flush()

    async def run():
        node = FlowSyncNode(url, node_id="cli-monitor")
        await node.connect()
        node.join_room("monitor")
        node.on_event("broadcast:stats", display_stats)
        
        # Keep loop running until user interrupts
        try:
            while True:
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
        finally:
            await node.disconnect()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        click.echo("\nMonitor stopped.")


@main.command()
@click.argument("directory", default=".")
def init(directory):
    """Scaffold a new FlowSync project template."""
    os.makedirs(directory, exist_ok=True)
    click.echo(f"Scaffolding new FlowSync project in '{directory}'...")
    # Create a basic docker-compose.yml and example scripts
    compose_content = """version: '3.8'
services:
  hub:
    image: python:3.11-slim
    command: python -c "import sys; sys.path.append('/app'); from flowsync.cli import main; main()" dev --host 0.0.0.0 --port 8765 --redis redis://redis:6379
    ports:
      - "8765:8765"
    volumes:
      - .:/app
    depends_on:
      - redis
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
"""
    with open(os.path.join(directory, "docker-compose.yml"), "w") as f:
        f.write(compose_content)
    click.echo("Scaffolded docker-compose.yml.")
    click.echo("Initialization complete!")


if __name__ == "__main__":
    main()
