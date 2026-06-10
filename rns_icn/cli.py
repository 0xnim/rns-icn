"""ICN CLI — icn-client and icn-server binaries."""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from pathlib import Path

from .config import load_client_config, load_server_config
from .client import ICNClient
from .rns_server import ICNServer
from .name import Name
from .packet import Data, Interest
from .health import is_health_interest
from .metrics import metrics


async def client_main() -> int:
    """icn-client binary entry point."""
    parser = argparse.ArgumentParser(prog="icn-client", description="ICN Client")
    parser.add_argument("--config", default="icn.toml", help="Config file path")
    parser.add_argument("--fetch", help="Name to fetch (e.g., /producer/manifest)")
    parser.add_argument("--peer", help="Peer destination hex hash")
    parser.add_argument("--timeout", type=float, default=30.0, help="Fetch timeout seconds")
    parser.add_argument("--max-retries", type=int, help="Max retries (overrides config)")
    args = parser.parse_args()

    config = load_client_config(args.config)

    # Override config from CLI
    if args.max_retries is not None:
        config.max_retries = args.max_retries

    if not args.fetch or not args.peer:
        parser.print_help()
        return 1

    try:
        peer_hash = bytes.fromhex(args.peer)
    except ValueError:
        print(f"Error: Invalid peer hash: {args.peer}", file=sys.stderr)
        return 1

    # Parse name
    try:
        name = Name.from_string(args.fetch)
    except Exception as e:
        print(f"Error parsing name: {e}", file=sys.stderr)
        return 1

    async with ICNClient(config) as client:
        try:
            data = await client.fetch(name, peer_hash, timeout=args.timeout)
            if data:
                print(f"✓ Received {len(data.content)} bytes")
                if data.content:
                    # Try to decode as UTF-8
                    try:
                        print(data.content.decode("utf-8"))
                    except UnicodeDecodeError:
                        print(f"[binary: {len(data.content)} bytes]")
                return 0
            else:
                print("✗ No data received (timeout)", file=sys.stderr)
                return 1
        except Exception as e:
            print(f"✗ Fetch failed: {e}", file=sys.stderr)
            return 1


async def server_main() -> int:
    """icn-server binary entry point."""
    parser = argparse.ArgumentParser(prog="icn-server", description="ICN Server")
    parser.add_argument("--config", default="icn.toml", help="Config file path")
    args = parser.parse_args()

    config = load_server_config(args.config)

    # Setup graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass  # Windows

    print(f"Starting ICN Server...")
    print(f"  Identity: {config.identity_path}")
    print(f"  CS: {config.cs_path} (max {config.cs_max_entries}, TTL {config.cs_ttl_seconds}s)")

    server = ICNServer(config)
    try:
        await server.start()
        print(f"  Destination: {server.hexhash}")
        print(f"  Listening on /{config.app_name}/{config.aspect}")
        print("Ready. Press Ctrl+C to stop.")

        # If HTTP enabled, start it
        if config.http_enabled:
            from .health import setup_http_api
            from .metrics import metrics
            runner = await setup_http_api(server, metrics, config.http_host, config.http_port)
            print(f"  HTTP API: http://{config.http_host}:{config.http_port} (health, metrics)")

        await stop_event.wait()
        print("\nShutting down...")
    finally:
        await server.shutdown()
    return 0


def client_main_sync() -> int:
    """Synchronous wrapper for icn-client entry point."""
    return asyncio.run(client_main())


def server_main_sync() -> int:
    """Synchronous wrapper for icn-server entry point."""
    return asyncio.run(server_main())


def main() -> int:
    """Entry point for both binaries."""
    if "icn-client" in sys.argv[0] or (len(sys.argv) > 0 and sys.argv[0].endswith("icn-client")):
        return asyncio.run(client_main())
    elif "icn-server" in sys.argv[0] or (len(sys.argv) > 0 and sys.argv[0].endswith("icn-server")):
        return asyncio.run(server_main())
    else:
        # Called as module: python -m rns_icn.cli client|server
        if len(sys.argv) < 2:
            print("Usage: python -m rns_icn.cli [client|server] [args...]")
            return 1
        subcommand = sys.argv[1]
        sys.argv = [sys.argv[0]] + sys.argv[2:]  # Remove subcommand
        if subcommand == "client":
            return asyncio.run(client_main())
        elif subcommand == "server":
            return asyncio.run(server_main())
        else:
            print(f"Unknown subcommand: {subcommand}")
            return 1


if __name__ == "__main__":
    sys.exit(main())