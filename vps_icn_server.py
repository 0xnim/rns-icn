#!/usr/bin/env python3
"""ICN server for VPS deployment — persistent identity across restarts.

Usage:
    python3 vps_icn_server.py

Set RNS_CONFIG to a custom config path if needed (defaults to ~/.reticulum/config).
The identity is persisted at ~/.icn/identity — first run creates it, subsequent
runs load it so the destination hash stays the same.

Environment variables:
    RNS_CONFIG    Path to RNS config file (optional)
    ICN_IDENTITY  Path to identity file (default: ~/.icn/identity)
"""

import asyncio
import os
import signal

import RNS

from rns_icn.rns_server import RNSICNServer
from rns_icn.rns_utils import default_identity_path


async def main_async():
    identity_path = os.environ.get("ICN_IDENTITY") or default_identity_path("icn")

    print("╔══════════════════════════════════════════════╗")
    print("║        ICN Server — VPS Deployment           ║")
    print("╚══════════════════════════════════════════════╝")
    print()
    print(f"Identity file: {identity_path}")

    # Initialise Reticulum (reads ~/.reticulum/config or RNS_CONFIG)
    # This must be done before the event loop starts because RNS blocks
    # during initialization (interface creation, connection attempts).
    RNS.Reticulum()

    # Load or create persistent identity via the utility
    server = RNSICNServer(identity_path=identity_path, app_name="icn", aspect="default")
    server.start()

    print()
    print(f"  Identity hex hash : {server.identity.hexhash}")
    print(f"  Identity hash (16B): {server.identity.hash.hex()}")
    print(f"  Destination hexhash: {server.hexhash}")
    print(f"  Listening on       : /icn/default")
    print()
    print("  Set RNS_DEST on clients to:")
    print(f"    export RNS_DEST={server.hexhash}")
    print()
    print("Press Ctrl+C to stop.")
    print()

    # Graceful shutdown
    stop_event = asyncio.Event()

    def shutdown():
        print("\n[Server] Shutting down...")
        server.stop()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, shutdown)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: shutdown())

    await stop_event.wait()
    print("[Server] Bye.")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
