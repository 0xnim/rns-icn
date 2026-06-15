#!/usr/bin/env python3
"""Run a long-lived ICN origin server with some demo content.

Rides whatever Reticulum instance the host provides (e.g. a shared rnsd that
is already connected to the public RNS network), publishes a few named blobs
plus a manifest, announces the destination, and serves Interests until killed.

Usage:
    python scripts/run_icn_server.py [identity_path] [cs_path]

Prints a JSON line ``ICN_SERVER_READY {...}`` with the destination hash and
producer (identity) hash once content is published.
"""

import asyncio
import json
import os
import signal
import sys

from rns_icn.config import ServerConfig
from rns_icn.rns_server import ICNServer
from rns_icn.name import Name

DEFAULT_IDENTITY = os.path.expanduser("~/.icn/server_identity")
DEFAULT_CS = os.path.expanduser("~/.icn/server_cs.db")

# Demo content published under the server's producer namespace.
CONTENT = {
    "hello": b"Hello from ICN over the Reticulum network!",
    "motd": b"ICN on RNS: content-addressed named data over the mesh.",
}


async def main() -> int:
    identity_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_IDENTITY
    cs_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_CS
    os.makedirs(os.path.dirname(identity_path), exist_ok=True)

    config = ServerConfig(
        identity_path=identity_path,
        app_name="icn",
        aspect="default",
        cs_path=cs_path,
        announce_interval=30.0,
        http_enabled=False,
    )

    server = ICNServer(config)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    await server.start()
    try:
        producer = server.identity.hash
        for label, body in CONTENT.items():
            server.publish_content(Name(producer, [label.encode()]), body)
        server.publish_manifest()

        print(
            "ICN_SERVER_READY "
            + json.dumps(
                {
                    "destination": server.hexhash,
                    "producer": producer.hex(),
                    "content": list(CONTENT.keys()),
                }
            ),
            flush=True,
        )
        await stop.wait()
    finally:
        await server.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
