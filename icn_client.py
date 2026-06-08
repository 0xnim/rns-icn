#!/usr/bin/env python3
"""ICN Client — connects to the VPS server, fetches manifest and content.

Usage: RNS_DEST=<destination_hex> python3 icn_client.py
"""

import asyncio
import os
import sys
import RNS
from rns_icn.rns_server import RNSICNServer
from rns_icn.name import Name
from rns_icn.packet import Interest
from rns_icn.manifest import Manifest


async def main():
    print("╔══════════════════════════════════════════════╗")
    print("║         ICN Client — VPS Fetch Demo          ║")
    print("╚══════════════════════════════════════════════╝\n")

    # Config
    peer_hash = os.environ.get("RNS_DEST")
    if not peer_hash:
        print("ERROR: Set RNS_DEST env var to the VPS server's destination hex hash")
        print(f"  From VPS output: destination_hex value")
        sys.exit(1)

    print(f"[Client] Initializing RNS...")
    RNS.Reticulum()

    # Create a local ICN server instance (for the forwarder, not listening)
    server = RNSICNServer(app_name="icn", aspect="default")
    server.start()

    print(f"[Client] Local identity: {server.identity.hexhash}")
    print(f"[Client] Connecting to peer: {peer_hash}")

    # Establish outbound link to VPS server
    face_id = await server.connect(peer_hash)
    if face_id is None:
        print("[Client] ✗ Failed to establish link to VPS server!")
        server.stop()
        return

    print(f"[Client] ✓ Link established (face #{face_id})")

    # Add route to the VPS server's namespace
    vps_addr = bytes.fromhex(peer_hash)
    server.forwarder.add_route(Name(vps_addr, []), face_id, 10)

    await asyncio.sleep(1)  # Let link settle

    # ── Fetch Manifest ──
    manifest_name = Name(vps_addr, [b"manifest"])
    print(f"\n[Client] Express Interest: {manifest_name}")

    manifest_interest = (
        Interest(name=manifest_name)
        .with_can_be_prefix()
        .with_lifetime(10000)
    )

    result = await server.forwarder.express(manifest_interest, 0)

    if result is None:
        print("[Client] ✗ Manifest not found (timeout)")
        server.stop()
        return

    manifest = Manifest.from_data(result)
    print(f"[Client] ✓ Got manifest v{manifest.sequence}")
    print(f"[Client]   Entries:")

    # ── Fetch each content item ──
    for entry in manifest.entries:
        print(f"\n  [{entry.kind.value}] {entry.label}")
        print(f"    Name: {entry.name}")

        interest = Interest(name=entry.name).with_lifetime(10000)
        content_result = await server.forwarder.express(interest, 0)

        if content_result:
            text = content_result.content.decode("utf-8", errors="replace")
            print(f"    ✓ Received ({len(content_result.content)} bytes)")
            for line in text.strip().split("\n"):
                print(f"      {line}")
        else:
            print(f"    ✗ Timeout")

    print(f"\n{'='*50}")
    print(f"Demo complete!")
    print(f"{'='*50}")

    server.stop()

if __name__ == "__main__":
    asyncio.run(main())
