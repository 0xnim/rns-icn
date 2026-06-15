#!/usr/bin/env python3
"""Direct ICN fetch over raw TCP — bypasses RNS routing.

Connects directly to the VPS's TCPServerInterface port,
establishes an RNS Link manually, fetches manifest + content.
"""

import asyncio
import os
import RNS
from rns_icn.rns_server import RNSICNServer
from rns_icn.name import Name
from rns_icn.packet import Interest
from rns_icn.manifest import Manifest


async def main():
    peer_hash = os.environ.get("RNS_DEST")
    if not peer_hash:
        print("SET RNS_DEST")
        return

    print("╔══════════════════════════════════════════════╗")
    print("║      ICN Direct TCP — VPS Fetch Demo        ║")
    print("╚══════════════════════════════════════════════╝\n")

    RNS.Reticulum()

    # Create a minimal client identity (no listening destination)
    identity = RNS.Identity()
    print(f"[Client] Identity: {identity.hexhash}")

    # Create the outbound destination for the VPS ICN server
    dest = RNS.Destination(
        identity, RNS.Destination.OUT, RNS.Destination.SINGLE,
        "icn", "default",
    )
    dest.hash = bytes.fromhex(peer_hash)
    dest.hexhash = peer_hash

    print(f"[Client] Establishing Link to {peer_hash}...")
    link = RNS.Link(dest)

    timeout = 30.0
    start = asyncio.get_event_loop().time()
    while link.status != RNS.Link.ACTIVE:
        await asyncio.sleep(0.2)
        if asyncio.get_event_loop().time() - start > timeout:
            print("[Client] ✗ Link timed out")
            return

    print("[Client] ✓ Link established!")

    # Create LinkFace for this link
    from rns_icn.face import LinkFace
    loop = asyncio.get_running_loop()
    link_face = LinkFace(1, link, loop=loop)

    vps_addr = bytes.fromhex(peer_hash)

    # Fetch manifest
    manifest_name = Name(vps_addr, [b"manifest"])
    print(f"\n[Client] Fetching manifest: {manifest_name}")
    interest = Interest(name=manifest_name).with_can_be_prefix().with_lifetime(15000)
    result = await link_face.express_interest(interest)

    if result is None:
        print("[Client] ✗ Manifest not found")
        link.teardown()
        return

    manifest = Manifest.from_data(result)
    print(f"[Client] ✓ Got manifest v{manifest.sequence}")

    for entry in manifest.entries:
        print(f"\n  [{entry.kind.value}] {entry.label}")
        ci = Interest(name=entry.name).with_lifetime(15000)
        cr = await link_face.express_interest(ci)
        if cr:
            print(f"  ✓ ({len(cr.content)} bytes): {cr.content.decode('utf-8')[:80]}")
        else:
            print(f"  ✗ Timeout")

    link.teardown()
    print(f"\n{'='*50}")
    print("Done!")

if __name__ == "__main__":
    asyncio.run(main())
