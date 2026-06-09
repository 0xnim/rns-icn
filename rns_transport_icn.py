#!/usr/bin/env python3
"""RNS Transport + ICN Server — single process, single identity.

LXMF-style: one RNS instance owns the transport identity, mesh interfaces,
and the ICN destination. Announces propagate through mesh. RNS.Link works.
"""

import asyncio
import os
import signal
import sys

import RNS

CONFIG_DIR = os.environ.get("RNS_CONFIG", "/etc/rnsd-icn")


async def main():
    print("╔══════════════════════════════════════════════╗")
    print("║     RNS Transport + ICN Server              ║")
    print("╚══════════════════════════════════════════════╝")
    print(f"\nConfig: {CONFIG_DIR}")

    # 1. Initialize RNS — standalone, owns transport + interfaces
    RNS.Reticulum(configdir=CONFIG_DIR)
    RNS.log("Reticulum initialized")

    # 2. Get or create transport identity
    identity = RNS.Identity.from_file(os.path.join(CONFIG_DIR, "transport_identity"))
    if identity is None:
        identity = RNS.Identity()
        identity.to_file(os.path.join(CONFIG_DIR, "transport_identity"))

    # 3. Create ICN destination — uses transport identity
    from rns_icn.rns_server import RNSICNServer
    server = RNSICNServer(identity=identity, app_name="icn", aspect="default")
    server.start()

    print(f"  Identity      : {identity.hexhash}")
    print(f"  ICN dest      : {server.destination.hexhash}")
    print(f"  Set RNS_DEST  : {server.destination.hexhash}\n")

    # 4. Seed content
    await _populate_content(server)

    # 5. Print interfaces
    print(f"  Interfaces ({len(RNS.Transport.interfaces)}):")
    for i in RNS.Transport.interfaces:
        name = getattr(i, 'name', '?')
        online = getattr(i, 'online', '?')
        if online:
            print(f"    {name}")

    print("\nPress Ctrl+C to stop.")

    stop = asyncio.Event()
    def shutdown():
        print("\n[Shutting down...]")
        server.stop()
        stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, shutdown)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: shutdown())
    await stop.wait()


async def _populate_content(server):
    from rns_icn.packet import Data, Name
    from rns_icn.manifest import Manifest, ManifestEntry, EntryKind

    peer_addr = server.destination.hash
    items = [
        ("hello", b"Hello from ICN over RNS transport!"),
        ("quote", b"The only way to learn is by writing programs."),
        ("readme", b"ICN embedded in RNS transport. One identity, one mesh."),
    ]

    for label, content in items:
        name = Name(peer_addr, [label.encode()])
        data = Data.new(name=name, content=content)
        server.forwarder.cs.insert(name, data)

    entries = []
    for label, content in items:
        name = Name(peer_addr, [label.encode()])
        data = server.forwarder.cs.get(name)
        entries.append(ManifestEntry(
            kind=EntryKind.BLOB, label=label, name=name,
            content_hash=data.metadata.content_hash if data else None,
            size=len(content),
        ))

    manifest = Manifest.create(producer=peer_addr, entries=entries)
    md = manifest.to_json()
    mp = Data.new(name=manifest.manifest_name(), content=md)
    mp.with_sequence(manifest.sequence)
    server.forwarder.cs.insert(mp.name, mp)
    RNS.log(f"Published {len(items)} items + manifest ({sum(len(c) for _,c in items)} bytes)")


if __name__ == "__main__":
    asyncio.run(main())
