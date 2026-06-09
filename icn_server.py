#!/usr/bin/env python3
"""ICN Server — standalone RNS instance with own mesh interfaces.

Announces ICN destination directly through mesh.
No shared instance dependency.
"""

import asyncio
import os
import signal

import RNS

CONFIG_DIR = "/etc/rns-icn"


async def main():
    print("ICN Server (standalone)...")

    RNS.Reticulum(configdir=CONFIG_DIR)
    RNS.log("Reticulum initialized (standalone)")

    # Persistent ICN identity
    identity_file = os.path.join(CONFIG_DIR, "identity")
    identity = RNS.Identity.from_file(identity_file)
    if identity is None:
        identity = RNS.Identity()
        identity.to_file(identity_file)
        RNS.log("Created new ICN identity")

    from rns_icn.rns_server import RNSICNServer
    server = RNSICNServer(identity=identity, app_name="icn", aspect="default")
    server.start()

    print(f"  ICN identity  : {identity.hexhash}")
    print(f"  ICN dest      : {server.destination.hexhash}")
    print(f"  Interfaces    : {len(RNS.Transport.interfaces)}")

    for i in RNS.Transport.interfaces:
        name = getattr(i, "name", "?")
        online = getattr(i, "online", "?")
        if online:
            print(f"    {name}")

    # Seed test content
    await _populate_content(server)

    print("\nPress Ctrl+C to stop.")
    stop = asyncio.Event()
    def shutdown():
        print("\n[Shutting down...]")
        server.stop()
        stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda s, f: shutdown())
    await stop.wait()


async def _populate_content(server):
    from rns_icn.packet import Data, Name
    from rns_icn.manifest import Manifest, ManifestEntry, EntryKind

    peer_addr = server.destination.hash
    items = [
        ("hello", b"Hello from ICN over RNS!"),
        ("quote", b"The only way to learn is by writing programs."),
        ("readme", b"ICN standalone on VPS mesh."),
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
