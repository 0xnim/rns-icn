#!/usr/bin/env python3
"""Publish test content to ICN server on VPS."""
import asyncio
import sys
import os

# Explicitly set config directory before RNS init
os.environ["HOME"] = "/root"
configpath = "/root/.reticulum/config"
storagepath = "/root/.reticulum/storage"

sys.path.insert(0, "/opt/rns-icn")

import RNS

from rns_icn.rns_server import RNSICNServer
from rns_icn.name import Name
from rns_icn.packet import Data
from rns_icn.manifest import Manifest, ManifestEntry, EntryKind

async def main():
    # Use explicit config and storage paths
    RNS.Reticulum(configdir="/root/.reticulum")
    
    # Load transport identity from explicit storage path
    ti_path = os.path.join("/root/.reticulum/storage", "transport_identity")
    if not os.path.exists(ti_path):
        print(f"Transport identity not found at {ti_path}", file=sys.stderr)
        sys.exit(1)
        
    ident = RNS.Identity.from_file(ti_path)
    if not ident:
        print("Failed to load transport identity", file=sys.stderr)
        sys.exit(1)
    
    print(f"Using transport identity: {ident.hexhash}")
    
    server = RNSICNServer(identity=ident, app_name="icn", aspect="default")
    server.start()
    
    # Publish test content
    peer_addr = ident.hash
    
    contents = [
        ("hello", b"Hello from ICN over raw TCP!"),
        ("quote", b"The only way to learn a new programming language is by writing programs in it. -- Dennis Ritchie"),
        ("readme", b"This is a test content store for ICN over raw TCP.\nIt demonstrates fetching without RNS Links."),
    ]
    
    for label, content in contents:
        name = Name(peer_addr, [label.encode()])
        data = Data.new(name=name, content=content)
        server.forwarder.cs.insert(name, data)
        print(f"Published: /{peer_addr.hex()}/{label} ({len(content)} bytes)")
    
    # Build and publish manifest
    from rns_icn.manifest import Manifest, ManifestEntry, EntryKind
    
    entries = []
    for label, content in contents:
        name = Name(peer_addr, [label.encode()])
        data = server.forwarder.cs.get(Name(peer_addr, [label.encode()]))
        entries.append(ManifestEntry(
            kind=EntryKind.BLOB,
            label=label,
            name=name,
            content_hash=data.metadata.content_hash if data else None,
            size=len(content),
        ))
    
    manifest = Manifest.create(producer=peer_addr, entries=entries)
    manifest_data = manifest.to_json()
    manifest_data_pkt = Data.new(name=manifest.manifest_name(), content=manifest_data)
    manifest_data_pkt.with_sequence(manifest.sequence)
    server.forwarder.cs.insert(manifest_data_pkt.name, manifest_data_pkt)
    print(f"Published manifest with {len(entries)} entries")
    
    print(f"\nAll content published!")
    print(f"Tests:")
    print(f"  fetch 15ca97f4937572000d138211f8ad7d61 manifest")
    for label, _ in contents:
        print(f"  fetch 15ca97f4937572000d138211f8ad7d61 {label}")

if __name__ == "__main__":
    asyncio.run(main())
