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
    ICN_RAW_PORT  Raw TCP port for direct ICN fetch (default: 49202)
"""

import asyncio
import os
import signal
import struct

import RNS

from rns_icn.rns_server import RNSICNServer
from rns_icn.rns_utils import default_identity_path, load_transport_identity
from rns_icn.packet import Interest, parse_packet, Data
from rns_icn.name import Name


async def _populate_test_content(server: RNSICNServer):
    """Populate content store with test data."""
    from rns_icn.name import Name
    from rns_icn.packet import Data
    from rns_icn.manifest import Manifest, ManifestEntry, EntryKind
    
    peer_addr = server.destination.hash
    
    contents = [
        ("hello", b"Hello from ICN over raw TCP!"),
        ("quote", b"The only way to learn a new programming language is by writing programs in it. -- Dennis Ritchie"),
        ("readme", b"This is a test content store for ICN over raw TCP.\nIt demonstrates fetching without RNS Links."),
    ]
    
    for label, content in contents:
        name = Name(peer_addr, [label.encode()])
        data = Data.new(name=name, content=content)
        server.forwarder.cs.insert(name, data)
        RNS.log(f"ICN: Published test content /{peer_addr.hex()}/{label}")
    
    # Build and publish manifest
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
    RNS.log(f"ICN: Published test manifest with {len(entries)} entries")


async def handle_raw_tcp(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, server: RNSICNServer):
    """Handle a raw TCP connection for ICN fetch.
    
    Protocol:
    - Client sends: 4-byte length (big-endian) + Interest packet
    - Server replies: 4-byte length + Data packet
    """
    addr = writer.get_extra_info('peername')
    RNS.log(f"ICN: Raw TCP connection from {addr}")
    
    try:
        # Read length prefix (4 bytes big-endian)
        length_bytes = await reader.readexactly(4)
        length = struct.unpack('>I', length_bytes)[0]
        
        if length > 1024 * 1024:  # 1MB max
            RNS.log(f"ICN: Raw TCP packet too large: {length} bytes")
            writer.close()
            return
            
        # Read the Interest packet
        data = await reader.readexactly(length)
        
        # Parse Interest
        pkt = parse_packet(data)
        if pkt.interest is None:
            RNS.log(f"ICN: Raw TCP received non-Interest packet")
            writer.close()
            return
            
        interest = pkt.interest
        RNS.log(f"ICN: Raw TCP received Interest for {interest.name}")
        
        # Look up in forwarder
        result = await server.forwarder.express(interest, 0)
        
        if result is None:
            RNS.log(f"ICN: Raw TCP — no content for {interest.name}")
            writer.close()
            return
            
        # Send Data packet back
        if result.content is not None:
            data = result.to_bytes()
            writer.write(struct.pack('>I', len(data)))
            writer.write(data)
            await writer.drain()
            RNS.log(f"ICN: Raw TCP sent Data for {result.name} ({len(data)} bytes)")
        else:
            writer.close()
            
    except asyncio.IncompleteReadError:
        pass
    except Exception as e:
        RNS.log(f"ICN: Raw TCP error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main_async():
    # Initialise Reticulum with standalone ICN config (share_instance=No)
    # This creates its own RNS instance with its own transport and mesh interfaces
    config_dir = os.environ.get("RNS_CONFIG", os.path.expanduser("~/.reticulum/icn"))
    RNS.Reticulum(configdir=config_dir)

    # Use persistent ICN identity
    identity_path = os.environ.get("ICN_IDENTITY") or os.path.expanduser("~/.reticulum/icn/identity")
    print(f"Identity file: {identity_path}")
    server = RNSICNServer(identity_path=identity_path, app_name="icn", aspect="default")
    server.start()

    print()
    print(f"  Identity hex hash : {server.identity.hexhash}")
    print(f"  Identity hash (16B): {server.identity.hash.hex()}")
    print(f"  Destination hexhash: {server.hexhash}")
    print(f"  Listening on       : /icn/default")
    print()

    # Start raw TCP listener for direct ICN fetch
    raw_port = int(os.environ.get("ICN_RAW_PORT", "49202"))
    async def handler(reader, writer):
        await handle_raw_tcp(reader, writer, server)
    raw_server = await asyncio.start_server(handler, '0.0.0.0', raw_port)
    RNS.log(f"ICN: Raw TCP listener on port {raw_port}")

    print("  Set RNS_DEST on clients to:")
    print(f"    export RNS_DEST={server.hexhash}")
    print()
    print("  Raw TCP fetch port:")
    print(f"    ICN_RAW_PORT={raw_port}")
    print()
    print("Press Ctrl+C to stop.")
    print()

    # Pre-populate with test content
    await _populate_test_content(server)

    # Graceful shutdown
    stop_event = asyncio.Event()

    def shutdown():
        print("\n[Server] Shutting down...")
        raw_server.close()
        server.stop()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, shutdown)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: shutdown())

    try:
        await stop_event.wait()
    finally:
        raw_server.close()
        await raw_server.wait_closed()
    
    print("[Server] Bye.")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
