#!/usr/bin/env python3
"""Publish test content to ICN server via raw TCP."""
import asyncio
import sys
import struct
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "."))

from rns_icn.name import Name
from rns_icn.packet import Interest, Data

VPS_HOST = "172.81.133.81"
VPS_RAW_PORT = 49202

DEST_HASH = "15ca97f4937572000d138211f8ad7d61"
peer_addr = bytes.fromhex(DEST_HASH)

async def publish_raw_tcp(name_str: str, content: bytes):
    """Publish content via raw TCP to VPS ICN server."""
    
    print(f"[publish] Connecting to {VPS_HOST}:{VPS_RAW_PORT}...", file=sys.stderr)
    
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(VPS_HOST, VPS_RAW_PORT), 
            timeout=10.0
        )
    except asyncio.TimeoutError:
        print(f"[publish] FAILED — connection timed out", file=sys.stderr)
        return False
    except Exception as e:
        print(f"[publish] FAILED — connection error: {e}", file=sys.stderr)
        return False
    
    print(f"[publish] Connected via raw TCP", file=sys.stderr)
    
    try:
        path_components = [c.encode("utf-8") for c in name_str.split("/") if c]
        target_name = Name(peer_addr, path_components)
        
        print(f"[publish] Publishing Data for {target_name} ({len(content)} bytes)", file=sys.stderr)
        
        # Create Data packet
        data_pkt = Data.new(name=target_name, content=content)
        
        # The server's handle_raw_tcp expects Interest, not Data
        # We need a different protocol for publishing
        # For now, let's just use the server's RNS API
        
        print(f"[publish] Note: raw TCP endpoint only handles Interest requests", file=sys.stderr)
        print(f"[publish] For publishing, use the server's RNS API instead", file=sys.stderr)
        writer.close()
        return False
        
    except Exception as e:
        print(f"[publish] FAILED — {e}", file=sys.stderr)
        return False
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <name> [content]", file=sys.stderr)
        sys.exit(1)
    
    name = sys.argv[1]
    content = sys.argv[2].encode() if len(sys.argv) > 2 else b"test content"
    
    await publish_raw_tcp(name, content)

if __name__ == "__main__":
    asyncio.run(main())
