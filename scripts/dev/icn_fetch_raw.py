#!/usr/bin/env python3
"""Fetch ICN content via raw TCP — bypasses RNS Links entirely."""
import asyncio
import sys
import struct
import os

# Add rns_icn to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "."))

from rns_icn.name import Name
from rns_icn.packet import Interest
from rns_icn.manifest import Manifest, ManifestEntry, EntryKind

VPS_HOST = "172.81.133.81"
VPS_RAW_PORT = 49202


async def fetch_raw_tcp(dest_hash: str, name_str: str, output_path: str = "-"):
    """Fetch content via raw TCP to VPS ICN server."""
    
    peer_addr_raw = bytes.fromhex(dest_hash)
    
    print(f"[fetch] Connecting to {VPS_HOST}:{VPS_RAW_PORT}...", file=sys.stderr)
    
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(VPS_HOST, VPS_RAW_PORT), 
            timeout=10.0
        )
    except asyncio.TimeoutError:
        print(f"[fetch] FAILED — connection to {VPS_HOST}:{VPS_RAW_PORT} timed out", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[fetch] FAILED — connection error: {e}", file=sys.stderr)
        sys.exit(1)
    
    print(f"[fetch] Connected via raw TCP", file=sys.stderr)
    
    try:
        is_manifest = name_str == "manifest"
        if is_manifest:
            target_name = Name(peer_addr_raw, [b"manifest"])
        else:
            path_components = [c.encode("utf-8") for c in name_str.split("/") if c]
            target_name = Name(peer_addr_raw, path_components)
        
        print(f"[fetch] Expressing Interest: {target_name}", file=sys.stderr)
        
        interest = Interest(name=target_name).with_can_be_prefix().with_lifetime(30000)
        interest_bytes = interest.to_bytes()
        
        # Send: length prefix + Interest packet
        writer.write(struct.pack('>I', len(interest_bytes)))
        writer.write(interest_bytes)
        await writer.drain()
        
        print(f"[fetch] Sent Interest for {target_name} ({len(interest_bytes)} bytes)", file=sys.stderr)
        
        # Read response: length prefix + Data packet
        length_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=30.0)
        length = struct.unpack('>I', length_bytes)[0]
        
        if length > 1024 * 1024:
            print(f"[fetch] FAILED — response too large: {length} bytes", file=sys.stderr)
            sys.exit(1)
            
        data = await asyncio.wait_for(reader.readexactly(length), timeout=30.0)
        
        # Parse Data packet
        from rns_icn.packet import parse_packet
        pkt = parse_packet(data)
        if pkt.data is None:
            print(f"[fetch] FAILED — received non-Data packet", file=sys.stderr)
            sys.exit(1)
            
        result = pkt.data
        
        content = result.content
        content_len = len(content)
        hash_hex = result.metadata.content_hash.hex() if result.metadata.content_hash else "N/A"
        
        print(f"[fetch] Received: {result.name}", file=sys.stderr)
        print(f"[fetch]   Size: {content_len} bytes", file=sys.stderr)
        print(f"[fetch]   Hash: {hash_hex}", file=sys.stderr)
        
        if is_manifest and Manifest is not None:
            try:
                manifest = Manifest.from_data(result)
                print(f"[fetch]   Sequence: v{manifest.sequence}", file=sys.stderr)
                print(f"[fetch]   Entries: {len(manifest.entries)}", file=sys.stderr)
            except Exception:
                pass
        
        if output_path == "-":
            sys.stdout.buffer.write(content)
            sys.stdout.buffer.flush()
        else:
            try:
                with open(output_path, "wb") as f:
                    f.write(content)
                print(f"[fetch] Wrote to {output_path}", file=sys.stderr)
            except IOError as e:
                print(f"[fetch] ERROR: Cannot write to {output_path}: {e}", file=sys.stderr)
                sys.exit(1)
                
        print(f"[fetch] Done — wrote {content_len} bytes", file=sys.stderr)
        
    except asyncio.TimeoutError:
        print(f"[fetch] FAILED — timeout waiting for response", file=sys.stderr)
        sys.exit(1)
    except asyncio.IncompleteReadError:
        print(f"[fetch] FAILED — incomplete response", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[fetch] FAILED — {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("dest", nargs="?", help="Destination hash (default: ICN_DEST env var)")
    parser.add_argument("name", nargs="?", help="Content name (default: manifest)")
    parser.add_argument("output", nargs="?", default="-", help="Output file (default: stdout)")
    args = parser.parse_args()
    
    dest = args.dest or os.environ.get("ICN_DEST")
    if not dest:
        parser.error("Destination hash required (or set ICN_DEST)")
    
    name = args.name or "manifest"
    output = args.output
    
    print(f"[fetch] Using dest: {dest}, name: {name}", file=sys.stderr)
    await fetch_raw_tcp(dest, name, output)


if __name__ == "__main__":
    asyncio.run(main())
