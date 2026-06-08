#!/usr/bin/env python3
"""Direct ICN fetch — connects to VPS via direct TCP only.
Bypasses mesh routing issues by using an isolated RNS config."""

import asyncio, os, sys, tempfile, RNS

from rns_icn.rns_server import RNSICNServer
from rns_icn.name import Name
from rns_icn.packet import Interest

VPS_HOST = "172.81.133.81"
VPS_PORT = 49200

def main():
    args = sys.argv[1:]
    if len(args) < 1:
        print("Usage: icn-fetch-direct <peer_hash> [name] [output]", file=sys.stderr)
        sys.exit(1)

    peer_hash = args[0]
    name_str = args[1] if len(args) > 1 else "manifest"
    output_path = args[2] if len(args) > 2 else "-"
    
    asyncio.run(_fetch(peer_hash, name_str, output_path))

async def _fetch(peer_hash: str, name_str: str, output_path: str):
    # Create an isolated RNS config with ONLY the VPS interface
    configdir = tempfile.mkdtemp(prefix="icn_fetch_")
    os.makedirs(f"{configdir}/storage", exist_ok=True)
    
    config = f"""[reticulum]
enable_transport = No
share_instance = No

[logging]
loglevel = 3

[interfaces]
  [[VPS Direct]]
    type = TCPClientInterface
    enabled = yes
    target_host = {VPS_HOST}
    target_port = {VPS_PORT}
    ifac_size = 6
"""
    with open(f"{configdir}/config", "w") as f:
        f.write(config)

    os.environ["RNS_CONFIGDIR"] = configdir
    try:
        RNS.Reticulum(configdir=configdir)
    except TypeError:
        RNS.Reticulum(configdir=configdir, loglevel=3)

    server = RNSICNServer(app_name="icn", aspect="default")
    server.start()

    print(f"[fetch] Local: {server.identity.hexhash}", file=sys.stderr)
    print(f"[fetch] Target: {peer_hash}", file=sys.stderr)

    # Wait for path
    peer_raw = bytes.fromhex(peer_hash)
    print(f"[fetch] Waiting for path...", file=sys.stderr)
    for i in range(24):
        if peer_raw in list(RNS.Transport.path_table.keys()):
            hops = RNS.Transport.hops_to(peer_raw)
            print(f"[fetch] Path found! hops={hops}", file=sys.stderr)
            break
        await asyncio.sleep(5)
    else:
        print("[fetch] Path not found after 2min", file=sys.stderr)
        server.stop()
        sys.exit(1)

    # Connect
    print(f"[fetch] Connecting...", file=sys.stderr)
    face_id = await server.connect(peer_hash)
    if face_id is None:
        print("[fetch] Link failed", file=sys.stderr)
        server.stop()
        sys.exit(1)

    print(f"[fetch] Linked! face #{face_id}", file=sys.stderr)

    vps_addr = bytes.fromhex(peer_hash)
    server.forwarder.add_route(Name(vps_addr, []), face_id, 10)
    await asyncio.sleep(1)

    is_manifest = name_str == "manifest"
    target_name = Name(vps_addr, [b"manifest"]) if is_manifest else \
        Name(vps_addr, [c.encode() for c in name_str.split("/") if c])

    print(f"[fetch] Interest: {target_name}", file=sys.stderr)
    interest = Interest(name=target_name).with_can_be_prefix().with_lifetime(30000)
    result = await server.forwarder.express(interest, 0)

    if result is None:
        print(f"[fetch] No response", file=sys.stderr)
        server.stop()
        sys.exit(1)

    text = result.content.decode("utf-8", errors="replace")
    print(text)
    server.stop()

if __name__ == "__main__":
    main()
