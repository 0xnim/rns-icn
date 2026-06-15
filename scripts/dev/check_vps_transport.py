"""Check if the VPS transport identity is in our path table."""
import RNS, time, sys

RNS.Reticulum()
time.sleep(5)

vps_transport = bytes.fromhex("98f7daa4daed7064aa8e25d7ae7305a4")
vps_icn_id = bytes.fromhex("f6b084e021f71c7a109d713365bae960")

print(f"VPS transport in path: {vps_transport in RNS.Transport.path_table}", flush=True)
print(f"VPS ICN id in path: {vps_icn_id in RNS.Transport.path_table}", flush=True)

# Search for any partial match
for h in list(RNS.Transport.path_table.keys()):
    if h.hex().startswith("98f7") or h.hex().startswith("f6b0"):
        print(f"  FOUND: {h.hex()}", flush=True)
    if h.hex().startswith("d005"):
        print(f"  FOUND DEST: {h.hex()}", flush=True)

# Also check announce table
for h in list(RNS.Transport.announce_table.keys()):
    if h.hex().startswith("98f7") or h.hex().startswith("f6b0") or h.hex().startswith("d005"):
        print(f"  ANNOUNCE: {h.hex()}", flush=True)

print(f"Path table size: {len(RNS.Transport.path_table)}", flush=True)
print(f"Announce table size: {len(RNS.Transport.announce_table)}", flush=True)
sys.stdout.flush()
