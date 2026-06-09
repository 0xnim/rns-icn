"""Check connectivity through local rnsd daemon."""
import RNS, time, sys

RNS.Reticulum()
time.sleep(5)

vps_id = bytes.fromhex("f6b084e021f71c7a109d713365bae960")
vps_dest = bytes.fromhex("d005497acc72fb103257d20e0a2314db")

print(f"Interfaces:")
for iface in RNS.Transport.interfaces:
    print(f"  {type(iface).__name__} {getattr(iface,'name','?')} online={getattr(iface,'online','?')}")

print(f"\nPath table: {len(RNS.Transport.path_table)}")
print(f"Announce table: {len(RNS.Transport.announce_table)}")
print(f"Identity f6b0 in path: {vps_id in RNS.Transport.path_table}")
print(f"Dest d005 in path: {vps_dest in RNS.Transport.path_table}")
print(f"Dest d005 in announce: {vps_dest in RNS.Transport.announce_table}")
print(f"Identity f6b0 in announce: {vps_id in RNS.Transport.announce_table}")
sys.stdout.flush()

# Show a few announces
count = 0
for h, entry in list(RNS.Transport.announce_table.items())[:10]:
    hops = entry[1] if entry and len(entry) > 1 else "?"
    print(f"  Announce: {h.hex()[:24]}... hops={hops}")
    count += 1
if count == 0:
    print("  (no announces)")

# Show a few path entries
count = 0
for h in list(RNS.Transport.path_table.keys())[:10]:
    print(f"  Path: {h.hex()[:24]}...")
    count += 1

# Try linking
print(f"\nLinking to d005497acc72fb103257d20e0a2314db...")
sys.stdout.flush()
identity = RNS.Identity()
dest = RNS.Destination(
    identity,
    RNS.Destination.OUT,
    RNS.Destination.SINGLE,
    "icn", "default",
)
dest.hash = vps_dest

link = RNS.Link(dest)
start = time.time()
while link.status == RNS.Link.PENDING and time.time() - start < 60:
    time.sleep(0.5)

if link.status == RNS.Link.ACTIVE:
    print(f"\n✅ LINK ACTIVE! RTT={link.rtt*1000:.1f}ms")
    link.teardown()
else:
    print(f"\n❌ Link failed. status={link.status}, elapsed={time.time()-start:.0f}s")
sys.stdout.flush()
