"""Check for VPS identity through local rnsd."""
import RNS, time, sys

RNS.Reticulum()
time.sleep(8)

vps_id = bytes.fromhex("f6b084e021f71c7a109d713365bae960")
vps_dest = bytes.fromhex("d005497acc72fb103257d20e0a2314db")

print(f"Path table: {len(RNS.Transport.path_table)}")
print(f"Announce table: {len(RNS.Transport.announce_table)}", flush=True)

found_id = vps_id in RNS.Transport.path_table
found_dest = vps_dest in RNS.Transport.announce_table
print(f"VPS identity in path: {found_id}")
print(f"VPS dest in announce: {found_dest}", flush=True)

# Show all announce entries
count = 0
for h in list(RNS.Transport.announce_table.keys())[:15]:
    print(f"  Announce: {h.hex()[:24]}...")
    count += 1
print(f"  Total announces shown: {count}", flush=True)

# Show all path entries
count = 0
for h in list(RNS.Transport.path_table.keys())[:15]:
    print(f"  Path: {h.hex()[:24]}...")
    count += 1
print(f"  Total paths shown: {count}", flush=True)

# Check ICN VPS Server interface
for iface in RNS.Transport.interfaces:
    name = getattr(iface, 'name', '?')
    online = getattr(iface, 'online', '?')
    print(f"  Iface: {name} online={online}", flush=True)

# Try linking
print(f"\nLinking to d005497acc72fb103257d20e0a2314db...", flush=True)
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
else:
    print(f"\n❌ Link failed. status={link.status}")
sys.stdout.flush()
