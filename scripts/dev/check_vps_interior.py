"""Check VPS interior state."""
import RNS, time, os, sys

os.environ["HOME"] = "/root"
RNS.Reticulum()
time.sleep(15)

vps_id = bytes.fromhex("f6b084e021f71c7a109d713365bae960")
vps_dest = bytes.fromhex("d005497acc72fb103257d20e0a2314db")

print(f"Path table: {len(RNS.Transport.path_table)}")
print(f"Announce table: {len(RNS.Transport.announce_table)}")
print(f"Identity f6b0 in path: {vps_id in RNS.Transport.path_table}")
print(f"Dest d005 in path: {vps_dest in RNS.Transport.path_table}")
print(f"Dest d005 in announce: {vps_dest in RNS.Transport.announce_table}")
print(f"Identity f6b0 in announce: {vps_id in RNS.Transport.announce_table}")

# Check interfaces
print("\nInterfaces:")
for iface in RNS.Transport.interfaces:
    name = getattr(iface, 'name', '?')
    online = getattr(iface, 'online', '?')
    print(f"  {type(iface).__name__} {name} online={online}")

# Some path entries
print(f"\nPath entries (first 15):")
for i, h in enumerate(list(RNS.Transport.path_table.keys())[:15]):
    print(f"  {h.hex()[:20]}...")

# Announce entries
print(f"\nAnnounce entries (first 15):")
for i, h in enumerate(list(RNS.Transport.announce_table.keys())[:15]):
    print(f"  {h.hex()[:20]}...")
