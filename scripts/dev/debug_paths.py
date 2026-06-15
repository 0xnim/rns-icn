"""Debug: check if TCP client to VPS is actually getting data."""
import RNS, time
RNS.Reticulum()

# Wait for interfaces
time.sleep(10)

# Find the ICN VPS interface
for iface in RNS.Transport.interfaces:
    name = getattr(iface, 'name', '?')
    if 'ICN VPS' in name or '49200' in name:
        print(f"VPS interface: {name}")
        print(f"  type: {type(iface).__name__}")
        print(f"  online: {getattr(iface, 'online', '?')}")
        print(f"  target: {getattr(iface, 'target_host', '?')}:{getattr(iface, 'target_port', '?')}")
        # Check if it has announced destinations
        if hasattr(iface, 'announce_queue'):
            print(f"  announce_queue: {len(iface.announce_queue)}")

# Check ALL known identities
print(f"\nAll path entries ({len(RNS.Transport.path_table)}):")
for h in list(RNS.Transport.path_table.keys()):
    e = RNS.Transport.path_table[h]
    hops = e[2] if len(e) > 2 else "?"
    via = e[1].hex()[:16] if len(e) > 1 and isinstance(e[1], bytes) else "?"
    print(f"  {h.hex()[:16]} hops={hops} via={via}")

# Check if VPS IP is in routing table
print(f"\nInterface stats:")
for iface in RNS.Transport.interfaces:
    name = getattr(iface, 'name', '?')
    if hasattr(iface, 'online'):
        print(f"  {name}: online={iface.online}")
