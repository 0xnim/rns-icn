"""Wait and check path table for VPS dest."""
import RNS, time

RNS.Reticulum()

# Wait for TCP connections and path sync
for i in range(12):
    time.sleep(5)
    pts = len(RNS.Transport.path_table)
    dest_hash = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
    known = dest_hash in list(RNS.Transport.path_table.keys())
    print(f"[{i*5}s] paths={pts} vps_known={known}")
    if known:
        entry = RNS.Transport.path_table[dest_hash]
        print(f"  VPS path: hops={entry[2]}")
        break

# Also list VPS-connected interface
for iface in RNS.Transport.interfaces:
    name = getattr(iface, 'name', '?')
    if 'VPS' in name or 'ICN' in name:
        print(f"  ICN VPS iface: {name} active")
