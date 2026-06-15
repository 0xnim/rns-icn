"""Check VPS path details - which interface is it pointing at."""
import RNS, time
RNS.Reticulum()
time.sleep(10)

dest = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
known = dest in list(RNS.Transport.path_table.keys())

if known:
    entry = RNS.Transport.path_table[dest]
    print(f"VPS dest: path table entry:")
    print(f"  entry[0] (via identity hash): {entry[0].hex() if isinstance(entry[0], bytes) else entry[0]}")
    print(f"  entry[1] (rx identity): {entry[1].hex() if isinstance(entry[1], bytes) else entry[1]}")
    print(f"  entry[2] (hops): {entry[2]}")
    
    # Try finding which interface has the via identity
    via_hash = entry[0] if isinstance(entry[0], bytes) else None
    if via_hash:
        for iface in RNS.Transport.interfaces:
            name = getattr(iface, 'name', '?')
            print(f"  Iface {name}: has_if={getattr(iface, 'hash', '?')}")
            if hasattr(iface, 'hash') and iface.hash == via_hash:
                print(f"    -> MATCH! {name}")
else:
    print("VPS dest not in path table")
    print(f"Paths: {len(RNS.Transport.path_table)}")
    for h in list(RNS.Transport.path_table.keys())[:5]:
        e = RNS.Transport.path_table[h]
        print(f"  {h.hex()[:16]} hops={e[2]}")
