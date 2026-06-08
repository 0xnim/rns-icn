"""Check ICN VPS direct TCP interface."""
import RNS, time
RNS.Reticulum()
time.sleep(15)
print("Interfaces:")
for iface in RNS.Transport.interfaces:
    name = getattr(iface, 'name', '?')
    if 'VPS' in name or 'ICN' in name or '49200' in name:
        print(f"  ACTIVE: {name} ({type(iface).__name__})")
        if hasattr(iface, 'target_host'):
            print(f"    {iface.target_host}:{iface.target_port}")

dest = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
known = dest in list(RNS.Transport.path_table.keys())
print(f"\nVPS dest known: {known}")
print(f"Paths: {len(RNS.Transport.path_table)}")
for h in list(RNS.Transport.path_table.keys())[:5]:
    e = RNS.Transport.path_table[h]
    hops = e[2] if len(e) > 2 else "?"
    print(f"  {h.hex()[:16]}... hops={hops}")
