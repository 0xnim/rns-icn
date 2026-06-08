"""Check ICN VPS interface status."""
import RNS, time
RNS.Reticulum()
time.sleep(5)

for iface in RNS.Transport.interfaces:
    name = getattr(iface, "name", "")
    if "VPS" in name or "ICN" in name:
        print(f"{name}:")
        print(f"  online={getattr(iface, 'online', '?')}")
        print(f"  target_ip={getattr(iface, 'target_ip', '?')}")
        print(f"  target_port={getattr(iface, 'target_port', '?')}")
        if hasattr(iface, "target_host"):
            print(f"  target_host={iface.target_host}")
        break

db = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
known = db in list(RNS.Transport.path_table.keys())
print(f"VPS dest in path: {known}")
print(f"Paths: {len(RNS.Transport.path_table)}")

for h in list(RNS.Transport.path_table.keys()):
    hs = h.hex()
    if hs.startswith("f6b084") or hs.startswith("d00549"):
        e = RNS.Transport.path_table[h]
        print(f"  FOUND: {hs} hops={e[2]}")
