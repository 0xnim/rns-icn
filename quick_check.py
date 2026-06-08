"""Quick check if VPS dest is in local path table."""
import RNS, time
RNS.Reticulum()
time.sleep(20)
dest = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
known = dest in list(RNS.Transport.path_table.keys())
print(f"VPS dest known: {known}")
pts = len(RNS.Transport.path_table)
print(f"Paths: {pts}")
for h in list(RNS.Transport.path_table.keys())[:5]:
    e = RNS.Transport.path_table[h]
    print(f"  {h.hex()[:16]}... hops={e[2]}")
