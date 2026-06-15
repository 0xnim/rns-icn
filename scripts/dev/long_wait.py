"""Wait extended time for path to VPS."""
import RNS, time
RNS.Reticulum()

dest = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
for i in range(24):  # 2 minutes
    time.sleep(5)
    known = dest in list(RNS.Transport.path_table.keys())
    pts = len(RNS.Transport.path_table)
    print(f"[{i*5}s] paths={pts} vps_known={known}", flush=True)
    if known:
        e = RNS.Transport.path_table[dest]
        print(f"  FOUND! hops={e[2]}")
        break
print("Done.")
