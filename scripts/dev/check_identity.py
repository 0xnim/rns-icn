"""Check if VPS identity (not just dest) appears in local path table."""
import RNS, time
RNS.Reticulum()
time.sleep(15)

# VPS identity hash
id_hash = bytes.fromhex("f6b084e021f71c7a109d713365bae960")
known_id = id_hash in list(RNS.Transport.path_table.keys())
print(f"VPS identity f6b0... in path table: {known_id}")

# VPS destination hash
dest_hash = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
known_dest = dest_hash in list(RNS.Transport.path_table.keys())
print(f"VPS dest d005... in path table: {known_dest}")

# Also check for the VPS's RNS node identity (from the backbone daemon)
backbone_id = bytes.fromhex("2884660")  # not real
print(f"\nPaths: {len(RNS.Transport.path_table)}")

# Filter for VPS IP
for h in list(RNS.Transport.path_table.keys()):
    e = RNS.Transport.path_table[h]
    hops = e[2] if len(e) > 2 else "?"
    if h == id_hash or h == dest_hash:
        print(f"  FOUND: {h.hex()[:16]} hops={hops}")
