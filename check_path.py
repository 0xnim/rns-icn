"""Check if VPS destination is in local path table."""
import RNS

RNS.Reticulum()
import time
time.sleep(15)  # Wait for mesh connections to establish + path table sync
print(f"Local Identity: {RNS.Identity().hexhash}")

# Check if we know the VPS destination path
dest_hash = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
known = dest_hash in list(RNS.Transport.path_table.keys())
print(f"VPS dest d005... known: {known}")

# Also check VPS identity path
id_hash = bytes.fromhex("f6b084e021f71c7a109d713365bae960")
known_id = id_hash in list(RNS.Transport.path_table.keys())
print(f"VPS identity f6b0... known: {known_id}")

print(f"\nTotal path entries: {len(RNS.Transport.path_table)}")
for i, h in enumerate(list(RNS.Transport.path_table.keys())[:10]):
    entry = RNS.Transport.path_table[h]
    print(f"  {i}: {h.hex()[:16]}... hops={entry[2]}")
