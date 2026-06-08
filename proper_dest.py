"""Try link with proper destination creation."""
import RNS, time, sys
RNS.Reticulum()

# The VPS identity hash (from the persistent identity file)
vps_id_hash = bytes.fromhex("f6b084e021f71c7a109d713365bae960")

# Compute destination hash the correct way
vps_dest_hash = RNS.Destination.hash_from_name_and_identity(
    "icn.default", vps_id_hash
)
print(f"Computed dest hash: {vps_dest_hash.hex()}")
expected = "d005497acc72fb103257d20e0a2314db"
print(f"Expected: {expected}")
print(f"Match: {vps_dest_hash.hex() == expected}")

# Wait for path
db = vps_dest_hash
for i in range(36):
    time.sleep(5)
    if db in list(RNS.Transport.path_table.keys()):
        entry = RNS.Transport.path_table[db]
        hops = entry[2]
        print(f"\nPath found at [{i*5}s]! hops={hops}")
        
        # Create OUT destination WITHOUT overriding hash
        # Use the remote identity properly
        vps_identity = RNS.Identity()
        vps_identity.hash = vps_id_hash  # This might not work
        
        # Alternative: create destination and let RNS compute the hash
        dest = RNS.Destination(
            vps_identity, RNS.Destination.OUT, RNS.Destination.SINGLE,
            "icn", "default",
        )
        print(f"Dest hash after creation: {dest.hash.hex()}")
        
        if dest.hash == db:
            print("Hash matches! Trying link...")
            link = RNS.Link(dest)
            start = time.time()
            while link.status != RNS.Link.ACTIVE:
                time.sleep(0.2)
                if time.time() - start > 15:
                    print(f"Failed. status={link.status} reason={link.teardown_reason}")
                    break
                if link.status == RNS.Link.CLOSED:
                    print(f"Closed. reason={link.teardown_reason}")
                    break
            else:
                print(f"ACTIVE! ({time.time()-start:.1f}s)")
                link.teardown()
        else:
            print(f"Hash MISMATCH! computed={dest.hash.hex()} expected={db.hex()}")
        break
    sys.stdout.write(f"\r[{i*5}s] waiting...")
    sys.stdout.flush()
else:
    print("\nNot found after 3min")
