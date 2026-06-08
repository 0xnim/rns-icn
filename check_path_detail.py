"""Check VPS path details when it's known."""
import RNS, time, sys
RNS.Reticulum()

dest = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
print("Waiting for VPS path...")
for i in range(24):
    time.sleep(5)
    if dest in list(RNS.Transport.path_table.keys()):
        entry = RNS.Transport.path_table[dest]
        print(f"\nFOUND at [{i*5}s]!")
        print(f"  entry type: {type(entry)}")
        print(f"  entry repr: {entry}")
        if isinstance(entry, tuple):
            for j, e in enumerate(entry):
                if isinstance(e, bytes):
                    print(f"  [{j}]: {e.hex()}")
                else:
                    print(f"  [{j}]: {e}")
        print(f"  hops: {entry[2] if len(entry) > 2 else '?'}")
        print(f"  via: {entry[0].hex() if isinstance(entry[0], bytes) else entry[0]}")
        break
    sys.stdout.write(f"\r  [{i*5}s] still waiting...")
    sys.stdout.flush()
else:
    print("\nNot found after 2min")
