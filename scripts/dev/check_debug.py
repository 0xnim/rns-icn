"""Debug: check if dest.hash is actually overridden."""
import RNS, time, sys

RNS.Reticulum()
time.sleep(5)

vps_dest = bytes.fromhex("d005497acc72fb103257d20e0a2314db")

identity = RNS.Identity()
dest = RNS.Destination(
    identity,
    RNS.Destination.OUT,
    RNS.Destination.SINGLE,
    "icn", "default",
)

print(f"After creation: hash={dest.hash.hex()}, hexhash={dest.hexhash}")
sys.stdout.flush()

dest.hash = vps_dest
dest.hexhash = vps_dest.hex()

print(f"After override: hash={dest.hash.hex()}, hexhash={dest.hexhash}")
print(f"Override target: {vps_dest.hex()}")
print(f"Match: {dest.hash == vps_dest}")
sys.stdout.flush()

# Now try to link
print(f"\nLinking to {vps_dest.hex()}...", flush=True)
link = RNS.Link(dest)
start = time.time()
while link.status == RNS.Link.PENDING and time.time() - start < 30:
    time.sleep(0.5)
print(f"Link: status={link.status}, elapsed={time.time()-start:.0f}s", flush=True)
