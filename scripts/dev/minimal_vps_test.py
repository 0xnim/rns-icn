"""Minimal RNS client — connect only to VPS, see what announces arrive."""
import RNS, os, tempfile, time

# Create a temporary config dir with only the VPS interface
configdir = tempfile.mkdtemp(prefix="rns_vps_test_")
os.makedirs(f"{configdir}/storage", exist_ok=True)

config = f"""[reticulum]
enable_transport = No
share_instance = No

[logging]
loglevel = 3

[interfaces]
  [[Direct VPS]]
    type = TCPClientInterface
    enabled = yes
    target_host = 172.81.133.81
    target_port = 49200
    ifac_size = 6
"""
with open(f"{configdir}/config", "w") as f:
    f.write(config)

print(f"[Test] Config dir: {configdir}")
print(f"[Test] Starting isolated RNS (only VPS)...")

os.environ["RNS_CONFIGDIR"] = configdir
try:
    RNS.Reticulum(configdir=configdir)
except TypeError:
    RNS.Reticulum(configdir=configdir, loglevel=3)

print("[Test] RNS initialized")

# Wait for connection and announces
for i in range(24):  # 2 minutes
    time.sleep(5)
    pts = len(RNS.Transport.path_table)
    # Check for VPS dest and identity
    dest_b = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
    id_b = bytes.fromhex("f6b084e021f71c7a109d713365bae960")
    known_dest = dest_b in list(RNS.Transport.path_table.keys())
    known_id = id_b in list(RNS.Transport.path_table.keys())
    print(f"  [{i*5}s] paths={pts} dest={known_dest} ident={known_id}")
    if known_dest or known_id:
        break

# Show all paths
print(f"\nFinal paths ({len(RNS.Transport.path_table)}):")
for h in list(RNS.Transport.path_table.keys())[:15]:
    e = RNS.Transport.path_table[h]
    print(f"  {h.hex()[:16]} hops={e[2]}")
