"""Wait for VPS re-announce (fires every 300s from startup)."""
import RNS, os, tempfile, time, threading, sys

configdir = tempfile.mkdtemp(prefix="rns_vps_wait_")
os.makedirs(f"{configdir}/storage", exist_ok=True)

config = """[reticulum]
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

os.environ["RNS_CONFIGDIR"] = configdir
RNS.Reticulum(configdir=configdir)

dest_b = bytes.fromhex("d005497acc72fb103257d20e0a2314db")
id_b = bytes.fromhex("f6b084e021f71c7a109d713365bae960")

print(f"Waiting for VPS re-announce... (dest=d005..., ident=f6b0...)")
for i in range(72):  # 6 minutes
    time.sleep(5)
    known_dest = dest_b in list(RNS.Transport.path_table.keys())
    known_id = id_b in list(RNS.Transport.path_table.keys())
    sys.stdout.write(f"\r  [{i*5}s] paths={len(RNS.Transport.path_table)} dest={known_dest} ident={known_id}  ")
    sys.stdout.flush()
    if known_dest:
        print(f"\nFound VPS destination!")
        break

print()
if not known_dest:
    print("Still not found after 6 minutes.")

# Check if we're connected 
for iface in RNS.Transport.interfaces:
    name = getattr(iface, 'name', '?')
    online = getattr(iface, 'online', '?')
    print(f"  {name}: online={online}")
