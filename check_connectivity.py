"""Test direct link to echo server on VPS."""
import RNS, time, os

echo_dest_hex = "d323188d4c9b4267a0576947a13e7fda"
echo_id_hex = "6704c532491b8768d34ac2f53bda7267"
echo_dest = bytes.fromhex(echo_dest_hex)
echo_id = bytes.fromhex(echo_id_hex)

config_dir = "/tmp/rns_echo_client"
os.makedirs(config_dir, exist_ok=True)
with open(os.path.join(config_dir, "config"), "w") as f:
    f.write(f"""\
[reticulum]
  enable_transport = No
  share_instance = No
[logging]
  loglevel = 3
[interfaces]
  [[Echo Direct]]
    type = TCPClientInterface
    enabled = yes
    target_host = 172.81.133.81
    target_port = 49201
    ifac_size = 6
""")

print("Starting Reticulum with direct echo connection...", flush=True)
RNS.Reticulum(configdir=config_dir)
time.sleep(10)

print(f"\nInterfaces:", flush=True)
for iface in RNS.Transport.interfaces:
    name = getattr(iface, 'name', '?')
    online = getattr(iface, 'online', '?')
    print(f"  {name} online={online}", flush=True)

print(f"\nPath table: {len(RNS.Transport.path_table)} entries", flush=True)
print(f"Announce table: {len(RNS.Transport.announce_table)} entries", flush=True)

found_id = echo_id in RNS.Transport.path_table
found_dest = echo_dest in RNS.Transport.path_table
found_id_ann = echo_id in RNS.Transport.announce_table
print(f"Echo identity in path: {found_id}", flush=True)
print(f"Echo dest in path: {found_dest}", flush=True)
print(f"Echo identity in announce: {found_id_ann}", flush=True)

# Print any paths we got
for h in list(RNS.Transport.path_table.keys())[:10]:
    print(f"  Path: {h.hex()[:24]}...", flush=True)
for h in list(RNS.Transport.announce_table.keys())[:10]:
    print(f"  Announce: {h.hex()[:24]}...", flush=True)

# Try to link
print(f"\nLinking to echo server...", flush=True)
identity = RNS.Identity()
dest = RNS.Destination(
    identity,
    RNS.Destination.OUT,
    RNS.Destination.SINGLE,
    "echo", "test",
)
dest.hash = echo_dest

link = RNS.Link(dest)
start = time.time()
while link.status == RNS.Link.PENDING and time.time() - start < 30:
    time.sleep(0.5)

if link.status == RNS.Link.ACTIVE:
    print(f"\n✅ LINK ACTIVE! RTT={link.rtt*1000:.1f}ms", flush=True)
    link.teardown()
else:
    print(f"\n❌ Link failed. status={link.status}, elapsed={time.time()-start:.0f}s", flush=True)
