"""Minimal local link test on the same RNS instance."""
import RNS, time, os, sys

RNS.Reticulum()
time.sleep(3)

# Create server identity and destination (IN)
server_identity = RNS.Identity()
server_dest = RNS.Destination(
    server_identity,
    RNS.Destination.IN,
    RNS.Destination.SINGLE,
    "test", "echo",
)

print(f"Server identity: {server_identity.hexhash}")
print(f"Server dest: {server_dest.hexhash}")
sys.stdout.flush()

link_established = [False]

def on_link(link):
    link_established[0] = True
    print(f"✅ Link established from {link.get_remote_identity()}")
    sys.stdout.flush()

server_dest.set_link_established_callback(on_link)
server_dest.announce()

time.sleep(2)

# Client: create OUT destination pointing to server
client_identity = RNS.Identity()
client_dest = RNS.Destination(
    client_identity,
    RNS.Destination.OUT,
    RNS.Destination.SINGLE,
    "test", "echo",
)

# Override with server's hash - the icn-fetch approach
client_dest.hash = server_dest.hash
client_dest.hexhash = server_dest.hexhash

print(f"\nClient identity: {client_identity.hexhash}")
print(f"Target hash: {client_dest.hexhash}")
print(f"Announce table: {len(RNS.Transport.announce_table)}")
print(f"Path table: {len(RNS.Transport.path_table)}")

# Does the path table contain server's identity?
sid = server_identity.hash
print(f"Server identity in path: {sid in RNS.Transport.path_table}")
print(f"Server identity in announce: {sid in RNS.Transport.announce_table}")
sys.stdout.flush()

link = RNS.Link(client_dest)
time.sleep(5)

if link_established[0]:
    print(f"✅ SERVER received the link!")
elif link.status == RNS.Link.ACTIVE:
    print(f"✅ Link ACTIVE from client side!")
else:
    print(f"❌ Link failed. status={link.status}")
    print(f"  Client dest hash: {client_dest.hash.hex()}")
    print(f"  Server dest hash: {server_dest.hash.hex()}")
    print(f"  Same aspects: test/echo")
sys.stdout.flush()
