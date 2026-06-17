# Tutorials

Task-oriented walkthroughs for `rns-icn`. Each is self-contained and runnable.
For the symbol-by-symbol reference see [api-reference.md](api-reference.md); for
the wire format and security contract see [PROTOCOL.md](../PROTOCOL.md).

**Prerequisites.** Python ≥ 3.10 and the package installed:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .          # add ".[dev]" for the test/lint toolchain
```

This installs the CLIs: `icn-server`, `icn-router`, `icn-client`, `icn-publish`,
`icn-fetch`, `icn-subscribe`.

Contents:

1. [Run an origin and fetch from it (CLI)](#1-run-an-origin-and-fetch-from-it-cli)
2. [Publish and fetch from Python](#2-publish-and-fetch-from-python)
3. [Discover content with a manifest](#3-discover-content-with-a-manifest)
4. [Add a caching router (multi-hop)](#4-add-a-caching-router-multi-hop)
5. [Subscribe to a stream (pub/sub)](#5-subscribe-to-a-stream-pubsub)
6. [Restrict a prefix with access control](#6-restrict-a-prefix-with-access-control)
7. [Verifiable "latest version"](#7-verifiable-latest-version)
8. [Test against the forwarding engine (no RNS)](#8-test-against-the-forwarding-engine-no-rns)

A note on **names**: a name is `/<producer-hash>/<labels…>`. The first component
is the producer's 16-byte RNS address, which makes names *self-certifying* — the
name binds to the key that signs the content. You give a consumer two things: the
producer's **destination hash** (to reach it on the mesh) and a **name** (what to
ask for). See PROTOCOL.md §5.

---

## 1. Run an origin and fetch from it (CLI)

The fastest end-to-end loop. An origin serves content from its store and announces
on the mesh; `icn-fetch` dials it and retrieves a named blob.

Copy the example config and set an identity path and your mesh interface(s):

```bash
cp icn.toml.example icn.toml
# edit icn.toml: set [server] identity_path and mesh_interfaces
```

Start the origin:

```bash
icn-server --config icn.toml
```

It prints its **destination hash** on startup — that's the `peer_hash` consumers
use. From another shell, fetch a name (writes content to stdout; stderr carries
progress):

```bash
icn-fetch <peer_hash> docs/readme            # → content on stdout
icn-fetch <peer_hash> docs/readme out.bin    # → write to a file
icn-fetch <peer_hash> manifest               # → the producer's content index
```

`icn-publish` writes content *into* a peer's store the same way:

```bash
icn-publish <peer_hash> docs/readme ./README.md   # from a file
echo "hello" | icn-publish <peer_hash> greeting -  # from stdin
```

> Over a real mesh the first fetch waits for a *path* to the peer (path request +
> announce). If a fetch hangs at "Waiting for path", the peer either hasn't
> announced yet or isn't reachable on your configured interfaces.

---

## 2. Publish and fetch from Python

The same thing programmatically. An origin publishes content under its namespace;
a consumer fetches it with full verification.

**Producer:**

```python
import asyncio
from rns_icn import RNSICNServer, Name
from rns_icn.config import ServerConfig

async def main():
    config = ServerConfig(identity_path="~/.icn/origin.id")
    async with RNSICNServer(config) as server:
        addr = server.rns_identity.hash          # our 16-byte producer address
        name = Name(addr, [b"docs", b"readme"])  # /<us>/docs/readme
        server.publish_content(name, b"# Hello from ICN\n")
        print("serving as", server.hexhash)       # give this to consumers
        await asyncio.Event().wait()              # serve until interrupted

asyncio.run(main())
```

**Consumer:**

```python
import asyncio
from rns_icn.client import ICNClient
from rns_icn.config import ClientConfig
from rns_icn import Name

async def main():
    peer_hash = "…the origin's hexhash…"
    peer_addr = bytes.fromhex(peer_hash)         # producer address == identity hash

    async with ICNClient(ClientConfig()) as client:
        name = Name(peer_addr, [b"docs", b"readme"])
        data = await client.fetch(name, peer_addr)
        if data:
            print(data.content.decode())          # already hash-verified

asyncio.run(main())
```

`fetch()` handles retry/backoff, content-hash verification, signature
verification, rollback protection, and decryption. To **require** a producer
signature (reject unsigned data), set it on the config:

```python
ClientConfig(require_signature=True, reject_rollback=True)
```

---

## 3. Discover content with a manifest

Names are opaque; a **manifest** is the producer's index, so a consumer who only
knows the producer can discover what it serves. The origin builds one from its
store:

```python
server.publish_content(Name(addr, [b"a"]), b"alpha")
server.publish_content(Name(addr, [b"b"]), b"beta")
server.publish_manifest()                         # cache the index under /<us>/manifest
```

The consumer fetches the manifest, then content by entry:

```python
async with ICNClient(ClientConfig()) as client:
    manifest = await client.fetch_manifest(peer_addr)
    for entry in manifest.entries:
        content = await client.fetch_content(entry, peer_addr)
        print(entry.name, "→", content)
```

From the CLI, `icn-fetch <peer_hash> manifest` prints the manifest and its entry
count.

---

## 4. Add a caching router (multi-hop)

A router forwards Interests toward configured upstream peers and **caches** the
Data on the reverse path, so the second consumer to ask for a name gets it from
the router without bothering the origin.

Configure a router with the origin as a known peer. In its `icn.toml`:

```toml
[server]
identity_path = "router.id"
role = "CACHE"

[[server.known_peers]]
name = "origin"
destination_hash = "…origin hexhash…"
identity_path = "origin.pub"   # the origin's public identity, for announce injection
```

Run it:

```bash
icn-router --config icn.toml
```

Now point a consumer at the **router's** hash instead of the origin's. The first
fetch traverses router → origin and caches at the router; the next identical fetch
is served from the router's cache. This is also what gives **partition tolerance**:
if the origin goes away, the router keeps serving what it already cached, while a
*fresh* (uncached) name fails cleanly within its lifetime rather than black-holing.

Routes are dynamic: a dead upstream's route is **withdrawn** on link close (so it
stops black-holing) and **re-installed** when the peer re-announces. To force a
revalidation past the cache for a single fetch, mark the Interest `must_be_fresh`
(or use `fetch_latest`, below).

---

## 5. Subscribe to a stream (pub/sub)

Instead of polling, a consumer can subscribe to a prefix and have the producer
**push** each new Data. The CLI upgrades the link to push mode via an APS Subscribe
handshake:

```bash
icn-subscribe <peer_hash> feed                 # print each pushed Data
icn-subscribe <peer_hash> sensors/temp --from-now --out-dir ./pushed
icn-subscribe <peer_hash> feed --count 5       # stop after 5
```

On the producer side, publishing under the subscribed prefix pushes to current
subscribers; disconnected subscribers are served from an `OfflineQueue` when they
return. See PROTOCOL.md §9.1.

---

## 6. Restrict a prefix with access control

Because content lives in caches the producer doesn't control, access control is
**encryption**, not "don't serve it." A producer encrypts content under a
restricted prefix with a key derived from its own identity (caches relay opaque
ciphertext), and hands authorized consumers a **capability** carrying that key
wrapped to their identity.

**Producer** — declare the ACL, publish (auto-encrypted), and mint a capability:

```python
from rns_icn.config import ServerConfig, AccessRuleConfig

config = ServerConfig(
    identity_path="~/.icn/origin.id",
    access_rules=[
        AccessRuleConfig(prefix=["private"], consumers=[consumer_hex_hash]),
    ],
)

async with RNSICNServer(config) as server:
    addr = server.rns_identity.hash
    # content under /<us>/private/* is encrypted at publish:
    server.publish_content(Name(addr, [b"private", b"secret"]), b"classified")

    # mint a capability for the authorized consumer:
    consumer = RNS.Identity.recall(bytes.fromhex(consumer_hex_hash))
    cap = server.issue_capability(["private"], consumer, ttl_seconds=3600)
    # serialise `cap` and deliver it to the consumer out of band
```

**Consumer** — load the capability file and fetch transparently:

```python
config = ClientConfig(capabilities=["./my-capability.bin"])
async with ICNClient(config) as client:
    name = Name(peer_addr, [b"private", b"secret"])
    data = await client.fetch(name, peer_addr)   # decrypted in place via the capability
    print(data.content)
```

Without a valid capability the consumer still receives the (signed, hash-verified)
ciphertext but cannot decrypt it — it fails closed. See PROTOCOL.md §11.

---

## 7. Verifiable "latest version"

A cache's idea of "latest" is unverifiable — it could withhold a newer version. So
a producer publishes a **signed latest-version pointer** at a reserved name under
each collection, and the consumer uses `fetch_latest`, which fetches the pointer
`must_be_fresh` (forcing a stale cache to revalidate to the origin), verifies the
producer signature, then fetches the exact content-hash-pinned target.

**Producer** — publish versions with a sequence; the pointer refreshes
automatically:

```python
coll = Name(addr, [b"posts"])
server.publish_content(Name(addr, [b"posts", b"v1"]), b"first",  sequence=1)
server.publish_content(Name(addr, [b"posts", b"v2"]), b"second", sequence=2)
```

**Consumer:**

```python
data = await client.fetch_latest(Name(peer_addr, [b"posts"]), peer_addr)
print(data.content)          # → b"second", authenticated, not just cache-ranked
```

See PROTOCOL.md §14.1.

---

## 8. Test against the forwarding engine (no RNS)

The forwarding core (`Forwarder` + `Pit` + `Fib` + `ContentStore` + strategy) runs
fully in-process against mock faces — no RNS, no network — which is how the unit
and load tests drive it. This is the way to write fast tests or prototype
forwarding behaviour.

```python
import asyncio
from rns_icn import Forwarder, Name, Interest, Data, test_face_pair

async def main():
    addr = bytes(16)                       # a stand-in producer address
    fw = Forwarder(cs_max=128, pit_max=128)

    # a connected in-memory face pair: `up` is our "upstream"
    near, up = test_face_pair()
    fw.register_face(near)
    fw.add_route(Name(addr), near.id(), cost=10)

    name = Name(addr, [b"item"])
    # pre-seed the content store so express() resolves from cache:
    fw.cs.insert(name, Data.new(name=name, content=b"cached!"))

    result = await fw.express(Interest(name=name), in_face_id=999)
    assert result.content == b"cached!"
    print("ok")

asyncio.run(main())
```

For driving a real upstream, subclass `Face` (see `tests/test_load.py` for a mock
that answers forwarded Interests via `Forwarder.receive_data`). The same engine
powers PIT aggregation, nearest-expiry eviction, loop detection, and multi-path
failover — all testable without a mesh.

---

## Where to next

- **[api-reference.md](api-reference.md)** — every public class and function.
- **[PROTOCOL.md](../PROTOCOL.md)** — the normative wire format and security model.
- **[../ICN_ON_RNS_MESH.md](../ICN_ON_RNS_MESH.md)** — design narrative and mesh
  deployment notes.
- **[../ICN_ROADMAP.md](../ICN_ROADMAP.md)** — status matrix and what's next.
