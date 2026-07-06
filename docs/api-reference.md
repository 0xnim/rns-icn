# API Reference

The Python API for `rns-icn`. This documents the **public, supported surface** —
the classes and functions you build applications against. For the wire format and
security model (the contract a *re-implementer* needs), see
[PROTOCOL.md](../PROTOCOL.md). For task-oriented walkthroughs, see
[tutorials.md](tutorials.md).

> **Stability.** The wire protocol is versioned and frozen at 1.0 (see PROTOCOL.md
> §17). The Python API is still pre-1.0 (`0.x`) and may change between releases;
> anything underscore-prefixed is private regardless.

Most symbols are re-exported from the package root:

```python
import rns_icn
from rns_icn import Name, Interest, Data, Forwarder, ContentStore
```

The one exception worth knowing: **`ICNClient` is imported from its module**, not
the package root —

```python
from rns_icn.client import ICNClient
from rns_icn.config import ClientConfig, load_client_config
```

---

## Table of contents

1. [High-level: consumer (`ICNClient`)](#1-high-level-consumer-icnclient)
2. [High-level: producer / router (`RNSICNServer`)](#2-high-level-producer--router-rnsicnserver)
3. [Configuration](#3-configuration)
4. [Core types: `Name`, `Interest`, `Data`](#4-core-types-name-interest-data)
5. [The forwarding engine](#5-the-forwarding-engine)
6. [Identity helpers](#6-identity-helpers)
7. [Large content & chunking](#7-large-content--chunking)
8. [Errors](#8-errors)

---

## 1. High-level: consumer (`ICNClient`)

`rns_icn.client.ICNClient` — the consumer side. It initialises RNS, manages an
identity and a `LinkPool`, and runs a local `Forwarder`, exposing a small
`fetch` surface that handles retry/backoff, signature verification, rollback
protection, and decryption for you.

```python
from rns_icn.client import ICNClient
from rns_icn.config import ClientConfig
```

### `ICNClient(config: ClientConfig)`

Construct against a [`ClientConfig`](#clientconfig). Does no I/O until `start()`.

### `async start() -> ICNClient`

Initialises RNS (only if not already running — RNS is a process-global
singleton), loads or creates the identity, and starts the link pool. Returns
`self`, so it composes with the async context-manager form:

```python
async with await ICNClient(config).start() as client:
    ...
# — or equivalently —
async with ICNClient(config) as client:   # __aenter__ calls start()
    ...
```

### `async shutdown() -> None`

Stops the link pool and, if this client started RNS, tears it down. Called
automatically on context-manager exit.

### `async fetch(name, peer_hash, timeout=None, max_retries=None) -> Data | None`

Express an Interest for `name` to the peer identified by `peer_hash` (16 raw
bytes — the producer's RNS address), retrying with exponential backoff up to
`max_retries`. The returned `Data` has already passed:

- **content-hash verification** (always),
- **signature verification** — required if `config.require_signature`, otherwise
  verify-if-present,
- **rollback protection** — if `config.reject_rollback`, a stale-but-validly-signed
  replay is rejected,
- **decryption** — transparent when a matching capability is loaded
  (`config.capabilities`).

`timeout` and `max_retries` default to the config values. Raises the last error
on exhausted retries (e.g. `TimeoutError`); returns `None` only if the data
couldn't be validated.

### `async fetch_latest(prefix, peer_hash, timeout=None, max_retries=None) -> Data | None`

Fetch the **authenticated** latest version under a collection `prefix`. Fetches
the producer-signed latest-version pointer with `must_be_fresh` (so a stale cache
revalidates to the origin), verifies it, then fetches the exact content-hash-pinned
target it names. Falls back to the best-effort `latest` child selector only when
no pointer exists. See PROTOCOL.md §14.1.

### `async fetch_range(prefix, peer_hash, start_sequence=0, max_items=None, timeout=None, max_retries=None) -> AsyncIterator[Data]`

Walk a collection's editions in sequence order, each passing the full verify
pipeline — the backfill primitive. Gap-tolerant: every step asks for "the next
edition ≥ N" (OLDEST child selector with a `min_sequence` floor), never an
exact sequence. Stops after `max_items` editions, or when nothing newer is
available — which may mean caught-up, or partitioned from anyone holding more.

### `async subscribe(prefix, peer_hash, callback, start_from_now=True) -> None`

Upgrade the link to push mode: APS-subscribe to `prefix` on the peer. Each
pushed `Data` runs the same verify pipeline as a fetch before `callback` fires
on the event loop. With `start_from_now=False` the producer first replays its
existing content; either way anything offline-queued for this subscriber is
drained, so a resubscribe after a disconnect delivers what was missed.

### `async resolve(destination_hash, timeout=None) -> RNS.Identity | None`

The producer's identity from its RNS destination hash. ICN names are rooted in
the producer's *identity* hash (the name is the key), but what apps share and
dial are *destination* hashes; `resolve` bridges the two — the returned
identity's `.hash` is the address the producer's names live under, and its keys
verify their signatures. If the identity isn't known locally, the mesh is asked
for the producer's path and the answering announce carries the keys. `timeout`
defaults to `config.path_request_timeout`; returns `None` when no announce
arrives (e.g. partitioned from everyone who has heard the producer).

### `async fetch_manifest(peer_hash, ...) -> Manifest | None` · `async fetch_content(...)`

Manifest-driven discovery: fetch a producer's content index, then fetch entries
by label. See [tutorials.md](tutorials.md) for the manifest walkthrough.

### Properties

| Property | Returns | Use |
|----------|---------|-----|
| `identity` | `RNS.Identity` | this client's RNS identity |
| `forwarder` | `Forwarder` | the local forwarding engine (advanced) |
| `link_pool` | `LinkPool` | the outbound link pool (advanced) |

(These are properties — access without parentheses, e.g. `client.forwarder`.)

---

## 2. High-level: producer / router (`RNSICNServer`)

`rns_icn.rns_server.ICNServer`, re-exported from the package root as
**`RNSICNServer`** to avoid the name clash with the lower-level
[`ICNServer`](#5-the-forwarding-engine). This is the full RNS-integrated node: an
origin (serves and signs its own content), a cache/router (forwards Interests and
caches Data), or a propagation node, selected by `config.role`.

```python
from rns_icn import RNSICNServer
from rns_icn.config import ServerConfig
```

### `RNSICNServer(config: ServerConfig, link_pool: LinkPool | None = None)`

Loads the identity from `config.identity_path` and wires up the content store
(SQLite), forwarding strategy, access controller, and announce/aging tasks. Pass a
shared `link_pool` to co-locate multiple roles on one pool; otherwise one is
created. The RNS destination isn't created until `start()`.

### `async start() -> None` · `async shutdown() -> None`

Create the RNS destination, begin accepting inbound links as faces, start the
announce loop and PIT-aging loop, and (if `config.http_enabled`) the HTTP
health/metrics API. Both are also driven by the async context manager:

```python
async with RNSICNServer(config) as server:
    server.publish_content(name, b"hello")
    ...
```

### `publish_content(name, content, sequence=None, latest_under=None) -> None`

Publish `content` under `name` into the content store. Content under a restricted
prefix (`config.access_rules`) is **encrypted** before storage, so caches relay
opaque ciphertext. Also refreshes the collection's verifiable latest-version
pointer; `latest_under` overrides the collection prefix (default: the name's
parent). `sequence` stamps a version for rollback protection and `latest`
selection.

### `issue_capability(prefix_labels, consumer, ttl_seconds=0) -> Capability`

Mint a producer-signed capability that lets `consumer` (an `RNS.Identity`) read a
restricted prefix. `prefix_labels` are the labels under this producer's namespace
(e.g. `["private"]` → `/<us>/private`) and must match a configured access rule
listing that consumer. The capability carries the content-encryption key wrapped
to the consumer's identity. `ttl_seconds=0` means no expiry. See PROTOCOL.md §11.

### `async connect(peer_hash: str) -> FaceId | None`

Establish (or reuse) an outbound link to a peer by hex hash, returning the face
id. Used by routers to dial configured upstreams.

### `announce(app_data: bytes | None = None) -> None`

Send an RNS announce now (the role byte is encoded into `app_data` by default).
The server also announces on `config.announce_interval`.

### `publish_manifest() -> None`

Build a manifest from the current content store and cache it under the producer's
manifest name, so consumers can discover content by label.

### Properties & misc

| Member | Returns | Use |
|--------|---------|-----|
| `hexhash` | `str` | this node's destination hash (hex) — give it to consumers |
| `rns_identity` | `RNS.Identity` | the node identity |
| `link_pool` | `LinkPool` | the link pool |
| `resource_threshold` | `int` (read/write) | byte size above which content goes over RNS Resource transfer |

(`hexhash`, `rns_identity`, `link_pool` are properties; `resource_threshold` is a
read/write property.)

---

## 3. Configuration

`rns_icn.config`. Dataclasses plus TOML loaders. The loaders expand relative
paths (`identity_path`, `cs_path`, peer identity files, capability files)
relative to the config file's directory.

```python
from rns_icn.config import (
    ClientConfig, ServerConfig, KnownPeer, AccessRuleConfig,
    load_client_config, load_server_config,
)

client_cfg = load_client_config("icn.toml")   # reads the [client] table
server_cfg = load_server_config("icn.toml")   # reads the [server] table
```

### `ClientConfig`

| Field | Default | Meaning |
|-------|---------|---------|
| `identity_path` | `None` | identity file; `None` → ephemeral identity |
| `mesh_interfaces` | `["UTN Oregon"]` | RNS interface names |
| `known_peers` | `[]` | `KnownPeer` list for announce-table injection |
| `connect_timeout` | `60.0` | link establishment timeout (s) |
| `fetch_timeout` | `30.0` | per-fetch timeout (s) |
| `max_retries` | `5` | retries before giving up |
| `base_retry_delay` / `max_retry_delay` | `1.0` / `30.0` | exponential backoff bounds (s) |
| `require_signature` | `False` | reject unsigned/unverifiable Data |
| `reject_rollback` | `False` | reject stale-but-signed replays |
| `capabilities` | `[]` | capability files granting read access to restricted prefixes |
| `log_level` / `log_json` | `"INFO"` / `False` | logging |

### `ServerConfig`

| Field | Default | Meaning |
|-------|---------|---------|
| `identity_path` | *(required)* | node identity file |
| `role` | `ServerRole.ORIGIN` | `ORIGIN` / `CACHE` / `PROPAGATION` |
| `rns_configdir` | `None` | point at a shared `rnsd` configdir to ride its transport |
| `mesh_interfaces` | `["UTN Oregon"]` | RNS interface names |
| `access_rules` | `[]` | `AccessRuleConfig` list — per-prefix ACLs |
| `announce_interval` | `30.0` | announce cadence (s) |
| `reannounce_on_link` | `True` | re-announce when a link comes up |
| `cs_max_entries` | `10000` | content-store LRU cap |
| `cs_ttl_seconds` | `None` | default cache TTL |
| `cs_path` | `~/.icn/content_store.db` | SQLite store path |
| `cs_prefix_ttls` | `{}` | per-prefix TTL overrides |
| `cs_stale_while_revalidate` | `0` | stale-serve window (s); `0` disables |
| `meta_freshness_period` | `15` | latest-pointer freshness (s) |
| `pit_max_entries` | `10000` | in-flight Interest cap (nearest-expiry eviction) |
| `pit_purge_interval` | `5.0` | PIT/nonce aging cadence (s) |
| `resource_threshold` | `100000` | bytes above which content uses Resource transfer |
| `known_peers` | `[]` | upstream peers (routers dial these) |
| `http_enabled` / `http_host` / `http_port` | `False` / `127.0.0.1` / `8080` | health/metrics HTTP API |
| `log_level` / `log_json` | `"INFO"` / `False` | logging |

### `KnownPeer(name, destination_hash, identity_path=None, aliases=())`

A pre-configured peer. `destination_hash` is the 32-char hex hash; `identity_path`
(optional) is a file with the peer's public identity, needed for announce-table
injection so a router can reach a peer it hasn't heard announce yet.

### `AccessRuleConfig(prefix: list[str], consumers: list[str] = ())`

One ACL entry: the label path (under this producer's namespace) and the hex
consumer identity hashes allowed to read it. Empty `consumers` is allowed but
pointless; an empty rule list means everything is public.

---

## 4. Core types: `Name`, `Interest`, `Data`

`rns_icn.name` and `rns_icn.packet`. The data model. See PROTOCOL.md §5–§8 for the
wire encoding.

### `Name`

A name is a routable prefix (first component is the producer's 16-byte RNS
address, then label components) plus an optional 32-byte content-hash suffix for
self-certification.

```python
from rns_icn import Name

addr = bytes.fromhex("…32 hex chars = 16 bytes…")
n = Name(addr, [b"docs", b"readme"])      # /<addr>/docs/readme
n = Name.from_string("/<addr-hex>/docs/readme")
n.to_bytes()                               # wire encoding
n.rns_addr()                               # the producer address (first component)
n.len()                                    # component count
n.starts_with(Name(addr, [b"docs"]))       # prefix test → True
pinned = n.with_content_hash(h)            # add ?hash=… self-certification
```

| Member | Signature | Notes |
|--------|-----------|-------|
| `Name(rns_addr, path=None, content_hash=None)` | `bytes, list[bytes], bytes` | `rns_addr` must be 16 bytes; `content_hash` 32 bytes |
| `Name.from_string(s)` | classmethod | parses `/<hex>/a/b?hash=<hex>` |
| `Name.from_bytes(data)` / `to_bytes()` | wire | round-trips |
| `with_content_hash(h)` / `without_content_hash()` | → `Name` | self-certifying suffix |
| `rns_addr()` / `len()` / `is_root()` | accessors | |
| `starts_with(prefix)` / `is_prefix_of(other)` | prefix relation | |

Constants: `RNS_ADDR_BYTES = 16`, `CONTENT_HASH_BYTES = 32`, `MAX_COMPONENTS = 32`.

### `Interest`

```python
from rns_icn import Interest

i = Interest(name=n)                       # nonce auto-generated, lifetime 4000ms
i = (Interest(name=n)
       .with_lifetime(30_000)
       .with_must_be_fresh()
       .with_can_be_prefix())
```

| Field / builder | Default | Meaning |
|-----------------|---------|---------|
| `name` | *(required)* | what's wanted |
| `nonce` | random 8 bytes | loop/duplicate suppression |
| `lifetime_ms` | `4000` | PIT lifetime |
| `can_be_prefix` | `False` | match a longer name under this prefix |
| `must_be_fresh` | `False` | bypass stale cache, revalidate upstream |
| `selector` | `None` | `latest`/`oldest`/`min_sequence` (see `InterestSelector`, `ChildSelector`) |
| `hop_limit` | `DEFAULT_HOP_LIMIT` (16) | forwarding TTL |

Builders (`with_lifetime`, `with_must_be_fresh`, `with_can_be_prefix`,
`with_selector`, `with_hop_limit`) mutate and return `self` for chaining.

### `Data`

```python
from rns_icn import Data

d = Data.new(name=n, content=b"hello")     # content hash computed
d.with_sequence(7).with_freshness_period(60)
d.sign(identity.sign)                       # Ed25519 over the signed envelope
d.verify_content_hash()                     # bool
d.verify_signature(RNS.Identity.validate)   # bool, given a validator
```

| Member | Signature | Notes |
|--------|-----------|-------|
| `Data.new(name, content)` | classmethod | computes content hash into `metadata` |
| `with_sequence(seq)` / `with_freshness_period(s)` / `with_staleness(age)` | builders | |
| `sign(signer)` | `signer: bytes->bytes` | signs `name+content+hash+sequence+signed_at` |
| `verify_signature(validator)` | `validator: (msg, sig)->bool` | |
| `verify_content_hash()` | → bool | |
| `.content` / `.metadata` | fields | `metadata` is a `DataMetadata` |

`DataMetadata` carries `content_hash`, `sequence`, `signed_at`, `freshness_period`,
`encrypted`, and the signature. See PROTOCOL.md §8.1.

### `parse_packet(raw: bytes) -> Packet`

Parse any wire packet (Interest / Data / control) by its type+version header. The
returned `Packet` exposes `.interest`, `.data`, etc. for the populated variant.
Raises on an unsupported protocol generation rather than mis-parsing.

---

## 5. The forwarding engine

The NDN triad, usable standalone and **fully in-process** (no RNS) — which is how
the unit and load tests drive it. Use these to embed forwarding, write tests
against mock faces, or build a custom node.

### `Forwarder`

```python
from rns_icn import Forwarder

fw = Forwarder(cs_max=1000, pit_max=10000)
fw.register_face(face)
fw.add_route(Name(addr), face.id(), cost=10)
data = await fw.express(Interest(name=n), in_face_id)   # consumer side
await fw.receive_data(data, in_face_id)                  # data return path
```

Owns a `ContentStore` (`fw.cs`), `Pit` (`fw.pit`), `Fib` (`fw.fib`), and a
`Strategy`. `express()` is the consumer entry point; `receive_data()` /
`receive_nack()` are the return paths. Routes are added with `add_route` and torn
down with `withdraw_face` on link close.

### `ICNServer` (low-level) & `ServerRole`

`rns_icn.server.ICNServer` — the transport-agnostic forwarding server that
[`RNSICNServer`](#2-high-level-producer--router-rnsicnserver) wraps. Handles
`handle_interest` / `handle_data` / `handle_nack` / `handle_invalidate` /
`handle_subscribe` over an abstract `Face`. `ServerRole` is the `IntEnum`
`{ORIGIN, CACHE, PROPAGATION}`.

### `Pit`, `Fib`, `ContentStore`

| Class | Role | Key methods |
|-------|------|-------------|
| `Pit` | pending Interests + loop suppression | `insert_or_aggregate`, `satisfy`, `remove`, `purge_expired`, `check_loop`, `record_nonce`, `is_full` |
| `Fib` | prefix → faces, longest-prefix match | `lookup`, `add`, `remove_all_for_face` |
| `ContentStore` | SQLite cache, TTL + LRU + freshness | `insert`, `get`, `get_prefix`, `invalidate` |

### Faces & strategy

- `Face` — abstract endpoint; `LinkFace` (over an RNS `Link`), `TestFace` (in-memory).
- `test_face_pair()` — a connected `(a, b)` `TestFace` pair for tests.
- `Strategy` / `BestRoute` / `StrategyDecision` — pluggable forwarding decisions
  (cost-ordered failover, stale-while-revalidate).

---

## 6. Identity helpers

`rns_icn.rns_utils`.

```python
from rns_icn import load_or_create_identity, default_identity_path

ident = load_or_create_identity(default_identity_path())  # ~/.icn/identity
```

- `load_or_create_identity(path) -> RNS.Identity` — load if present, else create
  and persist.
- `default_identity_path() -> str` — the conventional identity location.

---

## 7. Large content & chunking

For content above `resource_threshold`, or to stream large files with per-chunk
verification. `rns_icn.chunker`, `rns_icn.assembler`, `rns_icn.resource_transport`.

```python
from rns_icn import chunk_content, assemble_verified

chunks = chunk_content(name, big_bytes, signer=identity.sign)   # ChunkResult
content = assemble_verified(chunks, validator=RNS.Identity.validate)
```

- `chunk_content(name, content, chunk_size=DEFAULT_CHUNK_SIZE, signer=None) -> ChunkResult`
  — split into signed chunk `Data` plus a manifest.
- `assemble(...)` / `assemble_verified(..., validator)` / `assemble_fast(...)` —
  reassemble, optionally verifying every chunk signature (raises `SignatureError`
  on a missing/forged chunk).
- `verify_chunk` / `verify_chunks` / `missing_labels` — partial verification helpers.
- `ResourcePublisher` / `ResourceListener` / `LargeContentPublisher` — drive RNS
  Resource transfers for very large blobs.

See PROTOCOL.md §15–§16.

---

## 8. Errors

Exceptions are exported from the package root. The notable ones:

| Exception | Raised when |
|-----------|-------------|
| `NameError` | malformed name (wrong address/hash length, bad string) |
| `InterestError` / `DataError` | malformed Interest/Data on construction or parse |
| `SignatureError` | a chunk signature is missing or forged during verified assembly |
| `HashMismatchError` / `IntegrityError` / `MissingChunkError` / `AssemblyError` | chunk reassembly failures |
| `ChunkerError` / `EmptyContentError` | chunking failures |
| `SubscribeError` | malformed APS Subscribe |
| `ResourceTransportError` / `ResourceTimeoutError` | Resource transfer failures |

`fetch()` surfaces transport failures as standard `TimeoutError` / `RuntimeError`
rather than custom types.
