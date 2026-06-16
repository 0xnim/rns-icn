# rns-icn

**Information-Centric Networking (ICN) over [Reticulum](https://reticulum.network/).**

Content-addressed, named-data retrieval over the RNS mesh: clients express
*Interests* for named content and receive verified *Data* in return, with
caching, multi-hop forwarding, and persistence — an NDN/CCN-style stack riding
RNS encrypted Links.

> Status: Phases 1 & 2 of the [roadmap](ICN_ROADMAP.md) are complete — reliable
> single-hop fetch and multi-hop router forwarding, proven end-to-end over real
> RNS. Cache coherency (Phase 2.4) has landed: Data declares a freshness period,
> caches age it to stale and revalidate upstream, serve-stale-while-revalidate
> is configurable, and producers can issue signed cache invalidations. Phase 3
> signing has landed: origins sign Data with their Ed25519 RNS identity and
> clients verify it (cache-poisoning defence), with the sequence number and a
> signing timestamp now bound into the signed envelope so clients can reject a
> relay replaying a stale-but-validly-signed version (rollback). Producers can
> rotate their signing key via a self-certifying chain of signed delegation
> certificates (`rns_icn/rotation.py`), so clients keep verifying across a key
> change without trusting the mesh. Access control and name resolution (Phase
> 3.3/3.4) are the main remaining work.

## How it works

Names are `/<producer-hash>/<label...>`, self-certifying to the producer's RNS
identity. The protocol engine is the classic NDN triad:

| Component | Role |
|-----------|------|
| **Forwarder** | Routes Interests, returns Data along the reverse path |
| **PIT** | Pending Interest Table — in-flight Interests, loop/duplicate suppression |
| **FIB** | Forwarding Information Base — prefix → next-hop face |
| **Content Store** | SQLite-backed cache with TTL + LRU eviction + crash recovery |

Transport is an RNS `Link` (AES-encrypted) per face, with a `LinkPool` for reuse
and health monitoring. See [ICN_ON_RNS_MESH.md](ICN_ON_RNS_MESH.md) for the
design narrative and [ICN_ROADMAP.md](ICN_ROADMAP.md) for the full status matrix.

## Install

Requires Python ≥ 3.10.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .            # add ".[dev]" for the test/lint toolchain
```

This installs the CLI entry points: `icn-server`, `icn-router`, `icn-client`,
`icn-publish`, `icn-fetch`.

## Quickstart

Copy and edit the example config:

```bash
cp icn.toml.example icn.toml   # set identity_path, mesh_interfaces, known_peers
```

Run an origin server (serves content from its store, announces on the mesh):

```bash
icn-server --config icn.toml
```

Fetch a named blob from a peer (writes to stdout, or a file):

```bash
icn-fetch <peer_hash> <name> [output|-]
```

Publish content to a peer's store:

```bash
icn-publish <peer_hash> <name> [file|-]
```

Run a caching router that forwards Interests to configured upstream peers and
caches reverse-path Data:

```bash
icn-router --config icn.toml   # needs known_peers with identity_path set
```

## Configuration

All binaries read a TOML file (`icn.toml` by default, `--config` to override).
See [`icn.toml.example`](icn.toml.example) for the full documented surface:
identities, mesh interfaces, known peers, timeouts/retries, content-store sizing
and TTLs, structured logging, and the optional HTTP health/metrics API.

## Deployment

[`deploy/icn.toml`](deploy/icn.toml) and [`rns-icn.service`](rns-icn.service)
run an origin server as a systemd unit that **rides a shared `rnsd` transport
daemon** ([`rnsd.service`](rnsd.service)) rather than owning mesh interfaces
itself — set `rns_configdir` to the same configdir `rnsd` uses so RNS attaches
to the running instance.

## Development

```bash
pip install -e ".[dev]"
pytest          # full suite (real-RNS integration tests run over localhost TCP)
ruff check .    # lint
mypy rns_icn    # type check (informational; tree not fully annotated yet)
```

CI runs the test suite and `ruff check` on every push and pull request.
