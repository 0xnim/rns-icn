# rns-icn Documentation

The documentation set for **Information-Centric Networking over Reticulum**.

## Start here

| If you want to… | Read |
|-----------------|------|
| **Build an app** against the Python API | [tutorials.md](tutorials.md), then [api-reference.md](api-reference.md) |
| **Look up a class or function** | [api-reference.md](api-reference.md) |
| **Re-implement the protocol** / understand the wire format & security model | [PROTOCOL.md](../PROTOCOL.md) |
| **Understand the design** and how it rides an RNS mesh | [ICN_ON_RNS_MESH.md](../ICN_ON_RNS_MESH.md) |
| **See project status** and what's planned | [ICN_ROADMAP.md](../ICN_ROADMAP.md) |
| **Report a vulnerability** / understand the trust model | [SECURITY.md](../SECURITY.md) |
| **Install and run quickly** | [README.md](../README.md) |

## The documents

- **[tutorials.md](tutorials.md)** — eight runnable, task-oriented walkthroughs:
  CLI fetch/publish, the Python `ICNClient`/`RNSICNServer` API, manifests, caching
  routers, pub/sub, access control, verifiable "latest", and testing the
  forwarding engine without a mesh.
- **[api-reference.md](api-reference.md)** — the public Python API: the high-level
  consumer (`ICNClient`) and node (`RNSICNServer`), configuration, the core
  `Name`/`Interest`/`Data` types, the forwarding engine, and large-content helpers.
- **[PROTOCOL.md](../PROTOCOL.md)** — the normative specification: packet framing,
  names, cryptographic constructions, access control, forwarding semantics, cache
  coherency, versioning, and test vectors. This is the authoritative contract.

## How they relate

```
README.md            quickstart + install + deployment
   │
   ├── docs/tutorials.md      learn by doing (uses the Python API + CLIs)
   ├── docs/api-reference.md  the Python API surface (what tutorials call)
   │
   └── PROTOCOL.md            the wire/security contract (what the API implements)
       ICN_ON_RNS_MESH.md     why it's built this way
       ICN_ROADMAP.md         where it's going
```

Tutorials and the API reference describe the **Python implementation** (pre-1.0,
may change). PROTOCOL.md describes the **wire protocol** (versioned, frozen at
1.0) — the contract any re-implementation must honour.
