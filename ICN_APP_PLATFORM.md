# ICN as an App Platform — What We Actually Want

> A companion to [ICN_ROADMAP.md](ICN_ROADMAP.md). The roadmap says what the
> *protocol* is. This says what we'd build *on* it, and — more importantly — what
> shape that thing should be.

## 0. Framing: ignore the solvable problems

We are deliberately setting aside the problems that have known solutions and that
other people can solve:

- **Naming** (human-readable → producer hash): petnames/TOFU, an application-layer
  map. Solved elsewhere; off the wire; add later.
- **UI / rendering**: framebuffer vs. declarative markup vs. retained widget tree.
  A real engineering cost, but a *known* one — it's a rendering-engine problem,
  not an architecture problem.
- **Sandbox mechanics**: WASM + a host ABI. Off-the-shelf.

None of those decide the *identity* of what we're building. This document is about
the one question that does: **given a substrate that inverts nearly every core
assumption of the web, do we want a browser — or something else? And why?**

## 1. What a web browser actually is

Strip the chrome and a browser is a small set of load-bearing assumptions, every
one of them an accident of the always-connected, location-addressed,
server-authoritative internet it grew up on:

1. **Location addressing.** You fetch from a *place* (a server at a domain). The
   place is authoritative and live; the content is whatever it says *right now*.
2. **Origin = location *and* trust, conflated.** `bank.com` is both where the
   bytes live and who you're trusting. You cannot separate them.
3. **Live, stateful server; thin client.** Authority lives server-side. The client
   is a terminal; every meaningful action can round-trip to a machine that mutates
   state you don't hold.
4. **Server-side identity.** You are who each origin says you are (accounts,
   cookies). Your identity is *granted per-origin*, not owned.
5. **Ephemeral, non-verifiable views.** What you got is not addressable, not
   reproducible, and gone when you navigate away. Two loads need not be identical.
6. **CA-mediated transitive trust.** You trust the content because you trust the
   transport because you trust a certificate authority.
7. **Documents, with apps retrofitted.** It began as a document viewer; JS → SPA →
   WASM bolted an app platform onto a page-navigation model that is now vestigial.

**The essence, in one line:** *a browser is a thin client for renting someone
else's always-on computer.* Fat server, thin tenant. Your data and the authority
both live on their machine.

## 2. The substrate inverts all of it

| Browser assumption | ICN-over-RNS reality |
|---|---|
| Fetch from a *place* | Fetch a *thing* (content address); who serves it is irrelevant |
| Origin = location + trust | Trust is in the *signature*; anyone may serve the bytes |
| Live authoritative server | A cache three hops away is as authoritative as the origin |
| Content mutates under you | Content is immutable and versioned |
| Identity granted per-origin | Identity is self-certifying and *yours* |
| Views are ephemeral | Everything is addressable, reproducible, pinnable |
| CA-mediated trust | Self-certifying — the name *is* the key |

**There is no always-on server in the loop.** That single fact detonates
assumptions 1–6. A browser on this substrate is a client with nothing to be a
client *of*. It is a category error.

## 3. Thesis: we do not want a browser

A browser is the shell for the *old* substrate. Porting it here would import
assumptions that no longer hold — worse, it would *disguise* the new powers by
dressing them as the old thing. Users would see "a website" and never notice that
it can't be un-published, can't be silently changed, works offline, and carries
its own proof.

What we want is not browser-shaped underneath. It is **an operating environment
over a permanent, verifiable, user-owned object space.** Closer kin: git, a
game console's signed cartridge, Unix pipes, the Smalltalk image, the Solid/dat/
IPFS dream — and specifically *not* the browser.

The one-line contrast:

> A browser **fetches a live view from a place you trust.**
> This thing **runs a verified edition over data you own.**

## 4. What we actually want (the wants, argued)

### 4.1 Permanence over liveness
Apps are **artifacts, not services.** They keep running — with their last cached
data — when the network partitions or the author vanishes. Nothing can be
un-published or silently altered. The web's default is the opposite: every site is
a liability that dies with its server, its company, or its funding. We want apps
that *cannot rot* and *cannot be revoked from under you*.

### 4.2 Editions over deployments
Every version is an addressable, signed, immutable **edition** — like an issue of a
magazine, not a `deploy`. You pin edition 5 forever. You audit the diff to edition
6. Updates are opt-in and reviewable. A *later* author compromise cannot
retroactively poison an *earlier* edition, because the earlier bytes are frozen and
hashed. The web lives in an eternal mutable present; we want durable history with
opt-in motion through it.

### 4.3 The user owns state; apps are lenses
Data is named content guarded by **user-held capabilities.** An app is code you
grant scoped, revocable access to *your* data — it does not own a silo of it. Move
between apps, keep your data. This is what Solid tried to retrofit onto the web and
couldn't, because the web's gravity is server-owned state. Here it's the path of
least resistance: data is content + capabilities; an app is a function over names
you let it see.

### 4.4 Verify, don't trust
Trust attaches to the **artifact** (signature + content hash), never the location.
Running an app means running *exactly these reviewed bytes* — not "whatever the
server returned this time." "View source" is not a courtesy; the hash *is* the
identity of the thing. This is supply-chain integrity by construction — the exact
problem the rest of the industry is currently on fire about.

### 4.5 Composition over a shared fabric
Apps read and write **one addressable content space.** They compose through content
and capabilities, not through APIs, OAuth, or a business-development deal. App A
publishes signed data at a name; app B consumes it if granted the capability. Unix
pipes, not walled gardens. The web structurally *prevents* this (integration needs
someone's permission); here it's the default.

### 4.6 No privileged middle
No live server means **no rent, no platform cut, no kill switch.** Publishing is
seeding, not hosting. And the load curve inverts: a popular app is cached at more
hops, so it gets *more* available and *more* resilient under demand — the opposite
of a server that buckles. Distribution is epidemic/gossip, not hub-and-spoke.

### 4.7 One object model
A document is just an app with no code. A page and a program are the *same* signed,
content-addressed bundle, differing only in whether executable logic is present. We
get to **unify** what the web awkwardly bolts together, instead of inheriting the
document-vs-app schism.

## 5. What we *do* steal from the browser

The browser got two things profoundly right, and we keep exactly those — at the UX
seam, not in the architecture:

- **Permissionless publish.** Anyone can put an app up, under their own key, with
  no registrar and no store approval.
- **Zero-install run.** You "navigate" to an app and it runs; nothing to install.
- **The sandbox.** Untrusted third-party code runs safely. (This is the *only*
  reason WASM is in the picture: you need a sandbox the moment a stranger's app can
  run on your machine. If the app set were curated/first-party, you wouldn't need
  WASM at all. WASM = permissionless third-party apps.)
- **Addressable navigation.** A stable address you can bookmark, share, and return
  to.

**Browser-shaped at the UX seam; git-shaped underneath.** We keep the *distribution
ergonomics* and discard the *architecture* (live servers, location trust,
server-side state, ephemeral non-verifiable views, CA cartel).

## 6. So what is it, really?

An **edition player over a shared object space.** You point it at a name; it pulls a
signed, content-addressed bundle (code + data + assets), verifies it, caches it at
every hop it crossed, and runs it in a sandbox whose only door to the world is a
capability-scoped API back into the same object space. The app operates on *your*
data, by *your* grant, and keeps working when the author and the network are both
gone.

That is not a browser. It has more in common with an operating system's program
loader over a permanent, verifiable, user-owned filesystem than with a client for
rented servers.

## 7. The apps (concrete first targets)

Named to make the vision tangible; each is chosen to exercise a real slice of the
protocol. Small-payload apps at the top work across the *whole* mesh (including slow
links); bulk apps only sing where caching amortizes across many local fetchers.

| App | What it is | Protocol surface it proves | Tier |
|-----|-----------|-----------------------------|------|
| **Wire** | Signed public feed reader — follow producers, get pushed new posts | pub/sub, verifiable latest-pointer, partition, signatures | Build first |
| **Notice** | Community/emergency advisory board — signed, timestamped bulletins | signatures, rollback protection, partition | Build first |
| **Keeper** | OTA updater for RNS nodes — signed, content-pinned software/config | content-addr, signatures, dedup, latest-pointer | Build first |
| **Drop** | Encrypted dead-drop / private file share through untrusted caches | encrypt + capability tokens, content-addr | High (only user of access control) |
| **Stash** | Offline wiki / knowledge-pack reader (articles, repair, medical) | cache-at-hop, partition, chunked | High |
| **Atlas** | Offline map/tile viewer | content-addr, cache, partition | High |
| **Mirror** | Folder sync — Syncthing-style but partition-tolerant | chunked, latest-pointer, cache | Medium |
| **Cairn** | Threaded forum / BBS as named content | pub/sub, named data, signatures | Medium |
| **Airwaves** | Mesh radio / podcast distribution | chunked, cache | Medium (bandwidth-heavy) |
| **Ledger** | Named-data sensor/telemetry historian + dashboard | pub/sub, named data | Niche |
| **Forge** | Package registry (apt/npm-over-ICN) | content-addr, dedup, signatures | Later |

**First three to build:** Wire, Notice, Keeper — all small-payload, and between them
they touch every novel part of the stack. **Drop** as a fourth: it's the only thing
that justifies the access-control machinery existing.

**These come before the platform, on purpose.** Build them native against the raw
Python API first — not as throwaways, but to *discover the host ABI*. Every host
call they reach for (fetch, subscribe, verify, unwrap-capability, store-local,
render) is a line item in the future sandbox interface. Build the shell first and
you'll guess that ABI wrong. Native apps → extract the API they converged on →
sandbox it → build the shell.

### 7.1 Host-ABI harvest from Wire (built — sibling repo `wire/`)

Wire is done (M0–M4: signed posts, verified latest + backfill, live push with
offline catch-up, and the partition demo — a follower backfilling full history
from a router's cache while the origin is unreachable, with a fresh post failing
cleanly until heal). The surface it converged on, now landed as protocol API:

| ABI call | Landed as | Notes |
|---|---|---|
| publish | `ICNServer.publish_post(name, bytes, sequence, latest_under)` | one call = CS + signed latest-pointer + APS push + propagation; composes what `publish_content`/`publish_pushed` each did halfway |
| fetch (exact/pinned) | `ICNClient.fetch` | existed; pinned (self-certifying) fetch had to be made real, see below |
| fetch-latest | `ICNClient.fetch_latest` | existed |
| fetch-range | `ICNClient.fetch_range(prefix, peer, start_sequence, max_items)` | verified, gap-tolerant sequence walk — the backfill primitive |
| subscribe | `ICNClient.subscribe(prefix, peer, callback, start_from_now)` | verified push: same pipeline as a fetch before the callback fires |
| resolve | `ICNClient.resolve(destination_hash, timeout)` | "destination hash → verified producer identity" — one call now; asks the mesh for the path, the answering announce carries the identity keys. Was hand-rolled in Wire (`recall` + `request_path` loop), extracted after the harvest |
| store-local | app-owned SQLite (`wire/store.py`) | follow list, per-feed read cursor, producer seq high-water: the "user owns state" primitive — a local KV in ABI terms |
| canonical encode | `cbor2` (RFC 8949 §4.2 canonical form) | first external dependency an app actually needed; the sandbox must provide canonical-CBOR encode/decode |

**The bigger finding:** the consumer path had never been exercised over a real
link — every existing CLI and test fetched via an ephemeral `ICNServer`. Building
Wire against `ICNClient` surfaced and fixed, in the protocol layer: the client
never pumped incoming packets into its forwarder; it had no FIB route (express()
is FIB-driven); content-hash-pinned names missed the CS and the PIT; prefix
Interests (all selector fetches) could never match their answering Data in the
PIT; a cache could answer a sequence walk with its own "oldest ≥ floor" and
silently skip history held elsewhere (walk-authority rule: only an exact-floor
hit, or the producer, may answer); and `must_be_fresh` was ignored in the server
serve path, so healed partitions kept serving the stale latest-pointer. This is
exactly the §7 argument working as intended: the native app is the probe.

## 8. Honest tensions (things this makes *harder*)

We accept these on purpose:

- **Interactivity that needs a live authority** (real-time multiplayer, a global
  counter, "is this seat still available") is genuinely awkward without a live
  server. The substrate is built for *published* content, not *live consensus*.
  Some apps simply don't fit, and we should say so rather than bolt a server back
  on and lose the whole thesis.
- **Discovery** without a live index is hard. Signed catalogs and feeds help, but
  there is no Google. This is a real cost of "no privileged middle."
- **Mutable shared state** (a wiki *anyone* edits, a shared doc) has to be modeled
  as append-only signed contributions + merge, not a mutated cell. More like git
  than a database. Powerful, but a different mental model for app authors.
- **Bandwidth.** A code+data bundle over LoRa is brutal *once*, then cached and
  diffed against a pinned hash. This pushes hard toward tiny apps and declarative
  UI — a constraint, but arguably a healthy one.

The through-line: whenever a decision is hard, we resolve it *toward* permanence,
verifiability, and user-owned state — and *away* from the live-server crutch. The
moment we reintroduce a privileged always-on middle, we've rebuilt the browser and
lost the reason to exist.
