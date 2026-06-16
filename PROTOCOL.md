# rns-icn Protocol Specification

**Version:** 1 (as implemented in rns-icn 0.1.0)
**Status:** Reference specification. This document is normative and is written to
match the reference implementation in this repository. Where this document and
the code disagree, that is a bug in one of them — please file an issue.

Information-Centric Networking (ICN) over [Reticulum](https://reticulum.network/):
consumers express **Interests** for named content and receive verified **Data**
in return, with content-addressed caching, multi-hop forwarding, producer
authentication, key rotation/revocation, and name-based access control.

## Table of contents

1. [Conventions](#1-conventions)
2. [Architecture overview](#2-architecture-overview)
3. [Transport binding (RNS)](#3-transport-binding-rns)
4. [Primitive encodings](#4-primitive-encodings)
5. [Names](#5-names)
6. [Packet framing](#6-packet-framing)
7. [Interest](#7-interest)
8. [Data](#8-data)
9. [Control packets](#9-control-packets)
10. [Cryptographic constructions](#10-cryptographic-constructions)
11. [Producer key rotation](#11-producer-key-rotation)
12. [Key revocation](#12-key-revocation)
13. [Access control](#13-access-control)
14. [Forwarding semantics](#14-forwarding-semantics)
15. [Cache coherency](#15-cache-coherency)
16. [Reserved names](#16-reserved-names)
17. [Manifests](#17-manifests)
18. [Large content](#18-large-content)
19. [Versioning and forward compatibility](#19-versioning-and-forward-compatibility)
20. [Security model](#20-security-model)
21. [Constant reference](#21-constant-reference)

---

## 1. Conventions

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHOULD**,
**SHOULD NOT**, **MAY**, and **OPTIONAL** are to be interpreted as in RFC 2119.

* All multi-byte integers are **unsigned, big-endian (network byte order)**
  unless explicitly stated otherwise.
* `u8`/`u16`/`u32`/`u64` denote unsigned integers of 1/2/4/8 bytes.
* `varint` denotes the variable-length integer in [§4.1](#41-varint).
* Byte offsets are zero-based. `x[a:b]` denotes bytes `a` (inclusive) to `b`
  (exclusive).
* Hashing is **BLAKE2b** with a 32-byte digest unless stated otherwise.
* Signatures are **Ed25519** (64 bytes), produced and verified via the
  producer's RNS `Identity`.
* A *producer address* is a 16-byte RNS identity hash. A *consumer* is likewise
  identified by its 16-byte RNS identity hash.

A conforming parser **MUST** treat any length field as untrusted: every field
read **MUST** be bounds-checked against the remaining buffer before use, and a
packet that is truncated, over-long, or otherwise malformed **MUST** be rejected
(and, on a forwarding node, dropped) rather than partially processed.

---

## 2. Architecture overview

The protocol is the classic NDN/CCN triad plus security and transport layers:

| Layer | Role |
|-------|------|
| **Naming** | Self-certifying `/<producer>/<label…>` names ([§5](#5-names)) |
| **Packets** | Interest / Data + control packets ([§6](#6-packet-framing)–[§9](#9-control-packets)) |
| **Security** | Producer signatures, key rotation/revocation, access control ([§10](#10-cryptographic-constructions)–[§13](#13-access-control)) |
| **Forwarding** | Content Store (CS), Pending Interest Table (PIT), Forwarding Information Base (FIB) ([§14](#14-forwarding-semantics)) |
| **Transport** | RNS encrypted `Link` + `Channel`, `Resource` for large content ([§3](#3-transport-binding-rns)) |

A **producer** owns a namespace (its RNS identity). It signs the Data it
originates. A **consumer** expresses Interests and verifies returned Data
against the producer. A **forwarder** (cache / router / propagation node)
relays Interests toward producers and Data back along the reverse path,
caching opaque Data without needing to read or re-sign it.

---

## 3. Transport binding (RNS)

ICN packets are carried over Reticulum, which provides the encrypted,
authenticated, multi-hop substrate. ICN does **not** define its own link
encryption; confidentiality of the *path* is provided by RNS, and
confidentiality of *content at rest in caches* is provided by ICN access
control ([§13](#13-access-control)).

* Each ICN **face** is an RNS `Link` (Curve25519 + AES, established between two
  endpoints' `Destination`s on the `icn` app namespace).
* ICN packets ([§6](#6-packet-framing)) are sent as **RNS `Channel`** messages.
  The implementation registers a `MessageBase` subclass with `MSGTYPE = 0x01`
  whose payload is the raw ICN packet bytes. `Channel` provides reliable,
  in-order delivery with retransmission and flow control.
* A single Data packet whose serialized size exceeds a node's
  `resource_threshold` (default **100 000 bytes**) **SHOULD** instead be sent as
  an **RNS `Resource`**, whose payload is the ICN Data bytes prefixed with a
  one-byte type tag `0x49` (`'I'`) ([§18](#18-large-content)). `Resource`
  handles segmentation and retransmission of large transfers.

A receiver demultiplexes by the first payload byte: a `Channel` message and an
unwrapped `Resource` payload both begin with the packet type discriminator
([§6](#6-packet-framing)); a `Resource` additionally carries the `0x49` tag
ahead of it.

RNS `Destination`s for ICN use app name `"icn"`; the default aspect is
`"default"`. Servers announce their destination so peers can discover a path.

---

## 4. Primitive encodings

### 4.1 Varint

A Bitcoin-style variable-length unsigned integer:

| First byte | Total length | Value |
|------------|--------------|-------|
| `0x00`–`0xFC` | 1 | the byte itself |
| `0xFD` | 3 | `u16` in the next 2 bytes |
| `0xFE` | 5 | `u32` in the next 4 bytes |
| `0xFF` | 9 | `u64` in the next 8 bytes |

Encoders **SHOULD** use the shortest form. Decoders **MUST** bounds-check the
trailing integer bytes before reading.

---

## 5. Names

A name is a routable prefix plus an OPTIONAL content-hash suffix. The first
component is **always** the 16-byte producer address; subsequent components are
arbitrary-length labels (typically UTF-8).

### 5.1 Wire format

```
[count:u8]
[len_1:u8][comp_1 …]
…
[len_n:u8][comp_n …]
( [0xFF][content_hash:32] )?      # optional, only if next byte == 0xFF
```

* `count` is the number of components, `1 ≤ count ≤ 32` (`MAX_COMPONENTS`).
  `count == 0` or `count > 32` **MUST** be rejected.
* `comp_1` (the producer address) **MUST** be exactly 16 bytes.
* Each `len_i` is a single byte, so each component is at most 255 bytes.
* If, after the last component, a byte equal to `0xFF` (`HASH_DISCRIMINATOR`)
  remains, it is followed by a 32-byte content hash. Otherwise the name has no
  content hash.

### 5.2 Self-certifying addressing

The producer address is the **truncated BLAKE2b hash of the producer's RNS
public key** (an RNS identity hash). The name therefore *is* the key: any party
can confirm that a given public key is authoritative for a namespace by hashing
it and comparing to the address — no PKI, no recall, fully offline. This
property underpins signature verification ([§10.2](#102-data-signature-envelope)),
key rotation ([§11](#11-producer-key-rotation)), and capability anchoring
([§13](#13-access-control)).

### 5.3 Display form

For logs/UIs only (not on the wire): `/<producer-hex>/<label>/…` with an
OPTIONAL `?hash=<hex>` suffix. The producer component is rendered as hex; other
components are rendered as UTF-8 where decodable, else hex.

### 5.4 Prefix relation

Name `A` *starts with* prefix `P` iff `P` has no more components than `A` and
every component of `P` equals the corresponding component of `A`. Content hash
is ignored for the prefix relation. PIT and FIB matching, access-control
prefixes, and `can_be_prefix` Interests all use this relation.

---

## 6. Packet framing

Every ICN packet begins with a one-byte **type discriminator**:

| Value | Type | Section |
|-------|------|---------|
| `0x01` | Interest | [§7](#7-interest) |
| `0x02` | Data | [§8](#8-data) |
| `0x03` | APS Subscribe | [§9.1](#91-aps-subscribe-0x03) |
| `0x04` | Propagation Peer | [§9.2](#92-propagation-peer-0x04) |
| `0x05` | Capability Peer | [§9.3](#93-capability-peer-0x05) |
| `0x06` | Invalidate | [§9.4](#94-invalidate-0x06) |

An unknown type byte **MUST** be rejected. There is no separate length prefix at
the framing layer: the transport (`Channel`/`Resource`) delivers whole packets.

---

## 7. Interest

```
[0x01]
[name_len:varint][name:name_len]          # §5
[nonce:8]
[lifetime_ms:u32]
[flags:u8]
( [min_sequence:u64] )?                    # iff flags bit 2
( [hop_limit:u8] )?                        # iff flags bit 3
```

**Flags:**

| Bit | Mask | Meaning |
|-----|------|---------|
| 0 | `0x01` | `can_be_prefix` — Data under the name (prefix match) may satisfy it |
| 1 | `0x02` | `must_be_fresh` — a stale cached copy MUST NOT satisfy it |
| 2 | `0x04` | `has_selector` — an 8-byte selector follows |
| 3 | `0x08` | `has_hop_limit` — a 1-byte hop limit follows |

* `nonce` is 8 random bytes; it is the per-Interest identifier used for loop /
  duplicate suppression ([§14.2](#142-loop-and-duplicate-suppression)).
* `lifetime_ms` is the Interest's lifetime (default 4000). It bounds how long a
  PIT entry and the consumer's wait persist.
* **Selector** (when present): `min_sequence:u64` — only Data with
  `sequence ≥ min_sequence` satisfies the Interest. Used for stream fetch
  ("give me segment ≥ N").
* **Hop limit**: remaining forwarding hops. Senders **SHOULD** always include it
  (the implementation always sets bit 3 on write). Each forwarding hop
  decrements it; an Interest received with `hop_limit == 0` **MUST NOT** be
  forwarded further (it MAY still be satisfied from a local cache). A peer that
  omits it is assumed to mean `DEFAULT_HOP_LIMIT = 16`. Valid range `0..255`.

---

## 8. Data

```
[0x02]
[name_len:varint][name:name_len]           # §5
[content_len:u32][content:content_len]
[flags:u8]
( [meta_len:varint][metadata:meta_len] )?  # iff flags bit 0
( [signature:64] )?                         # iff flags bit 1
```

**Flags:**

| Bit | Mask | Meaning |
|-----|------|---------|
| 0 | `0x01` | `has_metadata` — a metadata block follows |
| 1 | `0x02` | `has_signature` — a 64-byte Ed25519 signature follows |

`content` is the opaque payload. When the Data is encrypted
([§13](#13-access-control)) `content` is ciphertext; the content hash and
signature are computed over the **ciphertext**, so forwarders cache, verify, and
relay restricted Data without being able to read it.

### 8.1 DataMetadata

```
[meta_flags:u8]
( [content_hash:32]    )?   # iff bit 0
( [sequence:u64]       )?   # iff bit 1
( [age_seconds:u64]    )?   # iff bit 2   (present when the Data is STALE)
( [freshness_period:u64] )? # iff bit 3
( [signed_at:u64]      )?   # iff bit 4
                            #     bit 5 (encrypted) carries no body
```

Fields appear in the order above (ascending bit index).

| Bit | Mask | Field | Meaning |
|-----|------|-------|---------|
| 0 | `0x01` | `content_hash` | BLAKE2b-32 of `content`. Enables content-addressed verification. |
| 1 | `0x02` | `sequence` | Producer-assigned monotonic version/segment number. |
| 2 | `0x04` | *staleness* | When set, the Data is **stale** and `age_seconds` follows. When clear, the Data is fresh and no age field is present. |
| 3 | `0x08` | `freshness_period` | Seconds the Data stays fresh in a cache ([§15](#15-cache-coherency)). |
| 4 | `0x10` | `signed_at` | Unix seconds the producer signed at ([§10.2](#102-data-signature-envelope)). |
| 5 | `0x20` | `encrypted` | `content` is ciphertext for a restricted prefix ([§13](#13-access-control)). No body bytes. |

A decoder **MUST** tolerate unknown high metadata-flag bits only insofar as it
can still locate the fields it understands; because field presence is
positional, undefined bits in this version have no associated body and parsers
written to this spec ignore them. See [§19](#19-versioning-and-forward-compatibility).

### 8.2 Content hash verification

If `content_hash` is present, a consumer **MUST** verify
`BLAKE2b32(content) == content_hash` and reject the Data on mismatch. Absent a
content hash, no content verification is possible at this layer.

---

## 9. Control packets

### 9.1 APS Subscribe (`0x03`)

Asynchronous Publish-Subscribe handshake: upgrades a link to push mode for a
stream prefix. After it, the producer pushes matching Data without per-segment
Interests.

```
[0x03]
[name_len:varint][name:name_len]
[flags:u8]      # bit 0: start_from_now (do not push already-existing content)
```

Subscription matching is prefix-based in both directions (a subscribed prefix
matching a longer published name, or vice versa). A producer MAY queue pushes
for a disconnected subscriber (offline queue; default TTL 86 400 s) and drain
them on reconnect.

### 9.2 Propagation Peer (`0x04`)

Handshake establishing a propagation peering between two servers.

```
[0x04]
[version:u8]       # currently 1
[rns_addr:16]
[flags:u8]         # bit 0: wants_sync (peer wants to sync existing content now)
```

### 9.3 Capability Peer (`0x05`)

Exchanged immediately after link establishment so each side learns the other's
role and supported features.

```
[0x05]
[version:u8]       # protocol version, currently 1
[role:u8]          # 0 = ORIGIN, 1 = CACHE, 2 = PROPAGATION
[features:u32]     # feature bitmask
```

**Feature bits:**

| Mask | Feature |
|------|---------|
| `0x00000001` | APS push subscriptions |
| `0x00000002` | Content propagation |
| `0x00000004` | Offline queue |
| `0x00000008` | Content manifest |
| `0x00000010` | Chunked content |

This is the protocol's capability-negotiation surface: it is the only place a
peer-wide `version` is currently exchanged (see
[§19](#19-versioning-and-forward-compatibility)).

### 9.4 Invalidate (`0x06`)

A producer-signed cache-purge for a name or prefix.

```
[0x06]
[name_len:varint][name:name_len]
[epoch:u64]
[flags:u8]         # bit 0: is_prefix, bit 1: has_signature
( [signature:64] )?  # iff bit 1
```

* `epoch` is a producer-chosen monotonic value (e.g. unix time).
* A forwarder **MUST** verify the signature against the producer recalled from
  `name`'s address before acting; an unsigned or unverifiable Invalidate
  **MUST** be dropped.
* Replay/loop protection: a forwarder tracks the highest epoch applied per name
  and **MUST** ignore an Invalidate whose `epoch ≤` the highest already seen.
* When `is_prefix` is set, every name under `name` is purged.

Signature input is defined in [§10.3](#103-other-signed-objects).

---

## 10. Cryptographic constructions

### 10.1 Content hashing

`H(x) = BLAKE2b(x, digest_size=32)`. Used for content addressing
([§8.2](#82-content-hash-verification)), chunk integrity
([§18](#18-large-content)), and as the building block of the producer's identity
hash (the producer address).

### 10.2 Data signature envelope

A producer signature authenticates a Data packet. The **signed hash** is:

```
H_data = BLAKE2b32(
    name.to_bytes()
  || content
  || content_hash                       (if present)
  || ( 0x01 || u64(sequence) )          (if sequence present)
  || ( 0x02 || u64(signed_at) )         (if signed_at present)
  || ( 0x03 || 0x01 )                   (if encrypted)
)
```

The signature is `Ed25519_sign(producer_sk, H_data)`, 64 bytes, carried in the
Data packet ([§8](#8-data)).

Design notes (normative for interoperability):

* The optional fields are **appended in fixed order**, each behind a one-byte
  tag (`0x01`/`0x02`/`0x03`) that both disambiguates the field and distinguishes
  "value 0" from "absent". A producer **MUST** emit them in this order and
  **MUST** include a field's bytes iff that field is set on the Data.
* Because optional fields are *appended*, a signature produced before a field
  existed still verifies once that field is absent — this is the forward-compat
  rule in [§19](#19-versioning-and-forward-compatibility). Concretely, a signature
  over `name || content || content_hash` alone remains valid.
* `sequence`, `signed_at`, and `encrypted` are therefore **authenticated**:
  stripping or altering any of them breaks the signature. This is what lets a
  consumer trust `signed_at`/`sequence` for rollback detection
  ([§15.2](#152-rollback-protection)) and trust the `encrypted` flag
  ([§13](#13-access-control)).
* `H_data` has **no leading domain-separation tag**. Data signatures are only
  ever validated in the Data context, so there is no cross-protocol collision
  with the domain-tagged objects below; implementers extending the protocol
  SHOULD nonetheless be aware of this asymmetry.

A consumer verifies by recomputing `H_data` and checking the signature against
an authorized producer key ([§10.4](#104-resolving-an-authorized-producer-key)).

### 10.3 Other signed objects

| Object | Signed hash input | Signer |
|--------|-------------------|--------|
| Invalidate | `BLAKE2b32( name.to_bytes() \|\| u64(epoch) \|\| (0x01 if is_prefix else 0x00) )` | producer |
| KeyRotation | [§11.1](#111-rotation-certificate) | previous key |
| Revocation | [§12](#12-key-revocation) | anchor key |
| Capability | [§13.3](#133-capability-token) | producer signing key |

KeyRotation, Revocation, and Capability each prepend a distinct
**domain-separation tag** so a signature over one can never be replayed as
another:

| Object | Domain tag |
|--------|-----------|
| KeyRotation | `b"icn-key-rotation\x01"` |
| Revocation | `b"icn-key-revocation\x01"` |
| Capability | `b"icn-capability\x01"` |
| Content-key derivation | `b"icn-content-key\x01"` |

### 10.4 Resolving an authorized producer key

To verify any producer signature for address `A`, a verifier obtains the set of
public keys authorized to sign for `A`, in priority order:

1. **Rotation chain** (if held for `A`): the authorized set is computed from the
   chain and revocations ([§11](#11-producer-key-rotation),
   [§12](#12-key-revocation)). Self-certifying — no network needed.
2. **RNS recall**: recall the identity for `A` from the mesh (`A` is an identity
   hash) and use its key.

A present-but-invalid signature **MUST** always be rejected. Policy for
*unsigned* or *unverifiable* Data (`require_signature`) is a local consumer
decision: verify-if-present by default, or strict rejection.

---

## 11. Producer key rotation

A producer cannot simply swap keys: the name *is* the key hash
([§5.2](#52-self-certifying-addressing)). Instead the producer issues a chain of
signed **rotation certificates** delegating from the anchor key (whose hash is
the producer address) to a new key, that key to the next, and so on. This
delivers key **continuity**: a valid chain *widens* the set of keys authorized
to sign for the namespace, so content signed by any generation still verifies
and caches are not invalidated by a rotation.

### 11.1 Rotation certificate

```
[producer:16]
[epoch:u64]
[prev_public_key:64]
[new_public_key:64]
[has_sig:u8]            # 0x00 / 0x01
( [signature:64] )?     # iff has_sig
```

* `epoch` starts at 1 and increments by exactly 1 per link.
* Public keys are 64 bytes: 32-byte X25519 ‖ 32-byte Ed25519 (the RNS public
  key).
* **Signed hash:**
  `BLAKE2b32( b"icn-key-rotation\x01" || producer || u64(epoch) || prev_public_key || new_public_key )`,
  signed by **`prev_public_key`** (the delegating key).

### 11.2 Chain verification

Given `producer_addr` and an ordered list of certificates, a verifier **MUST**:

1. Return the empty set for an empty chain.
2. Let `anchor = certs[0].prev_public_key`; reject unless
   `truncated_BLAKE2b_hash(anchor) == producer_addr` (anchor binds to the name).
3. For each cert at index `i` (0-based):
   * reject unless `cert.producer == producer_addr`;
   * reject unless `cert.epoch == i + 1` (contiguous);
   * reject unless `cert.prev_public_key == expected_prev` (chained), where
     `expected_prev` is the anchor for `i == 0` else the previous cert's
     `new_public_key`;
   * reject unless the cert signature verifies against `cert.prev_public_key`.
4. The authorized set is `[anchor, certs[0].new, certs[1].new, …]`, then reduced
   by any revocations ([§12](#12-key-revocation)).

### 11.3 Rotation bundle

The unit distributed over the mesh and persisted to disk; carries a chain plus
its revocations.

```
[ncerts:u16]
( [cert_len:u16][cert:cert_len] ) × ncerts          # §11.1
( [nrev:u16]                                          # OPTIONAL section
  ( [rev_len:u16][rev:rev_len] ) × nrev )?            # §12
```

The revocation section is OPTIONAL: a bundle with no revocation section is
identical on the wire to a bare chain (`[ncerts] [certs…]`), and a reader for
the bare-chain form ignores any trailing revocation section. Old and new files
therefore interoperate (see [§19](#19-versioning-and-forward-compatibility)).

A producer publishes its bundle as **self-verifying Data** at the reserved name
`/<producer>/_rotation` ([§16](#16-reserved-names)). The bundle carries **no
producer Data signature** — it authenticates itself via its certificate chain —
so a consumer fetching it **MUST** verify it anchors to the requested producer
([§11.2](#112-chain-verification)) and **MUST NOT** rely on a Data-layer
signature.

---

## 12. Key revocation

Revocation is the *shrinking* counterpart to rotation: it removes a compromised
key from the authorized set. Only the **anchor** (the namespace root of trust)
may revoke.

### 12.1 Revocation certificate

```
[producer:16]
[revoked_at:u64]
[anchor_public_key:64]
[revoked_public_key:64]
[has_sig:u8]
( [signature:64] )?     # iff has_sig
```

* **Signed hash:**
  `BLAKE2b32( b"icn-key-revocation\x01" || producer || u64(revoked_at) || anchor_public_key || revoked_public_key )`,
  signed by `anchor_public_key`.
* `revoked_at` is unix seconds, for audit/ordering.

### 12.2 Applying revocations

When reducing the authorized set ([§11.2](#112-chain-verification) step 4), for
each revocation a verifier **MUST**:

* reject unless `revocation.producer == producer_addr`;
* reject unless `revocation.anchor_public_key == anchor` (the chain anchor);
* reject unless the revocation signature verifies.

Then it computes the **revoked closure**: start with each `revoked_public_key`,
and walking the chain in epoch order, additionally revoke `cert.new_public_key`
of any cert whose `cert.prev_public_key` is already revoked. (A compromised key
could have minted rogue delegations, so its entire delegated sub-branch is
revoked.) The final authorized set is the rotation set minus the revoked
closure.

> **Note:** revocation intentionally breaks continuity for the revoked branch —
> content previously signed by a now-revoked key stops verifying. That is the
> correct compromise response, and the opposite of routine rotation.

---

## 13. Access control

Because content lives in caches the producer does not control, access control
is enforced by **encryption**, not by withholding service (NDN-NAC style).

### 13.1 Per-prefix policy

A producer declares, per name prefix, the set of consumer identities allowed to
read it (an ACL). Content published under a restricted prefix is encrypted; all
other content is plaintext. When prefixes overlap, the **longest matching
prefix** governs.

### 13.2 Content encryption

* **Content-encryption key (CEK):** derived deterministically from the
  producer's private key and the restricted prefix:
  ```
  secret = BLAKE2b(producer_private_key, digest_size=64)
  CEK    = BLAKE2b( b"icn-content-key\x01" || prefix.to_bytes(),
                    key=secret, digest_size=32 )
  ```
  The CEK is stable across restarts (so cached ciphertext stays decryptable) and
  is never transmitted or stored. Only the producer can derive it.
* **Symmetric encryption:** `content = Token(CEK).encrypt(plaintext)`, where
  `Token` is the RNS authenticated symmetric construction (AES + HMAC). The
  `encrypted` metadata flag ([§8.1](#81-datametadata)) is set and is bound into
  the signature ([§10.2](#102-data-signature-envelope)).
* Caches store, verify (hash + signature over ciphertext), and relay restricted
  Data unchanged.

### 13.3 Capability token

To read restricted content, a consumer needs the CEK. The producer issues a
**capability**: a signed grant binding a consumer to a prefix for a validity
window, carrying the CEK **wrapped to the consumer's RNS identity** (ECIES via
`Identity.encrypt`, so only that consumer can unwrap it).

```
[producer:16]
[consumer:16]
[issued_at:u64]
[expires_at:u64]            # 0 = never expires
[prefix_len:u16][prefix:prefix_len]      # a Name (§5)
[cek_len:u16][wrapped_cek:cek_len]
[has_sig:u8]
( [signature:64] )?         # iff has_sig
```

* `prefix` **MUST** live under `producer` (its first component equals
  `producer`).
* **Signed hash:**
  `BLAKE2b32( b"icn-capability\x01" || producer || consumer || u64(issued_at) || u64(expires_at) || u16(len(prefix_bytes)) || prefix_bytes || u16(len(wrapped_cek)) || wrapped_cek )`,
  signed by the producer's signing key (which MAY be a rotation-delegated key,
  so consumers verify it via [§10.4](#104-resolving-an-authorized-producer-key)).

### 13.4 Consumer procedure

On receiving encrypted Data, a consumer with a capability:

1. selects a capability whose `prefix` covers the Data name and which is not
   expired;
2. verifies the capability signature against an authorized producer key when one
   is available ([§10.4](#104-resolving-an-authorized-producer-key)); if none is
   available it MAY proceed (step 4 fails closed regardless);
3. unwraps the CEK with its own identity;
4. decrypts `Token(CEK).decrypt(content)`.

Both the ECIES unwrap and the AEAD decryption are authenticated, so a forged or
mismatched capability **fails closed** even if its signature could not be
checked offline. A consumer without a usable capability keeps the ciphertext
(the `encrypted` flag stays set).

> Capability *distribution* is out of band of this spec. The wrapped CEK is
> opaque to non-recipients, so capabilities MAY be distributed over any channel.

---

## 14. Forwarding semantics

A forwarder maintains three tables: the **Content Store (CS)**, the **Pending
Interest Table (PIT)**, and the **Forwarding Information Base (FIB)**.

### 14.1 Interest processing

On receiving an Interest on `in_face`, a forwarder:

1. **Loop check** — if `(in_face, nonce)` was already seen, drop. Otherwise
   record it.
2. **CS lookup** — exact match, or longest-prefix match when `can_be_prefix`.
3. **PIT lookup** — find a pending entry for the same name.
4. **FIB lookup** — next-hop face(s) for the name's longest matching prefix.
5. **Strategy decision** over (CS hit, PIT hit, FIB faces), yielding one of:
   * **serve from cache** — return the cached Data;
   * **serve stale, revalidate** — return the stale cached Data now and fire a
     single background refresh ([§15](#15-cache-coherency));
   * **suppress / aggregate** — an equivalent Interest is already pending; attach
     to it and wait, rather than forwarding a duplicate;
   * **forward** — forward to a next-hop face;
   * **drop**.

`must_be_fresh` Interests **MUST NOT** be satisfied by a stale CS entry.

### 14.2 Loop and duplicate suppression

Loop suppression is by `(in_face, nonce)`. Independently, the **hop limit**
([§7](#7-interest)) bounds propagation: a forwarder decrements it before
forwarding and drops the Interest when it reaches 0 (CS/PIT satisfaction still
applies first).

### 14.3 Forwarding and the reverse path

When forwarding, the forwarder records a PIT entry mapping the name to the
`in_face` (aggregating if one exists) and the chosen `out_face`, then sends the
Interest. Returning Data is matched to the PIT entry by name (the content-hash
suffix is stripped for PIT matching), delivered to all aggregated downstream
faces, inserted into the CS, and the PIT entry is satisfied. A PIT entry that is
not satisfied within the Interest lifetime expires; the strategy MAY record the
failure against the out-face for future route selection.

### 14.4 Data processing

Data received on a face is inserted into the Content Store, and if it matches a
pending PIT entry it is additionally delivered to every aggregated downstream
face and the PIT entry is satisfied. (The implementation caches incoming Data
unconditionally — it does not gate insertion on a PIT match — so a forwarder MAY
cache Data it did not solicit; integrity against poisoning rests entirely on
consumer-side signature verification, which is therefore mandatory for any
security guarantee.)

A forwarder does **not** verify producer signatures (that is the consumer's
responsibility) and **MUST NOT** re-sign relayed Data — it relays the producer's
signature untouched. An origin signs only Data whose producer address equals its
own.

---

## 15. Cache coherency

### 15.1 Freshness

A Data MAY declare a `freshness_period` (seconds). A cache treats the entry as
fresh until it has been held longer than the period, then **stale**. A stale
entry MAY still be served per strategy ([§15.3](#153-stale-while-revalidate))
but never to a `must_be_fresh` Interest. Independently, the CS enforces a
storage TTL (configurable, per-prefix overrides) and LRU eviction; these are
local policy, not on-wire.

### 15.2 Rollback protection

`signed_at` and `sequence` are authenticated ([§10.2](#102-data-signature-envelope)),
so a consumer MAY track the highest `(signed_at, sequence)` it has accepted per
name and reject Data that rolls back to an older signed version — defeating a
cache/relay replaying stale-but-validly-signed content. This applies only to
signed Data (an unsigned timestamp is attacker-controlled).

### 15.3 Stale-while-revalidate

A cache MAY be configured with a stale-while-revalidate window: within it, a
stale hit is served immediately while a single background revalidation refreshes
the entry upstream (deduplicated per name). Outside the window, a stale entry is
revalidated before serving (or the Interest is forwarded).

### 15.4 Invalidation

A producer MAY actively purge cached content with a signed Invalidate
([§9.4](#94-invalidate-0x06)), subject to signature verification and
epoch-replay protection.

---

## 16. Reserved names

These labels under a producer's namespace have defined meaning:

| Name | Content | Signed? |
|------|---------|---------|
| `/<producer>/manifest` | Content manifest, JSON ([§17](#17-manifests)) | producer-signed Data |
| `/<producer>/_rotation` | Rotation bundle, binary ([§11.3](#113-rotation-bundle)) | self-verifying (no Data sig) |
| `/<producer>/health` | Health/status JSON | unsigned Data |

Producers **SHOULD NOT** publish ordinary content under these labels.

---

## 17. Manifests

A manifest is a producer-signed index published as Data at
`/<producer>/manifest`, encoded as compact JSON (UTF-8). It is versioned by a
monotonic `sequence`.

```jsonc
{
  "producer": "<32-hex>",            // 16-byte address
  "sequence": <int>,
  "timestamp": <unix-seconds>,
  "entries": [
    {
      "kind": "blob" | "stream" | "manifest",
      "label": "<string>",
      "name": "/<producer-hex>/<label>…",   // display form, §5.3
      "content_hash": "<64-hex>",           // optional
      "size": <int>,                         // optional
      "latest_sequence": <int>,              // optional, stream
      "total_items": <int>,                  // optional, stream
      "start_time": <unix>,                  // optional, stream
      "end_time": <unix>                     // optional, stream
    }
  ],
  "previous": "/<…>"                  // optional, link to prior manifest
}
```

A `manifest`-kind entry references a downstream peer's manifest, enabling
hierarchical content directories across propagation nodes. The reader **MUST**
verify the Data content hash when present.

A **content manifest** (for chunked large content, [§18](#18-large-content)) is
a distinct JSON object published as Data at the content's own name:

```jsonc
{
  "name": "/<…>",
  "chunks": [ { "label": "chunk_0000", "content_hash": "<64-hex>",
                "size": <int>, "sequence": <int> } ],
  "total_size": <int>,
  "content_hash": "<64-hex>",   // blake2b of the complete reassembled content
  "sequence": <int>,
  "timestamp": <unix>
}
```

JSON is used for debug-readability; a future revision MAY define a binary
encoding (see [§19](#19-versioning-and-forward-compatibility)).

### 17.1 Streams

A *stream* is a sequence of Data segments published under child names of a stream
prefix, each carrying a monotonic `sequence`. A consumer fetches incrementally
by expressing `can_be_prefix` Interests with a `min_sequence` selector,
advancing `min_sequence` past each received segment. A producer MAY instead push
segments to subscribers via APS ([§9.1](#91-aps-subscribe-0x03)).

---

## 18. Large content

Content larger than a node's `resource_threshold` (default 100 000 bytes) is
handled in one of two complementary ways:

* **Resource transport** — a single Data packet is sent as an RNS `Resource`,
  payload = `0x49 || Data.to_bytes()` ([§3](#3-transport-binding-rns)). The
  receiver strips the tag and parses the Data normally.
* **Chunking** — content is split into chunks (default 64 KiB) under child names
  `/<producer>/<path…>/chunk_NNNN?hash=<chunk_hash>`, each an independently
  hashed (and OPTIONALLY signed) Data packet, indexed by a content manifest
  ([§17](#17-manifests)). A consumer fetches the manifest, then each chunk,
  verifies each chunk against its `content_hash`, verifies any per-chunk
  producer signature, and reassembles in sequence order; it MAY also verify the
  whole against the manifest's overall `content_hash`.

Per-chunk signatures use the same envelope as single-fetch Data
([§10.2](#102-data-signature-envelope)), so a relay or cache cannot substitute
chunks without breaking the producer signature, and caches re-serve verifiable
chunks.

---

## 19. Versioning and forward compatibility

This protocol evolves primarily by **append-only extension**, not by a per-packet
version field. Implementers **MUST** observe the following rules.

* **Packet type bytes** ([§6](#6-packet-framing)) are a stable registry. New
  packet types take new discriminator values; existing values never change
  meaning. Unknown types are rejected/dropped.
* **Flag bits** (Interest, Data, DataMetadata, control packets) are append-only.
  New optional fields take a new flag bit and append their bytes after all
  currently-defined fields, in ascending bit order. A parser locates fields by
  the flags it understands; this is why an older parser can read a newer packet's
  understood fields.
* **Signature envelopes** ([§10.2](#102-data-signature-envelope)) append new
  authenticated fields behind a new one-byte tag, in fixed order, included only
  when set. This guarantees a signature produced before a field existed still
  verifies on a *newer* verifier (which sees the field absent and so does not
  append it). The guarantee is **one-directional**: Data signed with a field an
  older verifier does not know about will fail on that older verifier, because it
  cannot reconstruct the identical envelope. A verifier **MUST** compute the
  envelope exactly as in [§10.2](#102-data-signature-envelope) for the fields
  present on the Data it holds.
* **Bundle/structured blobs** ([§11.3](#113-rotation-bundle)) use optional
  trailing sections so older and newer encoders/decoders interoperate.
* **Negotiated capabilities** — peer role and feature support are exchanged via
  Capability Peer ([§9.3](#93-capability-peer-0x05)); the `version` byte there is
  the current peer-version handshake.

**Known limitation.** Interest and Data have **no explicit per-packet version
field** (only Propagation Peer and Capability Peer carry a `version`). The
append-only scheme above supports backward-compatible *additions* but not clean
backward-incompatible changes to Interest/Data framing. A future protocol
version that needs a breaking change SHOULD introduce a version negotiation
(e.g. a new Capability Peer `version` gating a new packet-type range) rather than
redefining existing fields.

---

## 20. Security model

**Trust anchor.** A name is self-certifying: the producer address is the hash of
the producer's public key ([§5.2](#52-self-certifying-addressing)). Trust in a
namespace is trust in that key. There is no global PKI.

**What signatures protect.** A producer signature over a Data packet
([§10.2](#102-data-signature-envelope)) authenticates the binding of *name →
content* and the authenticated metadata (`sequence`, `signed_at`, `encrypted`).
It defends against cache poisoning (a relay serving forged content under a
producer's name) and, via `signed_at`/`sequence`, against rollback.

**Key lifecycle.** A producer rotates keys via a self-certifying delegation
chain ([§11](#11-producer-key-rotation)) — verifiable entirely offline because
the anchor binds to the name — and revokes a compromised key (and its delegated
sub-branch) via an anchor-signed revocation ([§12](#12-key-revocation)). The
anchor key is the ultimate authority for the namespace; if the anchor key is
compromised, the namespace is compromised (inherent to self-certifying names).

**Confidentiality.** Path confidentiality is provided by RNS link encryption.
Content confidentiality at rest in caches is provided by ICN access control
([§13](#13-access-control)): restricted content is encrypted with a
producer-derived CEK, delivered to authorized consumers via capabilities, and
decryption fails closed.

**Forwarder trust.** Forwarders are untrusted for confidentiality and integrity:
they relay opaque, signed (and optionally encrypted) Data and cannot forge,
read (when encrypted), or silently downgrade it (the `encrypted` flag is
signed). A forwarder *can* drop or delay packets (availability is not
guaranteed) and *can* serve stale content, which is why `must_be_fresh`,
freshness periods, rollback protection, and signed invalidations exist.

**Parser hardening.** All wire parsers **MUST** bounds-check every length field
against the remaining buffer before use and reject malformed input
([§1](#1-conventions)); a Data `content_len` or name `len` field **MUST NOT**
drive an allocation before it is validated.

**Residual risks / non-goals.** This version does not provide consumer anonymity
(an observer of a face sees the names requested, modulo RNS path encryption),
traffic-analysis resistance, or protection against a compromised anchor key. The
Data signature envelope lacks a leading domain-separation tag
([§10.2](#102-data-signature-envelope)).

---

## 21. Constant reference

| Constant | Value | Meaning |
|----------|-------|---------|
| `RNS_ADDR_BYTES` | 16 | Producer/consumer address length |
| `CONTENT_HASH_BYTES` | 32 | BLAKE2b digest size |
| `SIGNATURE_BYTES` | 64 | Ed25519 signature length |
| `PUBLIC_KEY_BYTES` | 64 | RNS public key (X25519 32 ‖ Ed25519 32) |
| `CEK_BYTES` | 32 | Content-encryption key length |
| `MAX_COMPONENTS` | 32 | Max name components |
| `HASH_DISCRIMINATOR` | `0xFF` | Name content-hash marker |
| `DEFAULT_HOP_LIMIT` | 16 | Interest hop limit when absent |
| `DEFAULT_CHUNK_SIZE` | 65 536 | Default chunk size (bytes) |
| `resource_threshold` | 100 000 | Default Resource-transport threshold (bytes) |
| `RESOURCE_TYPE_ICN_DATA` | `0x49` | Resource payload type tag |
| Channel `MSGTYPE` | `0x01` | RNS Channel message type for ICN packets |
| Offline-queue TTL | 86 400 | Default queued-push lifetime (seconds) |

**Packet types:** `0x01` Interest, `0x02` Data, `0x03` APS Subscribe,
`0x04` Propagation Peer, `0x05` Capability Peer, `0x06` Invalidate.

**Domain tags:** `icn-key-rotation\x01`, `icn-key-revocation\x01`,
`icn-capability\x01`, `icn-content-key\x01`.
