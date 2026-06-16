# Security Policy

`rns-icn` is a security-focused reference implementation: it ships producer
authentication, key rotation/revocation, and name-based access control. We take
vulnerabilities in these mechanisms seriously.

## Reporting a vulnerability

**Please do not open a public issue for a security vulnerability.**

Report privately to **niklas@wojtkowiak.com** (or, on the GitHub mirror, via
*Security → Report a vulnerability* / a private security advisory). Include:

- a description of the issue and its impact,
- the affected component(s) and protocol version,
- steps to reproduce or a proof of concept, and
- any suggested remediation.

We aim to acknowledge a report within **7 days** and to provide a remediation
timeline after triage. Please allow a reasonable disclosure window before
publishing details.

## Scope

In scope (the security surface this project is responsible for):

- **Wire parsers** — malformed/adversarial input handling for every packet and
  signed object (see [PROTOCOL.md](PROTOCOL.md) §1, §20).
- **Producer signatures** — the Data signature envelope, Invalidate, and the
  domain-separated rotation/revocation/capability constructions ([PROTOCOL.md](PROTOCOL.md) §10).
- **Key lifecycle** — rotation chain verification and revocation cascade
  ([PROTOCOL.md](PROTOCOL.md) §11–§12).
- **Access control** — content-key derivation, encryption, capability tokens,
  and the fail-closed decryption path ([PROTOCOL.md](PROTOCOL.md) §13).
- **Forwarding** — cache-poisoning surfaces, replay/rollback, hop-limit and loop
  handling ([PROTOCOL.md](PROTOCOL.md) §14–§15).

Out of scope / known non-goals (documented in [PROTOCOL.md](PROTOCOL.md) §20):

- consumer anonymity and traffic-analysis resistance (an on-path observer sees
  requested names, modulo RNS path encryption);
- compromise of a namespace's **anchor** key (inherent to self-certifying names —
  the anchor *is* the namespace's root of trust);
- the security of Reticulum itself (report RNS issues upstream).

## The trust model in one paragraph

A name is self-certifying: the producer address is the hash of the producer's
public key. Trust in a namespace is trust in that key. Forwarders/caches are
untrusted for integrity and confidentiality — they relay opaque, signed (and
optionally encrypted) Data they cannot forge or read. Consumer-side signature
verification is therefore **mandatory** for any integrity guarantee. See
[PROTOCOL.md](PROTOCOL.md) §20 for the full model.

## Supported versions

This project is pre-1.0; security fixes are applied to the latest release on
`main`. The on-wire protocol version is documented in
[PROTOCOL.md](PROTOCOL.md); breaking security changes bump it and are recorded in
[CHANGELOG.md](CHANGELOG.md).
