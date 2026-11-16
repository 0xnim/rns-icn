"""Holder discovery — find the ICN nodes that hold a producer's content.

A node that follows producers (``rns_icn.follow``) lists them in its announce
(PROTOCOL.md §3.2). Anyone who hears the announce then knows a place to ask for
that producer's content without it being configured, and can pick the nearest.

The list is a hint, not a promise: a holder may have evicted an item or lie
about holding it. Data is still verified against the producer's signature, so a
wrong hint costs a round trip, never a wrong answer.

This module has no Reticulum dependency; whoever receives announces feeds them
to ``HolderDirectory.observe``.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from .name import RNS_ADDR_BYTES

ANNOUNCE_MAGIC = b"icn"
ANNOUNCE_FEATURE_REQUEST = 0x01
ANNOUNCE_FEATURE_HOLDS = 0x02

# Each entry is 16 bytes of every announce, and announces cross slow links.
MAX_ANNOUNCED_HOLDS = 8

# Forget a holder not heard from for this long.
HOLDER_TTL_SECONDS = 6 * 3600.0


def encode_app_data(role: int, features: int, holds: list[bytes] | None = None) -> bytes:
    """Build announce app_data: magic, role, features and the holds list."""
    holds = [h for h in (holds or []) if len(h) == RNS_ADDR_BYTES][:MAX_ANNOUNCED_HOLDS]
    if not holds:
        return ANNOUNCE_MAGIC + bytes([role, features & ~ANNOUNCE_FEATURE_HOLDS])
    return (
        ANNOUNCE_MAGIC
        + bytes([role, features | ANNOUNCE_FEATURE_HOLDS, len(holds)])
        + b"".join(holds)
    )


def decode_holds(app_data: bytes | None) -> list[bytes]:
    """Producer addresses listed in an announce; empty if absent or malformed."""
    if (
        app_data is None or len(app_data) < 6 or app_data[:3] != ANNOUNCE_MAGIC
        or not app_data[4] & ANNOUNCE_FEATURE_HOLDS
    ):
        return []
    count = app_data[5]
    if count > MAX_ANNOUNCED_HOLDS or len(app_data) < 6 + count * RNS_ADDR_BYTES:
        return []
    body = app_data[6:6 + count * RNS_ADDR_BYTES]
    return [body[i:i + RNS_ADDR_BYTES] for i in range(0, len(body), RNS_ADDR_BYTES)]


@dataclass(frozen=True)
class Holder:
    destination_hash: bytes
    last_seen: float
    is_origin: bool  # the producer itself, as opposed to a node following it


HolderCallback = Callable[[bytes, bytes], None]  # (producer address, destination hash)


class HolderDirectory:
    """Who holds which producer's content, learned from announces."""

    def __init__(self, ttl: float = HOLDER_TTL_SECONDS,
                 hops_to: Callable[[bytes], int | None] | None = None):
        """``hops_to`` maps a destination hash to its hop count, if known."""
        self._ttl = ttl
        self._hops_to = hops_to
        # producer address -> destination hash -> Holder
        self._by_producer: dict[bytes, dict[bytes, Holder]] = {}
        self._callbacks: list[HolderCallback] = []

    def add_callback(self, callback: HolderCallback) -> None:
        """Called for every (producer, holder) pair each time it is announced."""
        self._callbacks.append(callback)

    def observe(self, destination_hash: bytes, identity_hash: bytes | None,
                app_data: bytes | None, now: float | None = None) -> None:
        """Record one announce. ``identity_hash`` is the announcer's own address."""
        if app_data is None or app_data[:3] != ANNOUNCE_MAGIC:
            return
        now = time.time() if now is None else now
        holds = decode_holds(app_data)
        # The current list replaces the previous one from this node.
        for producer, holders in list(self._by_producer.items()):
            held = holders.get(destination_hash)
            if held is not None and not held.is_origin and producer not in holds:
                del holders[destination_hash]
                if not holders:
                    del self._by_producer[producer]
        pairs = [(p, False) for p in holds if p != identity_hash]
        if identity_hash is not None and len(identity_hash) == RNS_ADDR_BYTES:
            pairs.append((identity_hash, True))
        for producer, is_origin in pairs:
            self._by_producer.setdefault(producer, {})[destination_hash] = Holder(
                destination_hash, now, is_origin
            )
        for producer, _ in pairs:
            for callback in self._callbacks:
                callback(producer, destination_hash)

    def forget(self, destination_hash: bytes) -> None:
        for producer in list(self._by_producer):
            self._by_producer[producer].pop(destination_hash, None)
            if not self._by_producer[producer]:
                del self._by_producer[producer]

    def holders_of(self, producer: bytes, now: float | None = None) -> list[Holder]:
        """Live holders of a producer's content, nearest first.

        Ties (and unknown distances) go to nodes following the producer ahead
        of the producer itself, since the point is to spare the origin.
        """
        now = time.time() if now is None else now
        holders = self._by_producer.get(producer, {})
        for dest in [d for d, h in holders.items() if now - h.last_seen > self._ttl]:
            del holders[dest]
        if not holders:
            self._by_producer.pop(producer, None)
            return []

        def rank(holder: Holder) -> tuple[int, bool, float]:
            hops = self._hops_to(holder.destination_hash) if self._hops_to else None
            distance = hops if isinstance(hops, int) and 0 < hops < 128 else 128
            return (distance, holder.is_origin, -holder.last_seen)

        return sorted(holders.values(), key=rank)

    def producers(self) -> list[bytes]:
        return list(self._by_producer)
