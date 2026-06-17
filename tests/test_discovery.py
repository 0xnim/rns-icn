"""Tests for verifiable latest-version discovery (rns_icn.discovery, Part A).

Three layers:
  * the discovery convention itself (pure, no deps);
  * the producer emitting a signed latest pointer on publish;
  * the consumer's discovery-first fetch_latest (pointer → pinned target,
    namespace guard, best-effort fallback).
"""

import asyncio
from unittest.mock import patch

import pytest
import RNS

from rns_icn import discovery
from rns_icn.client import ICNClient
from rns_icn.config import ClientConfig, ServerConfig
from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import ChildSelector, Data, Interest
from rns_icn.rns_server import RNSICNServer
from rns_icn.server import ServerRole


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


# ── The convention (pure) ──


def test_meta_name_appends_reserved_label():
    prefix = Name(rns_addr(), [b"blog"])
    meta = discovery.meta_name(prefix)
    assert meta.rns_addr == prefix.rns_addr
    assert meta.components[1:] == [b"blog", discovery.META_LABEL]
    assert meta.content_hash is None


def test_meta_name_for_root_prefix():
    prefix = Name(rns_addr(), [])
    assert discovery.meta_name(prefix).components[1:] == [discovery.META_LABEL]


def test_meta_payload_round_trip():
    target = Name(rns_addr(), [b"blog", b"post-5"]).with_content_hash(b"\x07" * 32)
    decoded = discovery.decode_meta(discovery.encode_meta(target))
    assert decoded == target
    assert decoded.content_hash == b"\x07" * 32


def test_decode_meta_rejects_bad_format():
    with pytest.raises(discovery.DiscoveryError):
        discovery.decode_meta(b"")
    with pytest.raises(discovery.DiscoveryError):
        discovery.decode_meta(b"\x02whatever")


def test_decode_meta_rejects_truncated_name():
    with pytest.raises(discovery.DiscoveryError):
        discovery.decode_meta(b"\x01\xff\xff\xff")


def test_is_reserved():
    assert discovery.is_reserved(Name(rns_addr(), [b"blog", discovery.META_LABEL]))
    assert not discovery.is_reserved(Name(rns_addr(), [b"blog", b"post"]))


# ── Producer emission ──


@pytest.fixture
def origin() -> RNSICNServer:
    cfg = ServerConfig(
        identity_path="/unused",
        app_name="icn",
        aspect="test",
        cs_path=":memory:",
        role=ServerRole.ORIGIN,
        meta_freshness_period=15,
    )
    # RNSICNServer construction builds a LinkPool that binds the current event
    # loop; provide one for these otherwise-synchronous tests.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # A real keypair (generated offline) so the serve-time signature is genuine.
    with patch("rns_icn.rns_server.load_or_create_identity", return_value=RNS.Identity()):
        server = RNSICNServer(cfg)
    yield server
    loop.close()
    asyncio.set_event_loop(None)


def _serve(origin: RNSICNServer, name: Name) -> Data | None:
    return origin._serve_from_cs(Interest(name=name), in_face_id=0)


def test_publish_emits_signed_latest_pointer(origin):
    name = Name(origin.rns_addr, [b"blog", b"post"])
    origin.publish_content(name, b"hello world", sequence=5)

    meta = _serve(origin, discovery.meta_name(Name(origin.rns_addr, [b"blog"])))
    assert meta is not None
    # Producer-authenticated and rollback-checkable.
    assert meta.verify_signature(origin.identity.validate)
    assert meta.metadata.sequence == 5
    assert meta.metadata.freshness_period == 15
    assert meta.freshness_key() is not None


def test_pointer_target_is_content_hash_pinned(origin):
    name = Name(origin.rns_addr, [b"blog", b"post"])
    origin.publish_content(name, b"hello world", sequence=1)

    version = origin.forwarder.cs.get(name)
    meta = _serve(origin, discovery.meta_name(Name(origin.rns_addr, [b"blog"])))
    target = discovery.decode_meta(meta.content)
    assert target.without_content_hash() == name
    assert target.content_hash == version.metadata.content_hash


def test_republish_bumps_pointer_sequence(origin):
    prefix = Name(origin.rns_addr, [b"blog"])
    origin.publish_content(Name(origin.rns_addr, [b"blog", b"post"]), b"v1", sequence=1)
    origin.publish_content(Name(origin.rns_addr, [b"blog", b"post"]), b"v2", sequence=2)
    meta = _serve(origin, discovery.meta_name(prefix))
    assert meta.metadata.sequence == 2
    assert discovery.decode_meta(meta.content).without_content_hash() == Name(
        origin.rns_addr, [b"blog", b"post"]
    )


def test_root_blob_emits_no_pointer(origin):
    # A name with no parent collection (just the producer address) gets no
    # pointer — there is no collection to point at.
    origin.publish_content(Name(origin.rns_addr, []), b"x", sequence=1)
    assert _serve(origin, discovery.meta_name(Name(origin.rns_addr, []))) is None


def test_latest_under_overrides_collection_prefix(origin):
    name = Name(origin.rns_addr, [b"feed", b"2026", b"item"])
    origin.publish_content(
        name, b"x", sequence=3, latest_under=Name(origin.rns_addr, [b"feed"])
    )
    meta = _serve(origin, discovery.meta_name(Name(origin.rns_addr, [b"feed"])))
    assert meta is not None
    assert meta.metadata.sequence == 3
    # Default parent prefix should NOT have been used.
    assert _serve(origin, discovery.meta_name(Name(origin.rns_addr, [b"feed", b"2026"]))) is None


# ── Consumer discovery-first logic ──


class _Recorder:
    """Stand-in for the network layer: returns canned Data, records calls."""

    def __init__(self):
        self.verified_calls = []
        self.fetch_calls = []
        self.meta_result = None
        self.target_result = None
        self.fallback_result = None

    async def fetch_verified(self, name, peer_hash, **kw):
        self.verified_calls.append((name, kw))
        if kw.get("require_signature"):  # the meta pointer fetch
            return self.meta_result
        return self.fallback_result  # the selector fallback

    async def fetch(self, name, peer_hash, timeout=None, max_retries=None):
        self.fetch_calls.append(name)
        return self.target_result


def _client_with(recorder: _Recorder) -> ICNClient:
    client = ICNClient(ClientConfig())
    client._fetch_verified = recorder.fetch_verified
    client.fetch = recorder.fetch
    return client


@pytest.mark.asyncio
async def test_fetch_latest_follows_pointer():
    addr = rns_addr()
    prefix = Name(addr, [b"blog"])
    target = Name(addr, [b"blog", b"post-5"]).with_content_hash(b"\x09" * 32)

    rec = _Recorder()
    rec.meta_result = Data.new(discovery.meta_name(prefix), discovery.encode_meta(target))
    rec.target_result = Data.new(target, b"the latest post")

    result = await _client_with(rec).fetch_latest(prefix, addr)

    assert result is rec.target_result
    # Pointer was fetched with must_be_fresh + require_signature, then the exact
    # pinned target was fetched. No selector fallback.
    assert rec.verified_calls[0][1]["must_be_fresh"] is True
    assert rec.verified_calls[0][1]["require_signature"] is True
    assert rec.fetch_calls == [target]


@pytest.mark.asyncio
async def test_fetch_latest_rejects_cross_namespace_pointer():
    addr = rns_addr()
    prefix = Name(addr, [b"blog"])
    # Pointer escapes the producer's namespace (different rns_addr).
    evil = Name(rns_addr(0x02), [b"blog", b"post"]).with_content_hash(b"\x01" * 32)

    rec = _Recorder()
    rec.meta_result = Data.new(discovery.meta_name(prefix), discovery.encode_meta(evil))
    rec.fallback_result = Data.new(Name(addr, [b"blog", b"cached"]), b"fallback")

    result = await _client_with(rec).fetch_latest(prefix, addr)

    # The poisoned target is never fetched; we fall through to best-effort.
    assert rec.fetch_calls == []
    assert result is rec.fallback_result


@pytest.mark.asyncio
async def test_fetch_latest_falls_back_to_selector_when_no_pointer():
    addr = rns_addr()
    prefix = Name(addr, [b"blog"])

    rec = _Recorder()
    rec.meta_result = None  # producer published no pointer
    rec.fallback_result = Data.new(Name(addr, [b"blog", b"x"]), b"best effort")

    result = await _client_with(rec).fetch_latest(prefix, addr)

    assert result is rec.fallback_result
    # Fallback uses the LATEST child selector, best-effort (no must_be_fresh).
    fallback_kw = rec.verified_calls[-1][1]
    assert fallback_kw["selector"].child is ChildSelector.LATEST
    assert fallback_kw["can_be_prefix"] is True
    assert fallback_kw.get("must_be_fresh", False) is False
