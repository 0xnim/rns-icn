"""Tests for Follower — a hub pre-fills its cache from a followed producer.

Two in-process servers are joined by a pair of pipe faces that deliver raw
packets to the other side's ``handle_incoming``, so the real Interest/Data,
subscribe and push paths run end to end without RNS.
"""

import asyncio
import contextlib

import pytest
import pytest_asyncio

from rns_icn.face import Face, FaceCapabilities
from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import Data, Interest
from rns_icn.server import ICNServer, ServerRole

ORIGIN = bytes([0xAA] + [0] * (RNS_ADDR_BYTES - 1))
HUB = bytes([0xBB] + [0] * (RNS_ADDR_BYTES - 1))
OTHER = bytes([0xCC] + [0] * (RNS_ADDR_BYTES - 1))


class PipeFace(Face):
    """Delivers everything sent on it to a peer server's handle_incoming."""

    def __init__(self, face_id, peer_server, peer_face_id):
        self._id = face_id
        self._peer_server = peer_server
        self._peer_face_id = peer_face_id
        self._q: asyncio.Queue[bytes] = asyncio.Queue()
        self.sent: list[bytes] = []
        self._task = asyncio.ensure_future(self._pump())

    async def _pump(self):
        while True:
            raw = await self._q.get()
            await self._peer_server.handle_incoming(self._peer_face_id, raw)

    async def express_interest(self, interest):
        await self.send_raw(interest.to_bytes())
        return None

    async def send_data(self, data):
        await self.send_raw(data.to_bytes())

    async def send_raw(self, raw):
        self.sent.append(raw)
        await self._q.put(raw)

    def capabilities(self):
        return FaceCapabilities(is_local=True)

    def id(self):
        return self._id

    def close(self):
        self._task.cancel()


def join(a: ICNServer, b: ICNServer, a_face=1, b_face=2):
    """Connect two servers; returns (a's face toward b, b's face toward a)."""
    fa = PipeFace(a_face, b, b_face)
    fb = PipeFace(b_face, a, a_face)
    for srv, face in ((a, fa), (b, fb)):
        srv._faces[face.id()] = face
        srv.forwarder.register_face(face)
    return fa, fb


def put(server: ICNServer, labels: list[bytes], content: bytes, seq: int | None = None):
    data = Data.new(name=Name(server.rns_addr, labels), content=content)
    if seq is not None:
        data.with_sequence(seq)
    server.forwarder.cs.insert(data.name, data)
    return data


async def settle(rounds: int = 20):
    for _ in range(rounds):
        await asyncio.sleep(0)


@pytest_asyncio.fixture
async def pair():
    origin = ICNServer(ORIGIN, role=ServerRole.ORIGIN)
    hub = ICNServer(HUB, role=ServerRole.CACHE)
    to_origin, to_hub = join(hub, origin)
    hub.forwarder.add_route(Name(ORIGIN), to_origin.id(), cost=10)
    yield origin, hub, to_origin, to_hub
    hub.follower.stop()
    for face in (to_origin, to_hub):
        face.close()
    await settle()


def interests_sent(face: PipeFace) -> int:
    return sum(1 for raw in face.sent if raw[0] == 0x01)


@pytest.mark.asyncio
async def test_initial_sync_pulls_blobs_and_stream_editions(pair):
    origin, hub, to_origin, _ = pair
    put(origin, [b"about"], b"hello")
    for seq in (1, 2, 3):
        put(origin, [b"feed", f"item{seq}".encode()], f"post {seq}".encode(), seq)

    await hub.follower.follow(to_origin.id(), ORIGIN)

    cs = hub.forwarder.cs
    assert cs.get(Name(ORIGIN, [b"about"])).content == b"hello"
    for seq in (1, 2, 3):
        assert cs.get(Name(ORIGIN, [b"feed", f"item{seq}".encode()])) is not None
    assert hub.follower.stats()[ORIGIN.hex()].pulled == 4


@pytest.mark.asyncio
async def test_resync_fetches_only_what_is_missing(pair):
    origin, hub, to_origin, _ = pair
    put(origin, [b"about"], b"hello")
    for seq in (1, 2):
        put(origin, [b"feed", f"item{seq}".encode()], b"x", seq)
    await hub.follower.follow(to_origin.id(), ORIGIN)

    before = interests_sent(to_origin)
    assert await hub.follower.sync(ORIGIN) == 0
    # Nothing new upstream: one manifest Interest, no content Interests.
    assert interests_sent(to_origin) - before == 1

    put(origin, [b"feed", b"item3"], b"new", 3)
    before = interests_sent(to_origin)
    assert await hub.follower.sync(ORIGIN) == 1
    assert hub.forwarder.cs.get(Name(ORIGIN, [b"feed", b"item3"])).content == b"new"
    # manifest + the new edition; the manifest's latest_sequence ends the walk
    assert interests_sent(to_origin) - before == 2


@pytest.mark.asyncio
async def test_changed_blob_is_refetched(pair):
    origin, hub, to_origin, _ = pair
    put(origin, [b"about"], b"v1")
    await hub.follower.follow(to_origin.id(), ORIGIN)
    put(origin, [b"about"], b"v2")
    assert await hub.follower.sync(ORIGIN) == 1
    assert hub.forwarder.cs.get(Name(ORIGIN, [b"about"])).content == b"v2"


@pytest.mark.asyncio
async def test_live_push_is_cached_and_relayed_downstream(pair):
    origin, hub, to_origin, _ = pair
    await hub.follower.follow(to_origin.id(), ORIGIN)
    await settle()

    # A reader subscribed at the hub.
    reader = ICNServer(OTHER)
    to_reader, to_hub2 = join(hub, reader, a_face=7, b_face=8)
    hub.aps.subscribe(Name(ORIGIN, [b"feed"]), to_reader.id())

    pushed = Data.new(name=Name(ORIGIN, [b"feed", b"item9"]), content=b"breaking").with_sequence(9)
    await origin.publish_pushed(pushed)
    await settle()

    assert hub.forwarder.cs.get(pushed.name).content == b"breaking"
    assert hub.follower.stats()[ORIGIN.hex()].pushed == 1
    assert any(raw[0] == 0x02 and b"breaking" in raw for raw in to_reader.sent)
    for face in (to_reader, to_hub2):
        face.close()


@pytest.mark.asyncio
async def test_unsolicited_data_outside_the_follow_is_not_cached(pair):
    origin, hub, to_origin, to_hub = pair
    await hub.follower.follow(to_origin.id(), ORIGIN)
    await settle()

    # Followed face, but named under a different producer.
    foreign = Data.new(name=Name(OTHER, [b"feed", b"x"]), content=b"spoof")
    await to_hub.send_data(foreign)
    # Right producer, but arriving on a face we do not follow it over.
    stranger = ICNServer(OTHER)
    _, from_stranger = join(hub, stranger, a_face=7, b_face=8)
    forged = Data.new(name=Name(ORIGIN, [b"feed", b"y"]), content=b"forged")
    await from_stranger.send_data(forged)
    await settle()

    assert hub.forwarder.cs.get(foreign.name) is None
    assert hub.forwarder.cs.get(forged.name) is None
    from_stranger.close()


@pytest.mark.asyncio
async def test_push_with_bad_signature_is_rejected(pair):
    origin, hub, to_origin, to_hub = pair
    hub.follower._validator_for = lambda addr: (lambda sig, h: sig == b"G" * 64)
    await hub.follower.follow(to_origin.id(), ORIGIN)
    await settle()

    good = Data.new(name=Name(ORIGIN, [b"feed", b"good"]), content=b"ok")
    good.signature = b"G" * 64
    bad = Data.new(name=Name(ORIGIN, [b"feed", b"bad"]), content=b"evil")
    bad.signature = b"B" * 64
    await to_hub.send_data(good)
    await to_hub.send_data(bad)
    await settle()

    assert hub.forwarder.cs.get(good.name) is not None
    assert hub.forwarder.cs.get(bad.name) is None
    assert hub.follower.stats()[ORIGIN.hex()].rejected == 1


@pytest.mark.asyncio
async def test_label_filter_limits_sync_and_push(pair):
    origin, hub, to_origin, to_hub = pair
    put(origin, [b"feed", b"item1"], b"wanted", 1)
    put(origin, [b"logs", b"l1"], b"unwanted", 1)
    await hub.follower.follow(to_origin.id(), ORIGIN, labels=["feed"])
    await settle()

    assert hub.forwarder.cs.get(Name(ORIGIN, [b"feed", b"item1"])) is not None
    assert hub.forwarder.cs.get(Name(ORIGIN, [b"logs", b"l1"])) is None

    await to_hub.send_data(Data.new(name=Name(ORIGIN, [b"logs", b"l2"]), content=b"nope"))
    await settle()
    assert hub.forwarder.cs.get(Name(ORIGIN, [b"logs", b"l2"])) is None


@pytest.mark.asyncio
async def test_hub_serves_followed_content_with_origin_gone(pair):
    origin, hub, to_origin, _ = pair
    put(origin, [b"feed", b"item1"], b"cached ahead", 1)
    await hub.follower.follow(to_origin.id(), ORIGIN)

    # Origin disappears before any reader ever asked for the item.
    hub.follower.unfollow_face(to_origin.id())
    hub.forwarder.withdraw_face(to_origin.id())
    hub._faces.pop(to_origin.id())

    got = await hub.forwarder.express(
        Interest(name=Name(ORIGIN, [b"feed", b"item1"]), lifetime_ms=200), 0
    )
    assert got is not None and got.content == b"cached ahead"


@pytest.mark.asyncio
async def test_refollow_after_reconnect_keeps_stats_and_catches_up(pair):
    origin, hub, to_origin, _ = pair
    put(origin, [b"feed", b"item1"], b"a", 1)
    await hub.follower.follow(to_origin.id(), ORIGIN)
    hub.follower.unfollow_face(to_origin.id())

    put(origin, [b"feed", b"item2"], b"b", 2)
    await hub.follower.follow(to_origin.id(), ORIGIN)

    stats = hub.follower.stats()[ORIGIN.hex()]
    assert stats.syncs == 2 and stats.pulled == 2
    assert hub.forwarder.cs.get(Name(ORIGIN, [b"feed", b"item2"])).content == b"b"


@pytest.mark.asyncio
async def test_sync_with_unreachable_upstream_records_error(pair):
    _, hub, to_origin, _ = pair
    hub.follower._lifetime_ms = 50
    to_origin.close()  # nothing is delivered any more
    with contextlib.suppress(asyncio.CancelledError):
        await hub.follower.follow(to_origin.id(), ORIGIN)
    assert hub.follower.stats()[ORIGIN.hex()].last_error == "no manifest"


def test_manifest_names_with_hex_looking_labels_round_trip():
    """Labels such as "feed" or "2026" are text, not hex-encoded bytes."""
    from rns_icn.manifest import EntryKind, Manifest, ManifestEntry

    names = [Name(ORIGIN, [b"feed", b"cafe"]), Name(ORIGIN, [b"2026", b"11"])]
    manifest = Manifest.create(
        producer=ORIGIN,
        entries=[ManifestEntry(kind=EntryKind.BLOB, label="x", name=n) for n in names],
    )
    parsed = Manifest.from_dict(manifest.to_dict())
    assert [e.name for e in parsed.entries] == names


@pytest.mark.asyncio
async def test_origin_manifest_reflects_new_content_and_advances_sequence():
    from rns_icn.manifest import Manifest

    origin = ICNServer(ORIGIN)
    put(origin, [b"feed", b"item1"], b"a", 1)
    first = Manifest.from_data(await origin._build_manifest_data())
    origin.forwarder.cs.insert(Name(ORIGIN, [b"manifest"]), Data.new(
        name=Name(ORIGIN, [b"manifest"]), content=first.to_json()))
    again = Manifest.from_data(await origin._build_manifest_data())
    assert again.sequence == first.sequence          # nothing changed
    assert [e.label for e in again.entries] == ["feed"]  # does not list itself

    put(origin, [b"feed", b"item2"], b"b", 2)
    later = Manifest.from_data(await origin._build_manifest_data())
    assert later.sequence > first.sequence
    assert later.entries[0].latest_sequence == 2


@pytest.mark.asyncio
async def test_every_blob_under_a_label_is_pulled(pair):
    origin, hub, to_origin, _ = pair
    put(origin, [b"docs", b"a"], b"A")
    put(origin, [b"docs", b"b"], b"B")
    await hub.follower.follow(to_origin.id(), ORIGIN)
    assert hub.forwarder.cs.get(Name(ORIGIN, [b"docs", b"a"])).content == b"A"
    assert hub.forwarder.cs.get(Name(ORIGIN, [b"docs", b"b"])).content == b"B"
