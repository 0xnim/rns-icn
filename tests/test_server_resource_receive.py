"""Ordinary Resource responses must wake pending Interests via the server loop."""
import asyncio
import io
from types import SimpleNamespace

import pytest
import RNS
from rns_icn.face import TestFace
from rns_icn.name import Name
from rns_icn.packet import Data, Interest
from rns_icn.resource_transport import ResourceListener, _wrap_payload
from rns_icn.rns_server import ICNServer
from rns_icn.server import ICNServer as BaseServer


def listener(limit=1024, previous=None, mdu=100):
    link = SimpleNamespace(mdu=mdu, callbacks=SimpleNamespace(resource_started=previous))
    return ResourceListener(link, max_data_size=limit), link


def advert(**values):
    fields = dict(d=500, t=550, n=6, s=False, i=1, l=1, x=False, u=False, p=False, q=None)
    fields.update(values)
    return SimpleNamespace(**fields)


@pytest.mark.parametrize("change", [dict(d=1025), dict(t=2049), dict(n=65536), dict(d=0),
    dict(d=-1), dict(t=True), dict(s=True), dict(i=2), dict(l=2), dict(x=True),
    dict(u=True), dict(p=True), dict(q=b"request")])
def test_rejects_unsupported_or_oversized_ordinary_advert(change):
    receiver, _ = listener()
    assert receiver._accept_resource(advert())
    assert not receiver._accept_resource(advert(**change))


def test_bounds_actual_native_part_allocation_not_just_advertised_parts():
    receiver, _ = listener(limit=100000, mdu=1)
    assert not receiver._accept_resource(advert(t=70000, n=1))


def test_started_caps_decompression_and_preserves_request_response_policy():
    seen = []
    receiver, link = listener(previous=lambda resource: seen.append(resource))
    for flags in (0, 0x08, 0x10):
        resource = SimpleNamespace(total_size=600, flags=flags, request_id=None, max_decompressed_size=9999)
        link.callbacks.resource_started(resource)
        assert resource.max_decompressed_size == (600 if flags == 0 else 9999)
    assert len(seen) == 3


@pytest.mark.parametrize("declared,actual", [(100, 101), (1025, 1025)])
def test_completed_resource_read_is_bounded_and_matches_declared_size(declared, actual):
    receiver, _ = listener()
    calls = []
    receiver.set_on_raw(calls.append)
    resource = SimpleNamespace(status=RNS.Resource.COMPLETE, data=io.BytesIO(b"x" * actual), total_size=declared)
    receiver._on_resource_concluded(resource)
    assert calls == []
    assert resource.data.tell() <= 1025


def test_completed_ordinary_resource_delivers_tagged_data():
    receiver, _ = listener()
    data = Data.new(Name(b"p" * 16, [b"small"]), b"valid")
    payload = _wrap_payload(data.to_bytes())
    calls = []
    receiver.set_on_data(calls.append)
    receiver._on_resource_concluded(SimpleNamespace(status=RNS.Resource.COMPLETE,
        data=io.BytesIO(payload), total_size=len(payload)))
    assert len(calls) == 1 and calls[0].content == b"valid"


@pytest.mark.asyncio
async def test_resource_from_rns_thread_wakes_pending_interest_without_unsolicited_cache_insert():
    server = BaseServer(b"s" * 16)
    server._loop = asyncio.get_running_loop()
    data = Data.new(Name(b"p" * 16, [b"large"]), b"payload")
    face = TestFace(7)
    face._closed = False
    face._recv_queue = asyncio.Queue()
    server._faces[7] = face
    server.forwarder.register_face(face)
    server.forwarder.add_route(data.name, 7)
    handled = asyncio.Event()
    original_handler = server.handle_incoming
    async def handle_incoming(face_id, raw):
        await original_handler(face_id, raw)
        handled.set()
    server.handle_incoming = handle_incoming
    processing = asyncio.create_task(ICNServer._process_link(server, 7, face))
    pending = asyncio.create_task(server.forwarder.express(Interest(data.name, lifetime_ms=500), 100))
    await asyncio.wait_for(face._outgoing.get(), 1)  # pending Interest was sent
    await asyncio.to_thread(ICNServer._on_resource_data, server, data, 7)
    try:
        result = await asyncio.wait_for(pending, 1)
        assert result.content == b"payload"
        assert server.forwarder.cs.contains(data.name)
        unsolicited = Data.new(Name(b"p" * 16, [b"unsolicited"]), b"ignored")
        handled.clear()
        ICNServer._on_resource_data(server, unsolicited, 7)
        await asyncio.wait_for(handled.wait(), 1)
        assert not server.forwarder.cs.contains(unsolicited.name)
    finally:
        processing.cancel()
        await asyncio.gather(processing, return_exceptions=True)
        server.forwarder.cs.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["closed", "removed", "shutdown", "replaced"])
async def test_resource_callback_drops_stale_face_or_loop(state):
    loop = asyncio.get_running_loop()
    face = SimpleNamespace(_closed=False, _recv_queue=asyncio.Queue())
    server = SimpleNamespace(_loop=loop, _faces={7: face})
    data = Data.new(Name(b"p" * 16, [b"data"]), b"payload")
    ICNServer._on_resource_data(server, data, 7)
    if state == "closed": face._closed = True
    elif state == "removed": server._faces.clear()
    elif state == "shutdown": server._loop = None
    else: server._loop = object()
    await asyncio.sleep(0)
    assert face._recv_queue.empty()


def test_accepts_and_bounds_native_packed_advertisements():
    receiver, _ = listener()
    resource = SimpleNamespace(size=550, total_size=500, parts=[None] * 6,
        hash=bytes(32), random_hash=bytes(4), original_hash=bytes(32),
        hashmap=bytes(6 * RNS.Resource.MAPHASH_LEN), compressed=True, encrypted=True,
        app_flag=False, split=False, has_metadata=False, segment_index=1, total_segments=1, request_id=None)
    native = RNS.ResourceAdvertisement(resource)
    parsed = RNS.ResourceAdvertisement.unpack(native.pack())
    assert receiver._accept_resource(parsed)
    native.d = 1025
    assert not receiver._accept_resource(RNS.ResourceAdvertisement.unpack(native.pack()))
    native.d = 500
    native.f |= 0x08
    assert not receiver._accept_resource(RNS.ResourceAdvertisement.unpack(native.pack()))
