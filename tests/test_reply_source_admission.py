"""A different next hop cannot complete or overwrite an active request."""
import asyncio
from types import SimpleNamespace
import pytest
from rns_icn.face import LinkFace, TestFace
from rns_icn.forwarder import Forwarder
from rns_icn.name import Name
from rns_icn.packet import Data, Interest


def setup(prefix=False):
    fw = Forwarder()
    face = TestFace(7)
    fw.register_face(face)
    name = Name(b"p" * 16, [b"stream"])
    fw.add_route(name, face.id())
    interest = Interest(name, can_be_prefix=prefix, lifetime_ms=1000)
    return fw, face, interest


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix", [False, True])
@pytest.mark.parametrize("trusted_push", [False, True])
async def test_wrong_source_data_cannot_complete_pit_or_poison_cache(prefix, trusted_push):
    fw, face, interest = setup(prefix)
    pending = asyncio.create_task(fw.express(interest, 100))
    await face._outgoing.get()
    reply_name = Name(b"p" * 16, [b"stream", b"edition"]) if prefix else interest.name
    wrong = Data.new(reply_name, b"wrong next hop")
    try:
        entry = fw.pit.find(interest.name)
        await fw.receive_data(wrong, 99, cache_unsolicited=trusted_push)
        assert fw.pit.find(interest.name) is entry and not entry.satisfied
        assert not fw.cs.contains(reply_name)
        assert not pending.done()
        correct = Data.new(reply_name, b"requested next hop")
        await fw.receive_data(correct, face.id())
        result = await asyncio.wait_for(pending, 1)
        assert result.content == b"requested next hop"
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        fw.cs.close()


@pytest.mark.asyncio
async def test_wrong_source_nack_leaves_pending_waiter_intact():
    fw, face, interest = setup()
    pending = asyncio.create_task(fw.express(interest, 100))
    await face._outgoing.get()
    waiter = fw._pit_notifiers[interest.name][0]
    try:
        fw.receive_nack(interest.name, 99)
        assert fw._pit_notifiers.get(interest.name) == [waiter]
        assert not waiter.done()
        await fw.receive_data(Data.new(interest.name, b"valid"), face.id())
        assert (await asyncio.wait_for(pending, 1)).content == b"valid"
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        fw.cs.close()


@pytest.mark.asyncio
async def test_unrelated_trusted_push_remains_cacheable():
    fw = Forwarder()
    data = Data.new(Name(b"p" * 16, [b"push"]), b"trusted")
    await fw.receive_data(data, 99, cache_unsolicited=True)
    assert fw.cs.contains(data.name)
    fw.cs.close()


def make_link_face(loop, hook):
    callbacks = []
    channel = SimpleNamespace(register_message_type=lambda _: None, add_message_handler=lambda _: None)
    link = SimpleNamespace(get_channel=lambda: channel, set_link_closed_callback=callbacks.append)
    teardown_calls = []
    def teardown():
        teardown_calls.append(True)
        callbacks[0](link)
    link.teardown = teardown
    return LinkFace(7, link, loop=loop, on_closed=hook), link, callbacks, teardown_calls


@pytest.mark.asyncio
async def test_explicit_linkface_close_notifies_owner_and_receive_queue_once():
    calls = []
    face, link, callbacks, teardowns = make_link_face(asyncio.get_running_loop(), calls.append)
    face.close()
    face.close()
    callbacks[0](link)
    await asyncio.sleep(0)
    assert calls == [7]
    assert len(teardowns) == 1
    assert face._recv_queue.get_nowait() == b""
    assert face._recv_queue.empty()


def test_linkface_close_after_loop_shutdown_still_notifies_owner():
    loop = asyncio.new_event_loop()
    calls = []
    face, _, _, teardowns = make_link_face(loop, calls.append)
    loop.close()
    face.close()
    assert calls == [7] and len(teardowns) == 1


@pytest.mark.asyncio
async def test_concurrent_linkface_close_calls_only_notify_once():
    calls = []
    face, _, _, teardowns = make_link_face(asyncio.get_running_loop(), calls.append)
    await asyncio.gather(*(asyncio.to_thread(face.close) for _ in range(16)))
    await asyncio.sleep(0)
    assert calls == [7] and len(teardowns) == 1
    assert face._recv_queue.get_nowait() == b"" and face._recv_queue.empty()


@pytest.mark.asyncio
async def test_rejected_peer_data_is_not_propagated_to_other_peers():
    from rns_icn.server import ICNServer
    from unittest.mock import AsyncMock
    server = ICNServer(b"s" * 16)
    fw = server.forwarder
    name = Name(b"p" * 16, [b"request"])
    fw.pit.insert_or_aggregate(name, 100, Interest(name), 1000)
    fw.pit.set_out_face(name, 7)
    propagation = server.propagation
    propagation.propagate = AsyncMock()
    await propagation.handle_peer_data(Data.new(name, b"wrong peer"), 99)
    propagation.propagate.assert_not_awaited()
    assert not fw.cs.contains(name)
    assert fw.pit.find(name) is not None
    fw.cs.close()
