"""Reply ordering and cancellation must not orphan or overwrite pending requests."""
import asyncio

import pytest

from rns_icn.face import TestFace
from rns_icn.forwarder import Forwarder
from rns_icn.name import Name
from rns_icn.packet import Data, Interest


class ControlledFace(TestFace):
    def __init__(self, fw):
        super().__init__(7)
        self.fw = fw
        self.sent = asyncio.Queue()
        self.mode = "wait"

    async def send_raw(self, raw):
        interest = Interest.from_bytes(raw)
        self.sent.put_nowait(interest)
        if self.mode == "inline":
            await self.fw.receive_data(Data.new(interest.name, b"immediate"), self.id())
        elif self.mode == "nack":
            self.fw.receive_nack(interest.name, self.id())
        elif self.mode == "error":
            raise ConnectionError("send failed")


def setup():
    fw = Forwarder()
    face = ControlledFace(fw)
    fw.register_face(face)
    name = Name(b"p" * 16, [b"pending"])
    fw.add_route(name, face.id())
    return fw, face, name


@pytest.mark.asyncio
async def test_reply_during_send_is_not_lost():
    fw, face, name = setup()
    face.mode = "inline"
    result = await fw.express(Interest(name, lifetime_ms=30), 100)
    assert result is not None and result.content == b"immediate"
    assert len(fw.pit) == 0 and fw._pit_notifiers == {}


@pytest.mark.asyncio
async def test_cancelled_owner_does_not_block_same_name_successor():
    fw, face, name = setup()
    first = asyncio.create_task(fw.express(Interest(name, lifetime_ms=1000), 100))
    await face.sent.get()
    successor = asyncio.create_task(fw.express(Interest(name, lifetime_ms=100), 101))
    face.mode = "inline"
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    result = await successor
    assert result is not None and result.content == b"immediate"
    assert face.sent.qsize() == 1  # exactly one fresh upstream request
    assert len(fw.pit) == 0 and fw._pit_notifiers == {}


@pytest.mark.asyncio
async def test_send_failure_releases_pending_state():
    fw, face, name = setup()
    face.mode = "error"
    assert await fw.express(Interest(name, lifetime_ms=50), 100) is None
    assert len(fw.pit) == 0 and fw._pit_notifiers == {}


@pytest.mark.asyncio
async def test_send_failure_on_primary_falls_through_to_backup():
    # A primary whose link died raises on send; the backup must still be tried.
    fw, primary, name = setup()
    primary.mode = "error"
    backup = ControlledFace(fw)
    backup._id = 8
    backup.mode = "inline"
    fw.register_face(backup)
    fw.add_route(name, backup.id(), cost=20)
    result = await fw.express(Interest(name, lifetime_ms=100), 100)
    assert result is not None and result.content == b"immediate"
    assert len(fw.pit) == 0 and fw._pit_notifiers == {}


@pytest.mark.asyncio
async def test_unanswered_timeout_removes_notifier():
    fw, face, name = setup()
    assert await fw.express(Interest(name, lifetime_ms=10), 100) is None
    assert len(fw.pit) == 0 and fw._pit_notifiers == {}


@pytest.mark.asyncio
async def test_aggregate_cancellation_removes_only_its_notifier():
    fw, face, name = setup()
    fw.pit.insert_or_aggregate(name, 99, Interest(name), 1000)
    existing = asyncio.get_running_loop().create_future()
    fw._pit_notifiers[name] = [existing]
    request = asyncio.create_task(fw.express(Interest(name), 100))
    for _ in range(100):
        if len(fw._pit_notifiers[name]) == 2:
            break
        await asyncio.sleep(0)
    assert len(fw._pit_notifiers[name]) == 2
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert fw._pit_notifiers[name] == [existing]
    assert not existing.done() and fw.pit.find(name) is not None


@pytest.mark.asyncio
async def test_old_owner_cleanup_preserves_replacement_pit_entry():
    fw, face, name = setup()
    pending = asyncio.create_task(fw.express(Interest(name), 100))
    await face.sent.get()
    # Simulate expiry/eviction followed by a new owner while old I/O is pending.
    fw.pit.remove(name)
    fw.pit.insert_or_aggregate(name, 101, Interest(name), 1000)
    replacement = fw.pit.find(name)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert fw.pit.find(name) is replacement
    assert fw._pit_notifiers == {}


@pytest.mark.asyncio
async def test_inline_nack_reaches_waiter_and_falls_back_without_timeout():
    fw, primary, name = setup()
    primary.mode = "nack"
    backup = ControlledFace(fw)
    backup._id = 8
    backup.mode = "inline"
    fw.register_face(backup)
    fw.add_route(name, backup.id(), cost=20)
    result = await fw.express(Interest(name, lifetime_ms=100), 100)
    assert result is not None and result.content == b"immediate"
    assert primary.sent.qsize() == backup.sent.qsize() == 1
    assert len(fw.pit) == 0 and fw._pit_notifiers == {}


@pytest.mark.asyncio
async def test_many_same_name_readers_only_fill_cache_once():
    fw, face, name = setup()
    face.mode = "inline"
    results = await asyncio.gather(*(fw.express(Interest(name), reader + 100)
                                   for reader in range(32)))
    assert all(result is not None and result.content == b"immediate" for result in results)
    assert face.sent.qsize() == 1
    assert len(fw.pit) == 0 and fw._pit_notifiers == {}
