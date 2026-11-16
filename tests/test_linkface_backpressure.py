"""Channel capacity waits must retain packets and respect cancellation/deadlines."""
import asyncio
from types import SimpleNamespace

import pytest
from RNS.Channel import ChannelException, CEType

from rns_icn.face import LinkFace
from rns_icn.name import Name
from rns_icn.packet import Data, Interest


class WindowChannel:
    mdu = 500

    def __init__(self):
        self.capacity = 0
        self.sent = []
        self.error = None

    def register_message_type(self, _message):
        pass

    def add_message_handler(self, _handler):
        pass

    def send(self, message):
        if self.error:
            raise self.error
        if not self.capacity:
            raise ChannelException(CEType.ME_LINK_NOT_READY, "full")
        self.capacity -= 1
        self.sent.append(message.pack())


def make_face(**options):
    channel = WindowChannel()
    callbacks = []
    link = SimpleNamespace(get_channel=lambda: channel,
                           set_link_closed_callback=callbacks.append, teardown=lambda: None)
    return LinkFace(1, link, **options), channel, lambda: callbacks[0](link)


@pytest.mark.asyncio
async def test_send_waits_for_capacity_then_transmits_exactly_once():
    face, channel, _ = make_face()
    task = asyncio.create_task(face.send_raw(b"retained"))
    await asyncio.sleep(0.02)
    assert not task.done() and channel.sent == []
    channel.capacity = 1
    await asyncio.wait_for(task, 0.2)
    assert channel.sent == [b"retained"]


@pytest.mark.asyncio
async def test_concurrent_waiters_do_not_overrun_window_or_lose_bytes():
    face, channel, _ = make_face()
    tasks = [asyncio.create_task(face.send_raw(bytes([i]))) for i in range(6)]
    for expected in range(1, 7):
        channel.capacity = 1
        for _ in range(100):
            if len(channel.sent) == expected:
                break
            await asyncio.sleep(0.002)
        assert len(channel.sent) == expected
        assert channel.capacity == 0
    await asyncio.gather(*tasks)
    assert sorted(channel.sent) == [bytes([i]) for i in range(6)]


@pytest.mark.asyncio
async def test_send_cancellation_never_sends_later():
    face, channel, _ = make_face()
    task = asyncio.create_task(face.send_raw(b"cancelled"))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    channel.capacity = 1
    await asyncio.sleep(0.02)
    assert channel.sent == []


@pytest.mark.asyncio
@pytest.mark.parametrize("close_before", [False, True])
async def test_link_close_fails_waiters(close_before):
    face, channel, close = make_face()
    if close_before:
        close()
    task = asyncio.create_task(face.send_raw(b"closed"))
    if not close_before:
        await asyncio.sleep(0.01)
        close()
    with pytest.raises(ConnectionError, match="closed face"):
        await asyncio.wait_for(task, 0.2)
    assert channel.sent == []


@pytest.mark.asyncio
async def test_non_capacity_errors_surface_without_retry():
    face, channel, _ = make_face()
    error = ChannelException(CEType.ME_TOO_BIG, "oversized")
    channel.error = error
    with pytest.raises(ChannelException) as caught:
        await face.send_raw(b"too big")
    assert caught.value is error


@pytest.mark.asyncio
async def test_send_data_capacity_timeout_is_explicit():
    face, channel, _ = make_face()
    face._channel_send_timeout = 0.02
    data = Data(name=Name(b"p" * 16, [b"document"]), content=b"payload")
    with pytest.raises(asyncio.TimeoutError, match="capacity"):
        await face.send_data(data)
    assert channel.sent == []


@pytest.mark.asyncio
async def test_interest_lifetime_includes_send_wait(monkeypatch):
    face, _, _ = make_face()
    interest = Interest(name=Name(b"p" * 16, [b"document"]), lifetime_ms=60)
    clock = [10.0]
    face._loop = SimpleNamespace(time=lambda: clock[0])
    receive_budgets = []

    async def send(raw, timeout):
        assert raw == interest.to_bytes()
        assert timeout == pytest.approx(0.06)
        clock[0] += 0.035

    async def receive(awaitable, timeout):
        awaitable.close()
        receive_budgets.append(timeout)
        if len(receive_budgets) == 1:
            clock[0] += 0.015
            return b"\x01ignored"
        raise asyncio.TimeoutError

    monkeypatch.setattr(face, "_send", send)
    monkeypatch.setattr(asyncio, "wait_for", receive)
    assert await face.express_interest(interest) is None
    assert receive_budgets == pytest.approx([0.025, 0.010])


@pytest.mark.asyncio
async def test_interest_expires_without_capacity_and_does_not_send():
    face, channel, _ = make_face()
    interest = Interest(name=Name(b"p" * 16, [b"document"]), lifetime_ms=20)
    assert await face.express_interest(interest) is None
    assert channel.sent == []


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
async def test_capacity_timeout_must_be_finite_positive(timeout):
    with pytest.raises(ValueError, match="finite and positive"):
        make_face(channel_send_timeout=timeout)


@pytest.mark.asyncio
async def test_capacity_timeout_is_configurable():
    face, channel, _ = make_face(channel_send_timeout=0.02)
    with pytest.raises(asyncio.TimeoutError):
        await face.send_raw(b"bounded")
    assert channel.sent == []
