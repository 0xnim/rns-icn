"""Channel size selection and Resource completion/cancellation guarantees."""

import asyncio
from types import SimpleNamespace

import pytest
import RNS

from rns_icn.face import LinkFace
from rns_icn.name import Name
from rns_icn.packet import Data
from rns_icn.resource_transport import ResourcePublisher, ResourceTransportError


class FakeChannel:
    def __init__(self, mdu):
        self.mdu = mdu
        self.sent = []

    def register_message_type(self, _message_type):
        pass

    def add_message_handler(self, _handler):
        pass

    def send(self, message):
        self.sent.append(message.pack())


def link_face(mdu):
    channel = FakeChannel(mdu)
    link = SimpleNamespace(
        # A deliberately different Link MDU detects accidental use of it.
        mdu=mdu + 1000,
        get_channel=lambda: channel,
        set_link_closed_callback=lambda _callback: None,
    )
    return LinkFace(1, link), channel


@pytest.mark.asyncio
@pytest.mark.parametrize("over_limit", [False, True])
async def test_linkface_selects_transport_by_serialized_channel_size(monkeypatch, over_limit):
    data = Data(name=Name(b"p" * 16, [b"document"]), content=b"payload")
    raw = data.to_bytes()
    face, channel = link_face(len(raw) - int(over_limit))
    transferred = []

    async def publish(_self, payload, timeout=60.0):
        transferred.append(payload)
        return True

    monkeypatch.setattr(ResourcePublisher, "publish_raw", publish)
    await face.send_data(data)
    assert channel.sent == ([] if over_limit else [raw])
    assert transferred == ([raw] if over_limit else [])


@pytest.mark.asyncio
async def test_linkface_reports_failed_resource(monkeypatch):
    face, channel = link_face(1)

    async def publish(_self, _payload, timeout=60.0):
        return False

    monkeypatch.setattr(ResourcePublisher, "publish_raw", publish)
    data = Data(name=Name(b"p" * 16, [b"document"]), content=b"payload")
    with pytest.raises(ResourceTransportError, match="did not complete"):
        await face.send_data(data)
    assert channel.sent == []


def fake_resource_type(monkeypatch, status=None, duplicate=False):
    real_resource = RNS.Resource
    created = []

    class FakeResource:
        COMPLETE = real_resource.COMPLETE
        FAILED = real_resource.FAILED

        def __init__(self, payload, link, callback):
            self.payload = payload
            self.link = link
            self.callback = callback
            self.cancelled = False
            self.status = status
            created.append(self)
            if status is not None:
                callback(self)
                if duplicate:
                    callback(self)

        def cancel(self):
            self.cancelled = True
            self.status = self.FAILED
            self.callback(self)

    monkeypatch.setattr(RNS, "Resource", FakeResource)
    return created


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [RNS.Resource.COMPLETE, RNS.Resource.FAILED, RNS.Resource.CORRUPT])
async def test_resource_reports_only_complete_success(monkeypatch, status):
    expected = status == RNS.Resource.COMPLETE
    fake_resource_type(monkeypatch, status=status, duplicate=True)
    assert await ResourcePublisher(object()).publish_raw(b"packet") is expected
    # Duplicate callbacks must not trigger InvalidStateError on the loop.
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_resource_timeout_cancels_and_late_callback_is_safe(monkeypatch):
    created = fake_resource_type(monkeypatch)
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    errors = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    try:
        assert await ResourcePublisher(object()).publish_raw(b"packet", timeout=0.001) is False
        resource = created[0]
        assert resource.cancelled
        resource.status = resource.COMPLETE
        resource.callback(resource)
        resource.callback(resource)
        await asyncio.sleep(0)
        assert errors == []
    finally:
        loop.set_exception_handler(previous)


@pytest.mark.asyncio
async def test_resource_caller_cancellation_cancels_transfer(monkeypatch):
    created = fake_resource_type(monkeypatch)
    task = asyncio.create_task(ResourcePublisher(object()).publish_raw(b"packet"))
    await asyncio.sleep(0)
    assert created
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert created[0].cancelled
    await asyncio.sleep(0)
