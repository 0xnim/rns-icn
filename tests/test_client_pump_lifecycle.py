"""Client receive-task ownership across native LinkFace close and shutdown."""
import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from rns_icn.client import ICNClient
from rns_icn.config import ClientConfig
from rns_icn.forwarder import Forwarder


def client_with_forwarder():
    client = ICNClient(ClientConfig())
    client._forwarder = Forwarder()
    return client


@pytest.mark.asyncio
async def test_native_link_close_releases_finished_pump_entries():
    client = client_with_forwarder()
    try:
        for _ in range(32):
            link = MagicMock()
            face_id = client._get_or_create_face_id(link)
            task = client._pump_tasks[face_id]
            assert client._get_or_create_face_id(link) == face_id
            assert len(client._pump_tasks) == 1
            # Use the actual LinkFace callback installed on the native Link API.
            link.set_link_closed_callback.call_args.args[0](link)
            await task
            await asyncio.sleep(0)
            assert face_id not in client._pump_tasks
            assert face_id not in client.forwarder.faces
    finally:
        await client.shutdown()
        client.forwarder.cs.close()


@pytest.mark.asyncio
async def test_obsolete_pump_completion_preserves_replacement_task():
    client = client_with_forwarder()
    replacement = None
    try:
        link = MagicMock()
        face_id = client._get_or_create_face_id(link)
        previous = client._pump_tasks[face_id]
        replacement = asyncio.create_task(asyncio.Event().wait())
        client._pump_tasks[face_id] = replacement
        link.set_link_closed_callback.call_args.args[0](link)
        await previous
        await asyncio.sleep(0)
        assert client._pump_tasks[face_id] is replacement
        assert not replacement.done()
    finally:
        await client.shutdown()
        if replacement is not None:
            assert replacement.done()
        client.forwarder.cs.close()


@pytest.mark.asyncio
async def test_shutdown_waits_for_owned_pump_finalizers_before_link_pool_stop():
    client = client_with_forwarder()
    entered = asyncio.Event()
    finalized = asyncio.Event()

    async def pump(face_id, face):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            # Force cleanup to span a scheduling turn.
            await asyncio.sleep(0)
            finalized.set()

    async def stop():
        assert finalized.is_set()

    client._pump_face = pump
    client._link_pool = MagicMock()
    client._link_pool.stop = AsyncMock(side_effect=stop)
    try:
        face_id = client._get_or_create_face_id(MagicMock())
        task = client._pump_tasks[face_id]
        await entered.wait()
        await client.shutdown()
        assert task.done() and task.cancelled()
        assert client._pump_tasks == {}
        client._link_pool.stop.assert_awaited_once()
    finally:
        client.forwarder.cs.close()


@pytest.mark.asyncio
async def test_failed_pump_completion_is_removed_and_observed(monkeypatch):
    client = client_with_forwarder()
    logged = []
    monkeypatch.setattr("rns_icn.client.RNS.log", lambda message, level: logged.append(message))

    async def failed(face_id, face):
        raise RuntimeError("pump test failure")

    client._pump_face = failed
    try:
        face_id = client._get_or_create_face_id(MagicMock())
        task = client._pump_tasks[face_id]
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)
        assert face_id not in client._pump_tasks
        assert any("pump test failure" in message for message in logged)
    finally:
        await client.shutdown()
        client.forwarder.cs.close()
