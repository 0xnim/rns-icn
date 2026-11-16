"""Event-controlled shared establishment and owner shutdown regressions."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import RNS

from rns_icn.link_pool import LinkPool

PEER = b'p' * 16


def pool():
    return LinkPool(MagicMock(), 'icn', 'default', [], loop=MagicMock())


def link():
    return MagicMock(status=RNS.Link.ACTIVE)


@pytest.mark.asyncio
async def test_concurrent_waiters_share_one_establishment_before_start():
    owner = pool()
    entered, release = asyncio.Event(), asyncio.Event()
    result = link()

    async def create(peer):
        entered.set()
        await release.wait()
        return result

    with patch.object(owner, '_create_link', side_effect=create) as create_mock:
        waits = [asyncio.create_task(owner.get_link(PEER)) for _ in range(32)]
        await entered.wait()
        release.set()
        assert all(value is result for value in await asyncio.gather(*waits))
        create_mock.assert_awaited_once_with(PEER)
        assert await owner.get_link(PEER) is result
    await owner.stop()
    result.teardown.assert_called_once()


@pytest.mark.asyncio
async def test_cancel_one_waiter_preserves_shared_establishment():
    owner = pool()
    entered, release = asyncio.Event(), asyncio.Event()
    result = link()

    async def create(peer):
        entered.set()
        await release.wait()
        return result

    with patch.object(owner, '_create_link', side_effect=create) as create_mock:
        first = asyncio.create_task(owner.get_link(PEER))
        await entered.wait()
        second = asyncio.create_task(owner.get_link(PEER))
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        assert await second is result
        create_mock.assert_awaited_once()
    await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize('outcome', [None, OSError('failed')])
async def test_failed_establishment_is_shared_then_retryable(outcome):
    owner = pool()
    entered, release = asyncio.Event(), asyncio.Event()

    async def create(peer):
        entered.set()
        await release.wait()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    with patch.object(owner, '_create_link', side_effect=create) as create_mock:
        waits = [asyncio.create_task(owner.get_link(PEER)) for _ in range(4)]
        await entered.wait()
        release.set()
        assert all(value is outcome for value in await asyncio.gather(*waits, return_exceptions=True))
        create_mock.assert_awaited_once()
    result = link()
    with patch.object(owner, '_create_link', return_value=result):
        assert await owner.get_link(PEER) is result
    await owner.stop()


def test_delayed_invalidation_cannot_remove_replacement_or_teardown():
    owner = pool()
    old, new = link(), link()
    owner._links[PEER] = old
    owner._health[PEER] = 1
    assert owner.invalidate(PEER, old)
    assert PEER not in owner._health
    owner._links[PEER] = new
    owner._health[PEER] = 2
    assert not owner.invalidate(PEER, old)
    assert owner._links[PEER] is new and owner._health[PEER] == 2
    old.teardown.assert_not_called()
    new.teardown.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("via_pool_stop", [False, True])
async def test_native_handshake_cancellation_tears_down_link(via_pool_stop):
    owner = pool()
    created = asyncio.Event()
    native = MagicMock(status=RNS.Link.PENDING)

    def make_link(destination):
        created.set()
        return native

    # Patch the constructor, retaining native constants used by the loop.
    constructor = MagicMock(side_effect=make_link)
    constructor.ACTIVE, constructor.CLOSED = RNS.Link.ACTIVE, RNS.Link.CLOSED
    with patch.object(owner, '_resolve_identity', return_value=MagicMock()), \
         patch.object(owner, '_ensure_path', new=AsyncMock(return_value=True)), \
         patch('rns_icn.link_pool.RNS.Destination'), \
         patch('rns_icn.link_pool.RNS.Link', constructor):
        operation = owner.get_link(PEER) if via_pool_stop else owner._create_link(PEER)
        task = asyncio.create_task(operation)
        await created.wait()
        if via_pool_stop:
            await owner.stop()
        else:
            task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    native.teardown.assert_called_once()


@pytest.mark.asyncio
async def test_stop_cancels_and_awaits_pending_cleanup_then_explicit_restart():
    owner = pool()
    entered, cleaning, cleaned = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def create(peer):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cleaning.set()
            await cleaned.wait()

    with patch.object(owner, '_create_link', side_effect=create):
        waiter = asyncio.create_task(owner.get_link(PEER))
        await entered.wait()
        stopping = asyncio.create_task(owner.stop())
        await cleaning.wait()
        assert not stopping.done()
        with pytest.raises(RuntimeError, match='stopped'):
            await owner.get_link(PEER)
        # Cancellation of this stop waiter cannot abandon pool cleanup.
        stopping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopping
        cleaned.set()
        await owner.stop()
        with pytest.raises(asyncio.CancelledError):
            await waiter
    assert not owner._pending
    with pytest.raises(RuntimeError, match='stopped'):
        await owner.get_link(PEER)
    await owner.start()
    new = link()
    with patch.object(owner, '_create_link', return_value=new):
        assert await owner.get_link(PEER) is new
    await owner.stop()
    new.teardown.assert_called_once()


@pytest.mark.asyncio
async def test_stop_rejects_late_success_after_establishment_suppresses_cancel():
    owner = pool()
    entered = asyncio.Event()
    late = link()

    async def create(peer):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            return late

    with patch.object(owner, '_create_link', side_effect=create):
        waiter = asyncio.create_task(owner.get_link(PEER))
        await entered.wait()
        await owner.stop()
        with pytest.raises(RuntimeError, match="stopped"):
            await waiter
    late.teardown.assert_called_once()
    assert not owner._links and not owner._pending


@pytest.mark.asyncio
async def test_cancel_all_waiters_observes_background_failure():
    owner = pool()
    entered, release, done = asyncio.Event(), asyncio.Event(), asyncio.Event()
    errors = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda loop, context: errors.append(context))

    async def create(peer):
        entered.set()
        await release.wait()
        raise OSError('late failure')

    try:
        with patch.object(owner, '_create_link', side_effect=create):
            waiter = asyncio.create_task(owner.get_link(PEER))
            await entered.wait()
            task = owner._pending[PEER]
            task.add_done_callback(lambda task: done.set())
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            release.set()
            await done.wait()
            del task
        await owner.stop()
        assert not errors
    finally:
        loop.set_exception_handler(previous)


@pytest.mark.asyncio
async def test_distinct_peers_establish_independently():
    owner = pool()
    first_entered, release = asyncio.Event(), asyncio.Event()
    first_link, second_link = link(), link()
    other = b'q' * 16

    async def create(peer):
        if peer == PEER:
            first_entered.set()
            await release.wait()
            return first_link
        return second_link

    with patch.object(owner, '_create_link', side_effect=create):
        first = asyncio.create_task(owner.get_link(PEER))
        await first_entered.wait()
        assert await asyncio.wait_for(owner.get_link(other), 1) is second_link
        assert not first.done()
        release.set()
        assert await first is first_link
    await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["invalidate", "close", "stop"])
async def test_completed_acquisition_cannot_return_link_retired_before_resume(action):
    owner = pool()
    completed, resume = asyncio.Event(), asyncio.Event()
    result = link()
    real_wait = asyncio.wait

    async def delayed_wait(tasks):
        outcome = await real_wait(tasks)
        completed.set()
        await resume.wait()
        return outcome

    with patch.object(owner, '_create_link', return_value=result), \
         patch('rns_icn.link_pool.asyncio.wait', side_effect=delayed_wait):
        waiter = asyncio.create_task(owner.get_link(PEER))
        await completed.wait()
        if action == "invalidate":
            assert owner.invalidate(PEER, result)
        elif action == "close":
            result.status = RNS.Link.CLOSED
        else:
            await owner.stop()
        resume.set()
        if action == "stop":
            with pytest.raises(RuntimeError, match="stopped"):
                await waiter
        else:
            assert await waiter is None
    await owner.stop()
