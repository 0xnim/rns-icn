"""A configured SWR window never grants stale service beyond its boundary."""
import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from rns_icn.face import TestFace
from rns_icn.forwarder import Forwarder
from rns_icn.name import Name
from rns_icn.packet import Data, Interest
from rns_icn.strategy import BestRoute, StrategyDecision


@pytest.mark.asyncio
@pytest.mark.parametrize("age,expected", [(69, StrategyDecision.SERVE_STALE_REVALIDATE),
    (70, StrategyDecision.FORWARD_TO), (71, StrategyDecision.FORWARD_TO)])
async def test_positive_window_boundary(age, expected):
    name = Name(b'p' * 16, [b'age'])
    data = Data.new(name, b'old').with_freshness_period(60).with_staleness(age)
    decision, face = await BestRoute(stale_while_revalidate=10).decide(
        Interest(name), [(7, 1)], None, data)
    assert decision == expected and face == 7


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["none", "backoff"])
async def test_outside_window_cannot_fall_back_to_stale_without_route(route):
    name = Name(b'p' * 16, [b'age'])
    data = Data.new(name, b'old').with_freshness_period(60).with_staleness(70)
    strategy = BestRoute(stale_while_revalidate=10)
    faces = []
    if route == "backoff":
        faces = [(7, 1)]
        strategy.record_failure(7)
    decision, face = await strategy.decide(Interest(name), faces, None, data)
    assert decision == StrategyDecision.NO_ROUTE and face is None


@pytest.mark.asyncio
async def test_outside_window_aggregates_pending_request():
    name = Name(b'p' * 16, [b'age'])
    data = Data.new(name, b'old').with_freshness_period(60).with_staleness(70)
    decision, face = await BestRoute(stale_while_revalidate=10).decide(
        Interest(name), [(7, 1)], SimpleNamespace(satisfied=False), data)
    assert decision == StrategyDecision.SUPPRESS_AGGREGATE and face is None


@pytest.mark.asyncio
async def test_stale_without_declared_period_has_no_configured_window():
    name = Name(b'p' * 16, [b'age'])
    data = Data.new(name, b'old').with_staleness(1)
    decision, face = await BestRoute(stale_while_revalidate=10).decide(
        Interest(name), [(7, 1)], None, data)
    assert decision == StrategyDecision.FORWARD_TO and face == 7


@pytest.mark.asyncio
@pytest.mark.parametrize("upstream_fresh", [True, False])
async def test_real_forwarder_waits_for_upstream_before_serving_outside_window(upstream_fresh):
    fwd = Forwarder(strategy=BestRoute(stale_while_revalidate=10))
    name = Name(b'p' * 16, [b'page'])
    with patch('rns_icn.content_store.time.time', return_value=1000):
        fwd.cs.insert(name, Data.new(name, b'old').with_freshness_period(60))
    upstream = TestFace(7)
    fwd.register_face(upstream)
    fwd.add_route(name, 7)
    try:
        with patch('rns_icn.content_store.time.time', return_value=1070):
            pending = asyncio.create_task(fwd.express(Interest(name, lifetime_ms=1000), 99))
            raw = await asyncio.wait_for(upstream._outgoing.get(), 1)
            forwarded = Interest.from_bytes(raw)
            assert forwarded.name == name
            assert not forwarded.must_be_fresh  # preserve caller semantics
            assert not pending.done()  # no immediate stale result
            reply = Data.new(name, b'new').with_freshness_period(60)
            if not upstream_fresh:
                reply.with_staleness(100)
            await fwd.receive_data(reply, 7)
            result = await asyncio.wait_for(pending, 1)
            assert result.content == b'new'
            assert result.metadata.freshness.fresh is upstream_fresh
            assert fwd.cs.get(name).content == b'new'
    finally:
        fwd.cs.close()
