"""In-process load tests for the forwarder under concurrency (Phase 4.4).

The roadmap targets "10K+ concurrent fetches"; the real-RNS integration/chaos
suites prove correctness over the live stack but are far too slow to drive at
that scale (each fetch is a TCP-link round-trip between OS processes). These
tests instead exercise the *forwarding core* — ``Forwarder`` + ``Pit`` + ``CS``
+ strategy — entirely in-process against a mock upstream face, so thousands of
concurrent Interests run in a fraction of a second. That isolates exactly the
machinery the "PIT state explosion" risk is about (§Risks) and lets a regression
in PIT bounding, aggregation, or cleanup surface as a load test failure rather
than an OOM in production.

Scale is set by ``ICN_LOAD_N`` (default 1000, kept brisk for CI). To run the
full roadmap target::

    ICN_LOAD_N=10000 python -m pytest tests/test_load.py -v -s

``-s`` surfaces the printed throughput line.
"""

import asyncio
import os
import time

import pytest

from rns_icn.face import Face, FaceCapabilities, FaceId
from rns_icn.forwarder import Forwarder
from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import Data, Interest, parse_packet

LOAD_N = int(os.environ.get("ICN_LOAD_N", "1000"))


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


def content_for(name: Name) -> bytes:
    """Deterministic upstream content for a name — recomputable by the test."""
    return b"load-" + name.components[-1]


class _LoadFace(Face):
    """Mock upstream that answers (or stalls on) forwarded Interests.

    ``answer=True``  → schedules ``forwarder.receive_data`` with content derived
                       from the Interest name, after ``delay`` seconds.
    ``answer=False`` → stays silent, so the Interest sits in the PIT until its
                       lifetime expires (used to hold many entries in flight at
                       once for the bounded-PIT flood test).

    ``sends`` counts raw Interest sends, so a test can assert PIT aggregation
    collapsed N identical Interests into a single upstream send.
    """

    __test__ = False

    def __init__(self, face_id: FaceId, fw: Forwarder,
                 answer: bool = True, delay: float = 0.0):
        self._id = face_id
        self._fw = fw
        self._answer = answer
        self._delay = delay
        self.sends = 0

    async def express_interest(self, interest: Interest) -> Data | None:
        return None

    async def send_data(self, data: Data) -> None:
        pass

    async def send_raw(self, raw: bytes) -> None:
        self.sends += 1
        if not self._answer:
            return
        interest = parse_packet(raw).interest
        assert interest is not None
        data = Data.new(name=interest.name, content=content_for(interest.name))

        async def _answer() -> None:
            if self._delay:
                await asyncio.sleep(self._delay)
            await self._fw.receive_data(data, self._id)

        asyncio.ensure_future(_answer())

    def capabilities(self) -> FaceCapabilities:
        return FaceCapabilities(is_local=True)

    def id(self) -> FaceId:
        return self._id


class TestForwarderLoad:
    @pytest.mark.asyncio
    async def test_high_concurrency_distinct_all_succeed(self):
        """N distinct concurrent fetches all resolve, and the PIT drains to empty.

        Proves the forward/return path has no per-Interest leak under load: after
        all N complete, the PIT holds no residual entries.
        """
        fw = Forwarder(cs_max=LOAD_N + 16, pit_max=LOAD_N + 16)
        addr = rns_addr()
        upstream = _LoadFace(1, fw, answer=True)
        fw.register_face(upstream)
        fw.add_route(Name(addr), upstream.id(), cost=10)

        names = [Name(addr, [f"item{i}".encode()]) for i in range(LOAD_N)]

        async def fetch(name: Name) -> Data | None:
            return await fw.express(Interest(name=name, lifetime_ms=10000), 0)

        start = time.perf_counter()
        results = await asyncio.gather(*(fetch(n) for n in names))
        elapsed = time.perf_counter() - start

        assert all(r is not None for r in results), "some concurrent fetches failed"
        for name, data in zip(names, results, strict=True):
            assert data is not None and data.name == name
            assert data.content == content_for(name)

        # Each distinct name is a distinct upstream send (no spurious dedup).
        assert upstream.sends == LOAD_N
        # No PIT leak: every entry was satisfied and cleaned up.
        assert len(fw.pit) == 0, f"PIT leaked {len(fw.pit)} entries after load"

        rate = LOAD_N / elapsed if elapsed else float("inf")
        print(f"\n[load] {LOAD_N} distinct concurrent fetches in "
              f"{elapsed * 1000:.0f}ms ({rate:,.0f} fetch/s)")

    @pytest.mark.asyncio
    async def test_duplicate_load_collapses_to_one_upstream_send(self):
        """A flood of identical Interests aggregates to a single upstream send.

        This is the PIT's first line of defence against state explosion: M
        consumers asking for the same name in flight cost one PIT entry and one
        upstream Interest, and all M get the Data.
        """
        fw = Forwarder(pit_max=LOAD_N + 16)
        addr = rns_addr(0x02)
        # Slow upstream so the first Interest is still pending while the rest pile
        # on and aggregate behind its PIT entry.
        upstream = _LoadFace(1, fw, answer=True, delay=0.3)
        fw.register_face(upstream)
        name = Name(addr, [b"hot"])
        fw.add_route(Name(addr), upstream.id(), cost=10)

        async def fetch() -> Data | None:
            # Distinct in_face per waiter so each is its own consumer, not a loop.
            return await fw.express(Interest(name=name, lifetime_ms=5000), 0)

        # Kick off the first Interest, give it a tick to register its PIT entry
        # and send upstream, then pile the rest on behind it.
        first = asyncio.ensure_future(fetch())
        await asyncio.sleep(0.05)
        rest = await asyncio.gather(*(fetch() for _ in range(LOAD_N - 1)))
        first_result = await first

        results = [first_result, *rest]
        assert all(r is not None for r in results), "aggregated waiters were not all satisfied"
        assert all(r.content == content_for(name) for r in results)
        assert upstream.sends == 1, (
            f"expected one upstream send for {LOAD_N} identical Interests, "
            f"got {upstream.sends}"
        )
        assert len(fw.pit) == 0, "aggregated PIT entry was not cleaned up"

    @pytest.mark.asyncio
    async def test_pit_stays_bounded_under_flood(self):
        """A flood of distinct in-flight Interests never exceeds ``pit_max``.

        Directly exercises the "PIT state explosion" mitigation: with a silent
        upstream, far more distinct Interests than the cap are in flight at once,
        yet the PIT caps itself by evicting nearest-expiry entries.
        """
        pit_max = 128
        flood = pit_max * 6
        fw = Forwarder(pit_max=pit_max)
        addr = rns_addr(0x03)
        # Silent upstream: Interests stay pending (until lifetime) so they all
        # contend for PIT slots simultaneously.
        upstream = _LoadFace(1, fw, answer=False)
        fw.register_face(upstream)
        fw.add_route(Name(addr), upstream.id(), cost=10)

        async def fetch(i: int) -> Data | None:
            name = Name(addr, [f"flood{i}".encode()])
            return await fw.express(Interest(name=name, lifetime_ms=400), 0)

        tasks = [asyncio.ensure_future(fetch(i)) for i in range(flood)]
        # Sample while the flood is in flight: the PIT must never exceed its cap.
        await asyncio.sleep(0.1)
        peak = len(fw.pit)
        assert peak <= pit_max, f"PIT grew to {peak}, exceeding cap {pit_max}"
        assert fw.pit.is_full(), "PIT should be saturated under a flood this size"
        assert fw.pit.evictions >= flood - pit_max, "nearest-expiry eviction did not fire under load"

        # Drain: every Interest resolves (to None — upstream never answered) with
        # no exception, and the PIT fully reclaims once lifetimes expire.
        results = await asyncio.gather(*tasks)
        assert all(r is None for r in results)
        fw.pit.purge_expired()
        assert len(fw.pit) == 0, f"PIT did not reclaim after flood ({len(fw.pit)} left)"
