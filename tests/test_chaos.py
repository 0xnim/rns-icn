"""Real-RNS chaos tests for the forwarding-hardening primitives.

The bounded/aged PIT, Interest-NACK failover, multi-path primary/backup
failover, and dynamic-FIB withdraw/re-install all have in-process unit tests
(``test_pit``, ``test_nack``, ``test_multipath``, ``test_dynamic_fib``) that
drive ``TestFace`` and mocks. Those never exercise the *real* close hook firing
on a torn-down ``RNS.Link``, nor the announce-driven re-install over the live
stack. These tests close that gap by spawning real ICN nodes as separate
Reticulum instances over localhost TCP and injecting faults:

- ``TestReannounceRecovery`` — a router's upstream link is dropped (the same
  event keepalive raises on a dead peer, injected deterministically); the route
  is withdrawn so it stops black-holing, then re-installed off the origin's real
  announce cadence, and a fresh Interest reaches the origin again.
- ``TestMultipathFailover`` — a client routes the origin via a primary and a
  backup router; the primary is *killed* mid-test and the forwarder falls
  through to the backup so the fetch still completes.

Distinct content labels are used throughout: an as-yet-unfetched name is
uncached at every hop, so a successful fetch proves the Interest traversed a
working path to the origin rather than being answered by a cache.

Gated behind ``RNS_INTEGRATION=1`` (spins up Reticulum, slow)::

    RNS_INTEGRATION=1 python -m pytest tests/test_chaos.py -v
"""

import itertools
import os
import shutil
import sys
import tempfile
import time

import pytest
from _chaos_harness import (
    CLIENT_SCRIPT,
    ORIGIN_SCRIPT,
    ROUTER_SCRIPT,
    Node,
    expected_content,
    free_port,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("RNS_INTEGRATION"),
    reason="Set RNS_INTEGRATION=1 to run chaos tests",
)

PY = sys.executable


class _ChaosBase:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.tmproot = tempfile.mkdtemp(prefix="rns_icn_chaos_")
        self.nodes: list[Node] = []
        # Monotonic label generator so every fetch targets a fresh, uncached name.
        self._labels = (f"item{i}" for i in itertools.count())
        yield
        for node in self.nodes:
            node.stop()
        shutil.rmtree(self.tmproot, ignore_errors=True)

    def _cfg(self, name: str) -> str:
        return os.path.join(self.tmproot, name)

    def _spawn(self, argv: list[str]) -> Node:
        node = Node(argv)
        self.nodes.append(node)
        return node

    def next_label(self) -> str:
        return next(self._labels)

    def start_origin(self, configdir: str, port: int) -> dict:
        node = self._spawn([PY, ORIGIN_SCRIPT, configdir, str(port)])
        info = node.read_until("ORIGIN_READY ", timeout=60)
        info["node"] = node
        return info


class TestReannounceRecovery(_ChaosBase):
    """A dropped upstream stops black-holing, then recovers on re-announce."""

    def test_withdraw_then_reinstall(self):
        origin_port = free_port()
        router_port = free_port()

        origin = self.start_origin(self._cfg("origin"), origin_port)
        router = self._spawn([
            PY, ROUTER_SCRIPT, self._cfg("router"), str(router_port),
            str(origin_port), origin["hexhash"], origin["identity_path"],
        ])
        router_info = router.read_until("ROUTER_READY ", timeout=120)

        client = self._spawn([
            PY, CLIENT_SCRIPT, self._cfg("client"), origin["identity_path"],
            f'[{{"port": {router_port}, "hexhash": "{router_info["hexhash"]}", '
            f'"identity_path": "{router_info["identity_path"]}", "cost": 10}}]',
        ])
        ready = client.read_until("CLIENT_READY ", timeout=120)
        assert router_info["hexhash"] in ready["connected"], "client never linked to router"

        # Baseline: fetch through the (healthy) two-hop path.
        label = self.next_label()
        res = client.fetch(label)
        assert res["ok"], "baseline fetch failed before any fault"
        assert bytes.fromhex(res["content_hex"]) == expected_content(label)

        # Inject the upstream link drop and confirm the route is withdrawn.
        router.send("DROP_UPSTREAM")
        router.read_until("DROPPED ", timeout=30)
        deadline = time.time() + 30
        while time.time() < deadline:
            if router_route_count(router) == 0:
                break
            time.sleep(1)
        assert router_route_count(router) == 0, "route was not withdrawn on link drop"
        # (The "no black-hole / fast NO_ROUTE" property is covered deterministically
        # by the in-process unit tests; here recovery fires automatically off the
        # origin's announce cadence, so a "must fail while down" fetch would race it.)

        # Recovery is driven only by the origin's real announce cadence (~5s).
        deadline = time.time() + 90
        while time.time() < deadline:
            if router_route_count(router) >= 1:
                break
            time.sleep(2)
        assert router_route_count(router) >= 1, "route was never re-installed on re-announce"

        # A fresh (uncached) name now reaches the origin again through the
        # re-installed route — proving recovery, not a stale cache hit.
        recovered = False
        deadline = time.time() + 60
        while time.time() < deadline:
            label = self.next_label()
            res = client.fetch(label, lifetime_ms=12000)
            if res["ok"]:
                assert bytes.fromhex(res["content_hex"]) == expected_content(label)
                recovered = True
                break
            time.sleep(2)
        assert recovered, "fetch never succeeded after route re-install"


class TestMultipathFailover(_ChaosBase):
    """Killing the primary router fails the fetch over to the backup."""

    def test_primary_crash_falls_through_to_backup(self):
        origin_port = free_port()
        router_a_port = free_port()
        router_b_port = free_port()

        origin = self.start_origin(self._cfg("origin"), origin_port)

        router_a = self._spawn([
            PY, ROUTER_SCRIPT, self._cfg("router_a"), str(router_a_port),
            str(origin_port), origin["hexhash"], origin["identity_path"],
        ])
        info_a = router_a.read_until("ROUTER_READY ", timeout=120)
        router_b = self._spawn([
            PY, ROUTER_SCRIPT, self._cfg("router_b"), str(router_b_port),
            str(origin_port), origin["hexhash"], origin["identity_path"],
        ])
        info_b = router_b.read_until("ROUTER_READY ", timeout=120)

        # Client routes the origin via A (primary, cost 10) and B (backup, 20).
        routes = (
            f'[{{"port": {router_a_port}, "hexhash": "{info_a["hexhash"]}", '
            f'"identity_path": "{info_a["identity_path"]}", "cost": 10}}, '
            f'{{"port": {router_b_port}, "hexhash": "{info_b["hexhash"]}", '
            f'"identity_path": "{info_b["identity_path"]}", "cost": 20}}]'
        )
        client = self._spawn([
            PY, CLIENT_SCRIPT, self._cfg("client"), origin["identity_path"], routes,
        ])
        ready = client.read_until("CLIENT_READY ", timeout=120)
        assert info_a["hexhash"] in ready["connected"], "client never linked to primary"
        assert info_b["hexhash"] in ready["connected"], "client never linked to backup"

        # Baseline via the primary.
        label = self.next_label()
        res = client.fetch(label)
        assert res["ok"], "baseline fetch failed before killing the primary"
        assert bytes.fromhex(res["content_hex"]) == expected_content(label)

        # Crash the primary router (hard kill, no graceful shutdown).
        router_a.kill()

        # The forwarder times out on the dead primary face and falls through to
        # the backup, so a fresh name still resolves. Allow more than one
        # lifetime for the first fall-through.
        label = self.next_label()
        res = client.fetch(label, lifetime_ms=12000, timeout=40)
        assert res["ok"], "fetch did not fail over to the backup after primary crash"
        assert bytes.fromhex(res["content_hex"]) == expected_content(label)


class TestPartitionTolerance(_ChaosBase):
    """A *held* partition: cache stays available, uncached fails cleanly, heal recovers.

    ``TestReannounceRecovery`` drops the upstream and lets the origin's announce
    cadence auto-recover it — which is why it can't assert "uncached must fail
    while down" (recovery races the assertion). Here the router runs in ``manual``
    mode, so a ``DROP_UPSTREAM`` holds the partition open until an explicit
    ``HEAL_UPSTREAM``. That stable window lets us assert the three properties that
    matter under partition:

    1. **Cache availability** — content already cached at the router is still
       served to a *cold* (empty-CS) client while the origin is unreachable. This
       is ICN's core partition win: a cache satisfies the Interest, no origin
       round-trip. A cold second client is required because the warming client
       caches the Data itself (``forwarder.py`` reverse-path insert), so a re-fetch
       from the *same* client would never reach the router.
    2. **Clean failure** — a fresh (uncached-everywhere) name fails within its
       lifetime rather than black-holing, because the dead upstream's route was
       withdrawn.
    3. **Recovery** — after ``HEAL_UPSTREAM`` re-installs the route, a fresh name
       reaches the origin again.
    """

    def _start_cold_client(self, name: str, origin_identity_path: str,
                           router_port: int, router_hexhash: str,
                           router_identity_path: str) -> Node:
        """Spawn a fresh client (empty CS) routed at the origin via the router."""
        client = self._spawn([
            PY, CLIENT_SCRIPT, self._cfg(name), origin_identity_path,
            f'[{{"port": {router_port}, "hexhash": "{router_hexhash}", '
            f'"identity_path": "{router_identity_path}", "cost": 10}}]',
        ])
        ready = client.read_until("CLIENT_READY ", timeout=120)
        assert router_hexhash in ready["connected"], "cold client never linked to router"
        return client

    def test_cache_serves_during_partition(self):
        origin_port = free_port()
        router_port = free_port()

        origin = self.start_origin(self._cfg("origin"), origin_port)
        # "manual" mode: a DROP_UPSTREAM holds the partition open (no announce-
        # driven re-install) until we explicitly HEAL_UPSTREAM.
        router = self._spawn([
            PY, ROUTER_SCRIPT, self._cfg("router"), str(router_port),
            str(origin_port), origin["hexhash"], origin["identity_path"], "manual",
        ])
        router_info = router.read_until("ROUTER_READY ", timeout=120)

        # A warm client fetches a label through the healthy two-hop path, caching
        # it at the router (and at the warm client itself).
        warm = self._start_cold_client(
            "warm", origin["identity_path"], router_port,
            router_info["hexhash"], router_info["identity_path"],
        )
        cached_label = self.next_label()
        res = warm.fetch(cached_label)
        assert res["ok"], "baseline fetch failed before partition"
        assert bytes.fromhex(res["content_hex"]) == expected_content(cached_label)

        # Partition: drop the router's upstream and confirm it stays withdrawn
        # (manual mode → no auto re-install).
        router.send("DROP_UPSTREAM")
        router.read_until("DROPPED ", timeout=30)
        deadline = time.time() + 30
        while time.time() < deadline:
            if router_route_count(router) == 0:
                break
            time.sleep(1)
        assert router_route_count(router) == 0, "route was not withdrawn on partition"
        # Hold briefly and re-check: manual mode must NOT auto-recover.
        time.sleep(8)
        assert router_route_count(router) == 0, "partition auto-recovered in manual mode"

        # A cold client (empty CS) joins during the partition. The previously
        # cached label is served from the router's cache even though the origin is
        # unreachable — the partition-availability property.
        cold = self._start_cold_client(
            "cold", origin["identity_path"], router_port,
            router_info["hexhash"], router_info["identity_path"],
        )
        res = cold.fetch(cached_label)
        assert res["ok"], "cached content was not served during partition"
        assert bytes.fromhex(res["content_hex"]) == expected_content(cached_label)

        # A fresh (uncached) name has no path to the origin: it must fail within
        # its lifetime, not black-hole.
        fresh = self.next_label()
        res = cold.fetch(fresh, lifetime_ms=8000)
        assert not res["ok"], "uncached fetch unexpectedly succeeded during partition"

        # Heal the partition deterministically, then confirm the route is back.
        router.send("HEAL_UPSTREAM")
        router.read_until("HEALED ", timeout=60)
        deadline = time.time() + 30
        while time.time() < deadline:
            if router_route_count(router) >= 1:
                break
            time.sleep(1)
        assert router_route_count(router) >= 1, "route was not re-installed on heal"

        # A fresh name now reaches the origin again through the healed route.
        recovered = False
        deadline = time.time() + 60
        while time.time() < deadline:
            label = self.next_label()
            res = cold.fetch(label, lifetime_ms=12000)
            if res["ok"]:
                assert bytes.fromhex(res["content_hex"]) == expected_content(label)
                recovered = True
                break
            time.sleep(2)
        assert recovered, "fetch never succeeded after partition heal"


def router_route_count(router: Node) -> int:
    """Ask a chaos router for its live origin next-hop count."""
    router.send("ROUTES")
    return router.read_until("ROUTES ", timeout=15)["count"]
