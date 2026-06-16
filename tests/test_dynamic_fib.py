"""Tests for 2.3 dynamic FIB updates (withdrawal on link drop, re-install signal).

A dropped next-hop must stop being a black hole: on link close the server
withdraws the face's routes (so Interests fall through to a backup or cleanly
hit NO_ROUTE), and a later re-announce from the peer is surfaced as a reconnect
signal so the route can be re-installed. RNS supplies both events — keepalive
drives the close, announce cadence drives the recovery.
"""

from unittest.mock import MagicMock

import pytest

from rns_icn.face import TestFace
from rns_icn.fib import Fib
from rns_icn.forwarder import Forwarder
from rns_icn.name import RNS_ADDR_BYTES, Name
from rns_icn.packet import Interest
from rns_icn.peer_discovery import PeerDiscoveryManager
from rns_icn.strategy import StrategyDecision


def rns_addr(byte_val: int = 0x01) -> bytes:
    return bytes([byte_val] + [0] * (RNS_ADDR_BYTES - 1))


class TestFibRemoveAllForFace:
    def test_withdraws_face_from_all_prefixes(self):
        fib = Fib()
        a = Name(rns_addr(0x01))
        b = Name(rns_addr(0x02))
        fib.insert(a, 11, 10)
        fib.insert(a, 22, 20)
        fib.insert(b, 11, 10)

        withdrawn = fib.remove_all_for_face(11)

        assert withdrawn == 2  # face 11 was on both prefixes
        # Prefix a keeps its other face; prefix b is gone (no faces left).
        assert fib.lookup(Name(rns_addr(0x01), [b"x"])) == [(22, 20)]
        assert fib.lookup(Name(rns_addr(0x02), [b"x"])) is None

    def test_unknown_face_is_noop(self):
        fib = Fib()
        fib.insert(Name(rns_addr(0x01)), 11, 10)
        assert fib.remove_all_for_face(99) == 0
        assert fib.lookup(Name(rns_addr(0x01), [b"x"])) == [(11, 10)]


class TestForwarderWithdrawFace:
    def test_withdraw_removes_route_and_face(self):
        fw = Forwarder()
        name = Name(rns_addr(0x01))
        face = TestFace(11)
        fw.register_face(face)
        fw.add_route(name, face.id(), 10)
        assert fw.fib.lookup(Name(rns_addr(0x01), [b"x"])) == [(11, 10)]

        fw.withdraw_face(face.id())

        assert fw.fib.lookup(Name(rns_addr(0x01), [b"x"])) is None
        assert face.id() not in fw.faces

    @pytest.mark.asyncio
    async def test_express_no_route_after_withdrawal(self):
        fw = Forwarder()
        name = Name(rns_addr(0x01), [b"file"])
        face = TestFace(11)
        fw.register_face(face)
        fw.add_route(Name(rns_addr(0x01)), face.id(), 10)
        fw.withdraw_face(face.id())

        # No route left → strategy returns NO_ROUTE → express yields None.
        decision, target = await fw.strategy.decide(
            Interest(name=name), fw.fib.lookup(name) or [], None, None
        )
        assert decision == StrategyDecision.NO_ROUTE
        assert target is None
        assert await fw.express(Interest(name=name, lifetime_ms=50), 0) is None

    def test_backup_survives_primary_withdrawal(self):
        # Withdrawing the dropped face leaves a backup face routable.
        fw = Forwarder()
        name = Name(rns_addr(0x01))
        primary, backup = TestFace(11), TestFace(22)
        fw.register_face(primary)
        fw.register_face(backup)
        fw.add_route(name, primary.id(), 10)
        fw.add_route(name, backup.id(), 20)

        fw.withdraw_face(primary.id())

        assert fw.fib.lookup(Name(rns_addr(0x01), [b"x"])) == [(22, 20)]


class TestDiscoveryReconnectSignal:
    def test_clear_face_keeps_peer(self):
        pdm = PeerDiscoveryManager(server=None)
        pdm._on_announce(b"\xaa" * 16, MagicMock(), b"icn")
        pdm.update_peer_face("aa" * 16, 42)
        assert pdm.get_peer("aa" * 16).face_id == 42

        pdm.clear_face(42)

        info = pdm.get_peer("aa" * 16)
        assert info is not None          # peer entry retained for reconnect
        assert info.face_id is None

    def test_reannounce_while_disconnected_fires_callback(self):
        pdm = PeerDiscoveryManager(server=None)
        seen: list[str] = []
        pdm.add_callback(lambda h, info: seen.append(h))

        # First announce (new peer) fires once.
        pdm._on_announce(b"\xbb" * 16, MagicMock(), b"icn")
        assert seen == ["bb" * 16]

        # Linked up → re-announce stays quiet (no churn while connected).
        pdm.update_peer_face("bb" * 16, 7)
        pdm._on_announce(b"\xbb" * 16, MagicMock(), b"icn")
        assert seen == ["bb" * 16]

        # Link dropped → face cleared → next re-announce is the reconnect signal.
        pdm.clear_face(7)
        pdm._on_announce(b"\xbb" * 16, MagicMock(), b"icn")
        assert seen == ["bb" * 16, "bb" * 16]
