"""Metrics collection for ICN observability."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import Lock


@dataclass
class MetricsCollector:
    """Thread-safe metrics collection for ICN."""

    # Fetch latency (seconds)
    fetch_latencies: list = field(default_factory=list)
    _fetch_lock: Lock = field(default_factory=Lock)

    # Link uptime tracking
    link_up_times: dict[str, float] = field(default_factory=dict)  # peer_hash -> start_time
    link_total_uptime: dict[str, float] = field(default_factory=dict)  # peer_hash -> total seconds
    _link_lock: Lock = field(default_factory=Lock)

    # Operation counters
    fetch_total: int = 0
    fetch_errors: int = 0
    # Packets dropped because they failed to parse (malformed / truncated /
    # unknown type). A nonzero value means a peer is sending garbage or a
    # parser bug exists — surfaced rather than silently swallowed.
    malformed_packets: int = 0
    # PIT state (sampled by the forwarder's aging loop): current entry count
    # (gauge) and cumulative entries dropped under capacity pressure (counter).
    # A climbing pit_evictions means the PIT is undersized for the load.
    pit_size: int = 0
    pit_evictions: int = 0
    _counter_lock: Lock = field(default_factory=Lock)

    def record_fetch(self, latency: float, success: bool = True) -> None:
        """Record a fetch operation latency."""
        with self._fetch_lock:
            self.fetch_latencies.append(latency)
            if len(self.fetch_latencies) > 10000:
                self.fetch_latencies = self.fetch_latencies[-5000:]
        with self._counter_lock:
            self.fetch_total += 1
            if not success:
                self.fetch_errors += 1

    def record_malformed_packet(self) -> None:
        """Record a packet that could not be parsed (dropped)."""
        with self._counter_lock:
            self.malformed_packets += 1

    def record_pit(self, size: int, evictions: int) -> None:
        """Sample PIT state — current size (gauge) and cumulative evictions."""
        with self._counter_lock:
            self.pit_size = size
            self.pit_evictions = evictions

    def record_link_up(self, peer_hash: str) -> None:
        """Record link establishment."""
        now = time.time()
        with self._link_lock:
            self.link_up_times[peer_hash] = now

    def record_link_down(self, peer_hash: str) -> None:
        """Record link teardown and update uptime."""
        now = time.time()
        with self._link_lock:
            if peer_hash in self.link_up_times:
                uptime = now - self.link_up_times[peer_hash]
                self.link_total_uptime[peer_hash] = (
                    self.link_total_uptime.get(peer_hash, 0) + uptime
                )
                del self.link_up_times[peer_hash]

    def get_fetch_stats(self) -> dict[str, float]:
        """Get fetch latency statistics."""
        with self._fetch_lock:
            if not self.fetch_latencies:
                return {}
            sorted_lat = sorted(self.fetch_latencies)
            n = len(sorted_lat)
            return {
                "count": n,
                "mean": sum(sorted_lat) / n,
                "p50": sorted_lat[n // 2],
                "p95": sorted_lat[int(n * 0.95)],
                "p99": sorted_lat[int(n * 0.99)],
            }

    def get_link_uptime(self, peer_hash: str) -> float | None:
        """Get current link uptime if link is up, else total uptime."""
        now = time.time()
        with self._link_lock:
            if peer_hash in self.link_up_times:
                return now - self.link_up_times[peer_hash]
            return self.link_total_uptime.get(peer_hash)

    def get_link_stats(self) -> dict[str, dict[str, float]]:
        """Get uptime stats for all known links."""
        now = time.time()
        with self._link_lock:
            stats = {}
            for peer_hash, start in self.link_up_times.items():
                stats[peer_hash] = {
                    "current_uptime": now - start,
                    "total_uptime": self.link_total_uptime.get(peer_hash, 0),
                    "is_up": True,
                }
            for peer_hash, total in self.link_total_uptime.items():
                if peer_hash not in stats:
                    stats[peer_hash] = {
                        "current_uptime": 0,
                        "total_uptime": total,
                        "is_up": False,
                    }
            return stats

    def get_counters(self) -> dict[str, int]:
        """Get operation counters."""
        with self._counter_lock:
            return {
                "fetch_total": self.fetch_total,
                "fetch_errors": self.fetch_errors,
                "malformed_packets": self.malformed_packets,
                "pit_size": self.pit_size,
                "pit_evictions": self.pit_evictions,
            }

    def reset(self) -> None:
        """Reset all metrics."""
        with self._fetch_lock:
            self.fetch_latencies.clear()
        with self._link_lock:
            self.link_up_times.clear()
            self.link_total_uptime.clear()
        with self._counter_lock:
            self.fetch_total = 0
            self.fetch_errors = 0
            self.malformed_packets = 0
            self.pit_size = 0
            self.pit_evictions = 0


# Global metrics instance
metrics = MetricsCollector()