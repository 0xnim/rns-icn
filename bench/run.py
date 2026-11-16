"""Benchmark: ICN through a caching hub vs fetching a page from its origin.

Topology (all real Reticulum instances, one process each)::

    reader --[edge link]-- hub --[backhaul link]-- origin

Both links run through a rate-limiting relay that counts every byte, so the
numbers are bytes actually sent over each link (Reticulum headers, link setup,
proofs and TCP-interface framing included) and wall-clock seconds at the
configured bitrate.

Each fetch is a fresh reader process, i.e. a different reader arriving with no
state. Four ways of getting the same item are measured:

    page          NomadNet-style: link to the origin's page node, request a page.
                  The hub only relays packets (Reticulum transport).
    icn cold      ICN fetch through the hub; hub has not seen the item.
    icn warm      ICN fetch through the hub; an earlier reader already fetched it.
    icn followed  ICN fetch through a hub that follows the origin, so the item
                  was pulled before any reader asked (rns_icn.follow).

Usage::

    uv run python bench/run.py                       # 1200 bit/s backhaul, fast edge
    uv run python bench/run.py --edge-bps 1200       # both links slow
    uv run python bench/run.py --readers 5 --json out.json
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import random
import socket
import sys
import tempfile
import time

from shaper import Shaper

HERE = os.path.dirname(os.path.abspath(__file__))
NODES = os.path.join(HERE, "nodes.py")

_WORDS = (  # noqa: SIM905
    "wind north south gusts knots swell rain clearing pressure falling rising steady "
    "visibility moderate good poor fog patches later tonight tomorrow morning coast "
    "inland valley ridge temperature degrees humidity front passing showers isolated "
    "thunder risk low high tide water level station report update forecast issued valid "
    "until warning advisory small craft sea state slight rough calm backing veering"
).split()


def make_text(size: int, seed: int) -> str:
    """Deterministic forecast-like text of exactly ``size`` bytes."""
    rng = random.Random(seed)
    out: list[str] = []
    length = 0
    while length < size:
        word = rng.choice(_WORDS)
        out.append(word)
        length += len(word) + 1
    return " ".join(out)[:size]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Node:
    """A node subprocess speaking ``TAG {json}`` lines on stdout."""

    def __init__(self, proc: asyncio.subprocess.Process, name: str, log):
        self.proc = proc
        self.name = name
        self._log = log  # Reticulum logs to stdout; keep them for debugging
        self._messages: asyncio.Queue[tuple[str, dict] | None] = asyncio.Queue()
        # Drain stdout continuously so a chatty node never blocks on a full pipe.
        self._drain = asyncio.ensure_future(self._read())

    @classmethod
    async def spawn(cls, workdir: str, name: str, *args: str) -> Node:
        err = open(os.path.join(workdir, f"{name}.stderr"), "wb")  # noqa: SIM115
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-u", NODES, *args,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=err,
            cwd=os.path.dirname(HERE),
        )
        return cls(proc, name, open(os.path.join(workdir, f"{name}.log"), "w"))

    async def _read(self) -> None:
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                await self._messages.put(None)
                return
            text = line.decode(errors="replace").strip()
            tag, _, rest = text.partition(" ")
            if tag.isupper() and rest.startswith("{"):
                with contextlib.suppress(json.JSONDecodeError):
                    await self._messages.put((tag, json.loads(rest)))
                    continue
            self._log.write(text + "\n")
            self._log.flush()

    async def expect(self, tag: str, timeout: float) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"{self.name}: no {tag} within {timeout}s")
            try:
                message = await asyncio.wait_for(self._messages.get(), remaining)
            except asyncio.TimeoutError:
                raise TimeoutError(f"{self.name}: no {tag} within {timeout}s") from None
            if message is None:
                raise RuntimeError(f"{self.name}: exited before {tag}")
            if message[0] == tag:
                return message[1]
            if message[0] == "ERROR":
                raise RuntimeError(f"{self.name}: {message[1]}")

    async def send(self, payload: dict) -> None:
        self.proc.stdin.write((json.dumps(payload) + "\n").encode())
        await self.proc.stdin.drain()

    async def stop(self) -> None:
        if self.proc.returncode is None:
            self.proc.terminate()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.proc.wait(), 10)
            if self.proc.returncode is None:
                self.proc.kill()


class Bench:
    def __init__(self, args: argparse.Namespace, workdir: str):
        self.args = args
        self.workdir = workdir
        self.origin_port = free_port()
        self.hub_port = free_port()
        self.backhaul = Shaper(free_port(), self.origin_port, args.backhaul_bps, "backhaul")
        self.edge = Shaper(free_port(), self.hub_port, args.edge_bps, "edge")
        self.items = {f"s{size}": make_text(size, size) for size in args.sizes}
        self.origin: Node | None = None
        self.hub: Node | None = None
        self.origin_info: dict = {}
        self.hub_info: dict = {}
        self._run = 0

    async def quiet(self, idle: float = 3.0, limit: float = 120.0) -> None:
        """Wait until neither link has carried anything for ``idle`` seconds."""
        deadline = time.monotonic() + limit
        while time.monotonic() < deadline:
            last = max(self.backhaul.last_activity, self.edge.last_activity)
            if time.monotonic() - last >= idle:
                return
            await asyncio.sleep(0.2)

    def counters(self) -> tuple[int, int]:
        return self.backhaul.total, self.edge.total

    async def start_origin(self) -> None:
        self.origin = await Node.spawn(
            self.workdir, "origin", "origin", os.path.join(self.workdir, "origin"),
            str(self.origin_port), json.dumps(self.items),
        )
        self.origin_info = await self.origin.expect("READY", 60)

    async def start_hub(self, follow: bool) -> dict:
        """(Re)start the hub with an empty cache. Returns startup cost."""
        if self.hub is not None:
            await self.hub.stop()
            await asyncio.sleep(1.0)
        await self.quiet()
        self._run += 1
        before = self.counters()
        started = time.monotonic()
        self.hub = await Node.spawn(
            self.workdir, f"hub{self._run}", "hub",
            os.path.join(self.workdir, f"hub{self._run}"),
            str(self.hub_port), str(self.backhaul.listen_port),
            self.origin_info["icn_hash"], self.origin_info["identity_path"],
            "1" if follow else "0",
        )
        self.hub_info = await self.hub.expect("READY", 600)
        ready = time.monotonic() - started
        await self.quiet()
        after = self.counters()
        # Outside the measured window: let the hub learn the page node's path,
        # which periodic announces would have given it on a running network.
        await self.origin.send({"announce": True})
        await self.origin.expect("ANNOUNCED", 30)
        await asyncio.sleep(1.0)
        return {
            "seconds_to_ready": round(ready, 2),
            "backhaul_bytes": after[0] - before[0],
            "follow": self.hub_info.get("follow"),
        }

    async def fetch(self, mode: str, label: str) -> dict:
        """One fresh reader fetching one item. Returns time and bytes per link."""
        await self.quiet()
        self._run += 1
        spec = {
            "label": label,
            "expect": self.items[label],
            "timeout": self.args.timeout,
            "page_hash": self.origin_info["page_hash"],
            "producer": self.origin_info["producer"],
            "hub_hash": self.hub_info["icn_hash"],
            "hub_identity": self.hub_info["identity_path"],
        }
        before = self.counters()
        marks = (len(self.backhaul.frames), len(self.edge.frames))
        node = await Node.spawn(
            self.workdir, f"reader{self._run}", "reader",
            os.path.join(self.workdir, f"reader{self._run}"),
            str(self.edge.listen_port), mode, json.dumps(spec),
        )
        try:
            result = await node.expect("RESULT", self.args.timeout + 60)
        finally:
            await node.stop()
        await self.quiet()
        after = self.counters()
        result["backhaul_bytes"] = after[0] - before[0]
        result["edge_bytes"] = after[1] - before[1]
        if self.args.trace:
            # Packet sizes in order; "<" = toward the reader, ">" = away from it.
            def fmt(frames):
                return " ".join(("<" if d == "down" else ">") + str(n) for d, n in frames)
            print(f"    edge packets:     {fmt(self.edge.frames[marks[1]:])}", flush=True)
            print(f"    backhaul packets: {fmt(self.backhaul.frames[marks[0]:])}", flush=True)
        return result

    async def publish(self, label: str, text: str, seq: int) -> dict:
        """Origin publishes a new item; measure what the push costs the backhaul."""
        await self.quiet()
        before = self.counters()
        self.items[label] = text
        await self.origin.send({"publish": {label: text}, "seq": seq})
        await self.origin.expect("PUBLISHED", 60)
        await asyncio.sleep(1.0)
        await self.quiet()
        return {"backhaul_bytes": self.counters()[0] - before[0]}

    async def run(self) -> dict:
        args = self.args
        await self.backhaul.start()
        await self.edge.start()
        report: dict = {
            "config": {
                "backhaul_bps": args.backhaul_bps, "edge_bps": args.edge_bps,
                "readers": args.readers, "sizes": args.sizes,
            },
            "items": {},
        }
        try:
            await self.start_origin()
            report["hub_start_plain"] = await self.start_hub(follow=False)
            print(f"hub up (no follow): {report['hub_start_plain']}", flush=True)

            for label in self.items:
                rows: dict[str, list[dict]] = {"page": [], "icn": []}
                for mode in args.methods:
                    for i in range(args.readers):
                        r = await self.fetch(mode, label)
                        rows[mode].append(r)
                        print(f"  {label} {mode} #{i + 1}: {r}", flush=True)
                report["items"][label] = {
                    "size": len(self.items[label]),
                    "page": rows["page"],
                    "icn_cold": rows["icn"][:1],
                    "icn_warm": rows["icn"][1:],
                }

            report["hub_start_follow"] = await self.start_hub(follow=True)
            print(f"hub up (following): {report['hub_start_follow']}", flush=True)
            for label in list(self.items):
                r = await self.fetch("icn", label)
                print(f"  {label} icn followed: {r}", flush=True)
                report["items"][label]["icn_followed"] = [r]

            # A new item published while the hub follows: pushed once, then read.
            label = "pushed"
            text = make_text(args.sizes[0], 99)
            report["push"] = await self.publish(label, text, seq=1000)
            report["push"]["size"] = len(text)
            report["push"]["fetch"] = await self.fetch("icn", label)
            print(f"  push: {report['push']}", flush=True)
        finally:
            for node in (self.hub, self.origin):
                if node is not None:
                    await node.stop()
            await self.backhaul.stop()
            await self.edge.stop()
        return report


def mean(rows: list[dict], key: str) -> float | None:
    values = [r[key] for r in rows if r.get("ok")]
    return sum(values) / len(values) if values else None


def render(report: dict) -> str:
    cfg = report["config"]
    edge = f"{cfg['edge_bps']} bit/s" if cfg["edge_bps"] else "unlimited"
    lines = [
        f"backhaul (hub-origin): {cfg['backhaul_bps']} bit/s   edge (reader-hub): {edge}",
        "",
        f"{'item':<8}{'method':<14}{'ok':<7}{'seconds':>9}{'backhaul B':>12}{'edge B':>9}",
    ]
    for item in report["items"].values():
        for method in ("page", "icn_cold", "icn_warm", "icn_followed"):
            rows = item.get(method) or []
            if not rows:
                continue
            ok = sum(1 for r in rows if r.get("ok") and r.get("matches"))
            secs, back, edge_b = (mean(rows, k) for k in
                                  ("seconds", "backhaul_bytes", "edge_bytes"))
            lines.append(
                f"{item['size']:<8}{method:<14}{f'{ok}/{len(rows)}':<7}"
                f"{secs if secs is None else round(secs, 1)!s:>9}"
                f"{back if back is None else round(back)!s:>12}"
                f"{edge_b if edge_b is None else round(edge_b)!s:>9}"
            )
        lines.append("")
    plain, follow = report.get("hub_start_plain"), report.get("hub_start_follow")
    if plain and follow:
        lines.append(
            f"hub start, backhaul bytes: {plain['backhaul_bytes']} without follow, "
            f"{follow['backhaul_bytes']} with follow "
            f"(pre-fill cost {follow['backhaul_bytes'] - plain['backhaul_bytes']})"
        )
    push = report.get("push")
    if push:
        f = push["fetch"]
        lines.append(
            f"push of a new {push['size']}-byte item: {push['backhaul_bytes']} backhaul bytes; "
            f"then read in {f['seconds']} s with {f['backhaul_bytes']} backhaul bytes"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--backhaul-bps", type=int, default=1200)
    parser.add_argument("--edge-bps", type=int, default=0, help="0 = unlimited")
    parser.add_argument("--readers", type=int, default=3,
                        help="readers per method per item (first ICN reader is the cold one)")
    parser.add_argument("--sizes", type=int, nargs="+", default=[200, 2000])
    parser.add_argument("--methods", nargs="+", default=["page", "icn"],
                        choices=["page", "icn"])
    parser.add_argument("--trace", action="store_true",
                        help="print the size of every packet on each link per fetch")
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--json", help="write the full report here")
    parser.add_argument("--keep", action="store_true", help="keep the work directory")
    args = parser.parse_args()

    workdir = tempfile.mkdtemp(prefix="icn-bench-")
    print(f"work directory: {workdir}", flush=True)
    report = asyncio.run(Bench(args, workdir).run())
    print()
    print(render(report))
    if args.json:
        with open(args.json, "w") as f:
            json.dump(report, f, indent=2)
    if not args.keep:
        import shutil
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
