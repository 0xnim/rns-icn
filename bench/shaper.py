"""Rate-limiting, byte-counting TCP relay for the benchmark.

Sits between two Reticulum TCP interfaces so a localhost link behaves like a
slow radio link and every byte that crosses it is counted. Each direction has
its own budget of ``bps`` bits per second (0 = unlimited); the two directions do
not contend, which is kinder than a real half-duplex channel.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

_CHUNK = 32  # bytes released per step, so pacing is smooth at low bitrates


class Shaper:
    def __init__(self, listen_port: int, target_port: int, bps: int = 0, name: str = ""):
        self.listen_port = listen_port
        self.target_port = target_port
        self.bps = bps
        self.name = name
        self.bytes_up = 0     # listener side -> target (toward the origin)
        self.bytes_down = 0   # target -> listener side
        self.last_activity = time.monotonic()
        # Per-packet trace: (direction, bytes) for each HDLC frame Reticulum's
        # TCP interface sends, in order. "up" is toward the target.
        self.frames: list[tuple[str, int]] = []
        self._partial = {True: 0, False: 0}
        self._server: asyncio.AbstractServer | None = None
        self._tasks: set[asyncio.Task] = set()

    @property
    def total(self) -> int:
        return self.bytes_up + self.bytes_down

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", self.listen_port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            up_reader, up_writer = await asyncio.open_connection("127.0.0.1", self.target_port)
        except OSError:
            writer.close()
            return
        for coro in (
            self._pipe(reader, up_writer, up=True),
            self._pipe(up_reader, writer, up=False),
        ):
            task = asyncio.ensure_future(coro)
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    def _trace(self, chunk: bytes, up: bool) -> None:
        """Split the byte stream into HDLC frames (0x7E-delimited) and log sizes."""
        for byte in chunk:
            self._partial[up] += 1
            if byte == 0x7E and self._partial[up] > 1:
                self.frames.append(("up" if up else "down", self._partial[up]))
                self._partial[up] = 0

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                    up: bool) -> None:
        # ``ready_at`` is when the channel next falls idle; pacing against it
        # (rather than sleeping per chunk) keeps the long-run rate exact.
        ready_at = time.monotonic()
        try:
            while True:
                chunk = await reader.read(_CHUNK)
                if not chunk:
                    break
                if self.bps > 0:
                    now = time.monotonic()
                    ready_at = max(ready_at, now) + len(chunk) * 8 / self.bps
                    if ready_at > now:
                        await asyncio.sleep(ready_at - now)
                if up:
                    self.bytes_up += len(chunk)
                else:
                    self.bytes_down += len(chunk)
                self._trace(chunk, up)
                self.last_activity = time.monotonic()
                writer.write(chunk)
                await writer.drain()
        except (ConnectionError, asyncio.CancelledError, OSError):
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()
