"""NomadNet page front end — read cached ICN content from existing clients.

Every Reticulum client that browses pages (NomadNet, MeshChat, rBrowser, Ren
Browser, Columba, ...) speaks the same small protocol: link to a
``nomadnetwork.node`` destination and request ``/page/<name>.mu``. Serving that
from an ICN node puts whatever the node holds in front of those users with
nothing new for them to install.

Three pages:

    /page/index.mu     producers this node holds content from
    /page/producer.mu  that producer's items (``p`` = producer address, hex)
    /page/item.mu      one item (``n`` = hex of the wire-format name)

Items are served from the local Content Store, so they stay readable while
their producer is offline. An item that is not held is requested upstream once,
with a short lifetime, before the page reports it missing.

``PageRenderer`` builds the pages and has no Reticulum dependency; ``PageNode``
binds it to a destination.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from . import discovery
from .name import RNS_ADDR_BYTES, Name
from .packet import Data, Interest

if TYPE_CHECKING:
    from .server import ICNServer

logger = logging.getLogger(__name__)

APP_NAME = "nomadnetwork"
ASPECT = "node"
PATH_INDEX = "/page/index.mu"
PATH_PRODUCER = "/page/producer.mu"
PATH_ITEM = "/page/item.mu"

# Largest item rendered inline; bigger ones are described, not dumped.
MAX_INLINE_BYTES = 64 * 1024
# Editions listed per collection on a listing page, newest first.
MAX_EDITIONS_LISTED = 4

Validator = Callable[[bytes, bytes], bool]


def _age(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds}s"
    if seconds < 90 * 60:
        return f"{seconds // 60}m"
    if seconds < 36 * 3600:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def _size(n: int) -> str:
    return f"{n} B" if n < 1024 else f"{n / 1024:.1f} KB"


def _label(text: str) -> str:
    """Text safe to use as a micron link label or inline text."""
    return text.replace("`", "'").replace("[", "(").replace("]", ")")


def _link(label: str, path: str, **fields: str) -> str:
    field_str = ""
    if fields:
        field_str = "`" + "|".join(f"{k}={v}" for k, v in fields.items())
    return f"`[{_label(label)}`:{path}{field_str}]"


def _literal(text: str) -> str:
    """Wrap arbitrary text so micron shows it verbatim."""
    body = "\n".join(
        ("\\" + line) if line.startswith("`=") else line for line in text.splitlines()
    )
    return f"`=\n{body}\n`=\n"


def _path_text(name: Name) -> str:
    parts = [c.decode("utf-8", errors="replace") for c in name.components[1:]]
    return "/".join(parts) if parts else "(root)"


class PageRenderer:
    """Builds micron pages from a server's Content Store."""

    def __init__(
        self,
        server: ICNServer,
        node_name: str = "ICN cache",
        validator_for: Callable[[bytes], Validator | None] | None = None,
        fetch: Callable[[Name], Data | None] | None = None,
        nicknames: dict[bytes, str] | None = None,
    ):
        """Args:
            server: source of content (forwarder Content Store, follower stats).
            validator_for: producer address -> signature validator, or None.
            fetch: blocking upstream fetch for an item not held locally.
            nicknames: producer address -> display name (e.g. known peer names).
        """
        self._server = server
        self.node_name = node_name
        self._validator_for = validator_for
        self._fetch = fetch
        self._nicknames = nicknames or {}

    # ── helpers ──

    def _producer_title(self, addr: bytes) -> str:
        if addr == self._server.rns_addr:
            return self.node_name
        nick = self._nicknames.get(addr)
        short = addr.hex()[:12]
        return f"{nick} ({short})" if nick else addr.hex()

    def _items(self, producer: bytes) -> list[tuple[Name, int | None, int, int]]:
        manifest = Name(producer, [b"manifest"])
        return [
            row for row in self._server.forwarder.cs.listing(Name(producer))
            if row[0] != manifest and not discovery.is_reserved(row[0])
            and row[0].len() > 1
        ]

    def _signature_line(self, data: Data) -> str:
        if data.name.rns_addr == self._server.rns_addr:
            return "published by this node"
        if data.signature is None:
            return "unsigned"
        validator = self._validator_for(data.name.rns_addr) if self._validator_for else None
        if validator is None:
            return "signed (producer key not known here, not checked)"
        if data.verify_signature(validator):
            return "signature verified"
        return "`!SIGNATURE INVALID`!"

    def _header(self, crumbs: str) -> str:
        return f">{_label(self.node_name)}\n{crumbs}\n-\n"

    # ── pages ──

    @staticmethod
    def _newest_first(rows):
        # By sequence where there is one, else by arrival.
        return sorted(rows, key=lambda r: (r[1] is not None, r[1] or 0, r[2]), reverse=True)

    def _collections(self, producer: bytes) -> str:
        """A producer's items grouped by the folder they are in.

        A folder is a collection of editions: the newest is listed first and
        marked, with a few earlier ones after it.
        """
        groups: dict[str, list[tuple[Name, int | None, int, int]]] = {}
        for row in self._items(producer):
            folder = "/".join(
                c.decode("utf-8", errors="replace") for c in row[0].components[1:-1]
            )
            groups.setdefault(folder, []).append(row)
        now = time.time()
        out: list[str] = []
        # Loose items (no folder) first, then collections by name.
        for folder in sorted(groups, key=lambda f: (f != "", f)):
            rows = self._newest_first(groups[folder])
            if folder:
                count = len(rows)
                out.append(f">>{_label(folder)}\n")
                shown = rows[:MAX_EDITIONS_LISTED]
            else:
                shown = rows
            for position, (name, _seq, inserted_at, size_b) in enumerate(shown):
                leaf = name.components[-1].decode("utf-8", errors="replace")
                mark = "latest  " if folder and position == 0 else ""
                out.append(
                    f"{_link(leaf, PATH_ITEM, n=name.to_bytes().hex())}"
                    f"  {mark}{_size(size_b)}, {_age(now - inserted_at)} ago\n"
                )
            if folder and count > len(shown):
                out.append(f"{count - len(shown)} earlier not listed\n")
        return "".join(out)

    def index(self) -> str:
        producers: dict[bytes, list[int]] = {}
        for name, _seq, inserted_at, _size_b in self._server.forwarder.cs.listing():
            addr = name.rns_addr
            if name.len() <= 1 or name.components[1] == b"manifest" \
                    or discovery.is_reserved(name):
                continue
            entry = producers.setdefault(addr, [0, 0])
            entry[0] += 1
            entry[1] = max(entry[1], inserted_at)

        out = [self._header(_link("Home", PATH_INDEX))]
        if not producers:
            out.append("Nothing is cached on this node yet.\n")
            return "".join(out)
        # What this node publishes itself is the reason most visitors came.
        own = self._server.rns_addr
        if own in producers:
            out.append(self._collections(own))
            del producers[own]
        if not producers:
            return "".join(out)
        following = self._server.follower.stats()
        now = time.time()
        out.append(">Kept from other nodes\n" if len(out) > 1 else ">>Producers\n")
        for addr, (count, newest) in sorted(
            producers.items(), key=lambda kv: kv[1][1], reverse=True
        ):
            tag = " [followed]" if addr.hex() in following else ""
            out.append(
                f"{_link(self._producer_title(addr), PATH_PRODUCER, p=addr.hex())}"
                f"  {count} item{'s' if count != 1 else ''}, newest {_age(now - newest)} ago"
                f"{_label(tag)}\n"
            )
        return "".join(out)

    def producer(self, producer_hex: str) -> str:
        try:
            addr = bytes.fromhex(producer_hex)
        except ValueError:
            addr = b""
        crumbs = _link("Home", PATH_INDEX)
        if len(addr) != RNS_ADDR_BYTES:
            return self._header(crumbs) + "Unknown producer.\n"
        crumbs += " / " + _label(self._producer_title(addr))
        listing = self._collections(addr)
        if not listing:
            return self._header(crumbs) + "No items from this producer are cached here.\n"
        return self._header(crumbs) + listing

    def item(self, name_hex: str) -> str:
        home = _link("Home", PATH_INDEX)
        try:
            name = Name.from_bytes(bytes.fromhex(name_hex)).without_content_hash()
        except Exception:
            return self._header(home) + "Malformed item name.\n"
        crumbs = (
            f"{home} / "
            f"{_link(self._producer_title(name.rns_addr), PATH_PRODUCER, p=name.rns_addr.hex())}"
            f" / {_label(_path_text(name))}"
        )
        out = [self._header(crumbs)]

        cs = self._server.forwarder.cs
        data = cs.get(name)
        source = "served from this node's cache"
        if data is None and self._fetch is not None:
            data = self._fetch(name)
            source = "fetched from upstream just now"
        if data is None:
            out.append("This item is not cached here and could not be fetched.\n")
            return "".join(out)

        details = [self._signature_line(data), source]
        if data.metadata.sequence is not None:
            details.insert(0, f"#{data.metadata.sequence}")
        if not data.metadata.freshness.fresh:
            details.append(f"stale by {_age(data.metadata.freshness.age_seconds)}")
        if data.metadata.signed_at is not None:
            details.append(f"signed {_age(time.time() - data.metadata.signed_at)} ago")
        out.append(f"`*{_label(' · '.join(details))}`*\n\n")

        if data.metadata.encrypted:
            out.append("This item is encrypted for specific readers.\n")
        elif len(data.content) > MAX_INLINE_BYTES:
            out.append(f"{_size(len(data.content))} — too large to show as a page.\n")
        else:
            try:
                text = data.content.decode("utf-8")
            except UnicodeDecodeError:
                out.append(f"Binary content, {_size(len(data.content))}.\n")
            else:
                # A ".mu" item is the producer's own micron page; anything else
                # is shown verbatim.
                is_micron = name.components[-1].endswith(b".mu")
                out.append(text + "\n" if is_micron else _literal(text))
        return "".join(out)


class PageNode:
    """Serves a ``PageRenderer`` on a NomadNet-compatible node destination."""

    def __init__(self, server, node_name: str = "ICN cache",
                 fetch_lifetime_ms: int = 15_000,
                 nicknames: dict[bytes, str] | None = None):
        """``server`` is a started ``rns_icn.rns_server.ICNServer``."""
        import RNS

        from .rns_server import _producer_validator

        self._server = server
        self._fetch_lifetime_ms = fetch_lifetime_ms
        self.renderer = PageRenderer(
            server, node_name=node_name, validator_for=_producer_validator,
            fetch=self._fetch_blocking, nicknames=nicknames,
        )
        self.destination = RNS.Destination(
            server.identity, RNS.Destination.IN, RNS.Destination.SINGLE, APP_NAME, ASPECT
        )
        self.destination.set_default_app_data(lambda: node_name.encode("utf-8"))
        for path, handler in (
            (PATH_INDEX, self._serve_index),
            (PATH_PRODUCER, self._serve_producer),
            (PATH_ITEM, self._serve_item),
        ):
            self.destination.register_request_handler(
                path, response_generator=handler, allow=RNS.Destination.ALLOW_ALL
            )

    @property
    def hexhash(self) -> str:
        return self.destination.hexhash

    def announce(self) -> None:
        self.destination.announce()

    # Request handlers run on a Reticulum thread.

    @staticmethod
    def _field(data, key: str) -> str:
        if isinstance(data, dict):
            value = data.get(f"var_{key}", "")
            if isinstance(value, bytes):
                value = value.decode("utf-8", errors="replace")
            return str(value)
        return ""

    def _guard(self, build: Callable[[], str]) -> bytes:
        try:
            return build().encode("utf-8")
        except Exception:
            logger.exception("page rendering failed")
            return b">Error\nThis page could not be built.\n"

    def _serve_index(self, path, data, request_id, link_id, remote_identity, requested_at):
        return self._guard(self.renderer.index)

    def _serve_producer(self, path, data, request_id, link_id, remote_identity, requested_at):
        return self._guard(lambda: self.renderer.producer(self._field(data, "p")))

    def _serve_item(self, path, data, request_id, link_id, remote_identity, requested_at):
        return self._guard(lambda: self.renderer.item(self._field(data, "n")))

    def _fetch_blocking(self, name: Name) -> Data | None:
        """Ask upstream for an item not held locally; blocks the request thread."""
        loop = self._server._loop
        if loop is None:
            return None
        interest = Interest(name=name, lifetime_ms=self._fetch_lifetime_ms)
        future = asyncio.run_coroutine_threadsafe(
            self._server.forwarder.express(interest, 0), loop
        )
        try:
            return future.result(self._fetch_lifetime_ms / 1000.0 + 1.0)
        except Exception:
            future.cancel()
            return None
