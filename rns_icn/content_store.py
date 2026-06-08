"""ContentStore — LRU cache of Data packets."""

from __future__ import annotations

from collections import OrderedDict
from typing import Optional

from .name import Name
from .packet import Data


class ContentStore:
    """LRU cache of Data packets, keyed by name."""

    def __init__(self, max_entries: int = 1000):
        self._max = max(1, max_entries)
        self._entries: OrderedDict[Name, Data] = OrderedDict()
        self._hits = 0
        self._misses = 0

    def get(self, name: Name) -> Optional[Data]:
        entry = self._entries.get(name)
        if entry is not None:
            self._hits += 1
            self._entries.move_to_end(name)
            return entry
        self._misses += 1
        return None

    def get_prefix(self, prefix: Name) -> Optional[Data]:
        best_key = None
        best_len = 0
        for key in self._entries:
            if key.is_prefix_of(prefix) and key.len() > best_len:
                best_key = key
                best_len = key.len()
        if best_key is not None:
            self._hits += 1
            self._entries.move_to_end(best_key)
            return self._entries[best_key]
        self._misses += 1
        return None

    def insert(self, name: Name, data: Data) -> None:
        if name in self._entries:
            self._entries.move_to_end(name)
            self._entries[name] = data
        else:
            self._entries[name] = data
            while len(self._entries) > self._max:
                self._entries.popitem(last=False)

    def contains(self, name: Name) -> bool:
        return name in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def hits(self) -> int:
        return self._hits

    @property
    def misses(self) -> int:
        return self._misses

    @property
    def capacity(self) -> int:
        return self._max

    @property
    def size_bytes(self) -> int:
        return sum(len(d.content) for d in self._entries.values())
