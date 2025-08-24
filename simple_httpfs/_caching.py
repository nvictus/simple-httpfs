from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Buffer, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Protocol
from weakref import WeakValueDictionary

from diskcache import Cache as DiskCache
from obspec import GetRange, Head, ListResult, ListWithDelimiter, ObjectMeta


class Store(GetRange, Head, ListWithDelimiter, Protocol): ...


@dataclass
class CacheMonitor:
    total_requests: int = 0
    total_blocks: int = 0
    lru_hits: int = 0
    lru_misses: int = 0
    disk_hits: int = 0
    disk_misses: int = 0

    @property
    def lru_hit_rate(self) -> float:
        total = self.lru_hits + self.lru_misses
        return self.lru_hits / total if total > 0 else 0.0

    @property
    def disk_hit_rate(self) -> float:
        total = self.disk_hits + self.disk_misses
        return self.disk_hits / total if total > 0 else 0.0

    def reset(self) -> None:
        self.total_requests = 0
        self.total_blocks = 0
        self.lru_hits = 0
        self.lru_misses = 0
        self.disk_hits = 0
        self.disk_misses = 0


class LRUCache(MutableMapping):
    def __init__(self, capacity: int = 128):
        self.capacity = capacity
        self.cache = OrderedDict()

    def __getitem__(self, key):
        """Like dict.__getitem__, but updates usage if key exists."""
        if key not in self.cache:
            raise KeyError(key)
        self.cache.move_to_end(key)
        return self.cache[key]

    def __setitem__(self, key, value):
        # If cache hit, move to end before updating
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        # Evict least recently used item if capacity exceeded
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def __delitem__(self, key):
        del self.cache[key]

    def __iter__(self):
        return iter(self.cache)

    def __len__(self):
        return len(self.cache)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}({dict(self.cache)}, capacity={self.capacity})"
        )

    def get(self, key, default=None):
        """Like dict.get, but updates usage if key exists."""
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return default

    def pop(self, key, *args):
        return self.cache.pop(key, *args)

    def clear(self):
        self.cache.clear()


class CachedStore(Store):
    def __init__(
        self,
        store: Store,
        prefix: str,
        meta_cache: LRUCache,
        mem_cache: LRUCache,
        disk_cache: DiskCache,
        block_size: int = 1024 * 1024,
    ):
        self.store = store
        self.prefix = prefix
        self.meta_cache = meta_cache
        self.mem_cache = mem_cache
        self.disk_cache = disk_cache
        self.block_size = block_size
        self._block_locks: WeakValueDictionary[str, threading.Lock] = (
            WeakValueDictionary()
        )
        self._lock: threading.Lock = threading.Lock()

    def head(self, path: str) -> ObjectMeta:
        fpath = "/".join((self.prefix, path)).replace("//", "/")
        if fpath in self.meta_cache:
            return self.meta_cache[fpath]
        meta = self.store.head(path)
        self.meta_cache[fpath] = meta
        return meta

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> Buffer:
        fpath = "/".join((self.prefix, path)).replace("//", "/")

        pos = start
        if length is not None:
            end = start + length
        else:
            raise ValueError("Either end or length must be provided")

        output = b""
        while pos < end:
            block_num = pos // self.block_size
            remaining = end - pos
            data_start = pos % self.block_size
            data_size = min(self.block_size - data_start, remaining)

            # Read the block from cache or from the obstore and write to cache.
            # This is thread-safe.
            block = self._get_block(fpath, block_num)
            if not block:
                break

            # Extract only the portion we need from this block
            output += block[data_start : data_start + data_size]
            pos += data_size

        return output

    def _get_lock(self, cache_key: str) -> threading.Lock:
        with self._lock:
            if cache_key not in self._block_locks:
                block_lock = threading.Lock()
                self._block_locks[cache_key] = block_lock
            else:
                block_lock = self._block_locks[cache_key]
            return block_lock

    def _get_block(self, path: str, block_num: int) -> Buffer:
        cache_key = f"{path}.{self.block_size}.{block_num}"

        with self._get_lock(cache_key):
            block = self.mem_cache.get(cache_key, None)
            if block is not None:
                return block

            block = self.disk_cache.get(cache_key, None)
            if block is not None:
                self.mem_cache[cache_key] = block
                return block

            block = self.store.get_range(
                "", start=block_num * self.block_size, length=self.block_size
            )
            self.mem_cache[cache_key] = block
            self.disk_cache[cache_key] = block  # Writes to disk

        return block

    def list_with_delimiter(
        self, prefix: str | None = None
    ) -> ListResult[Sequence[ObjectMeta]]:
        result = self.store.list_with_delimiter(prefix)
        for item in result["objects"]:
            path = prefix + item["path"] if prefix else item["path"]
            if path not in self.meta_cache:
                self.meta_cache[path] = item
        return result
