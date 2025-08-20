import logging
import os
import threading
from collections.abc import MutableMapping
from collections import OrderedDict
from stat import S_IFDIR, S_IFREG
from time import sleep, time
from typing import Any, Literal

import diskcache as dc
import obstore
from fuse import LoggingMixIn, Operations


class LRUCache(MutableMapping):
    def __init__(self, capacity: int = 128):
        self.capacity = capacity
        self.cache = OrderedDict()

    def __getitem__(self, key):
        if key not in self.cache:
            raise KeyError(key)

        # Recently used - move to end
        self.cache.move_to_end(key)

        return self.cache[key]

    def __setitem__(self, key, value):
        # If cache hit, move to end before updating
        if key in self.cache:
            self.cache.move_to_end(key)

        self.cache[key] = value

        # Remove least recently used item if capacity exceeded
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def __delitem__(self, key):
        del self.cache[key]

    def __iter__(self):
        return iter(self.cache)

    def __len__(self):
        return len(self.cache)

    def __repr__(self):
        return f"{self.__class__.__name__}({dict(self.cache)}, capacity={self.capacity})"

    def get(self, key, default=None):
        """Like dict.get, but updates usage if key exists."""
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return default

    def pop(self, key, *args):
        """Like dict.pop, removes item completely (no recency update)."""
        return self.cache.pop(key, *args)

    def clear(self):
        self.cache.clear()


class HttpFs(LoggingMixIn, Operations):
    """
    A read-only http(s)/ftp/object storage filesystem.
    """
    def __init__(
        self,
        schema: Literal["http", "https", "s3", "gcs", "azure", "ftp"],
        block_size: int = 2**20,
        disk_cache_size: int = 2**30,
        disk_cache_dir: str = "/tmp/xx",
        lru_capacity: int = 400,
        aws_profile: str = None,
        logger: logging.Logger = None,
    ):
        """
        Called on filesystem initialization. Path is always `/`.

        Parameters
        ----------
        schema : str
            The URL schema to use (http, https, ftp, s3, gcs, azure).
        block_size : int
            The block size to use for reads and writes.
        disk_cache_size : int
            The size of the disk cache to use.
        disk_cache_dir : str
            The directory to use for the disk cache.
        lru_capacity : int
            The capacity of the LRU cache.
        aws_profile : str
            The AWS profile to use for S3 access.
        logger : logging.Logger
            The logger to use for logging.
        """
        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger(__name__)

        self.schema = schema
        self.lru_cache = LRUCache(capacity=lru_capacity)
        self.lru_attrs = LRUCache(capacity=lru_capacity)
        self.disk_cache = dc.Cache(disk_cache_dir, size_limit=disk_cache_size)

        self.logger.info(f"Starting with disk_cache_size: {disk_cache_size}")

        self.total_requests = 0
        self.total_blocks = 0
        self.lru_hits = 0
        self.lru_misses = 0
        self.disk_hits = 0
        self.disk_misses = 0
        self.block_size = block_size
        self._lock = threading.Lock()
        self._fetching = set()

    def destroy(self, path: str) -> int:
        """
        Called on filesystem destruction. Path is always `/`.
        """
        self.disk_cache.close()

    def getattr(self, path: str, fh: Any = None) -> dict[str, Any]:
        """
        Return an attribute dictionary for the given path.

        Notes
        -----
        * The dictionary has keys identical to the stat C structure of stat(2).
        * ``st_atime``, ``st_mtime`` and ``st_ctime`` should be unix timestamps
          (floats).
        """
        if path in self.lru_attrs:
            return self.lru_attrs[path]

        if path == "/":
            self.lru_attrs[path] = dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)
            return self.lru_attrs[path]

        if not path.endswith(".."):
            return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)
        url = path.replace("../", "/").rstrip(".")
        url = f"{self.schema}:/{url}"

        self.logger.info(f"getattr: HEAD: {url}")
        store = obstore.store.from_url(url, skip_signature=True)
        try:
            metadata = store.head("")
        except FileNotFoundError as e:
            return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

        now = time()
        timestamp = (
            metadata["last_modified"].timestamp()
            if "last_modified" in metadata else now
        )
        self.lru_attrs[path] = dict(
            st_mode=(S_IFREG | 0o644),
            st_nlink=1,
            st_size=metadata.get("size", 0),
            st_ctime=timestamp,
            st_mtime=timestamp,
            st_atime=now,
        )
        return self.lru_attrs[path]

    def readdir(self, path: str, fh: Any = None) -> list[str]:
        """
        Return a list of files in the directory.
        """
        if path == "/" or not path.endswith(".."):
            return [".", ".."]
        url = path.replace("../", "/").rstrip(".")
        url = f"{self.schema}:/{url}"

        self.logger.info(f"readdir: Fetching directory listing for {url}")
        store = obstore.store.from_url(url, skip_signature=True)
        return [".", "..", *[item['path'] + ".." for item in store.list().collect()]]

    def read(self, path: str, size: int, offset: int, fh: Any = None) -> bytes:
        """
        Return a byte string containing the data requested.

        Parameters
        ----------
        path: str
            The path to the file to read.
        size: int
            The number of bytes to read.
        offset: int
            The offset to start reading from.
        fh: Any
            File handle (not used).
        """
        if not path.endswith(".."):
            return b""
        url = path.replace("../", "/").rstrip(".")
        url = f"{self.schema}:/{url}"
        self.total_requests += 1
        final_offset = offset + size

        self.logger.info(
            f"READ: {path}\n"
            f"url: {url}\n"
            f"range: {offset} - {offset + size - 1}\n"
            f"request_size (KB): {size / 2 ** 10:.2f}\n"
        )

        output = b""
        curr_offset = offset
        while curr_offset < final_offset:
            block_num = curr_offset // self.block_size
            block_id = (url, block_num)
            block_start = curr_offset % self.block_size
            remaining = final_offset - curr_offset
            data_size = min(self.block_size - block_start, remaining)

            # Fetch the block. It may be getting written to cache in another
            # thread so we make the block read thread-safe.
            with self._lock:
                while block_id in self._fetching:
                    sleep(0.05)
                self._fetching.add(block_id)
            try:
                block = self._read_block(url, block_num)
            finally:
                with self._lock:
                    self._fetching.remove(block_id)

            # Extract only the portion we need from this block
            if not len(block):
                self.logger.info("empty block")
                break
            output += block[block_start : block_start + data_size]
            curr_offset += data_size

        return output

    def _read_block(self, url: str, block_num: int) -> bytes:
        """
        Return a data block from a URL.

        Parameters
        ----------
        url: string
            The url of the file we want to retrieve a block from.
        block_num: int
            The 0-based index of the block of size ``self.block_size``.
        """
        cache_key = f"{url}.{self.block_size}.{block_num}"
        self.total_blocks += 1

        block = self.lru_cache.get(cache_key, None)
        if block is not None:
            self.lru_hits += 1
            return block
        self.lru_misses += 1

        block = self.disk_cache.get(cache_key, None)
        if block is not None:
            self.disk_hits += 1
            self.lru_cache[cache_key] = block
            return block
        self.disk_misses += 1

        self.logger.info(f"Fetching block {cache_key}...")
        start = block_num * self.block_size
        store = obstore.store.from_url(url, skip_signature=True)
        block = store.get_range("", start=start, length=self.block_size)
        self.lru_cache[cache_key] = block
        self.disk_cache[cache_key] = block

        return block

    def link(self, target: str, source: str):
        ...

    def symlink(self, target: str, source: str):
        ...

    def unlink(self, path: str):
        ...

    def write(self, path: str, buf: bytes, size: int, offset: int, fip: Any) -> int:
        return 0

    def statfs(self, path: str) -> dict[str, int]:
        return dict(
            f_bsize=4096,         # preferred block size
            f_frsize=4096,        # fundamental block size
            f_blocks=1024*1024,   # pretend: 4 GiB total capacity
            f_bfree=512*1024,     # half free
            f_bavail=512*1024,    # same for non-root
            f_files=1000000,      # max number of files (arbitrary)
            f_ffree=999000,       # free inodes
            f_favail=999000,      # free inodes for unpriviledged users
            f_flag=0,             # mount flags, often 0
            f_namemax=8192,       # maximum filename length
        )

