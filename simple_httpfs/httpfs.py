from __future__ import annotations

import logging
from errno import ENOENT
from stat import S_IFDIR, S_IFREG
from time import time
from typing import Any
from urllib.parse import urlparse

import diskcache as dc
import obstore
from fuse import FuseOSError, LoggingMixIn, Operations

from ._caching import CachedStore, LRUCache, Store
from ._ftp import FTPStore


def path_to_url(path: str, sentinel: str) -> str | None:
    if path == "/" or not path.endswith(sentinel):
        return None

    return (
        path.lstrip("/")  # Trim leading "/"
        .replace(":/", "://", 1)  # Restore URL scheme
        .replace(f"{sentinel}/", "/")  # Remove sentinels from any "parent dirs"
        .rstrip(sentinel)  # Remove trailing sentinel
    )


def load_store(
    url,
    store_config: dict | None = None,
    client_options: dict | None = None,
    retry_config: dict | None = None,
    credential_provider: Any | None = None,
) -> Store:
    match urlparse(url).scheme:
        case "http" | "https":
            client_options = (client_options or {}).copy()
            client_options.setdefault("allow_http", True)
            return obstore.store.HTTPStore(
                url, client_options=client_options, retry_config=retry_config
            )
        case "ftp":
            return FTPStore(url)
        case _:
            return obstore.store.from_url(
                url,
                config=store_config,
                client_options=client_options,
                retry_config=retry_config,
                credential_provider=credential_provider,
            )


class HttpFs(LoggingMixIn, Operations):
    """
    A read-only http(s)/ftp/object storage filesystem.
    """

    def __init__(
        self,
        sentinel: str = "...",
        block_size: int = 2**20,
        disk_cache_size: int = 2**30,
        disk_cache_dir: str = "/tmp/xx",
        lru_capacity: int = 400,
        store_config: dict | None = None,
        client_options: dict | None = None,
        retry_config: dict | None = None,
        credential_provider: dict | None = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize a filesystem.

        Parameters
        ----------
        sentinel : str
            The terminal sentinel string to identify paths as URLs.
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

        Notes
        -----
        For a given path, the kernel will traverse its ``/``-based hierarchy
        and call ``getattr`` on each component. If any "directory" is not found,
        the entire lookup will fail. Therefore, for this filesystem to properly
        recognize a URI, each component of a path except for the very last one
        needs to be interpreted as a directory and the full URI needs to be
        interpreted as a file. To accomplish this, the fully qualified URI is
        identified by the presence of a trailing *sentinel* string.
        """
        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger(__name__)

        self.sentinel = sentinel

        self.logger.info(f"Starting with disk_cache_size: {disk_cache_size}")
        self.meta_cache = LRUCache(capacity=lru_capacity)
        self.mem_cache = LRUCache(capacity=lru_capacity)
        self.disk_cache = dc.Cache(disk_cache_dir, size_limit=disk_cache_size)
        self.block_size = block_size

        self.store_config = (
            store_config if store_config is not None else {"skip_signature": True}
        )
        self.client_options = client_options
        self.retry_config = retry_config
        self.credential_provider = credential_provider

    def getattr(self, path: str, fh: Any = None) -> dict[str, Any]:
        """
        Return an attribute dictionary for the given path.

        Notes
        -----
        The output has keys identical to the stat C structure of stat(2).
        ``st_atime``, ``st_mtime`` and ``st_ctime`` should be unix timestamps
        (floats).
        """
        url = path_to_url(path, self.sentinel)
        if url is None:
            return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)
        self.logger.info(f"getattr: HEAD: {url}")

        store = load_store(
            url,
            self.store_config,
            self.client_options,
            self.retry_config,
            self.credential_provider,
        )
        store = CachedStore(
            store,
            path,
            self.meta_cache,
            self.mem_cache,
            self.disk_cache,
            self.block_size,
        )
        try:
            metadata = store.head("")
        except FileNotFoundError:
            return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)
        except Exception as e:
            raise FuseOSError(ENOENT) from e

        now = time()
        timestamp = (
            metadata["last_modified"].timestamp()
            if "last_modified" in metadata
            else now
        )
        return dict(
            st_mode=(S_IFREG | 0o644),
            st_nlink=1,
            st_size=metadata["size"],
            st_ctime=timestamp,
            st_mtime=timestamp,
            st_atime=now,
        )

    def readdir(self, path: str, fh: Any = None) -> list[str]:
        """
        Return a list of files in the directory.
        """
        url = path_to_url(path, self.sentinel)
        if url is None:
            return [".", ".."]
        self.logger.info(f"readdir: LIST: {url}")

        store = load_store(
            url,
            self.store_config,
            self.client_options,
            self.retry_config,
            self.credential_provider,
        )
        store = CachedStore(
            store,
            path,
            self.meta_cache,
            self.mem_cache,
            self.disk_cache,
            self.block_size,
        )
        dirs = [
            (item + self.sentinel)
            for item in store.list_with_delimiter()["common_prefixes"]
        ]
        files = [
            (item["path"] + self.sentinel)
            for item in store.list_with_delimiter()["objects"]
        ]
        return [".", "..", *dirs, *files]

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
        url = path_to_url(path, self.sentinel)
        if url is None:
            return b""
        self.logger.debug(
            f"read: GET_RANGE: {url}\n"
            f"range: {offset} - {offset + size - 1}\n"
            f"request_size (KB): {size / 1024:.2f}\n"
        )

        store = load_store(
            url,
            self.store_config,
            self.client_options,
            self.retry_config,
            self.credential_provider,
        )
        store = CachedStore(
            store,
            path,
            self.meta_cache,
            self.mem_cache,
            self.disk_cache,
            self.block_size,
        )
        return store.get_range(url, start=offset, length=size)

    def link(self, target: str, source: str): ...

    def symlink(self, target: str, source: str): ...

    def unlink(self, path: str): ...

    def write(self, path: str, buf: bytes, size: int, offset: int, fip: Any) -> int:
        return 0

    def statfs(self, path: str) -> dict[str, int]:
        fs_block_size = 128 * 1024
        return dict(
            f_frsize=fs_block_size,  # fundamental block size
            f_bsize=fs_block_size,  # preferred block size
            f_blocks=1024 * 1024,  # pretend total capacity (in blocks)
            f_bfree=512 * 1024,  # half free for root
            f_bavail=512 * 1024,  # same for non-root
            f_namemax=8192,  # maximum filename length (in chars)
        )

    def destroy(self, path: str) -> int:
        """
        Called on filesystem destruction. Path is always `/`.
        """
        self.disk_cache.close()
