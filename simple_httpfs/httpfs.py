import collections
import logging
import os
import re
from errno import EIO, ENOENT
from ftplib import FTP
from stat import S_IFDIR, S_IFREG
from time import sleep, time
from urllib.parse import urlparse

import boto3
from botocore import UNSIGNED
import diskcache as dc
import requests
from fuse import FuseOSError, LoggingMixIn, Operations
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
    wait_random,
)
import slugid


DISK_CACHE_SIZE_ENV = "HTTPFS_DISK_CACHE_SIZE"
DISK_CACHE_DIR_ENV = "HTTPFS_DISK_CACHE_DIR"
FALSY = {0, "0", False, "false", "False", "FALSE", "off", "OFF"}


class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = collections.OrderedDict()

    def __getitem__(self, key):
        value = self.cache.pop(key)
        self.cache[key] = value
        return value

    def __setitem__(self, key, value):
        try:
            self.cache.pop(key)
        except KeyError:
            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = value

    def __contains__(self, key):
        return key in self.cache

    def __len__(self):
        return len(self.cache)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


class FtpFetcher:
    def server_path(self, url):
        o = urlparse(url)
        return (o.netloc, o.path)

    def login(self, server):
        ftp = FTP(server)
        ftp.login()
        try:
            # do a retrbinary on a non-existent file
            # to set the transfer mode to binary
            # use a dummy callback too
            ftp.retrbinary(slugid.nice(), lambda x: x + 1)
        except:
            pass
        return ftp

    def get_size(self, url):
        (server, path) = self.server_path(url)
        ftp = self.login(server)
        size = ftp.size(path)
        ftp.close()
        return size

    def get_data(self, url, start, end):
        (server, path) = self.server_path(url)

        ftp = self.login(server)
        conn = ftp.transfercmd(f"RETR {path}", rest=start)

        amt = end - start
        chunk_size = 1 << 15
        data = b""
        while len(data) < amt:
            chunk = conn.recv(chunk_size)
            if chunk:
                data += chunk
            else:
                break
        if len(data) < amt:
            data += b"\x00" * (amt - len(data))
        else:
            data = data[:amt]

        ftp.close()

        return data


class HttpFetcher:
    SSL_VERIFY = os.environ.get("SSL_VERIFY", True) not in FALSY

    def __init__(self, logger):
        self.logger = logger
        self.timeout = 5
        if not self.SSL_VERIFY:
            logger.warning(
                "You have set ssl certificates to not be verified. "
                "This may leave you vulnerable. "
                "http://docs.python-requests.org/en/master/user/advanced/#ssl-cert-verification"
            )

    def get_size(self, url: str) -> int:
        try:
            head = requests.head(
                url, allow_redirects=True, verify=self.SSL_VERIFY, timeout=self.timeout
            )
        except requests.exceptions.Timeout:
            self.logger.info(f"Timeout occurred while fetching head for {url}")
            raise FuseOSError(EIO)

        if head.ok:
            if "Content-Length" in head.headers:
                return int(head.headers["Content-Length"])
            else:
                # Try the byte range request trick
                r = requests.get(
                    url,
                    allow_redirects=True,
                    verify=self.SSL_VERIFY,
                    timeout=self.timeout,
                    headers={"Range": "bytes=0-0"},
                )
                content_range = r.headers.get("Content-Range", "")
                match = re.search(r"/(\d+)$", content_range)
                if match:
                    return int(match.group(1))

            self.logger.info(f"Failed to get size for {url}")
            return 0

        # Request failed: No such file or directory
        raise FuseOSError(ENOENT)

    @retry(wait=wait_fixed(1) + wait_random(0, 2), stop=stop_after_attempt(2))
    def get_data(self, url: str, start: int, end: int) -> bytes:
        headers = {"Range": f"bytes={start}-{end}", "Accept-Encoding": ""}
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r.content


class S3Fetcher:
    SSL_VERIFY = os.environ.get("SSL_VERIFY", True) not in FALSY

    def __init__(self, aws_profile, logger):
        self.logger = logger
        self.logger.info("Creating S3Fetcher with aws_profile=%s", aws_profile)
        config = None
        if aws_profile is None:
            config = boto3.session.Config(signature_version=UNSIGNED)
        self.session = boto3.Session(profile_name=aws_profile)
        self.client = self.session.client("s3", config=config)

    def parse_bucket_key(self, url):
        url_parts = urlparse(url, allow_fragments=False)
        bucket = url_parts.netloc
        key = url_parts.path.strip("/")
        return bucket, key

    def get_size(self, url):
        bucket, key = self.parse_bucket_key(url)
        response = self.client.head_object(Bucket=bucket, Key=key)
        size = response["ContentLength"]
        return size

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10))
    def get_data(self, url: str, start: int, end: int) -> bytes:
        bucket, key = self.parse_bucket_key(url)
        stream = self.client.get_object(
            Bucket=bucket, Key=key, Range="bytes={}-{}".format(start, end)
        )["Body"]
        contents = stream.read()
        return contents


class HttpFs(LoggingMixIn, Operations):
    """
    A read only http/https/ftp filesystem.

    """

    def __init__(
        self,
        schema,
        disk_cache_size=2**30,
        disk_cache_dir="/tmp/xx",
        lru_capacity=400,
        block_size=2**20,
        aws_profile=None,
        logger=None,
    ):
        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger(__name__)

        self.schema = schema
        if schema == "http" or schema == "https":
            self.fetcher = HttpFetcher(self.logger)
        elif schema == "ftp":
            self.fetcher = FtpFetcher()
        elif schema == "s3":
            self.fetcher = S3Fetcher(aws_profile, self.logger)
        else:
            raise ValueError(f"Unknown schema: {schema}")

        self.logger.info(f"Starting with disk_cache_size: {disk_cache_size}")
        self.lru_cache = LRUCache(capacity=lru_capacity)
        self.lru_attrs = LRUCache(capacity=lru_capacity)
        self.disk_cache = dc.Cache(disk_cache_dir, size_limit=disk_cache_size)

        self.total_requests = 0
        self.total_blocks = 0
        self.lru_hits = 0
        self.lru_misses = 0
        self.disk_hits = 0
        self.disk_misses = 0
        self.block_size = block_size
        self.getting = set()

    def getattr(self, path, fh=None):
        if path in self.lru_attrs:
            return self.lru_attrs[path]

        if path == "/":
            self.lru_attrs[path] = dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)
            return self.lru_attrs[path]

        if path[-2:] != "..":
            return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

        url = f"{self.schema}:/{path[:-2]}"
        size = self.fetcher.get_size(url)
        if size is not None:
            self.lru_attrs[path] = dict(
                st_mode=(S_IFREG | 0o644),
                st_nlink=1,
                st_size=size,
                st_ctime=time(),
                st_mtime=time(),
                st_atime=time(),
            )
        else:
            self.lru_attrs[path] = dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

        return self.lru_attrs[path]

    def read(self, path, size, offset, fh):
        url = f"{self.schema}:/{path[:-2]}"
        self.total_requests += 1
        final_offset = offset + size

        self.logger.info(
            f"path: {path}\n"
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

            # Fetch the block
            while block_id in self.getting:
                sleep(0.05)
            self.getting.add(block_id)
            block = self.read_block(url, block_num)
            self.getting.remove(block_id)
            if not len(block):
                self.logger.info("empty block")
                break

            # Extract only the portion we need from this block
            output += block[block_start : block_start + data_size]
            curr_offset += data_size

        return output

    def read_block(self, url, block_num):
        """
        Get a data block from a URL. Blocks are 256K bytes in size

        Parameters:
        -----------
        url: string
            The url of the file we want to retrieve a block from
        block_num: int
            The # of the 256K'th block of this file
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
        block = self.fetcher.get_data(url, start, start + self.block_size - 1)
        self.lru_cache[cache_key] = block
        self.disk_cache[cache_key] = block
        return block

    def unlink(self, path):
        return 0

    def create(self, path, mode, fi=None):
        return 0

    def write(self, path, buf, size, offset, fip):
        return 0

    def destroy(self, path):
        self.disk_cache.close()
