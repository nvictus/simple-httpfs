from collections.abc import Buffer, Sequence
from datetime import datetime
from ftplib import FTP, error_perm
from urllib.parse import urlparse

from obspec import GetRange, Head, ListResult, ListWithDelimiter, ObjectMeta


class FTPStore(GetRange, Head, ListWithDelimiter):
    def __init__(self, url: str, chunk_size: int = 32 * 1024):
        o = urlparse(url)
        self.server = o.netloc
        self.path = o.path
        self.chunk_size = chunk_size

    def _open(self, server) -> FTP:
        ftp = FTP(server)
        ftp.login()

        # Set the transfer mode to binary.
        ftp.voidcmd("TYPE I")

        return ftp

    def head(self, path: str) -> ObjectMeta:
        fpath = "/".join((self.path, path)).replace("//", "/")
        ftp = self._open(self.server)

        try:
            size = ftp.size(fpath)
        except error_perm as e:
            raise FileNotFoundError from e

        # 213 File Status means a modification time was returned
        try:
            resp = ftp.sendcmd(f"MDTM {fpath}")
            if resp.startswith("213"):
                last_modified = datetime.strptime(resp[4:].strip(), "%Y%m%d%H%M%S")
            else:
                last_modified = datetime.now()
        except Exception:
            last_modified = datetime.now()

        ftp.close()

        return {
            "e_tag": None,
            "last_modified": last_modified,
            "path": fpath,
            "size": size,
            "version": None,
        }

    def get_range(
        self, path: str, start: int, end: int | None = None, length: int | None = None
    ) -> Buffer:
        fpath = "/".join((self.path, path)).replace("//", "/")
        ftp = self._open(self.server)
        if start > ftp.size(fpath):
            data = b""
        else:
            conn = ftp.transfercmd(f"RETR {fpath}", rest=start)
            if length is not None:
                end = start + length
            amt = end - start

            # Fetch the data in chunks.
            data = b""
            while len(data) < amt:
                chunk = conn.recv(self.chunk_size)
                if chunk:
                    data += chunk
                else:
                    break

            # Pad with null bytes if we didn't get enough data, or trim.
            if len(data) < amt:
                data += b"\x00" * (amt - len(data))
            else:
                data = data[:amt]

        ftp.close()
        return data

    def list_with_delimiter(
        self, prefix: str | None = None
    ) -> ListResult[Sequence[ObjectMeta]]:
        fpath = "/".join((self.path, prefix or "")).replace("//", "/")
        ftp = self._open(self.server)
        try:
            listing = ftp.mlsd(fpath)
        except error_perm:
            metadata = [self.head(fpath)]
        else:
            metadata = []
            for filename, facts in listing:
                if filename in [".", ".."]:
                    continue
                size = int(facts.get("size", 0))
                modify_time = facts.get("modify")
                if modify_time:
                    # MLSD returns format: YYYYMMDDHHMMSS or YYYYMMDDHHMMSS.sss
                    modify_str = modify_time.split(".")[0]  # Remove fractional seconds
                    last_modified = datetime.strptime(modify_str, "%Y%m%d%H%M%S")
                else:
                    last_modified = datetime.now()
                metadata.append(
                    {
                        "e_tag": None,
                        "last_modified": last_modified,
                        "path": filename,
                        "size": size,
                        "version": None,
                    }
                )
        ftp.close()
        return {"common_prefixes": [], "objects": metadata}
