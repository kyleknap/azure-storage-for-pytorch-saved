import functools
import io
import os
from typing import Optional

from azure.storage.blob import BlobClient
from azure.identity import DefaultAzureCredential


def open_blob(blob_url: str, mode: str) -> "BlobIO":
    blob_client = BlobClient.from_blob_url(
        blob_url, credential=DefaultAzureCredential()
    )
    return BlobIO(blob_client, mode)


class BlobIO(io.IOBase):
    _SUPPORTED_MODES = {"rb", "wb"}

    def __init__(self, blob_client: BlobClient, mode: str):
        self._blob_client = blob_client

        self._validate_mode(mode)
        self._mode = mode

        self._blob_reader = self._get_blob_reader()
        self._blob_writer = self._get_blob_writer()
        self._closed = False

    def close(self) -> None:
        self._close_blob_reader()
        self._close_blob_writer()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def fileno(self) -> int:
        raise OSError("BlobIO object has no fileno")

    def flush(self) -> None:
        self._validate_not_closed()
        if self.readable():
            return
        self._blob_writer.flush()

    def isatty(self) -> bool:
        return False

    def read(self, size=-1, /) -> bytes:
        self._validate_not_closed()
        self._validate_readable()
        return self._blob_reader.read(size)

    def readable(self) -> bool:
        return self._mode == "rb"

    def seek(self, offset, whence=os.SEEK_SET, /) -> int:
        self._validate_not_closed()
        self._validate_seekable()
        return self._blob_reader.seek(offset, whence)

    def seekable(self) -> bool:
        return self.readable()

    def tell(self) -> int:
        self._validate_not_closed()
        self._validate_seekable()
        return self._blob_reader.tell()

    def write(self, b, /) -> int:
        self._validate_not_closed()
        self._validate_writable()
        return self._blob_writer.write(b)

    def writable(self) -> bool:
        return self._mode == "wb"

    def _validate_mode(self, mode) -> None:
        if mode not in self._SUPPORTED_MODES:
            raise ValueError(f"Unsupported mode: {mode}")

    def _validate_not_closed(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file")

    def _validate_readable(self) -> None:
        if not self.readable():
            raise io.UnsupportedOperation("read")

    def _validate_seekable(self) -> None:
        if not self.seekable():
            raise io.UnsupportedOperation("seek")

    def _validate_writable(self) -> None:
        if not self.writable():
            raise io.UnsupportedOperation("write")

    def _get_blob_reader(self) -> Optional["_BlobReader"]:
        if self.readable():
            return _BlobReader(self._blob_client)
        return None

    def _get_blob_writer(self) -> Optional["_BlobWriter"]:
        if self.writable():
            return _BlobWriter(self._blob_client)
        return None

    def _close_blob_reader(self) -> None:
        if self._blob_reader is not None:
            self._blob_reader.close()
            self._blob_reader = None

    def _close_blob_writer(self) -> None:
        if self._blob_writer is not None:
            self._blob_writer.close()
            self._blob_writer = None


class _BlobReader:
    _DEFAULT_MAX_CONCURRENCY = 8

    def __init__(self, blob_client: BlobClient):
        self._blob_client = blob_client
        self._position = 0
        self._prev_read_position = 0
        self._downloader = None

    def read(self, size=-1) -> bytes:
        if self._position >= self._blob_length:
            return b""
        if self._downloader is None or self._prev_read_position != self._position:
            self._downloader = self._get_downloader()
        content = self._downloader.read(size)
        self._position += len(content)
        self._prev_read_position = self._position
        return content

    def seek(self, offset, whence=os.SEEK_SET) -> int:
        new_position = self._compute_new_position(offset, whence)
        if new_position < 0:
            raise ValueError("Cannot seek to negative position")
        self._position = new_position
        return self._position

    def tell(self) -> int:
        return self._position

    def close(self) -> None:
        self._downloader = None

    @functools.cached_property
    def _blob_length(self) -> int:
        if self._downloader is None:
            return self._blob_client.get_blob_properties().size
        return self._downloader.size

    def _get_downloader(self):
        return self._blob_client.download_blob(
            max_concurrency=self._DEFAULT_MAX_CONCURRENCY,
            offset=self._position,
        )

    def _compute_new_position(self, offset, whence):
        if whence == os.SEEK_SET:
            return offset
        if whence == os.SEEK_CUR:
            return self._position + offset
        if whence == os.SEEK_END:
            return self._blob_length + offset
        raise ValueError(f"Unsupported whence: {whence}")


class _BlobWriter:
    def __init__(self, blob_client: BlobClient):
        self._blob_client = blob_client

    def write(self, b) -> int:
        return len(b)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass
