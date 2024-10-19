import io
import os
from typing import Optional

from azure.storage.blob import BlobClient

import azstoragetorch.downloaders


class BlobIO(io.IOBase):
    _SUPPORTED_MODES = {"rb", "wb"}

    def __init__(self, blob_url: str, mode: str):
        self._blob_url = blob_url
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
            return _BlobReader(self._blob_url)
        return None

    def _get_blob_writer(self) -> Optional["_BlobWriter"]:
        if self.writable():
            return _BlobWriter(self._blob_url)
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
    _DOWNLOADER_CLS = azstoragetorch.downloaders.AsyncSDKProcessPoolDownloader

    def __init__(self, blob_url: str):
        self._position = 0
        self._downloader = self._DOWNLOADER_CLS.from_blob_url(blob_url)

    def read(self, size=-1) -> bytes:
        if self._position >= self._downloader.get_blob_size():
            return b""
        content = self._downloader.download(self._position, size)
        self._position += len(content)
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
        self._downloader.close()

    def _compute_new_position(self, offset, whence):
        if whence == os.SEEK_SET:
            return offset
        if whence == os.SEEK_CUR:
            return self._position + offset
        if whence == os.SEEK_END:
            return self._downloader.get_blob_size() + offset
        raise ValueError(f"Unsupported whence: {whence}")


class _BlobWriter:
    def __init__(self, blob_url: str):
        self._blob_url = blob_url

    def write(self, b) -> int:
        return len(b)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass
