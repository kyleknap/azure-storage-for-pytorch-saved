# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import io
import os
from typing import get_args, Optional, Union, Literal
import urllib.parse

from azure.identity import DefaultAzureCredential
from azure.core.credentials import (
    AzureSasCredential,
    TokenCredential,
)


_SUPPORTED_MODES = Literal["rb"]
_SDK_CREDENTIAL_TYPE = Optional[
    Union[
        AzureSasCredential,
        TokenCredential,
    ]
]
_AZSTORAGETORCH_CREDENTIAL_TYPE = Union[_SDK_CREDENTIAL_TYPE, Literal[False]]


class BlobIO(io.IOBase):
    def __init__(
        self,
        blob_url: str,
        mode: _SUPPORTED_MODES,
        *,
        credential: _AZSTORAGETORCH_CREDENTIAL_TYPE = None,
    ):
        self._blob_url = blob_url
        self._validate_mode(mode)
        self._mode = mode
        self._sdk_credential = self._get_sdk_credential(blob_url, credential)

        self._blob_downloader = self._get_blob_downloader()
        self._position = 0
        self._closed = False

    def close(self) -> None:
        self._close_blob_downloader()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def fileno(self) -> int:
        raise OSError("BlobIO object has no fileno")

    def flush(self) -> None:
        self._validate_not_closed()

    def read(self, size: Optional[int] = -1, /) -> bytes:
        if size is not None:
            self._validate_is_integer("size", size)
            self._validate_min("size", size, -1)
        self._validate_not_closed()
        return self._read(size)

    def readable(self) -> bool:
        self._validate_not_closed()
        return self._mode == "rb"

    def readline(self, size: Optional[int] = -1, /) -> bytes:
        # BaseIO includes a default implementation of readline() that calls
        # read() with a size of 1 until it finds a newline character. As of now, this has very poor
        # performance implications as one read() will result in at least one HTTP GET request for each
        # read() call.
        #
        # Instead of allowing the default implementation and having users impacted by these
        # implications, we raise a NotImplementedError so that readline() and any other methods
        # that are automatically implemented from readline() (e.g. readlines() and __next__()) can't
        # be used until we implement a more appropriate strategy.
        raise NotImplementedError("readline")

    def seek(self, offset: int, whence: int = os.SEEK_SET, /) -> int:
        self._validate_is_integer("offset", offset)
        self._validate_is_integer("whence", whence)
        self._validate_not_closed()
        return self._seek(offset, whence)

    def seekable(self) -> bool:
        return self.readable()

    def tell(self) -> int:
        self._validate_not_closed()
        return self._position

    def write(self, b: Union[bytes, bytearray], /) -> int:
        raise NotImplementedError("write")

    def writable(self) -> bool:
        return False

    def _validate_mode(self, mode: str) -> None:
        if mode not in get_args(_SUPPORTED_MODES):
            raise ValueError(f"Unsupported mode: {mode}")

    def _validate_is_integer(self, param_name: str, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError(f"{param_name} must be an integer, not: {type(value)}")

    def _validate_min(self, param_name: str, value: int, min_value: int) -> None:
        if value < min_value:
            raise ValueError(
                f"{param_name} must be greater than or equal to {min_value}"
            )

    def _validate_not_closed(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file")

    def _get_sdk_credential(
        self, blob_url: str, credential: _AZSTORAGETORCH_CREDENTIAL_TYPE
    ) -> _SDK_CREDENTIAL_TYPE:
        if credential is False or self._blob_url_has_sas_token(blob_url):
            return None
        if credential is None:
            return DefaultAzureCredential()
        if isinstance(credential, (AzureSasCredential, TokenCredential)):
            return credential
        raise TypeError(f"Unsupported credential: {type(credential)}")

    def _blob_url_has_sas_token(self, blob_url: str) -> bool:
        parsed_url = urllib.parse.urlparse(blob_url)
        if parsed_url.query is None:
            return False
        parsed_qs = urllib.parse.parse_qs(parsed_url.query)
        # The signature is always required in a valid SAS token. So look for the "sig"
        # key to determine if the URL has a SAS token.
        return "sig" in parsed_qs

    def _get_blob_downloader(self) -> "_BlobDownloader":
        return _BlobDownloader(self._blob_url, self._sdk_credential)

    def _read(self, size: Optional[int]) -> bytes:
        if size == 0 or self._position >= self._blob_downloader.get_blob_size():
            return b""
        download_length = size
        if size is not None and size < 0:
            download_length = None
        content = self._blob_downloader.download(
            offset=self._position, length=download_length
        )
        self._position += len(content)
        return content

    def _seek(self, offset: int, whence: int) -> int:
        new_position = self._compute_new_position(offset, whence)
        if new_position < 0:
            raise ValueError("Cannot seek to negative position")
        self._position = new_position
        return self._position

    def _compute_new_position(self, offset: int, whence: int) -> int:
        if whence == os.SEEK_SET:
            return offset
        if whence == os.SEEK_CUR:
            return self._position + offset
        if whence == os.SEEK_END:
            return self._blob_downloader.get_blob_size() + offset
        raise ValueError(f"Unsupported whence: {whence}")

    def _close_blob_downloader(self) -> None:
        if not self._closed:
            self._blob_downloader.close()


class _BlobDownloader:
    def __init__(self, blob_url: str, sdk_credential: _SDK_CREDENTIAL_TYPE):
        self._blob_url = blob_url
        self._sdk_credential = sdk_credential

    def get_blob_size(self) -> int:
        raise NotImplementedError("get_blob_size")

    def download(self, offset: int = 0, length: Optional[int] = None) -> bytes:
        raise NotImplementedError("download")

    def close(self) -> None:
        raise NotImplementedError("close")
