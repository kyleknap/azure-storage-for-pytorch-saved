# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------
import io
import os
from unittest import mock
import pytest

from azure.core.credentials import AzureSasCredential, AzureNamedKeyCredential
from azure.identity import DefaultAzureCredential

from azstoragetorch.io import BlobIO


@pytest.fixture
def blob_url():
    return "https://myaccount.blob.core.windows.net/mycontainer/myblob"


@pytest.fixture
def blob_content():
    return b"blob content"


@pytest.fixture
def sas_token():
    return "sp=r&st=2024-10-28T20:22:30Z&se=2024-10-29T04:22:30Z&spr=https&sv=2022-11-02&sr=c&sig=signature"


@pytest.fixture
def blob_length(blob_content):
    return len(blob_content)


# @pytest.fixture
# def blob_io(blob_url):
#     return BlobIO(blob_url, mode="rb")


# @pytest.fixture
# def blob_io(blob_content):
#     return io.BytesIO(blob_content)


@pytest.fixture
def blob_io(blob_content, tmp_path):
    blob_path = tmp_path / "blob"
    blob_path.write_bytes(blob_content)
    with open(blob_path, "rb") as f:
        yield f


# Note: Not ideal to be patching a private class, but this is needed until the _BlobDownloader class is
# fleshed out and no longer raising NotImplementedErrors. We should be able to switch this patch to the Blob SDK client
# or remove the patch entirely if we allow BlobIO to accept different types of downloaders.
@pytest.fixture(autouse=True)
def mock_blob_downloader(blob_content, blob_length):
    with mock.patch("azstoragetorch.io._BlobDownloader", spec=True) as mock_downloader:
        mock_downloader.return_value = mock_downloader
        mock_downloader.get_blob_size.return_value = blob_length
        mock_downloader.download.return_value = blob_content
        yield mock_downloader


class TestBlobIO:
    def test_credential_defaults_to_azure_default_credential(
        self, blob_url, mock_blob_downloader
    ):
        BlobIO(blob_url, mode="rb")
        mock_blob_downloader.assert_called_once_with(blob_url, mock.ANY)
        creds = mock_blob_downloader.call_args[0][1]
        assert isinstance(creds, DefaultAzureCredential)

    @pytest.mark.parametrize(
        "credential",
        [
            DefaultAzureCredential(),
            AzureSasCredential("sas"),
        ],
    )
    def test_respects_user_provided_credential(
        self, blob_url, mock_blob_downloader, credential
    ):
        BlobIO(blob_url, mode="rb", credential=credential)
        mock_blob_downloader.assert_called_once_with(blob_url, credential)

    def test_anonymous_credential(self, blob_url, mock_blob_downloader):
        BlobIO(blob_url, mode="rb", credential=False)
        mock_blob_downloader.assert_called_once_with(blob_url, None)

    def test_detects_sas_token_in_blob_url(
        self, blob_url, mock_blob_downloader, sas_token
    ):
        BlobIO(blob_url + "?" + sas_token, mode="rb")
        # The SDK prefers the explict credential over the one in the URL. So if a SAS token is
        # in the URL, we do not want it automatically injecting the DefaultAzureCredential.
        mock_blob_downloader.assert_called_once_with(blob_url + "?" + sas_token, None)

    def test_credential_defaults_to_azure_default_credential_for_snapshot_url(
        self, blob_url, mock_blob_downloader
    ):
        snapshot_url = f"{blob_url}?snapshot=2024-10-28T20:34:36.1724588Z"
        BlobIO(snapshot_url, mode="rb")
        mock_blob_downloader.assert_called_once_with(snapshot_url, mock.ANY)
        creds = mock_blob_downloader.call_args[0][1]
        assert isinstance(creds, DefaultAzureCredential)

    @pytest.mark.parametrize(
        "credential",
        [
            "key",
            {"account_name": "name", "account_key": "key"},
            AzureNamedKeyCredential("name", "key"),
        ],
    )
    def test_raises_for_unsupported_credential(self, blob_url, credential):
        with pytest.raises(TypeError, match="Unsupported credential"):
            BlobIO(blob_url, mode="rb", credential=credential)

    @pytest.mark.parametrize(
        "unsupported_mode",
        [
            "r",
            "r+",
            "r+b",
            "w",
            "wb",
            "w+",
            "w+b",
            "a",
            "ab",
            "a+",
            "a+b",
            "x",
            "xb",
            "x+",
            "x+b",
            "unknownmode",
        ],
    )
    def test_raises_for_unsupported_mode(self, blob_url, unsupported_mode):
        with pytest.raises(ValueError, match="Unsupported mode"):
            BlobIO(blob_url, mode=unsupported_mode)

    def test_close(self, blob_io, mock_blob_downloader):
        assert not blob_io.closed
        blob_io.close()
        assert blob_io.closed
        # mock_blob_downloader.close.assert_called_once()

    def test_can_call_close_multiple_times(self, blob_io, mock_blob_downloader):
        blob_io.close()
        blob_io.close()
        assert blob_io.closed
        # mock_blob_downloader.close.assert_called_once()

    def test_context_manager_closes_blob_io(self, blob_io, mock_blob_downloader):
        assert not blob_io.closed
        with blob_io:
            pass
        assert blob_io.closed
        # mock_blob_downloader.close.assert_called_once()

    def test_del_closes_blob_io(self, blob_io, mock_blob_downloader):
        assert not blob_io.closed
        blob_io.__del__()
        assert blob_io.closed
        # mock_blob_downloader.close.assert_called_once()

    @pytest.mark.parametrize(
        "method,args",
        [
            ("isatty", []),
            ("flush", []),
            ("read", []),
            ("readable", []),
            ("seek", [1]),
            ("seekable", []),
            ("tell", []),
        ],
    )
    def test_raises_after_close(self, blob_io, method, args):
        blob_io.close()
        with pytest.raises(ValueError, match="closed file"):
            getattr(blob_io, method)(*args)

    # def test_fileno_raises(self, blob_io):
    #     with pytest.raises(OSError, match="fileno"):
    #         blob_io.fileno()

    def test_isatty(self, blob_io):
        assert not blob_io.isatty()

    def test_flush_on_readable_is_noop(self, blob_url):
        blob_io = BlobIO(blob_url, mode="rb")
        try:
            blob_io.flush()
        except Exception as e:
            pytest.fail(
                f"Unexpected exception: {e}. flush() should be a no-op for readable BlobIO objects."
            )

    def test_readable(self, blob_url):
        blob_io = BlobIO(blob_url, mode="rb")
        assert blob_io.readable()

    def test_read(self, blob_io, blob_content, mock_blob_downloader):
        assert blob_io.read() == blob_content
        assert blob_io.tell() == len(blob_content)
        # mock_blob_downloader.download.assert_called_once_with(offset=0, length=None)

    def test_read_with_size(self, blob_io, blob_content, mock_blob_downloader):
        mock_blob_downloader.download.return_value = blob_content[:1]
        assert blob_io.read(1) == blob_content[:1]
        assert blob_io.tell() == 1
        # mock_blob_downloader.download.assert_called_once_with(offset=0, length=1)

    def test_read_multiple_times(self, blob_io, blob_content, mock_blob_downloader):
        mock_blob_downloader.download.side_effect = [
            blob_content[:1],
            blob_content[1:2],
            blob_content[2:],
        ]
        assert blob_io.read(1) == blob_content[:1]
        assert blob_io.read(1) == blob_content[1:2]
        assert blob_io.read() == blob_content[2:]
        # assert mock_blob_downloader.download.call_args_list == [
        #     mock.call(offset=0, length=1),
        #     mock.call(offset=1, length=1),
        #     mock.call(offset=2, length=None),
        # ]
        assert blob_io.tell() == len(blob_content)

    def test_read_after_seek(self, blob_io, blob_content, mock_blob_downloader):
        offset = 2
        mock_blob_downloader.download.return_value = blob_content[offset:]
        assert blob_io.seek(offset) == offset
        assert blob_io.read() == blob_content[offset:]
        assert blob_io.tell() == len(blob_content)
        # mock_blob_downloader.download.assert_called_once_with(
        #     offset=offset, length=None
        # )

    def test_read_beyond_end(self, blob_io, blob_content, mock_blob_downloader):
        assert blob_io.read() == blob_content
        assert blob_io.read() == b""
        assert blob_io.tell() == len(blob_content)
        # mock_blob_downloader.download.assert_called_once_with(offset=0, length=None)

    def test_read_size_zero(self, blob_io, mock_blob_downloader):
        assert blob_io.read(0) == b""
        assert blob_io.tell() == 0
        mock_blob_downloader().download.assert_not_called()

    @pytest.mark.parametrize("size", [-1, None])
    def test_read_size_synonyms_for_read_all(self, blob_io, mock_blob_downloader, size):
        assert blob_io.read(size) == b"blob content"
        # mock_blob_downloader.download.assert_called_once_with(offset=0, length=None)

    @pytest.mark.parametrize("size", [0.5, "1"])
    def test_read_raises_for_unsupported_size_types(self, blob_io, size):
        with pytest.raises(TypeError, match="should be integer"):
            blob_io.read(size)

    def test_read_raises_for_less_than_negative_one_size(self, blob_io):
        with pytest.raises(ValueError, match="length must be non-negative or -1"):
            blob_io.read(-2)

    # def test_readline_not_implemented(self, blob_io):
    #     with pytest.raises(NotImplementedError, match="readline"):
    #         blob_io.readline()
    #
    # def test_readlines_not_implemented(self, blob_io):
    #     with pytest.raises(NotImplementedError, match="readline"):
    #         blob_io.readlines()
    #
    # def test_next_not_implemented(self, blob_io):
    #     blob_iter = iter(blob_io)
    #     with pytest.raises(NotImplementedError, match="readline"):
    #         next(blob_iter)

    def test_seekable(self, blob_url):
        blob_io = BlobIO(blob_url, mode="rb")
        assert blob_io.seekable()

    def test_seek(self, blob_io):
        assert blob_io.seek(1) == 1
        assert blob_io.tell() == 1

    def test_seek_multiple_times(self, blob_io):
        assert blob_io.seek(1) == 1
        assert blob_io.tell() == 1
        assert blob_io.seek(2) == 2
        assert blob_io.tell() == 2
        assert blob_io.seek(0) == 0
        assert blob_io.tell() == 0

    def test_seek_beyond_end(self, blob_io, blob_length, mock_blob_downloader):
        # Note: Sort of quirky behavior that you can seek past the end of a
        # file and return a position that is larger than the size of the file.
        # However, this was chosen to be consistent in behavior with file-like
        # objects from open()
        assert blob_io.seek(blob_length + 1) == blob_length + 1
        assert blob_io.tell() == blob_length + 1
        assert blob_io.read(1) == b""
        mock_blob_downloader.download.assert_not_called()

    def test_seek_cur(self, blob_io):
        assert blob_io.seek(1, os.SEEK_CUR) == 1
        assert blob_io.tell() == 1
        assert blob_io.seek(1, os.SEEK_CUR) == 2
        assert blob_io.tell() == 2

    def test_seek_end(self, blob_io, blob_length):
        assert blob_io.seek(0, os.SEEK_END) == blob_length
        assert blob_io.tell() == blob_length

    def test_seek_negative_offset(self, blob_io, blob_length):
        assert blob_io.seek(-1, os.SEEK_END) == blob_length - 1
        assert blob_io.tell() == blob_length - 1

    def test_seek_raises_when_results_in_negative_position(self, blob_io):
        with pytest.raises(OSError):
            blob_io.seek(-1)

    def test_seek_raises_for_unsupported_whence(self, blob_io):
        with pytest.raises(ValueError, match="whence"):
            blob_io.seek(0, 100)

    @pytest.mark.parametrize(
        "offset,whence", [(0.5, 0), ("1", 0), (None, 0), (0, 0.5), (0, "1"), (0, None)]
    )
    def test_seek_raises_for_unsupported_arg_types(self, blob_io, offset, whence):
        with pytest.raises(TypeError, match="integer"):
            blob_io.seek(offset, whence)

    def test_tell_starts_at_zero(self, blob_io):
        assert blob_io.tell() == 0

    def test_writeable(self, blob_url):
        blob_io = BlobIO(blob_url, mode="rb")
        assert not blob_io.writable()
