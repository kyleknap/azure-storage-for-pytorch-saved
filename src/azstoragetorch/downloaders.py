import asyncio

from azure.storage.blob import BlobClient
from azure.storage.blob.aio import BlobClient as AsyncBlobClient
from azure.identity import DefaultAzureCredential
from azure.identity.aio import DefaultAzureCredential as AsyncDefaultAzureCredential


class BaseDownloader:
    @classmethod
    def from_blob_url(cls, blob_url):
        raise NotImplementedError("from_blob_url")

    def download(self, pos, length):
        raise NotImplementedError("download")

    def get_blob_size(self):
        raise NotImplementedError("get_blob_size")

    def close(self):
        pass


class SyncSDKDownloader(BaseDownloader):
    _DEFAULT_MAX_CONCURRENCY = 8

    def __init__(self, blob_client):
        self._blob_client = blob_client
        self._blob_size = None

    @classmethod
    def from_blob_url(cls, blob_url):
        blob_client = BlobClient.from_blob_url(
            blob_url, credential=DefaultAzureCredential(),
            connection_data_block_size=64 * 1024,
        )
        return cls(blob_client)

    def download(self, pos, length):
        sdk_downloader = self._blob_client.download_blob(
            max_concurrency=self._DEFAULT_MAX_CONCURRENCY,
            offset=pos,
            length=length
        )
        if self._blob_size is None:
            self._blob_size = sdk_downloader.size
        return sdk_downloader.read()

    def get_blob_size(self):
        if self._blob_size is None:
            self._blob_size = self._blob_client.get_blob_properties().size
        return self._blob_size


class AsyncSDKDownloader(BaseDownloader):
    _DEFAULT_MAX_CONCURRENCY = 32

    def __init__(self, blob_client):
        self._blob_client = blob_client
        self._blob_size = None
        self._runner = asyncio.Runner()

    @classmethod
    def from_blob_url(cls, blob_url):
        blob_client = AsyncBlobClient.from_blob_url(
            blob_url, credential=AsyncDefaultAzureCredential(),
            connection_data_block_size=64 * 1024,
        )
        return cls(blob_client)

    def download(self, pos, length):
        sdk_downloader = self._runner.run(self._get_downloader(pos, length))
        if self._blob_size is None:
            self._blob_size = sdk_downloader.size
        return self._runner.run(self._read(sdk_downloader))

    def get_blob_size(self):
        if self._blob_size is None:
            self._blob_size = self._runner.run(self._get_blob_properties()).size
        return self._blob_size

    def close(self):
        self._runner.run(self._close_client())
        self._runner.close()

    async def _get_blob_properties(self):
        return await self._blob_client.get_blob_properties()

    async def _get_downloader(self, pos, size):
        return await self._blob_client.download_blob(
            max_concurrency=self._DEFAULT_MAX_CONCURRENCY,
            offset=pos,
            length=size,
        )

    async def _read(self, sdk_downloader):
        return await sdk_downloader.read()

    async def _close_client(self):
        await self._blob_client.close()
