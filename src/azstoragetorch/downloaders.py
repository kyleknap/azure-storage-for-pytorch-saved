import asyncio
import concurrent.futures
import math
import copy

from azure.storage.blob import BlobClient
from azure.storage.blob.aio import BlobClient as AsyncBlobClient
from azure.identity import DefaultAzureCredential
from azure.identity.aio import DefaultAzureCredential as AsyncDefaultAzureCredential


class BaseDownloader:
    @classmethod
    def from_blob_url(cls, blob_url, credential=None):
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
    def from_blob_url(cls, blob_url, credential=None):
        if credential is None:
            credential = DefaultAzureCredential()
        blob_client = BlobClient.from_blob_url(
            blob_url, credential=credential,
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
    def from_blob_url(cls, blob_url, credential=None):
        if credential is None:
            credential = AsyncDefaultAzureCredential()
        blob_client = AsyncBlobClient.from_blob_url(
            blob_url, credential=credential,
            # connection_data_block_size=64 * 1024,
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


class AsyncSDKProcessPoolDownloader(BaseDownloader):
    _NUM_WORKERS = 12
    _DEFAULT_MAX_CONCURRENCY = 12

    def __init__(self, blob_client, blob_url, credential):
        self._blob_client = blob_client
        self._blob_url = blob_url
        self._blob_size = None
        self._pool = concurrent.futures.ProcessPoolExecutor(
            max_workers=self._NUM_WORKERS,
            initializer=_init_process,
            initargs=(blob_url, copy.deepcopy(credential)),
        )
        self._runner = asyncio.Runner()

    @classmethod
    def from_blob_url(cls, blob_url, credential=None):
        if credential is None:
            credential = AsyncDefaultAzureCredential()
        blob_client = AsyncBlobClient.from_blob_url(
            blob_url, credential,
            # connection_data_block_size=64 * 1024,
        )
        return cls(blob_client, blob_url, credential)

    def download(self, pos, length):
        futures = []
        for partition in self._partition_read(pos, length):
            futures.append(self._pool.submit(_download, *partition))
        return b"".join(f.result() for f in futures)

    def get_blob_size(self):
        if self._blob_size is None:
            self._blob_size = self._runner.run(self._get_blob_properties()).size
        return self._blob_size

    def close(self):
        self._pool.shutdown()
        self._runner.run(self._close_client())
        self._runner.close()

    async def _get_blob_properties(self):
        return await self._blob_client.get_blob_properties()

    async def _close_client(self):
        await self._blob_client.close()

    def _partition_read(self, pos, length):
        blob_size = self.get_blob_size()
        if pos >= blob_size:
            return []

        if length < 4 * 1024 * 1024:
            return [(pos, length)]

        # TODO: Double check this
        if math.ceil(length / self._NUM_WORKERS) < 4 * 1024 * 1024:
            num_partitions = math.ceil(length / (4 * 1024 * 1024))
            partition_size = 4 * 1024 * 1024
        else:
            num_partitions = self._NUM_WORKERS
            partition_size = math.ceil(length / self._NUM_WORKERS)

        partitions = []

        for i in range(num_partitions):
            start = pos + i * partition_size
            if start >= blob_size:
                break
            end = min(start + partition_size, blob_size)
            partitions.append((start, end - start))

        return partitions


def _init_process(blob_url, credentials=None):
    global async_sdk_client
    async_sdk_client = AsyncBlobClient.from_blob_url(
        blob_url, credential=credentials,
        # connection_data_block_size=64 * 1024,
    )
    global async_runner
    async_runner = asyncio.Runner()


def _download(pos, length):
    global async_sdk_client
    global async_runner
    sdk_downloader = async_runner.run(
        _get_downloader(async_sdk_client, pos, length)
    )
    return async_runner.run(_read(sdk_downloader))


async def _get_downloader(blob_client, pos, size):
    return await blob_client.download_blob(
        max_concurrency=AsyncSDKProcessPoolDownloader._DEFAULT_MAX_CONCURRENCY,
        offset=pos,
        length=size,
    )

async def _read(sdk_downloader):
    return await sdk_downloader.read()
