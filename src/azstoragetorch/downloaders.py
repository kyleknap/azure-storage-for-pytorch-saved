import asyncio
import atexit
import concurrent.futures
import io
import math
import copy
import multiprocessing.shared_memory

from azure.storage.blob import BlobClient
from azure.storage.blob.aio import BlobClient as AsyncBlobClient
from azure.identity import DefaultAzureCredential
from azure.identity.aio import DefaultAzureCredential as AsyncDefaultAzureCredential

import certifi
import urllib3


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
    await sdk_downloader.read()
    return b""

class Urllib3Downloader(BaseDownloader):
    _WORKER_COUNT = 8
    _READ_SIZE = 64 * 1024
    _THRESHOLD = 4 * 1024 * 1024
    _PARTITION_SIZE = 4 * 1024 * 1024

    def __init__(self, blob_url, credential):
        self._blob_url = blob_url
        self._credential = credential
        self._blob_size = None
        self._request_pool = urllib3.PoolManager(
            maxsize=10,
            ca_certs=certifi.where(),
        )
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=self._WORKER_COUNT)

    @classmethod
    def from_blob_url(cls, blob_url, credential=None):
        return cls(blob_url, credential)

    def download(self, pos, length):
        if length < 4 * 1024 * 1024:
            return self._download(pos, length)
        futures = []
        for partition in self._partition_read(pos, length):
            futures.append(self._thread_pool.submit(self._download, *partition))
        return b"".join(f.result() for f in futures)

    def get_blob_size(self):
        if self._blob_size is None:
            self._blob_size = self._get_blob_size()
        return self._blob_size

    def close(self):
        self._thread_pool.shutdown()

    def _get_blob_size(self):
        resp = self._request_pool.request(
            "HEAD",
            f"{self._blob_url}?{self._credential}",
            headers={"x-ms-version": "2025-01-05"},
        )
        self._raise_for_status(resp)
        return int(resp.headers["Content-Length"])

    def _partition_read(self, pos, length):
        blob_size = self.get_blob_size()
        partition_size = 4 * 1024 * 1024
        num_partitions = math.ceil(length / (4 * 1024 * 1024))
        partitions = []
        for i in range(num_partitions):
            start = pos + i * partition_size
            if start >= blob_size:
                break
            end = min(start + partition_size, pos + length)
            partitions.append((start, end - start))
        return partitions

    def _download(self, pos, length):
        resp = self._request_pool.request(
            "GET",
            f"{self._blob_url}?{self._credential}",
            headers={
                "x-ms-version": "2025-01-05",
                "Range": f"bytes={pos}-{pos + length - 1}",
            },
            preload_content=False,
        )
        self._raise_for_status(resp)
        content = io.BytesIO()
        for chunk in resp.stream(self._READ_SIZE):
            content.write(chunk)
        ret = content.getvalue()
        return ret

    def _raise_for_status(self, response):
        if response.status >= 300:
            raise RuntimeError(f"Response failed: ({response.status}) {response.reason}")


class SyncLowLevelSDKDownloader(BaseDownloader):
    _DEFAULT_MAX_CONCURRENCY = 8
    _READ_SIZE = 64 * 1024
    _THRESHOLD = 4 * 1024 * 1024
    _PARTITION_SIZE = 8 * 1024 * 1024
    _EXECUTOR_CLS = concurrent.futures.ThreadPoolExecutor

    def __init__(self, blob_url, credential=None):
        self._blob_url = blob_url
        if credential is None:
            credential = DefaultAzureCredential()
        self._credential = credential
        blob_client = BlobClient.from_blob_url(
            blob_url, credential=credential,
            connection_data_block_size=64 * 1024,
        )
        self._blob_client = blob_client
        self._blob_size = None
        self._pool = self._EXECUTOR_CLS(max_workers=self._DEFAULT_MAX_CONCURRENCY)

    @classmethod
    def from_blob_url(cls, blob_url, credential=None):
        return cls(blob_url, credential)

    def download(self, pos, length):
        if length < self._THRESHOLD:
            return self._download(pos, length)
        futures = []
        for partition in self._partition_read(pos, length):
            futures.append(self._pool.submit(self._download, *partition))
        return b"".join(f.result() for f in futures)

    def get_blob_size(self):
        if self._blob_size is None:
            self._blob_size = self._blob_client.get_blob_properties().size
        return self._blob_size

    def close(self):
        self._pool.shutdown()

    def _partition_read(self, pos, length):
        blob_size = self.get_blob_size()
        num_partitions = math.ceil(length / (self._PARTITION_SIZE))
        partitions = []
        for i in range(num_partitions):
            start = pos + i * self._PARTITION_SIZE
            if start >= blob_size:
                break
            end = min(start + self._PARTITION_SIZE, pos + length)
            partitions.append((start, end - start))
        return partitions

    def _download(self, pos, length):
        stream = self._blob_client._client.blob.download(
            range=f"bytes={pos}-{pos + length - 1}",
        )
        content = io.BytesIO()
        for chunk in stream:
            content.write(chunk)
        ret = content.getvalue()
        return ret


class SyncLowLevelSDKProcessPoolDownloader(SyncLowLevelSDKDownloader):
    _DEFAULT_MAX_CONCURRENCY = 16
    _READ_SIZE = 64 * 1024
    _THRESHOLD = 8 * 1024 * 1024
    _PARTITION_SIZE = 8 * 1024 * 1024
    _EXECUTOR_CLS = concurrent.futures.ProcessPoolExecutor

    def __init__(self, blob_url, credential=None):
        self._blob_url = blob_url
        if credential is None:
            credential = DefaultAzureCredential()
        self._credential = credential
        blob_client = BlobClient.from_blob_url(
            blob_url, credential=credential,
            connection_data_block_size=64 * 1024,
        )
        self._blob_client = blob_client
        self._blob_size = None
        self._pool = self._EXECUTOR_CLS(
            max_workers=self._DEFAULT_MAX_CONCURRENCY,
            initializer=_init_sync_process,
            initargs=(blob_url, copy.deepcopy(credential)),
        )

    def download(self, pos, length):
        if length < self._THRESHOLD:
            return self._download(pos, length)
        futures = []
        for partition in self._partition_read(pos, length):
            futures.append(self._pool.submit(_sync_download, *partition))
        return b"".join(f.result() for f in futures)


def _init_sync_process(blob_url, credentials=None):
    global sync_sdk_client
    sync_sdk_client = BlobClient.from_blob_url(
        blob_url, credential=credentials,
        connection_data_block_size=64 * 1024,
    )


def _sync_download(pos, length):
    global sync_sdk_client
    stream = sync_sdk_client._client.blob.download(
        range=f"bytes={pos}-{pos + length - 1}",
    )
    content = io.BytesIO()
    for chunk in stream:
        content.write(chunk)
    ret = content.getvalue()
    # return b""
    return ret


class SharedMemoryProcessPoolDownloader(SyncLowLevelSDKDownloader):
    _DEFAULT_MAX_CONCURRENCY = 32
    _READ_SIZE = 64 * 1024
    _THRESHOLD = 8 * 1024 * 1024
    _PARTITION_SIZE = 8 * 1024 * 1024
    _EXECUTOR_CLS = concurrent.futures.ProcessPoolExecutor

    def __init__(self, blob_url, credential=None):
        self._blob_url = blob_url
        if credential is None:
            credential = DefaultAzureCredential()
        self._credential = credential
        blob_client = BlobClient.from_blob_url(
            blob_url, credential=credential,
            connection_data_block_size=64 * 1024,
        )
        self._blob_client = blob_client
        self._blob_size = None
        self._shm = multiprocessing.shared_memory.SharedMemory(create=True, size=self._PARTITION_SIZE)
        self._pool = self._EXECUTOR_CLS(
            max_workers=self._DEFAULT_MAX_CONCURRENCY,
            initializer=_init_sync_process,
            initargs=(blob_url, copy.deepcopy(credential)),
        )

    def download(self, pos, length):
        if length < self._THRESHOLD:
            return self._download(pos, length)
        futures = []
        shm_pos = 0
        new_shm_name = None
        if length > self._shm.size:
            new_shm_name=self._new_shm(length)
        for partition in self._partition_read(pos, length):
            futures.append(self._pool.submit(_sync_download_shm, shm_pos, *partition, new_shm_name=new_shm_name))
            shm_pos += partition[1]
        concurrent.futures.wait(futures)

        content = self._shm.buf[:length].tobytes()
        return content

    def close(self):
        super().close()
        self._cleanup_shm()

    def _cleanup_shm(self):
        self._shm.close()
        self._shm.unlink()

    def _new_shm(self, size):
        self._cleanup_shm()
        self._shm = multiprocessing.shared_memory.SharedMemory(create=True, size=size)
        return self._shm.name


def _init_shm_process(blob_url, shm_name, credentials=None):
    global sync_sdk_client
    sync_sdk_client = BlobClient.from_blob_url(
        blob_url, credential=credentials,
        connection_data_block_size=64 * 1024,
    )
    global shm
    shm = multiprocessing.shared_memory.SharedMemory(name=shm_name)
    atexit.register(_shm_cleanup)


def _sync_download_shm(shm_pos, pos, length, new_shm_name=None):
    global sync_sdk_client
    stream = sync_sdk_client._client.blob.download(
        range=f"bytes={pos}-{pos + length - 1}",
    )
    global shm
    if new_shm_name is not None:
        shm.close()
        shm = multiprocessing.shared_memory.SharedMemory(name=new_shm_name)
    for chunk in stream:
        shm.buf[shm_pos:shm_pos + len(chunk)] = chunk
        shm_pos += len(chunk)
    # content = b"".join(stream)
    # shm.buf[shm_pos:shm_pos + len(content)] = content
    # shm.close()


def _shm_cleanup():
    global shm
    if shm is not None:
        shm.close()
