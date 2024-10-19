from azure.storage.blob import BlobClient
from azure.identity import DefaultAzureCredential


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
