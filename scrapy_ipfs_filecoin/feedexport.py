import logging
from urllib.parse import urlparse

from scrapy.extensions.feedexport import BlockingFeedStorage
from scrapy.utils.project import get_project_settings

logger = logging.getLogger(__name__)


class Web3StorageFeedStorage(BlockingFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        settings = get_project_settings()
        from .client import Web3StorageClient

        u = urlparse(uri)
        self.file_name = u.path if u.path else u.netloc
        api_key = settings.get("W3S_API_KEY")
        self.client = Web3StorageClient(api_key)

    def _store_in_thread(self, file):
        file.seek(0)
        cid = self.client.upload(self.file_name, file)
        ipfs_url = self.client.get_url(cid)
        logging.info(ipfs_url)
        file.close()


class EstuaryFeedStorage(BlockingFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        settings = get_project_settings()
        from .client import EstuaryClient

        u = urlparse(uri)
        self.file_name = u.path if u.path else u.netloc
        api_key = settings.get("ES_API_KEY")
        self.client = EstuaryClient(api_key)

    def _store_in_thread(self, file):
        file.seek(0)
        cid = self.client.upload(self.file_name, file)
        ipfs_url = self.client.get_url(cid)
        logging.info(ipfs_url)
        file.close()


class LightStorageFeedStorage(BlockingFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        settings = get_project_settings()
        from .client import LightHouseClient

        u = urlparse(uri)
        self.file_name = u.path if u.path else u.netloc
        api_key = settings.get("LH_API_KEY")
        self.client = LightHouseClient(api_key)

    def _store_in_thread(self, file):
        file.seek(0)
        cid = self.client.upload(self.file_name, file)
        ipfs_url = self.client.get_url(cid)
        logging.info(ipfs_url)
        file.close()


def get_feed_storages():
    return {
        '': 'scrapy_ipfs_filecoin.feedexport.Web3StorageFeedStorage',
        'w3s': 'scrapy_ipfs_filecoin.feedexport.Web3StorageFeedStorage',
        'lh': 'scrapy_ipfs_filecoin.feedexport.LightStorageFeedStorage',
        'es': 'scrapy_ipfs_filecoin.feedexport.EstuaryFeedStorage',
    }
