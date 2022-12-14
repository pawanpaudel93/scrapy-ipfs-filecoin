import logging
import tempfile
from urllib.parse import urlparse

from scrapy.extensions.feedexport import BlockingFeedStorage
from scrapy.extensions.feedexport import S3FeedStorage as ParentS3FeedStorage
from scrapy.extensions.feedexport import build_storage
from scrapy.utils.project import get_project_settings

logger = logging.getLogger(__name__)


class IPFSFeedStorage(BlockingFeedStorage):
    def _store_in_thread(self, file):
        file.seek(0)
        cid = self.client.upload(self.file_name, file)
        ipfs_url = self.client.get_url(cid)
        logging.info(ipfs_url)
        file.close()


class Web3StorageFeedStorage(IPFSFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        settings = get_project_settings()
        from .client import Web3StorageClient

        u = urlparse(uri)
        self.file_name = u.path if u.path else u.netloc
        api_key = settings.get("W3S_API_KEY")
        self.client = Web3StorageClient(api_key)


class EstuaryFeedStorage(IPFSFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        settings = get_project_settings()
        from .client import EstuaryClient

        u = urlparse(uri)
        self.file_name = u.path if u.path else u.netloc
        api_key = settings.get("ES_API_KEY")
        self.client = EstuaryClient(api_key)


class LightStorageFeedStorage(IPFSFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        settings = get_project_settings()
        from .client import LightHouseClient

        u = urlparse(uri)
        self.file_name = u.path if u.path else u.netloc
        api_key = settings.get("LH_API_KEY")
        self.client = LightHouseClient(api_key)


class PinataFeedStorage(IPFSFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        settings = get_project_settings()
        from .client import PinataClient

        u = urlparse(uri)
        self.file_name = u.path if u.path else u.netloc
        api_key = settings.get("PN_JWT_TOKEN")
        self.client = PinataClient(api_key)


class MoralisFeedStorage(IPFSFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        settings = get_project_settings()
        from .client import MoralisClient

        u = urlparse(uri)
        self.file_name = u.path if u.path else u.netloc
        api_key = settings.get("MS_API_KEY")
        self.client = MoralisClient(api_key)


class S3FeedStorage(ParentS3FeedStorage):
    def __init__(
        self,
        uri,
        access_key=None,
        secret_key=None,
        acl=None,
        endpoint_url=None,
        *,
        feed_options=None,
        session_token=None,
        ipfs_url_format=None,
    ):
        from .client import S3Client

        self.client = S3Client(ipfs_url_format)
        super().__init__(
            uri, access_key, secret_key, acl, endpoint_url, feed_options=feed_options, session_token=session_token
        )

    @classmethod
    def from_crawler(cls, crawler, uri, *, feed_options=None):
        return build_storage(
            cls,
            uri,
            access_key=crawler.settings['S3_ACCESS_KEY_ID'],
            secret_key=crawler.settings['S3_SECRET_ACCESS_KEY'],
            session_token=crawler.settings['S3_SESSION_TOKEN'],
            acl=crawler.settings['FEED_STORAGE_S3_ACL'] or None,
            endpoint_url=crawler.settings['S3_ENDPOINT_URL'] or None,
            feed_options=feed_options,
            ipfs_url_format=crawler.settings['S3_IPFS_URL_FORMAT'] or None,
        )

    def _store_in_thread(self, file):
        with tempfile.NamedTemporaryFile() as temp:
            file.seek(0)
            temp.write(file.read())
            cid = self.client.cid_hash(temp.name, 0)
            ipfs_url = self.client.get_url(cid)
        file.seek(0)
        kwargs = {'ACL': self.acl} if self.acl else {}
        self.s3_client.put_object(Bucket=self.bucketname, Key=self.keyname, Body=file, **kwargs)
        logging.info(ipfs_url)
        file.close()


def get_feed_storages():
    return {
        '': 'scrapy_ipfs_filecoin.feedexport.Web3StorageFeedStorage',
        'w3s': 'scrapy_ipfs_filecoin.feedexport.Web3StorageFeedStorage',
        'lh': 'scrapy_ipfs_filecoin.feedexport.LightStorageFeedStorage',
        'es': 'scrapy_ipfs_filecoin.feedexport.EstuaryFeedStorage',
        'pn': 'scrapy_ipfs_filecoin.feedexport.PinataFeedStorage',
        'ms': 'scrapy_ipfs_filecoin.feedexport.MoralisFeedStorage',
        's3': 'scrapy_ipfs_filecoin.feedexport.S3FeedStorage',
    }
