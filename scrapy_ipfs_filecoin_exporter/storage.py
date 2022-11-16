from urllib.parse import urlparse

from scrapy.extensions.feedexport import BlockingFeedStorage
from scrapy.utils.project import get_project_settings


class W3SFeedStorage(BlockingFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        settings = get_project_settings()
        from .client import W3SClient

        u = urlparse(uri)
        self.file_name = u.path[1:]  # remove first "/"
        api_key = settings.get("W3S_API_KEY")
        self.client = W3SClient(api_key)

    def _store_in_thread(self, file):
        file.seek(0)
        self.client.upload(self.file_name, [file])
        file.close()


class EstuaryFeedStorage(BlockingFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        settings = get_project_settings()
        from .client import EstuaryClient

        u = urlparse(uri)
        self.file_name = u.path[1:]  # remove first "/"
        api_key = settings.get("ES_API_KEY")
        self.client = EstuaryClient(api_key)

    def _store_in_thread(self, file):
        file.seek(0)
        self.client.upload(self.file_name, [file])
        file.close()


class LightStorageFeedStorage(BlockingFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        settings = get_project_settings()
        from .client import LightHouseClient

        u = urlparse(uri)
        self.file_name = u.path[1:]  # remove first "/"
        api_key = settings.get("LH_API_KEY")
        self.client = LightHouseClient(api_key)

    def _store_in_thread(self, file):
        file.seek(0)
        self.client.upload(self.file_name, [file])
        file.close()
