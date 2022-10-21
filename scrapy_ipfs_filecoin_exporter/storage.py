from urllib.parse import urlparse

from scrapy.extensions.feedexport import BlockingFeedStorage


class W3SFeedStorage(BlockingFeedStorage):
    def __init__(self, uri, *, feed_options=None):
        from scrapy.crawler import settings
        from .client import WSClient

        u = urlparse(uri)
        self.file_name = u.path[1:]  # remove first "/"
        api_key = settings.get["W3_API_KEY"]
        self.ws_client = WSClient(api_key)

    def _store_in_thread(self, file):
        file.seek(0)
        self.ws_client.upload(self.file_name, [file])
        file.close()
