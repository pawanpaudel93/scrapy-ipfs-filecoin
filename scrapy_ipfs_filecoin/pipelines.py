import functools
import hashlib
import logging
import os
import tempfile
import warnings
from contextlib import suppress
from io import BytesIO
from urllib.parse import urlparse

from itemadapter import ItemAdapter
from scrapy.exceptions import NotConfigured, ScrapyDeprecationWarning
from scrapy.http import Request
from scrapy.pipelines.files import FileException
from scrapy.pipelines.files import FilesPipeline as ParentFilesPipeline
from scrapy.pipelines.files import FSFilesStore as ParentFSFilesStore
from scrapy.pipelines.files import S3FilesStore as ParentS3FilesStore
from scrapy.pipelines.images import ImageException
from scrapy.settings import Settings
from scrapy.utils.boto import is_botocore_available
from scrapy.utils.log import failure_to_exc_info
from scrapy.utils.python import get_func_args, to_bytes
from scrapy.utils.request import referer_str
from twisted.internet import defer, threads

logger = logging.getLogger(__name__)


class FSFilesStore(ParentFSFilesStore):
    API_KEY = None
    CIDS = {}

    def persist_file(self, path, buf, info, meta=None, headers=None):
        absolute_path = self._get_filesystem_path(path)
        self._mkdir(os.path.dirname(absolute_path), info)
        with open(absolute_path, 'wb') as f:
            f.write(buf.getvalue())
        filename = path.split("/")[-1]
        self.CIDS[path] = self.client.cid_hash(absolute_path)
        return threads.deferToThread(self.client.upload, name=filename, file=buf.getvalue())


class Web3StorageFilesStore(FSFilesStore):
    def __init__(self, basedir):
        from .client import Web3StorageClient

        super().__init__(basedir)
        self.client = Web3StorageClient(self.API_KEY)

    def stat_file(self, path, info):
        absolute_path = self._get_filesystem_path(path)
        if not os.path.exists(absolute_path):
            return {}

        cid = self.client.cid_hash(absolute_path)
        headers = self._get_response_headers(cid)
        if not headers.get("Content-Disposition"):
            return {}

        return {'cid': cid}

    def _get_response_headers(self, cid):
        try:
            response = self.client.session.head(f"https://api.web3.storage/upload/car/{cid}", timeout=3)
            return response.headers if response.ok else {}
        except:
            return {}


class EstuaryFilesStore(FSFilesStore):
    def __init__(self, basedir):
        from .client import EstuaryClient

        super().__init__(basedir)
        self.client = EstuaryClient(self.API_KEY)

    def stat_file(self, path, info):
        absolute_path = self._get_filesystem_path(path)
        if not os.path.exists(absolute_path):
            return {}

        cid = self.client.cid_hash(absolute_path)
        headers = self._get_response_headers(cid)
        if not headers.get("Content-Length"):
            return {}
        elif headers.get("Content-Length") and int(headers.get("Content-Length")) == 0:
            return {}

        return {'cid': cid}

    def _get_response_headers(self, cid):
        try:
            response = self.client.session.get(f"https://api.estuary.tech/gw/ipfs/{cid}", stream=True, timeout=3)
            return response.headers if response.ok else {}
        except:
            return {}


class LightHouseFilesStore(FSFilesStore):
    def __init__(self, basedir):
        from .client import LightHouseClient

        super().__init__(basedir)
        self.client = LightHouseClient(self.API_KEY)

    def stat_file(self, path, info):
        absolute_path = self._get_filesystem_path(path)
        if not os.path.exists(absolute_path):
            return {}

        cid = self.client.cid_hash(absolute_path, 0)
        headers = self._get_response_headers(cid)
        if not headers.get("Etag") or not headers.get("etag"):
            return {}

        return {'cid': cid}

    def _get_response_headers(self, cid):
        try:
            response = self.client.session.head(f"https://gateway.lighthouse.storage/ipfs/{cid}", timeout=3)
            return response.headers if response.ok else {}
        except:
            return {}


class PinataFilesStore(FSFilesStore):
    def __init__(self, basedir):
        from .client import PinataClient

        super().__init__(basedir)
        self.client = PinataClient(self.API_KEY)

    def stat_file(self, path, info):
        absolute_path = self._get_filesystem_path(path)
        if not os.path.exists(absolute_path):
            return {}

        cid = self.client.cid_hash(absolute_path, 0)
        headers = self._get_response_headers(cid)
        if not headers.get("ETag"):
            return {}

        return {'cid': cid}

    def _get_response_headers(self, cid):
        try:
            response = self.client.session.head(f"https://gateway.pinata.cloud/ipfs/{cid}", timeout=3)
            return response.headers if response.ok else {}
        except:
            return {}


class MoralisFilesStore(FSFilesStore):
    def __init__(self, basedir):
        from .client import MoralisClient

        super().__init__(basedir)
        self.client = MoralisClient(self.API_KEY)

    def stat_file(self, path, info):
        absolute_path = self._get_filesystem_path(path)
        if not os.path.exists(absolute_path):
            return {}

        cid = self.client.cid_hash(absolute_path, 0)
        headers = self._get_response_headers(cid)
        if not headers.get("ETag"):
            return {}

        return {'cid': cid}

    def _get_response_headers(self, cid):
        try:
            response = self.client.session.head(f"https://ipfs.moralis.io:2053/ipfs/{cid}", timeout=3)
            return response.headers if response.ok else {}
        except:
            return {}


class S3FilesStore(ParentS3FilesStore):
    S3_ACCESS_KEY_ID = None
    S3_SECRET_ACCESS_KEY = None
    S3_SESSION_TOKEN = None
    S3_ENDPOINT_URL = None
    S3_REGION_NAME = None
    S3_USE_SSL = None
    S3_VERIFY = None
    S3_IPFS_URL_FORMAT = None
    CIDS = {}

    def __init__(self, uri):
        if not is_botocore_available():
            raise NotConfigured('missing botocore library')
        import botocore.session

        from .client import S3Client

        session = botocore.session.get_session()
        self.client = S3Client(self.S3_IPFS_URL_FORMAT)
        self.s3_client = session.create_client(
            's3',
            aws_access_key_id=self.S3_ACCESS_KEY_ID,
            aws_secret_access_key=self.S3_SECRET_ACCESS_KEY,
            aws_session_token=self.S3_SESSION_TOKEN,
            endpoint_url=self.S3_ENDPOINT_URL,
            region_name=self.S3_REGION_NAME,
            use_ssl=self.S3_USE_SSL if self.S3_USE_SSL != None else True,
            verify=self.S3_VERIFY,
        )
        if not uri.startswith("s3://"):
            raise ValueError(f"Incorrect URI scheme in {uri}, expected 's3'")
        self.bucket, self.prefix = uri[5:].split('/', 1)

    def stat_file(self, path, info):
        def _onsuccess(boto_key):
            cid = boto_key['x-amz-meta-cid']
            return {'cid': cid}

        return self._get_boto_key(path).addCallback(_onsuccess)

    def persist_file(self, path, buf, info, meta=None, headers=None):
        """Upload file to S3 storage"""
        with tempfile.NamedTemporaryFile() as temp:
            buf.seek(0)
            temp.write(buf.getvalue())
            self.CIDS[path] = self.client.cid_hash(temp.name, 0)

        return super().persist_file(path, buf, info, meta, headers)


class FilesPipeline(ParentFilesPipeline):
    """Custom Files Abstract pipeline that implement the file downloading"""

    STORE_SCHEMES = {
        '': Web3StorageFilesStore,
        'w3s': Web3StorageFilesStore,
        'lh': LightHouseFilesStore,
        'es': EstuaryFilesStore,
        'pn': PinataFilesStore,
        'ms': MoralisFilesStore,
        's3': S3FilesStore,
    }

    @classmethod
    def init_settings(cls, settings):
        s3_store = cls.STORE_SCHEMES['s3']
        s3_store.S3_ACCESS_KEY_ID = settings['S3_ACCESS_KEY_ID']
        s3_store.S3_SECRET_ACCESS_KEY = settings['S3_SECRET_ACCESS_KEY']
        s3_store.S3_SESSION_TOKEN = settings['S3_SESSION_TOKEN']
        s3_store.S3_ENDPOINT_URL = settings['S3_ENDPOINT_URL']
        s3_store.S3_REGION_NAME = settings['S3_REGION_NAME']
        s3_store.S3_USE_SSL = settings['S3_USE_SSL']
        s3_store.S3_VERIFY = settings['S3_VERIFY']
        s3_store.POLICY = settings['FILES_STORE_S3_ACL']
        s3_store.S3_IPFS_URL_FORMAT = settings['S3_IPFS_URL_FORMAT']

        w3s_store = cls.STORE_SCHEMES['w3s']
        w3s_store.API_KEY = settings['W3S_API_KEY']

        lh_store = cls.STORE_SCHEMES['lh']
        lh_store.API_KEY = settings['LH_API_KEY']

        es_store = cls.STORE_SCHEMES['es']
        es_store.API_KEY = settings['ES_API_KEY']

        ms_store = cls.STORE_SCHEMES['ms']
        ms_store.API_KEY = settings['MS_API_KEY']

        pn_store = cls.STORE_SCHEMES['pn']
        pn_store.API_KEY = settings['PN_JWT_TOKEN']

    @classmethod
    def from_settings(cls, settings):
        cls.init_settings(settings)
        store_uri = settings['FILES_STORE']
        return cls(store_uri, settings=settings)

    def _get_store(self, uri):
        if os.path.isabs(uri):  # to support win32 paths like: C:\\some\dir
            scheme = "w3s"
        else:
            scheme = urlparse(uri).scheme
        store_cls = self.STORE_SCHEMES[scheme]
        return store_cls(uri)

    def media_to_download(self, request, info, *, item=None):
        def _onsuccess(result):
            if not result:
                return  # returning None force download

            referer = referer_str(request)
            logger.debug(
                'File (uptodate): Downloaded %(medianame)s from %(request)s ' 'referred in <%(referer)s>',
                {'medianame': self.MEDIA_NAME, 'request': request, 'referer': referer},
                extra={'spider': info.spider},
            )
            self.inc_stats(info.spider, 'uptodate')

            cid = result.get('cid', None)
            ipfs_url = self.store.client.get_url(cid)
            return {'url': request.url, 'cid': cid, 'ipfsUrl': ipfs_url}

        path = self.file_path(request, info=info, item=item)
        dfd = defer.maybeDeferred(self.store.stat_file, path, info)
        dfd.addCallbacks(_onsuccess, lambda _: None)
        dfd.addErrback(
            lambda f: logger.error(
                self.__class__.__name__ + '.store.stat_file',
                exc_info=failure_to_exc_info(f),
                extra={'spider': info.spider},
            )
        )
        return dfd

    def media_downloaded(self, response, request, info, *, item=None):
        referer = referer_str(request)

        if response.status != 200:
            logger.warning(
                'File (code: %(status)s): Error downloading file from ' '%(request)s referred in <%(referer)s>',
                {'status': response.status, 'request': request, 'referer': referer},
                extra={'spider': info.spider},
            )
            raise FileException('download-error')

        if not response.body:
            logger.warning(
                'File (empty-content): Empty file from %(request)s referred ' 'in <%(referer)s>: no-content',
                {'request': request, 'referer': referer},
                extra={'spider': info.spider},
            )
            raise FileException('empty-content')

        status = 'cached' if 'cached' in response.flags else 'downloaded'
        logger.debug(
            'File (%(status)s): Downloaded file from %(request)s referred in ' '<%(referer)s>',
            {'status': status, 'request': request, 'referer': referer},
            extra={'spider': info.spider},
        )
        self.inc_stats(info.spider, status)

        try:
            path = self.file_path(request, response=response, info=info, item=item)
            cid = self.file_downloaded(response, request, info, item=item)
        except FileException as exc:
            logger.warning(
                'File (error): Error processing file from %(request)s ' 'referred in <%(referer)s>: %(errormsg)s',
                {'request': request, 'referer': referer, 'errormsg': str(exc)},
                extra={'spider': info.spider},
                exc_info=True,
            )
            raise
        except Exception as exc:
            logger.error(
                'File (unknown-error): Error processing file from %(request)s ' 'referred in <%(referer)s>',
                {'request': request, 'referer': referer},
                exc_info=True,
                extra={'spider': info.spider},
            )
            raise FileException(str(exc))
        ipfs_url = self.store.client.get_url(cid)
        return {'url': request.url, 'cid': cid, "ipfsUrl": ipfs_url}

    def file_downloaded(self, response, request, info, *, item=None):
        path = self.file_path(request, response=response, info=info, item=item)
        buf = BytesIO(response.body)
        buf.seek(0)
        self.store.persist_file(path, buf, info)
        return self.store.CIDS[path]


class ImagesPipeline(FilesPipeline):
    """Custom Image Abstract pipeline that implement the image thumbnail generation logic"""

    MEDIA_NAME = 'image'

    # Uppercase attributes kept for backward compatibility with code that subclasses
    # ImagesPipeline. They may be overridden by settings.
    MIN_WIDTH = 0
    MIN_HEIGHT = 0
    EXPIRES = 90
    THUMBS = {}
    DEFAULT_IMAGES_URLS_FIELD = 'image_urls'
    DEFAULT_IMAGES_RESULT_FIELD = 'images'

    def __init__(self, store_uri, download_func=None, settings=None):
        try:
            from PIL import Image

            self._Image = Image
        except ImportError:
            raise NotConfigured('ImagesPipeline requires installing Pillow 4.0.0 or later')

        super().__init__(store_uri, settings=settings, download_func=download_func)

        if isinstance(settings, dict) or settings is None:
            settings = Settings(settings)

        resolve = functools.partial(self._key_for_pipe, base_class_name="ImagesPipeline", settings=settings)
        self.expires = settings.getint(resolve("IMAGES_EXPIRES"), self.EXPIRES)

        if not hasattr(self, "IMAGES_RESULT_FIELD"):
            self.IMAGES_RESULT_FIELD = self.DEFAULT_IMAGES_RESULT_FIELD
        if not hasattr(self, "IMAGES_URLS_FIELD"):
            self.IMAGES_URLS_FIELD = self.DEFAULT_IMAGES_URLS_FIELD

        self.images_urls_field = settings.get(resolve('IMAGES_URLS_FIELD'), self.IMAGES_URLS_FIELD)
        self.images_result_field = settings.get(resolve('IMAGES_RESULT_FIELD'), self.IMAGES_RESULT_FIELD)
        self.min_width = settings.getint(resolve('IMAGES_MIN_WIDTH'), self.MIN_WIDTH)
        self.min_height = settings.getint(resolve('IMAGES_MIN_HEIGHT'), self.MIN_HEIGHT)
        self.thumbs = settings.get(resolve('IMAGES_THUMBS'), self.THUMBS)

        self._deprecated_convert_image = None

    @classmethod
    def from_settings(cls, settings):
        cls.init_settings(settings)
        store_uri = settings['IMAGES_STORE']
        return cls(store_uri, settings=settings)

    def file_downloaded(self, response, request, info, *, item=None):
        return self.image_downloaded(response, request, info, item=item)

    def image_downloaded(self, response, request, info, *, item=None):
        for path, image, buf in self.get_images(response, request, info, item=item):
            buf.seek(0)
            width, height = image.size
            self.store.persist_file(
                path, buf, info, meta={'width': width, 'height': height}, headers={'Content-Type': 'image/jpeg'}
            )
        return self.store.CIDS[path]

    def get_images(self, response, request, info, *, item=None):
        path = self.file_path(request, response=response, info=info, item=item)
        orig_image = self._Image.open(BytesIO(response.body))

        width, height = orig_image.size
        if width < self.min_width or height < self.min_height:
            raise ImageException("Image too small " f"({width}x{height} < " f"{self.min_width}x{self.min_height})")

        if self._deprecated_convert_image is None:
            self._deprecated_convert_image = 'response_body' not in get_func_args(self.convert_image)
            if self._deprecated_convert_image:
                warnings.warn(
                    f'{self.__class__.__name__}.convert_image() method overriden in a deprecated way, '
                    'overriden method does not accept response_body argument.',
                    category=ScrapyDeprecationWarning,
                )

        if self._deprecated_convert_image:
            image, buf = self.convert_image(orig_image)
        else:
            image, buf = self.convert_image(orig_image, response_body=BytesIO(response.body))
        yield path, image, buf

        for thumb_id, size in self.thumbs.items():
            thumb_path = self.thumb_path(request, thumb_id, response=response, info=info, item=item)
            if self._deprecated_convert_image:
                thumb_image, thumb_buf = self.convert_image(image, size)
            else:
                thumb_image, thumb_buf = self.convert_image(image, size, buf)
            yield thumb_path, thumb_image, thumb_buf

    def convert_image(self, image, size=None, response_body=None):
        if response_body is None:
            warnings.warn(
                f'{self.__class__.__name__}.convert_image() method called in a deprecated way, '
                'method called without response_body argument.',
                category=ScrapyDeprecationWarning,
                stacklevel=2,
            )

        if image.format == 'PNG' and image.mode == 'RGBA':
            background = self._Image.new('RGBA', image.size, (255, 255, 255))
            background.paste(image, image)
            image = background.convert('RGB')
        elif image.mode == 'P':
            image = image.convert("RGBA")
            background = self._Image.new('RGBA', image.size, (255, 255, 255))
            background.paste(image, image)
            image = background.convert('RGB')
        elif image.mode != 'RGB':
            image = image.convert('RGB')

        if size:
            image = image.copy()
            try:
                # Image.Resampling.LANCZOS was added in Pillow 9.1.0
                # remove this try except block,
                # when updating the minimum requirements for Pillow.
                resampling_filter = self._Image.Resampling.LANCZOS
            except AttributeError:
                resampling_filter = self._Image.ANTIALIAS
            image.thumbnail(size, resampling_filter)
        elif response_body is not None and image.format == 'JPEG':
            return image, response_body

        buf = BytesIO()
        image.save(buf, 'JPEG')
        return image, buf

    def get_media_requests(self, item, info):
        urls = ItemAdapter(item).get(self.images_urls_field, [])
        return [Request(u) for u in urls]

    def item_completed(self, results, item, info):
        with suppress(KeyError):
            ItemAdapter(item)[self.images_result_field] = [x for ok, x in results if ok]
        return item

    def file_path(self, request, response=None, info=None, *, item=None):
        image_guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
        return f'full/{image_guid}.jpg'

    def thumb_path(self, request, thumb_id, response=None, info=None, *, item=None):
        thumb_guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
        return f'thumbs/{thumb_id}/{thumb_guid}.jpg'
