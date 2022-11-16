import requests
import subprocess
from urllib.parse import quote
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


class TimeoutHTTPAdapter(HTTPAdapter):
    DEFAULT_TIMEOUT = 30

    def __init__(self, *args, **kwargs):
        self.timeout = self.DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


class Client:
    def __init__(self, api_key) -> None:
        self._headers = {"Authorization": f"Bearer {api_key}"}
        self.session = requests.Session()
        retries = Retry(
            total=3, backoff_factor=1, status_forcelist=[413, 429, 502, 503, 504], allowed_methods=["POST"]
        )
        adapter = TimeoutHTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def cid_hash(self, file_path, cid_version=1):
        output = subprocess.run(["ipfs-only-hash", file_path, f'--cid-version={cid_version}'], capture_output=True)
        return output.stdout.decode("utf-8").strip()


class W3SClient(Client):
    def __init__(self, api_key, upload_url="https://api.web3.storage/upload") -> None:
        self.UPLOAD_URL = upload_url
        super().__init__(api_key)

    def upload(self, name, files=[]):
        if len(files) == 0:
            raise "No files to upload"
        response = self.session.post(
            self.UPLOAD_URL,
            headers={**self._headers, "X-NAME": quote(name)},
            files=[('file', file) for file in files] if len(files) > 1 else {'file': files[0]},
        )
        response.raise_for_status()
        return response.json()['cid']


class EstuaryClient(Client):
    def __init__(self, api_key, upload_url="https://upload.estuary.tech/content/add") -> None:
        self.UPLOAD_URL = upload_url
        super().__init__(api_key)

    def upload(self, name, files=[]):
        if len(files) == 0:
            raise "No files to upload"
        response = self.session.post(
            self.UPLOAD_URL,
            headers=self._headers,
            files=[('data', file) for file in files]
            if len(files) > 1
            else {'data': files[0], "filename": quote(name)},
        )
        response.raise_for_status()
        return response.json()['cid']


class LightHouseClient(Client):
    def __init__(self, api_key, upload_url="https://node.lighthouse.storage/api/v0/add") -> None:
        self.UPLOAD_URL = upload_url
        super().__init__(api_key)

    def upload(self, name, files=[]):
        if len(files) == 0:
            raise "No files to upload"
        response = self.session.post(
            self.UPLOAD_URL,
            headers={**self._headers, "X-NAME": quote(name)},
            files=[('file', file) for file in files] if len(files) > 1 else {'file': files[0]},
        )
        response.raise_for_status()
        return response.json()['Hash']
