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
    def cid_hash(self, file_path):
        output = subprocess.run(["ipfs-only-hash", file_path], capture_output=True)
        return output.stdout.decode("utf-8").strip()


class W3SClient(Client):
    def __init__(self, api_key, base_url="https://api.web3.storage") -> None:
        self.BASE_URL = base_url
        self._headers = {"Authorization": f"Bearer {api_key}"}
        self.session = requests.Session()
        retries = Retry(
            total=3, backoff_factor=1, status_forcelist=[413, 429, 502, 503, 504], allowed_methods=["POST"]
        )
        adapter = TimeoutHTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def upload(self, name, files=[]):
        if len(files) == 0:
            raise "No files to upload"
        response = self.session.post(
            f"{self.BASE_URL}/upload",
            headers={**self._headers, "X-NAME": quote(name)},
            files=[('file', file) for file in files] if len(files) > 1 else {'file': files[0]},
        )
        response.raise_for_status()
        return response.json()['cid']
