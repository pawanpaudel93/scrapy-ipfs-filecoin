import subprocess
from base64 import b64encode
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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


class Web3StorageClient(Client):
    def __init__(self, api_key, upload_url="https://api.web3.storage/upload") -> None:
        self.UPLOAD_URL = upload_url
        super().__init__(api_key)

    def upload(self, name, file):
        response = self.session.post(
            self.UPLOAD_URL,
            headers={**self._headers, "X-NAME": quote(name)},
            files={'file': (quote(name), file)},
        )
        response.raise_for_status()
        return response.json()['cid']

    def get_url(self, cid):
        return f"https://w3s.link/ipfs/{cid}"


class EstuaryClient(Client):
    def __init__(self, api_key, upload_url="https://upload.estuary.tech/content/add") -> None:
        self.UPLOAD_URL = upload_url
        super().__init__(api_key)

    def upload(self, name, file):
        response = self.session.post(
            self.UPLOAD_URL,
            headers=self._headers,
            files={'data': (quote(name), file)},
        )
        response.raise_for_status()
        return response.json()['cid']

    def get_url(self, cid):
        return f"https://api.estuary.tech/gw/ipfs/{cid}"


class LightHouseClient(Client):
    def __init__(self, api_key, upload_url="https://node.lighthouse.storage/api/v0/add") -> None:
        self.UPLOAD_URL = upload_url
        super().__init__(api_key)

    def upload(self, name, file):
        response = self.session.post(
            self.UPLOAD_URL,
            headers=self._headers,
            files={'file': (quote(name), file)},
        )
        response.raise_for_status()
        return response.json()['Hash']

    def get_url(self, cid):
        return f"https://gateway.lighthouse.storage/ipfs/{cid}"


class PinataClient(Client):
    def __init__(self, api_key, upload_url="https://api.pinata.cloud/pinning/pinFileToIPFS") -> None:
        self.UPLOAD_URL = upload_url
        super().__init__(api_key)

    def upload(self, name, file):
        response = self.session.post(
            self.UPLOAD_URL,
            headers=self._headers,
            files={'file': (quote(name), file)},
        )
        response.raise_for_status()
        return response.json()['IpfsHash']

    def get_url(self, cid):
        return f"https://gateway.pinata.cloud/ipfs/{cid}"


class MoralisClient(Client):
    def __init__(self, api_key, upload_url="https://deep-index.moralis.io/api/v2/ipfs/uploadFolder") -> None:
        self.UPLOAD_URL = upload_url
        super().__init__(api_key)
        self._headers = {"X-API-Key": api_key}

    def upload(self, name, file):
        try:
            content = b64encode(file).decode('utf-8')
        except:
            content = b64encode(file.file.read()).decode('utf-8')
        response = self.session.post(
            self.UPLOAD_URL,
            headers=self._headers,
            json=[{"path": quote(name), "content": content}],
        )
        response.raise_for_status()
        path = response.json()[0]['path']
        return path.split("/ipfs/")[-1]

    def get_url(self, cid):
        return f"https://ipfs.moralis.io:2053/ipfs/{cid}"
