import requests
from urllib.parse import quote


class W3SClient:
    def __init__(self, api_key, url="https://api.web3.storage/upload") -> None:
        self._url = url
        self._headers = {"Authorization": f"Bearer {api_key}"}

    def upload(self, name, files=[]):
        if len(files) == 0:
            raise "No files to upload"
        response = requests.post(
            self._url,
            headers={**self._headers, "X-NAME": quote(name)},
            files=[('file', file) for file in files] if len(files) > 1 else {'file': files[0]},
        )
        response.raise_for_status()
        return response.json()['cid']
