import os
from pathlib import Path

from decouple import config

from ..client import Web3StorageClient


def test_upload():
    file_path = os.path.join(Path(__file__).resolve().parent, "test.txt")
    ws_client = Web3StorageClient(config("W3S_API_KEY"), "https://api-staging.web3.storage/upload")
    cid = ws_client.upload("test.txt", open(file_path, "rb"))
    print("CID: ", cid)
    assert type(cid) == str
