from ..client import W3SClient
from decouple import config
from pathlib import Path
import os


def test_upload():
    file_path = os.path.join(Path(__file__).resolve().parent, "test.txt")
    ws_client = W3SClient(config("W3S_API_KEY"), "https://api-staging.web3.storage/upload")
    cid = ws_client.upload("test.txt", open(file_path, "rb"))
    print("CID: ", cid)
    assert type(cid) == str
