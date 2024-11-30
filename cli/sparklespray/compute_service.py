from googleapiclient.discovery import build

# from apiclient.discovery import build
import googleapiclient.errors
from googleapiclient.errors import HttpError

from googleapiclient.discovery_cache.base import Cache
import os
import hashlib
import tempfile
from dataclasses import dataclass
import time


@dataclass
class VolumeDetails:
    type: str
    size: float
    status: str


class DirCache(Cache):
    def __init__(self, path):
        self.path = path
        # attempt to create if not already existing
        try:
            os.makedirs(self.path)
        except FileExistsError:
            pass

    def _get_filename(self, url):
        return os.path.join(self.path, hashlib.sha256(url.encode("utf8")).hexdigest())

    def get(self, url):
        fn = self._get_filename(url)
        try:
            with open(fn, "rt") as fd:
                return fd.read()
        except FileNotFoundError:
            return None

    def set(self, url, content):
        fn = self._get_filename(url)
        tmp_fd = tempfile.NamedTemporaryFile(mode="wt", dir=self.path, delete=False)
        tmp_fd.write(content)
        tmp_fd.close()
        os.rename(tmp_fd.name, fn)

