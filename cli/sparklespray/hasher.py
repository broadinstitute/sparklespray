import os
import hashlib
import json
from typing import Dict

def hashes_from_file(filename):
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()

    with open(filename, "rb") as fd:
        for chunk in iter(lambda: fd.read(200000), b""):
            sha256.update(chunk)
            md5.update(chunk)
    return sha256.hexdigest(), md5.hexdigest()


class CachingHashFunction:
    def __init__(self, filename):
        self.filename = filename
        self.cache = {}
        self.dirty = False
        if os.path.exists(filename):
            with open(filename, "rt") as fd:
                self.cache = json.load(fd)

    def get_sha256(self, filename):
        sha256, _ = self.get_hashes(filename)
        return sha256

    def get_md5(self, filename):
        _, md5 = self.get_hashes(filename)
        return md5

    def get_hashes(self, filename):
        filename = os.path.normpath(filename)
        mtime = os.path.getmtime(filename)
        if filename in self.cache:
            cache_entry = self.cache[filename]
            if (
                "mtime" in cache_entry
                and cache_entry["mtime"] == mtime
                and "sha256" in cache_entry
                and "md5" in cache_entry
            ):
                return cache_entry["sha256"], cache_entry["md5"]
        sha256, md5 = hashes_from_file(filename)
        cache_entry = dict(sha256=sha256, md5=md5, mtime=mtime)
        self.cache[filename] = cache_entry
        self.dirty = True
        return sha256, md5

    def persist(self):
        if self.dirty:
            with open(self.filename, "wt") as fd:
                json.dump(self.cache, fd, indent=1)
            self.dirty = False

def compute_dict_hash(spec: Dict):
    as_bytes = json.dumps(spec, sort_keys=True).encode("utf8")
    # import time
    # import tempfile
#    fn = tempfile.mktemp(prefix=f"hash-{time.time()}-", suffix=".json", dir=".")
#    with open(fn, "wb") as fd:
#        fd.write(as_bytes)
    return hashlib.sha256(as_bytes).hexdigest()
