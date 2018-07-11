import os
import hashlib
import json


def hash_from_file(filename):
    h = hashlib.sha256()
    with open(filename, "rb") as fd:
        for chunk in iter(lambda: fd.read(100000), b''):
            h.update(chunk)
    return h.hexdigest()


class CachingHashFunction:
    def __init__(self, filename):
        self.filename = filename
        self.cache = {}
        self.dirty = False
        if os.path.exists(filename):
            with open(filename, "rt") as fd:
                self.cache = json.load(fd)

    def hash_filename(self, filename):
        filename = os.path.normpath(filename)
        mtime = os.path.getmtime(filename)
        if filename in self.cache:
            cache_entry = self.cache[filename]
            if "mtime" in cache_entry and cache_entry["mtime"] == mtime and "sha256" in cache_entry:
                return cache_entry["sha256"]
        h = hash_from_file(filename)
        cache_entry = dict(sha256=h, mtime=mtime)
        self.cache[filename] = cache_entry
        self.dirty = True
        return h

    def persist(self):
        if self.dirty:
            with open(self.filename, "wt") as fd:
                json.dump(self.cache, fd, indent=1)
            self.dirty = False
