from google.cloud.storage.client import Client as GSClient
import os
import re
import hashlib
import json
from .util import compute_hash
import logging

from .log import log

use_gustil = False
import datetime


class IO:
    def __init__(
        self, project, cas_url_prefix, credentials=None, compute_hash=compute_hash
    ):
        assert project is not None

        self.buckets = {}
        self.credentials = credentials
        self.project = project
        self.client = GSClient(project, credentials=credentials)
        if cas_url_prefix[-1] == "/":
            cas_url_prefix = cas_url_prefix[:-1]
        self.cas_url_prefix = cas_url_prefix
        self.compute_hash = compute_hash

    def generate_signed_url(self, path, expiry=datetime.timedelta(days=30)):
        bucket, key = self._get_bucket_and_path(path)
        blob = bucket.get_blob(key)
        return blob.generate_signed_url(expiry)

    def bulk_get_as_str(self, paths):
        from multiprocessing.pool import ThreadPool
        import threading

        my = threading.local()

        def init_thread():
            my.client = GSClient(self.project, credentials=self.credentials)

        pool = ThreadPool(processes=10, initializer=init_thread)

        def get_as_str(url):
            m = re.match("^gs://([^/]+)/(.*)$", url)
            assert m != None, "invalid remote path: {}".format(url)
            bucket_name = m.group(1)
            path = m.group(2)
            bucket = my.client.bucket(bucket_name)
            blob = bucket.blob(path)
            if not blob.exists():
                return (url, None)
            return (url, blob.download_as_string())

        result = dict(pool.map(get_as_str, paths))
        return result

    def bulk_exists_check(self, paths):
        from multiprocessing.pool import ThreadPool
        import threading

        my = threading.local()

        def init_thread():
            my.client = GSClient(self.project, credentials=self.credentials)

        pool = ThreadPool(processes=10, initializer=init_thread)

        def check(url):
            m = re.match("^gs://([^/]+)/(.*)$", url)
            assert m != None, "invalid remote path: {}".format(url)
            bucket_name = m.group(1)
            path = m.group(2)
            bucket = my.client.bucket(bucket_name)
            blob = bucket.blob(path)
            return (url, blob.exists())

        result = dict(pool.map(check, paths))
        return result

    def _get_bucket_and_path(self, path):
        m = re.match("^gs://([^/]+)/(.*)$", path)
        assert m != None, "invalid remote path: {}".format(path)
        bucket_name = m.group(1)
        path = m.group(2)

        if bucket_name in self.buckets:
            bucket = self.buckets[bucket_name]
        else:
            bucket = self.client.bucket(bucket_name)
        return bucket, path

    def exists(self, src_url):
        bucket, path = self._get_bucket_and_path(src_url)
        blob = bucket.blob(path)
        return blob.exists()

    def delete(self, url):
        bucket, path = self._get_bucket_and_path(url)
        blob = bucket.blob(path)
        blob.delete()

    def get_child_keys(self, src_url):
        bucket, path = self._get_bucket_and_path(src_url)
        keys = []

        # I'm unclear if _I_ am responsible for requesting the next page or whether iterator does it for me.
        for blob in bucket.list_blobs(prefix=path + "/"):
            keys.append("gs://" + bucket.name + "/" + blob.name)

        return keys

    def get(self, src_url, dst_filename, must=True):
        log.info("Downloading %s -> %s", src_url, dst_filename)
        bucket, path = self._get_bucket_and_path(src_url)
        blob = bucket.blob(path)
        if blob.exists():
            blob.download_to_filename(dst_filename)
        else:
            assert not must, "Could not find {}".format(path)

    def get_as_str_must(self, src_url): # Created to make type checker happy
        value = self.get_as_str(src_url, must=True)
        assert value is not None
        return value

    def get_as_str(self, src_url, must=True, start=None):
        bucket, path = self._get_bucket_and_path(src_url)
        blob = bucket.blob(path)
        if blob.exists():
            end = None
            if start is not None:
                blob.reload()
                end = blob.size
                if start == end:
                    return ""
            # log.warning("Downloading %s (%s, %s)", src_url, start, end)
            return blob.download_as_string(start=start, end=end).decode("utf8")
        else:
            assert not must, "Could not find {}".format(src_url)
            return None

    def put(self, src_filename, dst_url, must=True, skip_if_exists=False):
        if must:
            assert os.path.exists(src_filename), "{} does not exist".format(
                src_filename
            )

        bucket, path = self._get_bucket_and_path(dst_url)
        blob = bucket.blob(path)
        if skip_if_exists and blob.exists():
            log.info("Already in CAS cache, skipping upload of %s", src_filename)
            log.debug("skipping put %s -> %s", src_filename, dst_url)
        else:
            log.info("put %s -> %s", src_filename, dst_url)
            # if greater than 10MB ask gsutil to upload for us
            if use_gustil and os.path.getsize(src_filename) > 10 * 1024 * 1024:
                import subprocess

                subprocess.check_call(["gsutil", "cp"] + [src_filename, dst_url])
            else:
                blob.upload_from_filename(src_filename)

    def _get_url_prefix(self):
        return "gs://"

    def write_file_to_cas(self, filename):
        m = hashlib.sha256()
        with open(filename, "rb") as fd:
            for chunk in iter(lambda: fd.read(10000), b""):
                m.update(chunk)
        hash = m.hexdigest()
        dst_url = self.cas_url_prefix + hash
        bucket, path = self._get_bucket_and_path(dst_url)
        blob = bucket.blob(path)
        blob.upload_from_filename(filename)
        return self._get_url_prefix() + bucket.name + "/" + path

    def write_str_to_cas(self, text):
        text = text.encode("utf8")
        hash = hashlib.sha256(text).hexdigest()
        dst_url = self.cas_url_prefix + "/" + hash
        bucket, path = self._get_bucket_and_path(dst_url)
        blob = bucket.blob(path)
        blob.upload_from_string(text)
        return self._get_url_prefix() + bucket.name + "/" + path

    def write_json_to_cas(self, obj):
        obj_str = json.dumps(obj)
        return self.write_str_to_cas(obj_str)
