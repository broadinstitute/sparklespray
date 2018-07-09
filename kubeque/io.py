from google.cloud.storage.client import Client as GSClient
import os
import re
import hashlib
import json
from .util import compute_hash
import logging

log = logging.getLogger(__name__)

class IO:
    def __init__(self, project, cas_url_prefix, credentials=None, compute_hash=compute_hash):
        assert project is not None

        self.buckets = {}
        self.client = GSClient(project, credentials=credentials)
        if cas_url_prefix[-1] == "/":
            cas_url_prefix = cas_url_prefix[:-1]
        self.cas_url_prefix = cas_url_prefix
        self.compute_hash = compute_hash

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

    def get_as_str(self, src_url, must=True):
        bucket, path = self._get_bucket_and_path(src_url)
        blob = bucket.blob(path)
        if blob.exists():
            return blob.download_as_string().decode("utf8")
        else:
            assert not must, "Could not find {}".format(path)

    def put(self, src_filename, dst_url, must=True, skip_if_exists=False, is_public=False):
        if must:
            assert os.path.exists(src_filename), "{} does not exist".format(src_filename)

        bucket, path = self._get_bucket_and_path(dst_url)
        blob = bucket.blob(path)
        if skip_if_exists and blob.exists():
            if is_public:
                acl = blob.acl
                if not acl.has_entity(acl.all()):
                    log.info("Marking %s (%s) as publicly accessible", src_filename, dst_url)
                    acl.save_predefined("publicRead")
            log.info("Already in CAS cache, skipping upload of %s", src_filename)
            log.debug("skipping put %s -> %s", src_filename, dst_url)
        else:
            if is_public:
                canned_acl = "publicRead"
                acl_params = ["-a", "public-read"]
            else:
                canned_acl = None
                acl_params = []
            log.info("put %s -> %s (acl: %s)", src_filename, dst_url, canned_acl)
            # if greater than 10MB ask gsutil to upload for us
            if os.path.getsize(src_filename) > 10 * 1024 * 1024:
                import subprocess
                subprocess.check_call(['gsutil', 'cp'] + acl_params + [src_filename, dst_url])
            else:
                blob.upload_from_filename(src_filename)
                acl = blob.acl
                acl.save_predefined("publicRead")


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
