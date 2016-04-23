import re
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import datetime
import logging

log = logging.getLogger(__name__)

def timestamp():
    return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

def parse_remote(path, accesskey=None, secretaccesskey=None):
    m = re.match("^s3://([^/]+)/(.*)$", path)
    assert m != None, "invalid remote path: {}".format(path)
    bucket_name = m.group(1)
    path = m.group(2)

    c = S3Connection(accesskey, secretaccesskey)
    bucket = c.get_bucket(bucket_name)

    return bucket, path

class Remote:
    def __init__(self, remote_url, accesskey=None, secretaccesskey=None):
        self.remote_url = remote_url
        self.bucket, self.remote_path = parse_remote(remote_url, accesskey, secretaccesskey)
        self.job_id = remote_url.split("/")[-1]+"-"+timestamp()
        # self.accesskey = accesskey
        # self.secretaccesskey = secretaccesskey

    def download(self, remote, local):
        log.info("download(%s,%s)", remote, local)
        # maybe upload and download should use trailing slash to indicate directory should be uploaded instead of just a file
        remote_path = self.remote_path + "/" + remote

        key = self.bucket.get_key(remote_path)
        if key != None:
            # if it's a file, download it
            key.get_contents_to_filename(local)
        else:
            # download everything with the prefix
            for key in self.bucket.list(prefix=remote_path):
                rest = drop_prefix(remote_path+"/", key.key)
                if not os.path.exists(local):
                    os.makedirs(local)
                key.get_contents_to_filename(os.path.join(local, rest))

    def upload(self, local, remote):
        log.info("upload(%s,%s)", remote, local)
        # maybe upload and download should use trailing slash to indicate directory should be uploaded instead of just a file
        remote_path = self.remote_path + "/" + remote
        #local_path = os.path.join(local, remote)
        local_path = local

        if os.path.exists(local_path):
            if os.path.isfile(local_path):
                # if it's a file, upload it
                key = Key(self.bucket)
                key.name = remote_path
                key.set_contents_from_filename(local_path)
            else:
                # upload everything in the dir
                for fn in os.listdir(local):
                    full_fn = os.path.join(local, fn)
                    if os.path.isfile(full_fn):
                        k = Key(self.bucket)
                        k.key = os.path.join(remote_path, fn)
                        k.set_contents_from_filename(full_fn)

    def download_dir(self, remote, local):
        remote_path = self.remote_path + "/" + remote

        for key in self.bucket.list(prefix=remote_path):
            rest = drop_prefix(remote_path+"/", key.key)
            #print("local={}, rest={}, filename={}".format(local, rest, os.path.join(local, rest)))
            if not os.path.exists(local):
                os.makedirs(local)
            key.get_contents_to_filename(os.path.join(local, rest))

    def download_file(self, remote, local):
        remote_path = self.remote_path + "/" + remote

        local_dir = os.path.dirname(local)
        if local_dir != "" and not os.path.exists(local_dir):
            os.makedirs(local_dir)

        k = Key(self.bucket)
        k.key = remote_path
        k.get_contents_to_filename(local)

    def download_as_str(self, remote):
        remote_path = self.remote_path+"/"+remote
        key = self.bucket.get_key(remote_path)
        if key == None:
            return None
        return key.get_contents_as_string()

    def upload_str(self, remote, text):
        remote_path = self.remote_path+"/"+remote
        k = Key(self.bucket)
        k.key = remote_path
        k.set_contents_from_string(text)

    def upload_dir(self, local, remote):
        remote_path = self.remote_path+"/"+remote
        if not os.path.exists(local):
            return

        for fn in os.listdir(local):
            full_fn = os.path.join(local, fn)
            if os.path.isfile(full_fn):
                k = Key(self.bucket)
                k.key = os.path.join(remote_path, fn)
                k.set_contents_from_filename(full_fn)

    def upload_file(self, local, remote):
        remote_path = self.remote_path+"/"+remote
        k = Key(self.bucket)
        k.key = remote_path
        k.set_contents_from_filename(local)

def drop_prefix(prefix, value):
    assert value[:len(prefix)] == prefix
    return value[len(prefix):]
