import logging
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key
import hashlib
import re
import json
import time
import random
import os

log = logging.getLogger(__name__)

from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, JSONAttribute
)


def create_tables(prefix, _region, _host):
    class Task(Model):
        class Meta:
            table_name = prefix+"Task"
            region = _region
            host = _host
        
        # will be of the form: job_id + task_index
        task_id = UnicodeAttribute(hash_key=True)

        task_index = NumberAttribute()
        job_id = UnicodeAttribute()
        status = UnicodeAttribute() # one of: pending, claimed, success, failed, lost
        owner = UnicodeAttribute(null=True)
        args = UnicodeAttribute()
        history = JSONAttribute() # list of records (timestamp, status)  (maybe include owner?)
        version = NumberAttribute(default=1)

    class Job(Model):
        class Meta:
            table_name = prefix+"Job"
            region = _region
            host = _host

        job_id = UnicodeAttribute(hash_key=True)
        tasks = JSONAttribute()

    if not Job.exists():
        log.info("creating job")
        Job.create_table(wait=True, read_capacity_units=1, write_capacity_units=1)

    if not Task.exists():
        log.info("creating task")
        Task.create_table(wait=True, read_capacity_units=1, write_capacity_units=1)

    return Task, Job

class JobQueue:
    def __init__(self, prefix, region, host):
        self.Task, self.Job = create_tables(prefix, region, host) 

    def _find_pending(self, job_id):
        log.debug("_find_pending")
        tasks = self.Task.scan(status__eq = "pending", job_id__eq = job_id)
        return list(tasks)

    def get_tasks(self, job_id):
        tasks = self.Task.scan(job_id__eq = job_id)
        return tasks

    def get_status_counts(self, job_id):
        import collections
        counts = collections.defaultdict(lambda: 0)
        tasks = self.Task.scan(job_id__eq = job_id)
        for task in tasks:
            counts[task.status] += 1
        return counts

    def reset(self, job_id):
        tasks = self.Task.scan(job_id__eq = job_id)
        now = time.time()
        for task in tasks:
            original_version = task.version
            task.version = original_version + 1
            task.owner = None
            task.status = "pending"
            task.history.append( dict(timestamp=now, status="reset") )
            updated = task.save(version__eq = original_version)

    def submit(self, job_id, args):
        tasks = []
        now = time.time()
        with self.Task.batch_write() as batch:
            for i, arg in enumerate(args):
                task_id = "{}.{}".format(job_id, i)
                task = self.Task(task_id, task_index=i, job_id=job_id, status="pending", args=arg, history=[dict(timestamp=now, status="pending")])
                tasks.append(task)
                batch.save(task)
            log.info("Submitting job with %d tasks", len(args))

        job = self.Job(job_id, tasks=[t.task_id for t in tasks])
        job.save()


    def claim_task(self, job_id, new_owner):
        claim_start = time.time()
        while True:
            # fetch all pending with this job_id\n",
            tasks = self._find_pending(job_id)
            if len(tasks) == 0:
                per_status = self.get_status_counts(job_id)
                log.info("No unclaimed tasks remaining. (Claimed: %s)", per_status["claimed"])
                return None

            now = time.time()
            if now - claim_start > CLAIM_TIMEOUT:
                raise Exception("Timeout attempting to claim task")

            task = random.choice(tasks)
            original_version = task.version
            task.version = original_version + 1
            task.owner = new_owner
            task.status = "claimed"
            task.history.append( dict(timestamp=now, status="claimed", owner=new_owner) )
            log.info("Attempting to claim task %s", task.task_index)
            updated = task.save(version__eq = original_version)
            if updated is not None:
                return task.task_id, task.args
            else:
                log.info("Claim of task %s failed", task.task_index)

            # add exponential backoff?
            log.warn("Update failed")
            time.sleep(random.uniform(0, 1))

    def task_completed(self, task_id, was_successful):
        if was_successful:
            new_status = "success"
        else:
            new_status = "failed"
        self._update_status(task_id, new_status)

    def _update_status(self, task_id, new_status):
        task = self.Task.get(task_id, consistent_read=True)
        now = time.time()
        original_version = task.version
        task.version += 1
        task.history.append( dict(timestamp=now, status=new_status) )
        task.status = new_status
        task.owner = None
        log.debug("updating status. asserting version == %s, new %s", original_version, task.version)
        updated = task.save(version__eq = original_version)
        if updated is None:
            # I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
            raise Exception("Detected concurrent update, which should not be possible")

    def owner_lost(self, owner):
        tasks = self.Task.scan(owner == owner)
        for task in tasks:
            self._update_status(task_id, "lost")


CLAIM_TIMEOUT = 30



class IO:
    def __init__(self, accesskey, secretaccesskey, cas_url_prefix, fake_s3_port):
        self.buckets = {}
        self.fake_s3_port = None
        if fake_s3_port is not None:
            self.fake_s3_port = int(fake_s3_port)
            self.connection = S3Connection("", "", is_secure=False, port=self.fake_s3_port, host='localhost', calling_format=OrdinaryCallingFormat())
        else:
            self.connection = S3Connection(accesskey, secretaccesskey)
        self.cas_url_prefix = cas_url_prefix

    def _get_bucket_and_path(self, path):
        m = re.match("^fakes3://(\\d+)/([^/]+)/(.*)$", path)
        if m is not None:
            fakes3port  = m.group(1)
            bucket_name = m.group(2)
            path = m.group(3)
        else:
            m = re.match("^s3://([^/]+)/(.*)$", path)
            assert m != None, "invalid remote path: {}".format(path)
            bucket_name = m.group(1)
            path = m.group(2)

        if bucket_name in self.buckets:
            bucket = self.buckets[bucket_name]
        else:
            bucket = self.connection.get_bucket(bucket_name)
        return bucket, path

    def get(self, src_url, dst_filename, must=True):
        log.info("get %s -> %s", src_url, dst_filename)
        bucket, path = self._get_bucket_and_path(src_url)
        key = bucket.get_key(path)
        if key is not None:
            key.get_contents_to_filename(dst_filename)
        else:
            assert not must, "Could not find {}".format(path)

    def get_as_str(self, src_url):
        bucket, path = self._get_bucket_and_path(src_url)
        key = bucket.get_key(path, validate=False)
        return key.get_contents_as_string().decode("utf8")

    def put(self, src_filename, dst_url, must=True):
        log.info("put %s -> %s", src_filename, dst_url)
        if must:
            assert os.path.exists(src_filename)

        bucket, path = self._get_bucket_and_path(dst_url)
        key = Key(bucket)
        key.key = path
        key.set_contents_from_filename(src_filename)

    def _get_url_prefix(self):
        if self.fake_s3_port is None:
            return "s3://"
        else:
            return "fakes3://{}/".format(self.fake_s3_port)

    def write_file_to_cas(self, filename):
        m = hashlib.sha256()
        with open(filename, "rb") as fd:
            for chunk in iter(lambda: fd.read(10000), b""):
                m.update(chunk)
        hash = m.hexdigest()
        dst_url = self.cas_url_prefix+hash
        bucket, path = self._get_bucket_and_path(dst_url)
        key = Key(bucket)
        key.key = path
        key.set_contents_from_filename(filename)
        return self._get_url_prefix()+bucket.name+"/"+path         

    def write_str_to_cas(self, text):
        text = text.encode("utf8")
        hash = hashlib.sha256(text).hexdigest()
        dst_url = self.cas_url_prefix+"/"+hash
        bucket, path = self._get_bucket_and_path(dst_url)
        key = Key(bucket)
        key.key = path
        key.set_contents_from_string(text)
        return self._get_url_prefix()+bucket.name+"/"+path
        
    def write_json_to_cas(self, obj):
        obj_str = json.dumps(obj)
        return self.write_str_to_cas(obj_str)

