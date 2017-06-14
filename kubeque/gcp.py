# Authorize server-to-server interactions from Google Compute Engine.
from google.cloud import datastore, pubsub
from google.cloud.exceptions import Conflict
import logging
from oauth2client.client import GoogleCredentials
import oauth2client

from google.cloud.storage.client import Client as GSClient
import os
import re
import hashlib
import json

from fnmatch import fnmatch

from contextlib import contextmanager
import collections

import attr
import time 
import random
from collections import namedtuple

STATUS_CLAIMED = "claimed"
STATUS_PENDING = "pending"
STATUS_FAILED = "failed"

CLAIM_TIMEOUT = 5

log = logging.getLogger(__name__)

def get_credentials(account, cred_file="~/.config/gcloud/credentials"):
    return None
    # cred_file = os.path.expanduser(cred_file)
    # with open(cred_file, "rt") as fd:
    #     all_credentials = json.load(fd)
    #
    #     client_credentials = None
    # for c in all_credentials["data"]:
    #     if c["key"]["type"] == "google-cloud-sdk" and c["key"]["account"] == account:
    #         client_credentials = c["credential"]
    #
    # assert client_credentials is not None, "Could not find credentials for {} in {}".format(account, cred_file)
    #
    # return GoogleCredentials(
    #     access_token=None,
    #     client_id=client_credentials['client_id'],
    #     client_secret=client_credentials['client_secret'],
    #     refresh_token=client_credentials['refresh_token'],
    #     token_expiry=None,
    #     token_uri=oauth2client.GOOGLE_TOKEN_URI,
    #     user_agent='Python client library')

@attr.s
class TaskHistory(object):
    timestamp = attr.ib()
    status = attr.ib()
    owner = attr.ib(default=None)
    failure_reason = attr.ib(default=None)

@attr.s
class Task(object):
    # will be of the form: job_id + task_index
    task_id = attr.ib()

    task_index = attr.ib()
    job_id = attr.ib()
    status = attr.ib() # one of: pending, claimed, success, failed, lost
    owner = attr.ib()
    args = attr.ib()
    history = attr.ib() # list of TaskHistory
    failure_reason = attr.ib(default=None)
    version = attr.ib(default=1)

@attr.s
class Job(object):
    job_id = attr.ib()
    tasks = attr.ib()
    kube_job_spec = attr.ib()
    metadata = attr.ib()

class BatchAdapter:
    def __init__(self, client, cluster):
        self.client = client
        self.batch = client.batch()
        self.batch.begin()
        self.cluster = cluster

    def save(self, o):
        if isinstance(o, Task):
            # The name/ID for the new entity
            entity = task_to_entity(self.client, o, self.cluster)
        else:
            assert isinstance(o, Job)
            entity = job_to_entity(self.client, o, self.cluster)

        self.batch.put(entity)
    
    def commit(self):
        self.batch.commit()

def job_to_entity(client, o, cluster):
    entity_key = client.key("Job", o.job_id)
    entity = datastore.Entity(key=entity_key)
    entity['tasks'] = o.tasks
    entity['cluster'] = cluster
    entity['kube_job_spec'] = o.kube_job_spec
    metadata = []
    for k, v in o.metadata.items():
        m = datastore.Entity()
        m['name'] = k
        m['value'] = v
        metadata.append(m)
    entity['metadata'] = metadata

    return entity

def task_to_entity(client, o, cluster):
    entity_key = client.key("Task", o.task_id)
    entity = datastore.Entity(key=entity_key)
    entity['task_index'] = o.task_index
    entity['job_id'] = o.job_id
    entity['status'] = o.status
    assert isinstance(o.status, str) 
    entity['owner'] = o.owner
    entity['args'] = o.args
    entity['failure_reason'] = o.failure_reason
    entity['cluster'] = cluster

    history = []
    for h in o.history:
        e = datastore.Entity()
        e['timestamp'] = h.timestamp
        e['status'] = h.status
        history.append(e)

    entity['version'] = o.version
    return entity

def entity_to_task(entity):
    assert isinstance(entity['status'], str) 
    return Task(
        task_id = entity.key.name,
        task_index = entity['task_index'],
        job_id = entity['job_id'],
        status = entity['status'],
        owner = entity['owner'],
        args = entity['args'],
        history = [], # entity['history'],
        version = entity['version'],
        failure_reason = entity.get('failure_reason')
    )

def entity_to_job(entity):
    metadata = entity.get('metadata', [])
    return Job(job_id=entity.key.name,
               tasks=entity['tasks'],
               kube_job_spec=entity['kube_job_spec'],
               metadata=dict([(m['name'],m['value']) for m in metadata]))

class JobStorage:
    def __init__(self, client, cluster_name, pubsub):
        self.client = client
        self.cluster_name = cluster_name
        self.pubsub = pubsub

    def get_task(self, task_id):
        task_key = self.client.key("Task", task_id)
        return entity_to_task(self.client.get(task_key))

    def get_jobids(self):
        query = self.client.query(kind="Job")
        query.add_filter("cluster", "=", self.cluster_name)
        jobs_it = query.fetch()
        jobids = []
        for entity_job in jobs_it:
            jobids.append(entity_job.key.name)
        return jobids

    def _job_id_to_topic(self, job_id):
        return "job-"+job_id

    def store_job(self, job):
        with self.batch_write() as batch:
           batch.save(job)
           print("saved 1 job with {} tasks".format(len(job.tasks)))

        topic_name = self._job_id_to_topic(job.job_id)
        print("Creating topic {}".format(topic_name))
        topic = self.pubsub.topic(topic_name)
        topic.create()

    def delete_job(self, job_id):
        job_key = self.client.key("Job", job_id)
        entity_job = self.client.get(job_key)
        task_keys = [self.client.key("Task", taskid) for taskid in set(entity_job["tasks"])]
        self.client.delete_multi(task_keys + [job_key])

        topic_name = self._job_id_to_topic(job_id)
        topic = self.pubsub.topic(topic_name)
        if topic.exists():
            topic.delete()

    def get_job(self, job_id):
        job_key = self.client.key("Job", job_id)
        return entity_to_job(self.client.get(job_key))

    def get_tasks(self, job_id = None, status = None, max_fetch=None):
        query = self.client.query(kind="Task")
        query.add_filter("cluster", "=", self.cluster_name)
        if job_id is not None:
            query.add_filter("job_id", "=", job_id)
        if status is not None:
            query.add_filter("status", "=", status)
        start_time = time.time()
        tasks_it = query.fetch(limit=max_fetch)
        # do I need to use next_page?
        tasks = []
        for entity_task in tasks_it:
            #log.info("fetched: %s", entity_task)
            if status is not None:
                if entity_task["status"] != status:
                    log.warning("Query returned something that did not match query: %s", entity_task)
                    continue
            tasks.append(entity_to_task(entity_task))
            if max_fetch is not None and len(tasks) >= max_fetch:
                break
        end_time = time.time()
        log.debug("get_tasks took %s seconds", end_time-start_time)
        return tasks

    def atomic_task_update(self, task_id, mutate_task_callback):
        try:
            with self.client.transaction():
#                log.info("atomic update of task %s start version: %s", task_id, expected_version)
                task_key = self.client.key("Task", task_id)
                entity_task = self.client.get(task_key)
                print("atomic update of task", task_id, "start, status:", entity_task["status"])

                task = entity_to_task(entity_task)
                successful_update = mutate_task_callback(task)
                if not successful_update:
                    return False
                task.version = task.version + 1
                self.client.put(task_to_entity(self.client, task, self.cluster_name))

                task_as_json = json.dumps(attr.asdict(task)).encode("utf8")

                topic_name = self._job_id_to_topic(task.job_id)
                topic = self.pubsub.topic(topic_name)
                topic.publish(task_as_json)

                log.info("atomic update of task %s success, version: %s, status: %s", task_id, task.version, task.status)
                return True
        except Conflict:
            log.warning("Caught exception: Conflict")
            return False

    def update_task(self, task):
        original_version = task.version
        task.version = original_version + 1
        self.client.put(task_to_entity(self.client, task, self.cluster_name))
        return True
    
    @contextmanager
    def batch_write(self):
        batch = BatchAdapter(self.client, self.cluster_name)
        yield batch
        batch.commit()

class JobQueue:
    def __init__(self, storage):
        self.storage = storage

    def get_claimed_task_ids(self):
        tasks = self.storage.get_tasks(status=STATUS_CLAIMED)
        tasks = [t for t in tasks if t.status == STATUS_CLAIMED]
        for t in tasks:
            assert t.owner is not None
        return [(t.task_id, t.owner) for t in tasks]

    def get_tasks(self, job_id):
        return self.storage.get_tasks(job_id)

    def get_jobids(self, jobid_wildcard="*"):
        jobids = self.storage.get_jobids()
        return [jobid for jobid in jobids if fnmatch(jobid, jobid_wildcard)]

    def get_kube_job_spec(self, job_id):
        job = self.storage.get_job(job_id)
        return job.kube_job_spec

    def delete_job(self, job_id):
        self.storage.delete_job(job_id)

    def get_tasks(self, job_id):
        return self.storage.get_tasks(job_id)

    def get_status_counts(self, job_id):
        counts = collections.defaultdict(lambda: 0)
        for task in self.storage.get_tasks(job_id):
            counts[task.status] += 1
        return dict(counts)

    def reset(self, jobid, owner, statuses_to_clear=[STATUS_CLAIMED, STATUS_FAILED]):
        tasks = []
        for status_to_clear in statuses_to_clear:
            tasks.extend(self.storage.get_tasks(jobid, status=status_to_clear))

        now = time.time()
        for task in tasks:
            if owner is not None and owner != task.owner:
                continue
            task.owner = None
            task.status = STATUS_PENDING
            task.history.append( TaskHistory(timestamp=now, status="reset") )
            self.storage.update_task(task)

    def submit(self, job_id, args, kube_job_spec, metadata):
        tasks = []
        now = time.time()
        
        BATCH_SIZE = 300
        task_index = 0
        for chunk_start in range(0, len(args), BATCH_SIZE):
            args_batch = args[chunk_start:chunk_start+BATCH_SIZE]
            
            with self.storage.batch_write() as batch:
                for arg in args_batch:
                    task_id = "{}.{}".format(job_id, task_index)
                    task = Task(task_id=task_id,
                        task_index=task_index,
                        job_id=job_id, 
                        status="pending", 
                        args=arg,
                        history=[ TaskHistory(timestamp=now, status="pending")],
                        owner=None)
                    tasks.append(task)
                    batch.save(task)
                    task_index += 1
                print("saved {} tasks".format(len(args_batch)))

        job = Job(job_id=job_id, tasks=[t.task_id for t in tasks], kube_job_spec=kube_job_spec, metadata=metadata)
        self.storage.store_job(job)

    def claim_task(self, job_id, new_owner, min_try_time=0, claim_timeout=CLAIM_TIMEOUT):
        "Returns None if no unclaimed ready tasks. Otherwise returns instance of Task"
        claim_start = time.time()
        while True:
            # fetch all pending with this job_id
            tasks = self.storage.get_tasks(job_id, status="pending", max_fetch=10)
            if len(tasks) == 0:
                # We might have tasks we can't see yet
                if time.time() - claim_start < min_try_time:
                    time.sleep(1)
                    continue
                else:
                    return None

            now = time.time()
            if now - claim_start > claim_timeout:
                raise Exception("Timeout attempting to claim task")

            task = random.choice(tasks)

            def mutate_task(task):
                if task.status != STATUS_PENDING:
                    return False
                task.owner = new_owner
                task.status = STATUS_CLAIMED
                task.history.append( TaskHistory(timestamp=now, status=STATUS_CLAIMED, owner=new_owner) )
                return True

            updated = self.storage.atomic_task_update(task.task_id, mutate_task)

            if updated:
                return task.task_id, task.args

            # add exponential backoff?
            log.warning("Update failed")
            time.sleep(random.uniform(0, 1))

    def task_completed(self, task_id, was_successful, failure_reason=None):
        if was_successful:
            new_status = "success"
            assert failure_reason is None
        else:
            new_status = "failed"
        self._update_task_status(task_id, new_status, failure_reason)

    def _update_task_status(self, task_id, new_status, failure_reason):
        task = self.storage.get_task(task_id)
        now = time.time()
        task.history.append( TaskHistory(timestamp=now, status=new_status, failure_reason=failure_reason) )
        task.status = new_status
        task.failure_reason = failure_reason
#        task.owner = None
        updated = self.storage.update_task(task)
        if not updated:
            # I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
            raise Exception("Detected concurrent update, which should not be possible")

    def owner_lost(self, owner):
        tasks = self.Task.scan(owner == owner)
        for task in tasks:
            self._update_task_status(task.task_id, "lost")

def create_gcs_job_queue(project_id, cluster_name, credentials):
    client = datastore.Client(project_id, credentials=credentials)
    pubsub_client = pubsub.Client(project_id, credentials=credentials)
    storage = JobStorage(client, cluster_name, pubsub_client)
    return JobQueue(storage)

class IO:
    def __init__(self, project, cas_url_prefix, credentials):
        assert project is not None

        self.buckets = {}
        self.client = GSClient(project, credentials=credentials)
        if cas_url_prefix[-1] == "/":
            cas_url_prefix = cas_url_prefix[:-1]
        self.cas_url_prefix = cas_url_prefix

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

    def get(self, src_url, dst_filename, must=True):
        log.info("get %s -> %s", src_url, dst_filename)
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

    def put(self, src_filename, dst_url, must=True, skip_if_exists=False):
        if must:
            assert os.path.exists(src_filename)

        bucket, path = self._get_bucket_and_path(dst_url)
        blob = bucket.blob(path)
        if skip_if_exists and blob.exists():
            log.info("skipping put %s -> %s", src_filename, dst_url)
        else:
            log.info("put %s -> %s", src_filename, dst_url)
            blob.upload_from_filename(src_filename)

    def _get_url_prefix(self):
        return "gs://"

    def write_file_to_cas(self, filename):
        m = hashlib.sha256()
        with open(filename, "rb") as fd:
            for chunk in iter(lambda: fd.read(10000), b""):
                m.update(chunk)
        hash = m.hexdigest()
        dst_url = self.cas_url_prefix+hash
        bucket, path = self._get_bucket_and_path(dst_url)
        blob = bucket.blob(path)
        blob.upload_from_filename(filename)
        return self._get_url_prefix()+bucket.name+"/"+path         

    def write_str_to_cas(self, text):
        text = text.encode("utf8")
        hash = hashlib.sha256(text).hexdigest()
        dst_url = self.cas_url_prefix+"/"+hash
#        print("self.cas_url_prefix", self.cas_url_prefix)
        bucket, path = self._get_bucket_and_path(dst_url)
        blob = bucket.blob(path)
        blob.upload_from_string(text)
        return self._get_url_prefix()+bucket.name+"/"+path
        
    def write_json_to_cas(self, obj):
        obj_str = json.dumps(obj)
        return self.write_str_to_cas(obj_str)

NodePoolInfo = namedtuple("NodePoolInfo", "name machine_type disk_size_gb target_size running creating creating_without_retries recreating deleting abandoning restarting refreshing")
