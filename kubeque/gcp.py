# Authorize server-to-server interactions from Google Compute Engine.
from google.cloud import datastore, pubsub
import google.cloud.exceptions
import logging

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
from collections import namedtuple

STATUS_CLAIMED = "claimed"
STATUS_PENDING = "pending"
STATUS_FAILED = "failed"
STATUS_COMPLETE = "complete"
STATUS_KILLED = "killed"

CLAIM_TIMEOUT = 5

JOB_STATUS_SUBMITTED = "submitted"
JOB_STATUS_KILLED = "killed"

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
    command_result_url = attr.ib()
    cluster = attr.ib()
    failure_reason = attr.ib(default=None)
    version = attr.ib(default=1)
    exit_code = attr.ib(default=None)

@attr.s
class Job(object):
    job_id = attr.ib()
    tasks = attr.ib()
    kube_job_spec = attr.ib()
    metadata = attr.ib()
    cluster = attr.ib()
    status = attr.ib()
    submit_time = attr.ib()

class BatchAdapter:
    def __init__(self, client):
        self.client = client
        self.batch = client.batch()
        self.batch.begin()

    def save(self, o):
        if isinstance(o, Task):
            # The name/ID for the new entity
            entity = task_to_entity(self.client, o)
        else:
            assert isinstance(o, Job)
            entity = job_to_entity(self.client, o)

        self.batch.put(entity)
    
    def commit(self):
        self.batch.commit()

def job_to_entity(client, o):
    entity_key = client.key("Job", o.job_id)
    entity = datastore.Entity(key=entity_key)
    entity['tasks'] = o.tasks
    entity['cluster'] = o.cluster
    entity['kube_job_spec'] = o.kube_job_spec
    metadata = []
    for k, v in o.metadata.items():
        m = datastore.Entity()
        m['name'] = k
        m['value'] = v
        metadata.append(m)
    entity['metadata'] = metadata
    entity['status'] = o.status
    entity['submit_time'] = o.submit_time

    return entity

def task_to_entity(client, o):
    entity_key = client.key("Task", o.task_id)
    entity = datastore.Entity(key=entity_key)
    entity['task_index'] = o.task_index
    entity['job_id'] = o.job_id
    entity['status'] = o.status
    assert isinstance(o.status, str) 
    entity['owner'] = o.owner
    entity['args'] = o.args
    entity['failure_reason'] = o.failure_reason
    entity['cluster'] = o.cluster
    entity['command_result_url'] = o.command_result_url
    history = []
    for h in o.history:
        e = datastore.Entity()
        e['timestamp'] = h.timestamp
        e['status'] = h.status
        history.append(e)

    entity['history'] = history
    entity['version'] = o.version
    entity['exit_code'] = o.exit_code
    return entity

def entity_to_task(entity):
    assert isinstance(entity['status'], str)
    history = []
    for he in entity.get('history',[]):
        history.append(TaskHistory(timestamp=he['timestamp'], status=he['status'], owner=he.get('owner'), failure_reason=he.get('failure_reason')))

    return Task(
        task_id = entity.key.name,
        task_index = entity['task_index'],
        job_id = entity['job_id'],
        status = entity['status'],
        owner = entity['owner'],
        args = entity['args'],
        history = history,
        version = entity['version'],
        failure_reason = entity.get('failure_reason'),
        command_result_url = entity.get('command_result_url'),
        exit_code = entity.get('exit_code'),
        cluster = entity.get("cluster")
    )

def entity_to_job(entity):
    metadata = entity.get('metadata', [])
    return Job(job_id=entity.key.name,
               tasks=entity.get('tasks',[]),
               cluster=entity['cluster'],
               kube_job_spec=entity.get('kube_job_spec'),
               metadata=dict([(m['name'],m['value']) for m in metadata]),
               status=entity['status'],
               submit_time=entity.get('submit_time'))

class JobStorage:
    def __init__(self, client, pubsub, log_client):
        self.client = client
        self.pubsub = pubsub
        self.log_client = log_client

    def get_task(self, task_id):
        task_key = self.client.key("Task", task_id)
        return entity_to_task(self.client.get(task_key))

    def get_jobids(self):
        query = self.client.query(kind="Job")
        jobs_it = query.fetch()
        jobids = []
        for entity_job in jobs_it:
            jobids.append(entity_job.key.name)
        return jobids

    def _job_id_to_topic(self, job_id):
        return "job-"+job_id

    def store_job(self, job):
        existing_job = self.get_job(job.job_id, must=False)
        if existing_job is not None:
            raise Exception("Cannot create job \"{}\", ID is already used".format(job.job_id))

        with self.batch_write() as batch:
           batch.save(job)
           log.info("Saved job definition with %d tasks", len(job.tasks))

        topic_name = self._job_id_to_topic(job.job_id)
        if self.pubsub:
            log.info("Creating topic %s", topic_name)
            topic = self.pubsub.topic(topic_name)
            topic.create()

    def update_job(self, job_id, mutate_fn):
        job_key = self.client.key("Job", job_id)
        entity_job = self.client.get(job_key)
        job = entity_to_job(entity_job)
        update_ok = mutate_fn(job)
        if update_ok:
            entity_job = job_to_entity(self.client, job)
            self.client.put(entity_job)
        return update_ok, job

    def delete_job(self, job_id):
        job_key = self.client.key("Job", job_id)
        entity_job = self.client.get(job_key)

        task_ids = entity_job.get("tasks",[])
        task_keys = [self.client.key("Task", taskid) for taskid in set(task_ids)] + [job_key]
        BATCH_SIZE = 300
        for chunk_start in range(0, len(task_keys), BATCH_SIZE):
            key_batch = task_keys[chunk_start:chunk_start+BATCH_SIZE]
            self.client.delete_multi(key_batch)

        topic_name = self._job_id_to_topic(job_id)
        if self.pubsub:
            topic = self.pubsub.topic(topic_name)
            if topic.exists():
                topic.delete()

        # probably kind of a slow op. For now, only try to clean up if there are fewer than 5 tasks
        # in the future, put a flag on the job so we know whether we need to clean up logs
        if self.log_client and len(task_ids) <= 5:
            for task_id in task_ids:
                try:
                    self.log_client.logger(task_id).delete()
                except google.cloud.exceptions.NotFound:
                    pass

    def get_job(self, job_id, must = True):
        job_key = self.client.key("Job", job_id)
        job_entity = self.client.get(job_key)
        if job_entity is None:
            if must:
                raise Exception("Could not find job with id {}".format(job_id))
            else:
                return None
        return entity_to_job(job_entity)

    def get_last_job(self):
        query = self.client.query(kind="Job")
        query.order = ["-submit_time"]
        job_entity = list(query.fetch(limit=1))[0]
        return entity_to_job(job_entity)

    def get_tasks(self, job_id = None, status = None, max_fetch=None):
        query = self.client.query(kind="Task")
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

    def get_tasks_for_cluster(self, cluster_name, status, max_fetch=None):
        query = self.client.query(kind="Task")
        query.add_filter("cluster", "=", cluster_name)
        query.add_filter("status", "=", status)
        start_time = time.time()
        tasks_it = query.fetch(limit=max_fetch)
        tasks = []
        for entity_task in tasks_it:
            tasks.append(entity_to_task(entity_task))
            if max_fetch is not None and len(tasks) >= max_fetch:
                break
        end_time = time.time()
        log.debug("get_tasks took %s seconds", end_time-start_time)
        return tasks

    def update_task(self, task):
        original_version = task.version
        task.version = original_version + 1
        self.client.put(task_to_entity(self.client, task))
        return True
    
    @contextmanager
    def batch_write(self):
        batch = BatchAdapter(self.client)
        yield batch
        batch.commit()

class JobQueue:
    def __init__(self, storage):
        self.storage = storage

    def get_tasks_for_cluster(self, cluster_name, status, max_fetch=None):
        return self.storage.get_tasks_for_cluster(cluster_name, status, max_fetch)

    def get_claimed_task_ids(self):
        tasks = self.storage.get_tasks(status=STATUS_CLAIMED)
        tasks = [t for t in tasks if t.status == STATUS_CLAIMED]
        for t in tasks:
            assert t.owner is not None
        return [(t.task_id, t.owner) for t in tasks]

    def get_tasks(self, job_id, status=None):
        return self.storage.get_tasks(job_id, status=status)

    def get_job(self, job_id, must=True):
        return self.storage.get_job(job_id, must=must)

    def get_last_job(self):
        return self.storage.get_last_job()

    def get_jobids(self, jobid_wildcard="*"):
        jobids = self.storage.get_jobids()
        return [jobid for jobid in jobids if fnmatch(jobid, jobid_wildcard)]

    def get_kube_job_spec(self, job_id):
        job = self.storage.get_job(job_id)
        return job.kube_job_spec

    def delete_job(self, job_id):
        self.storage.delete_job(job_id)

    def kill_job(self, job_id):

        def mark_killed(job):
            job.status = JOB_STATUS_KILLED
            return True

        return self.storage.update_job(job_id, mark_killed)

    def get_status_counts(self, job_id):
        counts = collections.defaultdict(lambda: 0)
        for task in self.storage.get_tasks(job_id):
            counts[task.status] += 1
        return dict(counts)

    def reset(self, jobid, owner, statuses_to_clear=[STATUS_CLAIMED, STATUS_FAILED]):
        tasks = []
        for status_to_clear in statuses_to_clear:
            tasks.extend(self.storage.get_tasks(jobid, status=status_to_clear))

        updated = 0
        for task in tasks:
            if owner is not None and owner != task.owner:
                continue
            self._reset_task(task, STATUS_PENDING)
            updated += 1

        def mark_not_killed(job):
            job.status = JOB_STATUS_SUBMITTED
            return True

        self.storage.update_job(jobid, mark_not_killed)

        return updated

    def _reset_task(self, task, status):
        now = time.time()
        task.owner = None
        task.status = status
        task.history.append( TaskHistory(timestamp=now, status="reset") )
        self.storage.update_task(task)

    def reset_task(self, task_id, status=STATUS_PENDING):
        task = self.storage.get_task(task_id)
        self._reset_task(task, status)

    def submit(self, job_id, args, kube_job_spec, metadata, cluster):
        import json
        kube_job_spec = json.dumps(kube_job_spec)
        tasks = []
        now = time.time()
        
        BATCH_SIZE = 300
        task_index = 0
        for chunk_start in range(0, len(args), BATCH_SIZE):
            args_batch = args[chunk_start:chunk_start+BATCH_SIZE]
            
            with self.storage.batch_write() as batch:
                for arg, command_result_url in args_batch:
                    task_id = "{}.{}".format(job_id, task_index)
                    task = Task(task_id=task_id,
                        task_index=task_index,
                        job_id=job_id, 
                        status="pending", 
                        args=arg,
                        history=[ TaskHistory(timestamp=now, status="pending")],
                        owner=None,
                        command_result_url=command_result_url,
                                cluster=cluster)
                    tasks.append(task)
                    batch.save(task)
                    task_index += 1
                log.info("Saved task definition batch containing %d tasks", len(args_batch))

        job = Job(job_id=job_id, tasks=[t.task_id for t in tasks], kube_job_spec=kube_job_spec, metadata=metadata, cluster=cluster, status=JOB_STATUS_SUBMITTED,
                  submit_time=time.time())
        self.storage.store_job(job)

    def test_datastore_api(self, job_id):
        """Test we the datastore api is enabled by writing a value and deleting a value."""
        job = Job(job_id=job_id, tasks=[], kube_job_spec=None, metadata={}, cluster=job_id, status=JOB_STATUS_KILLED,
                  submit_time=time.time())
        self.storage.store_job(job)
        fetched_job = self.storage.get_job(job_id)
        assert fetched_job.job_id == job_id
        self.storage.delete_job(job_id)

    def _update_task_status(self, task_id, new_status, failure_reason, retcode):
        task = self.storage.get_task(task_id)
        now = time.time()
        task.history.append( TaskHistory(timestamp=now, status=new_status, failure_reason=failure_reason) )
        task.status = new_status
        task.failure_reason = failure_reason
        task.exit_code = retcode
#        task.owner = None
        updated = self.storage.update_task(task)
        if not updated:
            # I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
            raise Exception("Detected concurrent update, which should not be possible")

    def owner_lost(self, owner):
        tasks = self.Task.scan(owner == owner)
        for task in tasks:
            self._update_task_status(task.task_id, "lost")

def create_gcs_job_queue(project_id, credentials, use_pubsub):
    client = datastore.Client(project_id, credentials=credentials)
    if use_pubsub:
        pubsub_client = pubsub.Client(project_id, credentials=credentials)
    else:
        pubsub_client = None

    from google.cloud import logging as gcp_logging
    log_client = gcp_logging.Client()

    storage = JobStorage(client, pubsub_client, log_client)
    return JobQueue(storage)

def _compute_hash(filename):
    m = hashlib.sha256()
    with open(filename, "rb") as fd:
        for chunk in iter(lambda: fd.read(10000), b""):
            m.update(chunk)
    return m.hexdigest()

def _join(*args):
    concated = args[0]
    for x in args[1:]:
        if concated[-1] == "/":
            concated = concated[:-1]
        concated += "/" + x
    return concated

class IO:
    def __init__(self, project, cas_url_prefix, credentials=None, compute_hash=_compute_hash):
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
        for blob in bucket.list_blobs(prefix=path+"/"):
            keys.append("gs://"+bucket.name+"/"+blob.name)

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

    def put(self, src_filename, dst_url, must=True, skip_if_exists=False):
        if must:
            assert os.path.exists(src_filename), "{} does not exist".format(src_filename)

        bucket, path = self._get_bucket_and_path(dst_url)
        blob = bucket.blob(path)
        if skip_if_exists and blob.exists():
            log.info("Already in CAS cache, skipping upload of %s", src_filename)
            log.debug("skipping put %s -> %s", src_filename, dst_url)
        else:
            log.info("put %s -> %s", src_filename, dst_url)
            # if greater than 10MB ask gsutil to upload for us
            if os.path.getsize(src_filename) > 10 * 1024 * 1024:
                import subprocess
                subprocess.check_call(['gsutil', 'cp', src_filename, dst_url])
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
        dst_url = self.cas_url_prefix+hash
        bucket, path = self._get_bucket_and_path(dst_url)
        blob = bucket.blob(path)
        blob.upload_from_filename(filename)
        return self._get_url_prefix()+bucket.name+"/"+path         

    def write_str_to_cas(self, text):
        text = text.encode("utf8")
        hash = hashlib.sha256(text).hexdigest()
        dst_url = self.cas_url_prefix+"/"+hash
        bucket, path = self._get_bucket_and_path(dst_url)
        blob = bucket.blob(path)
        blob.upload_from_string(text)
        return self._get_url_prefix()+bucket.name+"/"+path
        
    def write_json_to_cas(self, obj):
        obj_str = json.dumps(obj)
        return self.write_str_to_cas(obj_str)

def _gcloud_cmd(args):
    return ["gcloud"] + list(args)

