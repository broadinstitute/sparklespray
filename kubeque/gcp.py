# Authorize server-to-server interactions from Google Compute Engine.
from google.cloud import datastore
from google.cloud.datastore.query import Query
from google.cloud.datastore.query import Iterator
import google.cloud.exceptions

from contextlib import contextmanager
import collections

import attr
import time 
import random

CLAIM_TIMEOUT = 5

import logging
log = logging.getLogger(__name__)

@attr.s
class Task:
    # will be of the form: job_id + task_index
    task_id = attr.ib()

    task_index = attr.ib()
    job_id = attr.ib()
    status = attr.ib() # one of: pending, claimed, success, failed, lost
    owner = attr.ib()
    args = attr.ib()
    history = attr.ib() # list of records (timestamp, status)  (maybe include owner?)
    version = attr.ib(default=1)

@attr.s
class Job:
    job_id = attr.ib()
    tasks = attr.ib()

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
            entity_key = self.client.key("Job", o.job_id)
            entity = datastore.Entity(key=entity_key)
            entity['tasks'] = o.tasks

        print("entity", entity)
        self.batch.put(entity)
    
    def commit(self):
        self.batch.commit()

def task_to_entity(client, o):
    entity_key = client.key("Task", o.task_id)
    entity = datastore.Entity(key=entity_key)
    entity['task_index'] = o.task_index
    entity['job_id'] = o.job_id
    entity['status'] = o.status
    entity['owner'] = o.owner
    entity['args'] = o.args
    # can't save a list of dicts?
#            entity['history'] = o.history
    entity['version'] = o.version
    return entity

def entity_to_task(entity):
    return Task(
        task_id = entity.key.name,
        task_index = entity['task_index'],
        job_id = entity['job_id'],
        status = entity['status'],
        owner = entity['owner'],
        args = entity['args'],
        history = [], # entity['history'],
        version = entity['version']
    )

class JobStorage:
    def __init__(self, client):
        self.client = client

    def get_task(self, task_id):
        task_key = self.client.key("Task", task_id)
        return entity_to_task(self.client.get(task_key))

    def get_tasks(self, job_id, status = None, max_fetch=None):
#        job = self.client.get(self.client.key("Job", job_id))
#        assert job is not None
#        tasks = []
#        for task_id in job["tasks"]:
#            task_key = self.client.key("Task", task_id)
#            tasks.append(entity_to_task(self.client.get(task_key)))
#        return tasks

        query = self.client.query(kind="Task")
        query.add_filter("job_id", "=", job_id)
        if status is not None:
            query.add_filter("status", "=", status)
        start_time = time.time()
        tasks_it = query.fetch(limit=max_fetch)
        # do I need to use next_page?
        tasks = []
        for entity_task in tasks_it:
            if status is not None:
                if entity_task["status"] != status:
                    print("Query returned something that did not match query", entity_task)
                    continue
            tasks.append(entity_to_task(entity_task))
            if max_fetch is not None and len(tasks) >= max_fetch:
                break
        end_time = time.time()
        print("get_tasks took ", end_time-start_time)
        return tasks

    def atomic_task_update(self, task_id, expected_version, mutate_task_callback):
        try:
            with self.client.transaction():
                print("atomic update of task", task_id, "start, version:", expected_version)
                task_key = self.client.key("Task", task_id)
                entity_task = self.client.get(task_key)
                print("atomic update of task", task_id, "start, version:", expected_version, "status:", entity_task["status"])
                if entity_task['version'] != expected_version:
                    return False

                task = entity_to_task(entity_task)
                mutate_task_callback(task)
                task.version = task.version + 1
                self.client.put(task_to_entity(self.client, task))
                
                print("atomic update of task", task_id, "success, version:", task.version, "status", task.status)
                return True
        except google.cloud.exceptions.Conflict:
            print("Caught exception: Conflict")
            return False            

    def update_task(self, task):
        original_version = task.version
        task.version = original_version + 1
        self.client.put(task_to_entity(self.client, task))
#        updated = task.save(version__eq = original_version)
        return True
    
    @contextmanager
    def batch_write(self):
        batch = BatchAdapter(self.client)
        yield batch
        batch.commit()

class JobQueue:
    def __init__(self, storage):
        self.storage = storage 

    def get_status_counts(self, job_id):
        counts = collections.defaultdict(lambda: 0)
        for task in self.storage.get_tasks(job_id):
            counts[task.status] += 1
        return counts

    def reset(self, job_id):
        tasks = self.storage.get_tasks(job_id)
        now = time.time()
        for task in tasks:
            task.owner = None
            task.status = "pending"
            task.history.append( dict(timestamp=now, status="reset") )
            self._update_task(task)

    def submit(self, job_id, args):
        tasks = []
        now = time.time()
        with self.storage.batch_write() as batch:
            for i, arg in enumerate(args):
                task_id = "{}.{}".format(job_id, i)
                task = Task(task_id=task_id, 
                    task_index=i, 
                    job_id=job_id, 
                    status="pending", 
                    args=arg, 
                    history=[dict(timestamp=now, status="pending")],
                    owner=None)
                tasks.append(task)
                print("status", batch.batch._status)
                print("task", task)
                batch.save(task)

            job = Job(job_id = job_id, tasks=[t.task_id for t in tasks])
            batch.save(job)

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
                task.owner = new_owner
                task.status = "claimed"
                task.history.append( dict(timestamp=now, status="claimed", owner=new_owner) )
            updated = self.storage.atomic_task_update(task.task_id, task.version, mutate_task)

            if updated:
                return task.task_id, task.args

            # add exponential backoff?
            log.warn("Update failed")
            time.sleep(random.uniform(0, 1))

    def task_completed(self, task_id, was_successful):
        if was_successful:
            new_status = "success"
        else:
            new_status = "failed"
        self._update_task_status(task_id, new_status)

    def _update_task_status(self, task_id, new_status):
        task = self.storage.get_task(task_id)
        now = time.time()
        task.history.append( dict(timestamp=now, status=new_status) )
        task.status = new_status
        task.owner = None
        updated = self.storage.update_task(task)
        if not updated:
            # I suppose this is not technically correct. Could be a simultaneous update of "success" or "failed" and "lost"
            raise Exception("Detected concurrent update, which should not be possible")

    def owner_lost(self, owner):
        tasks = self.Task.scan(owner == owner)
        for task in tasks:
            self._update_task_status(task_id, "lost")

def create_gcs_job_queue(project_id):
    client = datastore.Client(project_id)
    storage = JobStorage(client)
    return JobQueue(storage)



# from oauth2client.contrib import gce
# credentials = gce.AppAssertionCredentials(
#     scope='https://www.googleapis.com/auth/devstorage.read_write')
# http = credentials.authorize(httplib2.Http())
