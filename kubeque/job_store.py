from google.cloud import datastore
import google.cloud.exceptions
import logging

from google.cloud.storage.client import Client as GSClient
import os
import re
import hashlib
import json
import attr
from typing import List, Tuple

log = logging.getLogger(__name__)

@attr.s
class Job(object):
    job_id = attr.ib()
    tasks = attr.ib()
    kube_job_spec = attr.ib()
    metadata = attr.ib()
    cluster = attr.ib()
    status = attr.ib()
    submit_time = attr.ib()

JOB_STATUS_SUBMITTED = "submitted"
JOB_STATUS_KILLED = "killed"

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

def entity_to_job(entity):
    metadata = entity.get('metadata', [])
    return Job(job_id=entity.key.name,
               tasks=entity.get('tasks',[]),
               cluster=entity['cluster'],
               kube_job_spec=entity.get('kube_job_spec'),
               metadata=dict([(m['name'],m['value']) for m in metadata]),
               status=entity['status'],
               submit_time=entity.get('submit_time'))

class JobStore:
    def __init__(self, client : datastore.Client) -> None:
        self.client = client

    def get_jobids(self) -> List[Job]:
        query = self.client.query(kind="Job")
        jobs_it = query.fetch()
        jobids = []
        for entity_job in jobs_it:
            jobids.append(entity_job.key.name)
        return jobids

    def store_job(self, job : Job) -> None:
        existing_job = self.get_job(job.job_id, must=False)
        if existing_job is not None:
            raise Exception("Cannot create job \"{}\", ID is already used".format(job.job_id))

        with self.batch_write() as batch:
           batch.save(job)
           log.info("Saved job definition with %d tasks", len(job.tasks))

    def update_job(self, job_id : str, mutate_fn) -> Tuple[bool, Job]:
        job_key = self.client.key("Job", job_id)
        entity_job = self.client.get(job_key)
        job = entity_to_job(entity_job)
        update_ok = mutate_fn(job)
        if update_ok:
            entity_job = job_to_entity(self.client, job)
            self.client.put(entity_job)
        return update_ok, job

    def delete_job(self, job_id : str):
        job_key = self.client.key("Job", job_id)
        entity_job = self.client.get(job_key)

        task_ids = entity_job.get("tasks",[])
        # delete tasks
        keys = [self.client.key("Task", taskid) for taskid in set(task_ids)]
        # clean up associated node requests
        node_reqs = self.get_node_reqs(job_id)
        # log.info("Deleting records about %d node requests", len(node_reqs))
        node_req_keys = [self.client.key("NodeReq", x.operation_id) for x in node_reqs ]
        keys += node_req_keys
        # delete job
        keys += [job_key]
        # log.info("Deleting %s", repr(keys))

        # perform the delete
        BATCH_SIZE = 300
        for chunk_start in range(0, len(keys), BATCH_SIZE):
            key_batch = keys[chunk_start:chunk_start+BATCH_SIZE]
            self.client.delete_multi(key_batch)

    def get_job(self, job_id : str, must : bool = True) -> Job:
        job_key = self.client.key("Job", job_id)
        job_entity = self.client.get(job_key)
        if job_entity is None:
            if must:
                raise Exception("Could not find job with id {}".format(job_id))
            else:
                return None
        return entity_to_job(job_entity)

    def get_last_job(self) -> Job:
        query = self.client.query(kind="Job")
        query.order = ["-submit_time"]
        job_entity = list(query.fetch(limit=1))[0]
        return entity_to_job(job_entity)
