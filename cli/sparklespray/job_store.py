from google.cloud import datastore

from typing import List, Tuple, Optional, Dict
from .datastore_batch import ImmediateBatch


from dataclasses import dataclass


@dataclass
class Job:
    job_id: str
    tasks: List
    kube_job_spec: str
    metadata: Dict[str, str]
    cluster: str
    status: str
    submit_time: float
    max_preemptable_attempts: int
    target_node_count: int = 1


JOB_STATUS_SUBMITTED = "submitted"
JOB_STATUS_KILLED = "killed"


def job_to_entity(client, o):
    entity_key = client.key("Job", o.job_id)
    entity = datastore.Entity(key=entity_key, exclude_from_indexes=("kube_job_spec",))
    entity["tasks"] = o.tasks
    entity["cluster"] = o.cluster
    entity["kube_job_spec"] = o.kube_job_spec
    metadata = []
    for k, v in o.metadata.items():
        m = datastore.Entity()
        m["name"] = k
        m["value"] = v
        metadata.append(m)
    entity["metadata"] = metadata
    entity["status"] = o.status
    entity["submit_time"] = o.submit_time
    entity["target_node_count"] = o.target_node_count
    entity["max_preemptable_attempts"] = o.max_preemptable_attempts

    return entity


def entity_to_job(entity):
    metadata = entity.get("metadata", [])
    return Job(
        job_id=entity.key.name,
        tasks=entity.get("tasks", []),
        cluster=entity["cluster"],
        kube_job_spec=entity.get("kube_job_spec"),
        metadata=dict([(m["name"], m["value"]) for m in metadata]),
        status=entity["status"],
        submit_time=entity.get("submit_time"),
        target_node_count=entity.get("target_node_count", 1),
        max_preemptable_attempts=entity.get("max_preemptable_attempts", 0),
    )


class JobStore:
    def __init__(self, client: datastore.Client) -> None:
        self.client = client
        self.immediate_batch = ImmediateBatch(client)

    def delete(self, job_id, batch=None):
        if batch is None:
            batch = self.immediate_batch

        key = self.client.key("Job", job_id)
        batch.delete(key)

    def insert(self, job: Job, batch=None) -> None:
        if batch is None:
            batch = self.immediate_batch

        entity = job_to_entity(self.client, job)
        batch.put(entity)

    def get_job_ids(self) -> List[str]:
        query = self.client.query(kind="Job")
        jobs_it = query.fetch()
        jobids = []
        for entity_job in jobs_it:
            jobids.append(entity_job.key.name)
        return jobids

    def update_job(self, job_id: str, mutate_fn) -> Tuple[bool, Job]:
        job_key = self.client.key("Job", job_id)
        entity_job = self.client.get(job_key)
        job = entity_to_job(entity_job)
        update_ok = mutate_fn(job)
        if update_ok:
            entity_job = job_to_entity(self.client, job)
            self.client.put(entity_job)
        return update_ok, job

    def get_job(self, job_id: str, must: bool = True) -> Optional[Job]:
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
