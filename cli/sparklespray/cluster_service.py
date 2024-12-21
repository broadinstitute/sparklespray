from .task_store import (
    STATUS_FAILED,
    STATUS_COMPLETE,
)
from .node_req_store import (
    NodeReq,
    NODE_REQ_SUBMITTED,
    NODE_REQ_CLASS_PREEMPTIVE,
    NODE_REQ_CLASS_NORMAL,
    NODE_REQ_COMPLETE,
    REQUESTED_NODE_STATES,
    NODE_REQ_FAILED,
    REQUESTED_NODE_STATES,
    FINAL_NODE_STATES,
)

# from .node_service import NodeService, MachineSpec
from .job_store import JobStore
from .task_store import TaskStore
from typing import List, Set
from google.cloud import datastore
import time
from .util import get_timestamp

from .log import log

from .batch_api import ClusterAPI, JobSpec
from dataclasses import dataclass
from typing import Protocol


class MinConfig(Protocol):
    project: str
    zones: List[str]
    debug_log_prefix: str

    @property
    def location(self) -> str:
        ...


def create_cluster(config: MinConfig, jq, datastore_client, cluster_api, job_id):
    job = jq.get_job(job_id)

    return Cluster(
        config.project,
        config.location,
        job.cluster,
        job_id,
        config.zones,
        jq.job_storage,
        jq.task_storage,
        datastore_client,
        cluster_api,
        config.debug_log_prefix,
    )


class Cluster:
    def __init__(
        self,
        project: str,
        location: str,
        cluster_id: str,
        job_id: str,
        zones: List[str],
        job_store: JobStore,
        task_store: TaskStore,
        client: datastore.Client,
        cluster_api: ClusterAPI,
        debug_log_prefix: str,
    ) -> None:
        self.project = project
        self.zones = zones
        self.client = client
        self.job_store = job_store
        self.task_store = task_store
        self.debug_log_prefix = debug_log_prefix
        self.cluster_api = cluster_api
        self.job_id = job_id
        self.location = location
        self._cluster_id = None

    @property
    def cluster_id(self):
        if self._cluster_id is None:
            job = self.job_store.get_job_must(self.job_id)
            self._cluster_id = job.cluster
        return self._cluster_id

    def get_node_reqs(self):
        return self.cluster_api.get_node_reqs(
            self.project, self.location, self.cluster_id
        )

    def add_nodes(
        self, count: int
    ):  # job_id: str, preemptible: bool, debug_log_url: str):
        job = self.job_store.get_job_must(self.job_id)

        job_spec = JobSpec.model_validate_json(job.kube_job_spec)

        return self.cluster_api.create_job(self.project, self.location, job_spec, count)

    def has_active_node_requests(self):
        node_reqs = self.cluster_api.get_node_reqs(
            self.project, self.location, self.cluster_id
        )
        for node_req in node_reqs:
            if node_req.status in REQUESTED_NODE_STATES:
                return True
        return False

    def stop_cluster(self):
        self.cluster_api.delete_node_reqs(self.project, self.location, self.cluster_id)

    def delete_complete_requests(self):
        self.cluster_api.delete_node_reqs(self.project, self.location, self.cluster_id, only_terminal_reqs=True)



class CachingCaller:
    def __init__(self, fn, expiry_time=5):
        self.prev = {}
        self.expiry_time = expiry_time
        self.fn = fn

    def __call__(self, *args):
        now = time.time()
        immutable_args = tuple(args)
        if immutable_args in self.prev:
            value, timestamp = self.prev[immutable_args]
            if timestamp + self.expiry_time < now:
                return value

        value = self.fn(*args)

        self.prev[immutable_args] = (value, now)

        return value
