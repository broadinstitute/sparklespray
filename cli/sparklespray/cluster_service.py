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
    debug_log_prefix: str

    @property
    def location(self) -> str:
        ...


from .job_queue import JobQueue


def create_cluster(
    config: MinConfig, jq: JobQueue, datastore_client, cluster_api, job_id
):
    job = jq.get_job_must(job_id)

    return Cluster(
        config.project,
        config.location,
        job.cluster,
        job_id,
        jq.job_storage,
        jq.task_storage,
        datastore_client,
        cluster_api,
        config.debug_log_prefix,
    )


class Cluster:
    """
    Manages a compute cluster for executing distributed tasks.

    The Cluster class provides an interface for managing compute resources in Google Cloud,
    including provisioning nodes, tracking node requests, and monitoring task execution.
    It serves as the bridge between the job/task storage layer and the actual compute
    infrastructure.

    Attributes:
        project: Google Cloud project ID
        client: Datastore client for storage operations
        job_store: Storage for job metadata
        task_store: Storage for task metadata and status
        debug_log_prefix: Prefix for debug log files
        cluster_api: API for interacting with the batch service
        job_id: ID of the job associated with this cluster
        location: Google Cloud region where the cluster is deployed
        _cluster_id: Cached cluster ID (lazily loaded)
    """

    def __init__(
        self,
        project: str,
        location: str,
        cluster_id: str,
        job_id: str,
        job_store: JobStore,
        task_store: TaskStore,
        client: datastore.Client,
        cluster_api: ClusterAPI,
        debug_log_prefix: str,
    ) -> None:
        self.project = project
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
        self, count: int, max_retry_count: int
    ):  # job_id: str, preemptible: bool, debug_log_url: str):
        job = self.job_store.get_job_must(self.job_id)

        job_spec = JobSpec.model_validate_json(job.kube_job_spec)

        return self.cluster_api.create_job(self.project, self.location, job_spec, count, max_retry_count)

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
        self.cluster_api.delete_node_reqs(
            self.project, self.location, self.cluster_id, only_terminal_reqs=True
        )

    def is_live_owner(self, owner):
        return self.cluster_api.is_instance_running(owner)


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
