import re
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
import sys
from .util import get_timestamp
from .datastore_batch import Batch
from .model import ExistingDiskMount, DiskMountT

from .log import log
from googleapiclient.errors import HttpError

import json


def _unique_id():
    import uuid

    return str(uuid.uuid4())


from .batch_api import ClusterAPI, JobSpec
from dataclasses import dataclass


@dataclass
class MinConfig:
    project: str
    location: str
    zones: List[str]
    debug_log_prefix: str


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
            job = self.job_store.get_job(self.job_id)
            self._cluster_id = job.cluster
        return self._cluster_id

    def get_node_reqs(self):
        return self.cluster_api.get_node_reqs(
            self.project, self.location, self.cluster_id
        )

    def add_nodes(
        self, count: int
    ):  # job_id: str, preemptible: bool, debug_log_url: str):
        job = self.job_store.get_job(self.job_id)

        job_spec = JobSpec.model_validate_json(job.kube_job_spec)

        assert count == 1
        return self.cluster_api.create_job(self.project, self.location, job_spec)

    def has_active_node_requests(self):
        node_reqs = self.cluster_api.get_node_reqs(
            self.project, self.location, self.cluster_id
        )
        for node_req in node_reqs:
            if node_req.status in REQUESTED_NODE_STATES:
                return True
        return False

    # def ensure_named_volumes_exist(self, zone, pd_mounts: List[DiskMountT]):
    #     raise NotImplemented
    #     for pd_mount in pd_mounts:
    #         if isinstance(pd_mount, ExistingDiskMount):
    #             volume = self.compute.get_volume_details(zone, pd_mount.name)
    #             assert (
    #                 volume
    #             ), f"Requested mounting of an existing volume ({pd_mount.name}) but it does not exist in zone {zone}"

    def stop_cluster(self):
        self.cluster_api.stop_cluster(self.project, self.location, self.cluster_id)
        # node_reqs = self.node_req_store.get_node_reqs(cluster_name)
        # for i, node_req in enumerate(node_reqs):
        #     if node_req.status in FINAL_NODE_STATES:
        #         log.info(
        #             "Canceling node request %s (%s/%s)",
        #             node_req.operation_id,
        #             i,
        #             len(node_reqs),
        #         )
        #         # if we are trying to stop a cluster that was created with an old version of sparkles
        #         # which used the old pipeline API, don't even try to cancel the operation. Instead
        #         # just warn the use of the situation and move on. Typically the operation terminated log ago
        #         # and the person didn't upgrade sparkles in the middle of running a job.
        #         if re.match("projects/[^/]+/operations/[^/]+", node_req.operation_id):
        #             log.warn(
        #                 f"Cannot cancel past request to add a node because the operation_id ({ node_req.operation_id}) looks like it was created with the old deprecated google API. Assuming this node terminated long ago and moving on."
        #             )
        #         else:
        #             try:
        #                 self.cancel_add_node(node_req.operation_id)
        #             except HttpError as ex:
        #                 log.info("Got httpError canceling node request: %s", ex)

        # instances = self.compute.get_cluster_instances(self.zones, cluster_name)
        # if len(instances) == 0:
        #     log.warning(
        #         "Attempted to delete instances in cluster %s but no instances found!",
        #         cluster_name,
        #     )
        # else:
        #     for instance in instances:
        #         zone = instance["zone"].split("/")[-1]
        #         log.info(
        #             "deleting instance %s associated with cluster %s",
        #             instance["name"],
        #             cluster_name,
        #         )
        #         self.compute.stop(instance["name"], zone)

        #     for instance in instances:
        #         zone = instance["zone"].split("/")[-1]
        #         self.wait_for_instance_status(zone, instance["name"], "TERMINATED")


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
