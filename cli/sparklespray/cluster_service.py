import re
from .task_store import (
    STATUS_FAILED,
    STATUS_COMPLETE,
)
from .node_req_store import (
    AddNodeReqStore,
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
from .compute_service import ComputeService
from .node_service import NodeService, MachineSpec
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

# which step in the actions does kubeconsume run in
CONSUMER_ACTION_ID = 2


def _unique_id():
    import uuid

    return str(uuid.uuid4())



class Cluster:
    def __init__(
        self,
        project: str,
        zones: List[str],
        node_req_store: AddNodeReqStore,
        job_store: JobStore,
        task_store: TaskStore,
        client: datastore.Client,
        debug_log_prefix: str,
        credentials=None,
    ) -> None:
        self.compute = ComputeService(project, credentials)
        self.nodes = NodeService(project, zones, credentials)
        self.node_req_store = node_req_store
        self.project = project
        self.zones = zones
        self.client = client
        self._get_job = CachingCaller(job_store.get_job)
        self.job_store = job_store
        self.task_store = task_store
        self.debug_log_prefix = debug_log_prefix


    def add_node(self, job_id: str, preemptible: bool, debug_log_url: str):
        job = self._get_job(job_id)
        pipeline_def = json.loads(job.kube_job_spec)
        operation_id = self.nodes.add_node(pipeline_def, preemptible, debug_log_url)
        req = NodeReq(
            operation_id=operation_id,
            cluster_id=job.cluster,
            status=NODE_REQ_SUBMITTED,
            node_class=NODE_REQ_CLASS_PREEMPTIVE
            if preemptible
            else NODE_REQ_CLASS_NORMAL,
            sequence=get_timestamp(),
            job_id=job_id,
            instance_name=None,
        )
        self.node_req_store.add_node_req(req)
        log.info(
            f"Requested node (preemptible: {preemptible} operation_id: {operation_id} debug_log_url: {debug_log_url})"
        )
        return operation_id

    def has_active_node_requests(self, cluster_id):
        node_reqs = self.node_req_store.get_node_reqs(cluster_id)
        for node_req in node_reqs:
            if node_req.status in REQUESTED_NODE_STATES:
                return True
        return False

    def ensure_named_volumes_exist(self, zone, pd_mounts: List[DiskMountT]):
        for pd_mount in pd_mounts:
            if isinstance(pd_mount, ExistingDiskMount):
                volume = self.compute.get_volume_details(zone, pd_mount.name)
                assert (
                    volume
                ), f"Requested mounting of an existing volume ({pd_mount.name}) but it does not exist in zone {zone}"

    def stop_cluster(self, cluster_name: str):
        node_reqs = self.node_req_store.get_node_reqs(cluster_name)
        for i, node_req in enumerate(node_reqs):
            if node_req.status in FINAL_NODE_STATES:
                log.info(
                    "Canceling node request %s (%s/%s)",
                    node_req.operation_id,
                    i,
                    len(node_reqs),
                )
                # if we are trying to stop a cluster that was created with an old version of sparkles
                # which used the old pipeline API, don't even try to cancel the operation. Instead
                # just warn the use of the situation and move on. Typically the operation terminated log ago
                # and the person didn't upgrade sparkles in the middle of running a job.
                if re.match("projects/[^/]+/operations/[^/]+", node_req.operation_id):
                    log.warn(
                        f"Cannot cancel past request to add a node because the operation_id ({ node_req.operation_id}) looks like it was created with the old deprecated google API. Assuming this node terminated long ago and moving on."
                    )
                else:
                    try:
                        self.cancel_add_node(node_req.operation_id)
                    except HttpError as ex:
                        log.info("Got httpError canceling node request: %s", ex)

        instances = self.compute.get_cluster_instances(self.zones, cluster_name)
        if len(instances) == 0:
            log.warning(
                "Attempted to delete instances in cluster %s but no instances found!",
                cluster_name,
            )
        else:
            for instance in instances:
                zone = instance["zone"].split("/")[-1]
                log.info(
                    "deleting instance %s associated with cluster %s",
                    instance["name"],
                    cluster_name,
                )
                self.compute.stop(instance["name"], zone)

            for instance in instances:
                zone = instance["zone"].split("/")[-1]
                self.wait_for_instance_status(zone, instance["name"], "TERMINATED")

    def wait_for_instance_status(
        self, zone: str, instance_name: str, desired_status: str
    ):
        def p(msg):
            sys.stdout.write(msg)
            sys.stdout.flush()

        p("Waiting for {} to become {}...".format(instance_name, desired_status))
        prev_status = None
        while True:
            instance_status = self.compute.get_instance_status(zone, instance_name)
            if instance_status != prev_status:
                prev_status = instance_status
                p("(now {})".format(instance_status))
            if instance_status == desired_status:
                break
            time.sleep(5)
            p(".")
        p("\n")

    def update_node_reqs(self, cluster_id):
        from . import node_req_store
        node_reqs = self.node_req_store.get_node_reqs(cluster_id=cluster_id)
        
        for node_req in node_reqs:
            if not node_req_store.is_terminal_state(node_req.status):
                add_node_status = self.nodes.get_add_node_status(node_req.operation_id)
                real_status = add_node_status.status
                instance_name = add_node_status.instance_name
                if real_status != node_req.status or instance_name != node_req.instance_name:
                    self.node_req_store.update_node_req_status(node_req.operation_id, real_status, instance_name)

    # def update(self):
    #     # update tasks
    #     self.tasks = self.task_store.get_tasks(self.job_id)

    #     self.node_reqs = self.node_req_store.get_node_reqs(self.cluster_id)

    #     # poll all the operations which are not marked as dead
    #     log.debug("fetched %d node_reqs", len(self.node_reqs))

    #     # get all the status of each operation
    #     for node_req in self.node_reqs:
    #         if node_req.status not in [NODE_REQ_COMPLETE, NODE_REQ_FAILED]:
    #             op = self.cluster.nodes.get_add_node_status(node_req.operation_id)
    #             if op is None:
    #                 # if the operation is missing, google probably deleted it because it was old
    #                 new_status = NODE_REQ_COMPLETE
    #                 new_instance_name = None
    #             else:
    #                 new_status = op.status
    #                 new_instance_name = op.instance_name
    #                 if new_status == NODE_REQ_FAILED:
    #                     log.warning(
    #                         "Node request (%s) failed: %s",
    #                         node_req.operation_id,
    #                         op.error_message,
    #                     )
    #                 # print("fetched {} and status was {}".format(node_req.operation_id, new_status))

    #             if new_status != node_req.status:
    #                 self.node_req_store.update_node_req_status(
    #                     node_req.operation_id, new_status, new_instance_name
    #                 )
    #                 # reflect the change in memory as well
    #                 node_req.status = new_status


    def cleanup_node_reqs(self, job_id):
        # before updating node requests, make sure the state has been updated to
        # reflect which ones are actually still running and which are done.
        # node_req_store.cleanup_cluster() assumes the states are up to date.
        job = self._get_job(job_id)
        self.update_node_reqs(job.cluster)

        batch = Batch(self.client)

        job = self.job_store.get_job(job_id)
        assert job is not None

        self.node_req_store.cleanup_cluster(job.cluster, batch=batch)
        log.info(f"in cleanup_node_reqs flushing batch: {batch}")

        batch.flush()

    def delete_job(self, job_id: str):
        batch = Batch(self.client)

        self.job_store.delete(job_id, batch=batch)

        job_key = self.client.key("Job", job_id)
        entity_job = self.client.get(job_key)

        assert entity_job is not None
        task_ids = set(entity_job.get("tasks", []))
        # If we've got a mismatch between the data store and the data in the Job object, take the union
        # to get things back into sync
        for task in self.task_store.get_tasks(job_id):
            task_ids.add(task.task_id)

        # delete tasks
        for task_id in task_ids:
            self.task_store.delete(task_id, batch=batch)

        self.job_store.delete(job_id, batch=batch)
        log.info(f"in delete_job flushing batch: {batch}")

        batch.flush()

    def cancel_add_node(self, operation_id: str):
        self.nodes.cancel_add_node(operation_id)

    def test_pipeline_api(self):
        """Simple api call used to verify the service is enabled"""
        assert (
            len(self.zones) == 1
        ), "With the switch to the google's life science API, we can now only use one zone"
        region = self.zones[0][:-2]

    def test_image(
        self,
        project: str,
        zones: List[str],
        docker_image: str,
        debug_log_url: str,
        machine_specs: MachineSpec,
        monitor_port: int,
    ):
        from sparklespray.gcp_utils import (
            create_validation_pipeline_spec,
            get_region,
        )

        assert (
            len(zones) == 1
        ), f"Only one zone supported at this time, but got: {zones}"
        zone = zones[0]

        regions = set([get_region(zone) for zone in zones])
        assert (
            len(regions) == 1
        ), "Only single region supported but got zones in {regions}"
        region = list(regions)[0]

        cluster_name = "validationtest-" + _unique_id()

        self.nodes.test_pipeline_api(self.project, region)
        pipeline_def = create_validation_pipeline_spec(
            project=project,
            zones=zones,
            jobid="test-image",
            cluster_name=cluster_name,
            docker_image=docker_image,
            machine_specs=machine_specs,
            monitor_port=monitor_port,
        )
        operation_id = self.nodes.add_node(
            pipeline_def, preemptible=False, debug_log_url=debug_log_url
        )
        print(f"Submitted request for a new worker ( operation_id: {operation_id} )")

        prev_event_description = None
        while True:
            status = self.nodes.get_add_node_status(operation_id)
            assert status is not None
            event_description = status.last_event_description
            if event_description != prev_event_description:
                print(f"Latest event: {event_description}")
                prev_event_description = event_description

            if status.status == NODE_REQ_FAILED:
                successful_completion = False
                break
            elif status.status == NODE_REQ_COMPLETE:
                successful_completion = True
                break

            time.sleep(2)

        print(f"Test complete: successful_completion={successful_completion}")
        self.stop_cluster(cluster_name)
        assert (
            successful_completion
        ), f"Test failed. For more information run:\n  sparkles dump-operation {operation_id}\n\n"

    def get_cluster_mod(self, job_id):
        job = self._get_job(job_id)
        return ClusterMod(job_id, job.cluster, self, self.debug_log_prefix)



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


class ClusterMod:
    def __init__(self, job_id: str, cluster_id: str, cluster: Cluster, debug_log_prefix: str) -> None:
        self.job_id = job_id
        self.cluster_id = cluster_id
        self.cluster = cluster
        self.debug_log_prefix = debug_log_prefix
        self.node_counter = 0  # just used to make sure logs are unique

    def add_node(self, preemptable: bool) -> None:
        self.node_counter += 1
        debug_log_path = "{}/{}/{}-{}.txt".format(
            self.debug_log_prefix, self.job_id, get_timestamp(), self.node_counter
        )
        self.cluster.add_node(self.job_id, preemptable, debug_log_path)

    def cancel_nodes(self, count: int) -> None:
        pending_node_reqs = self.cluster.node_req_store.get_node_reqs(self.cluster_id, NODE_REQ_SUBMITTED)
        pending_node_reqs.sort(key=lambda x: x.sequence)
        pending_node_reqs = list(reversed(pending_node_reqs))
        if count < len(pending_node_reqs):
            pending_node_reqs = pending_node_reqs[:count]
        for x in pending_node_reqs:
            self.cluster.cancel_add_node(x.operation_id)
