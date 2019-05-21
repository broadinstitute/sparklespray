import re
from .task_store import (
    INCOMPLETE_TASK_STATES,
    Task,
    STATUS_FAILED,
    STATUS_COMPLETE,
    STATUS_CLAIMED,
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
    NODE_REQ_STAGING,
    NODE_REQ_RUNNING,
    FINAL_NODE_STATES,
)
from .compute_service import ComputeService
from .node_service import NodeService, MachineSpec
from .job_store import JobStore
from .task_store import TaskStore
from .job_queue import JobQueue
from typing import List, Dict, DefaultDict, Optional
from collections import defaultdict
from google.cloud import datastore
import time
import sys
import logging
import os
from .util import get_timestamp
from .datastore_batch import Batch
from abc import abstractmethod

from .log import log
from googleapiclient.errors import HttpError

# and image which has curl and sh installed, used to prep the worker node
SETUP_IMAGE = "sequenceiq/alpine-curl"

import json

# which step in the actions does kubeconsume run in
CONSUMER_ACTION_ID = 2


class ClusterStatus:
    def __init__(self, instances: List[dict]) -> None:
        # type: DefaultDict[str, int]
        instances_per_key = defaultdict(lambda: 0)

        running_count = 0
        for instance in instances:
            status = instance["status"]
            key = status
            instances_per_key[key] += 1
            if status != "STOPPING":
                running_count += 1

        table = []
        for key, count in instances_per_key.items():
            table.append([key] + [count])
        table.sort()

        self.table = table
        self.running_count = running_count

    def __eq__(self, other):
        return self.table == other.table

    def as_string(self):
        if len(self.table) == 0:
            return "(no nodes)"
        return ", ".join(
            ["{}: {}".format(status, count) for status, count in self.table]
        )

    def is_running(self):
        return self.running_count > 0


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

    def get_raw_operation_details(self, operation_id):
        return self.nodes.get_operation_details(operation_id)

    def get_state(self, job_id):
        job = self.job_store.get_job(job_id)
        return ClusterState(
            job_id, job.cluster, self.task_store, self.node_req_store, self
        )

    def add_nodes(
        self, job_id: str, preemptible: bool, debug_log_url_prefix: str, count: int
    ):
        for i in range(count):
            self.add_node(job_id, preemptible, "{}/{}".format(debug_log_url_prefix, i))

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
        )
        self.node_req_store.add_node_req(req)
        log.info(
            f"Requested node (preemptible: {preemptible} operation_id: {operation_id} debug_log_url: {debug_log_url})"
        )
        return operation_id

    def get_cluster_status(self, cluster_name: str) -> ClusterStatus:
        instances = self.compute.get_cluster_instances(self.zones, cluster_name)
        return ClusterStatus(instances)

    def has_active_node_requests(self, cluster_id):
        node_reqs = self.node_req_store.get_node_reqs(cluster_id)
        for node_req in node_reqs:
            if node_req.status in REQUESTED_NODE_STATES:
                return True
        return False

    def stop_cluster(self, cluster_name: str):
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

    def cleanup_node_reqs(self, job_id):
        batch = Batch(self.client)

        job = self.job_store.get_job(job_id)
        assert job is not None

        self.node_req_store.cleanup_cluster(job.cluster, batch=batch)

        batch.flush()

    def delete_job(self, job_id: str):
        batch = Batch(self.client)

        self.job_store.delete(job_id, batch=batch)

        job_key = self.client.key("Job", job_id)
        entity_job = self.client.get(job_key)

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

    # def get_node_req_status(self, operation_id):
    #     raise Exception("unimp")
    #     request = self.service.projects.operations().get(name=operation_id)
    #     response = request.execute()
    #
    #     runtimeMetadata = response.get("metadata", {}).get("runtimeMetadata", {})
    #     if "computeEngine" in runtimeMetadata:
    #         if response['done']:
    #             return NODE_REQ_COMPLETE
    #         else:
    #             return NODE_REQ_RUNNING
    #     else:
    #         log.info("Operation %s is not yet started. Events: %s", operation_id, response["metadata"]["events"])
    #         return NODE_REQ_SUBMITTED
    #
    def test_pipeline_api(self):
        """Simple api call used to verify the service is enabled"""
        self.nodes.test_pipeline_api(self.project)

    def test_image(self, docker_image, sample_url, logging_url, boot_volume_in_gb):
        self.nodes.test_pipeline_submit_api(
            setup_image=SETUP_IMAGE,
            job_image=docker_image,
            command=["sh", "-c", "echo okay"],
            machine_type="n1-standard-2",
            boot_volume_in_gb=boot_volume_in_gb,
        )

    def is_owner_running(self, owner: str) -> bool:
        if owner == "localhost":
            return False

        m = re.match("projects/([^/]+)/zones/([^/]+)/([^/]+)", owner)
        assert (
            m is not None
        ), "Expected a instance name with zone but got owner={}".format(owner)
        project_id, zone, instance_name = m.groups()
        # assert project_id == self.project, "project_id ({}) != self.project ({})".format(project_id, self.project)
        return self.compute.get_instance_status(zone, instance_name) == "RUNNING"

    def create_pipeline_spec(
        self,
        jobid: str,
        cluster_name: str,
        consume_exe_url: str,
        docker_image: str,
        consume_exe_args: List[str],
        machine_specs: MachineSpec,
        monitor_port: int,
    ) -> dict:
        mount_point = machine_specs.mount_point

        consume_exe_path = os.path.join(mount_point, "consume")
        consume_data = os.path.join(mount_point, "data")

        return self.nodes.create_pipeline_json(
            jobid=jobid,
            cluster_name=cluster_name,
            setup_image=SETUP_IMAGE,
            setup_parameters=[
                "sh",
                "-c",
                "curl -o {consume_exe_path} {consume_exe_url} && chmod a+x {consume_exe_path} && mkdir {consume_data} && chmod a+rwx {consume_data}".format(
                    consume_exe_url=consume_exe_url,
                    consume_data=consume_data,
                    consume_exe_path=consume_exe_path,
                ),
            ],
            docker_image=docker_image,
            docker_command=[
                consume_exe_path,
                "consume",
                "--cacheDir",
                os.path.join(consume_data, "cache"),
                "--tasksDir",
                os.path.join(consume_data, "tasks"),
            ]
            + consume_exe_args,
            machine_specs=machine_specs,
            monitor_port=monitor_port,
        )

    def get_cluster_mod(self, job_id):
        return ClusterMod(job_id, self, self.debug_log_prefix)


class ClusterState:
    def __init__(
        self,
        job_id: str,
        cluster_id: str,
        task_store: TaskStore,
        node_req_store: AddNodeReqStore,
        cluster: Cluster,
    ) -> None:
        self.job_id = job_id
        self.cluster_id = cluster_id
        self.cluster = cluster
        self.datastore = datastore
        self.tasks = []  # type: List[Task]
        self.node_reqs = []  # type: List[NodeReq]
        self.node_req_store = node_req_store
        self.task_store = task_store
        self.failed_node_req_count = 0
        # self.add_node_statuses = [] # type: List[AddNodeStatus]

    def update(self):
        # update tasks
        self.tasks = self.task_store.get_tasks(self.job_id)

        self.node_reqs = self.node_req_store.get_node_reqs(self.cluster_id)

        # poll all the operations which are not marked as dead
        log.debug("fetched %d node_reqs", len(self.node_reqs))

        # get all the status of each operation
        for node_req in self.node_reqs:
            if node_req.status not in [NODE_REQ_COMPLETE, NODE_REQ_FAILED]:
                op = self.cluster.nodes.get_add_node_status(node_req.operation_id)
                new_status = op.status
                if new_status == NODE_REQ_FAILED:
                    log.warning(
                        "Node request (%s) failed: %s",
                        node_req.operation_id,
                        op.error_message,
                    )
                    self.failed_node_req_count += 1
                # print("fetched {} and status was {}".format(node_req.operation_id, new_status))
                if new_status != node_req.status:
                    self.node_req_store.update_node_req_status(
                        node_req.operation_id, op.status, op.instance_name
                    )
                    # reflect the change in memory as well
                    node_req.status = new_status

    def get_tasks(self):
        return self.tasks

    def get_running_tasks(self):
        return [x for x in self.tasks if x.status == STATUS_CLAIMED]

    def is_task_running(self, task_id):
        by_id = {x.task_id: x for x in self.tasks}
        return by_id[task_id].status == STATUS_CLAIMED

    def get_summary(self) -> str:
        # compute status of tasks
        by_status: Dict[str, int] = defaultdict(lambda: 0)
        for t in self.tasks:
            if t.status == STATUS_COMPLETE:
                label = "{}(code={})".format(t.status, t.exit_code)
            elif t.status == STATUS_FAILED:
                label = "{}({})".format(t.status, t.failure_reason)
            else:
                label = t.status
            by_status[label] += 1
        statuses = sorted(by_status.keys())
        task_status = ", ".join(
            ["{} ({})".format(status, by_status[status]) for status in statuses]
        )

        # compute status of workers
        by_status = defaultdict(lambda: 0)
        to_desc = {
            NODE_REQ_CLASS_NORMAL: "non-preempt",
            NODE_REQ_CLASS_PREEMPTIVE: "preemptible",
        }
        for r in self.node_reqs:
            label = "{}(type={})".format(r.status, to_desc[r.node_class])
            by_status[label] += 1
        statuses = sorted(by_status.keys())
        node_status = ", ".join(
            ["{} ({})".format(status, by_status[status]) for status in statuses]
        )

        return "tasks: {}, worker nodes: {}".format(task_status, node_status)

    def get_incomplete_task_count(self) -> int:
        return len([t for t in self.tasks if t.status in INCOMPLETE_TASK_STATES])

    def get_requested_node_count(self) -> int:
        return len([o for o in self.node_reqs if o.status in REQUESTED_NODE_STATES])

    def get_preempt_attempt_count(self) -> int:
        return len(
            [o for o in self.node_reqs if o.node_class == NODE_REQ_CLASS_PREEMPTIVE]
        )

    def get_running_tasks_with_invalid_owner(self) -> List[str]:
        node_req_by_instance_name: Dict[str, NodeReq] = {}
        for node_req in self.node_reqs:
            if node_req.instance_name is not None:
                assert node_req.instance_name not in node_req_by_instance_name
                node_req_by_instance_name[node_req.instance_name] = node_req

        task_ids_needing_reset = []
        claimed_tasks = [task for task in self.tasks if task.status == STATUS_CLAIMED]

        for task in claimed_tasks:
            instance_name = task.get_instance_name()

            if instance_name not in node_req_by_instance_name:
                log.warning(
                    "instance {} was not listed among {}".format(
                        instance_name, ", ".join(node_req_by_instance_name.keys())
                    )
                )
            else:
                node_req = node_req_by_instance_name[instance_name]
                if node_req.status in [NODE_REQ_COMPLETE, NODE_REQ_FAILED]:
                    log.warning(
                        "task {} status = {}, but node_req was {}".format(
                            task.task_id, task.status, node_req.status
                        )
                    )
                    if node_req.node_class != NODE_REQ_CLASS_PREEMPTIVE:
                        log.error(
                            "instance %s terminated but task %s was reported to still be using instance and the instance was not preemptiable",
                            instance_name,
                            task.task_id,
                        )
                    task_ids_needing_reset.append(task.task_id)
                else:
                    assert node_req.status in [
                        NODE_REQ_SUBMITTED,
                        NODE_REQ_STAGING,
                        NODE_REQ_RUNNING,
                    ]

        return task_ids_needing_reset

    def get_successful_task_count(self):
        return len(self.get_successful_tasks())

    def get_failed_task_count(self):
        return len(self.get_failed_tasks())

    def get_failed_tasks(self):
        return [
            t
            for t in self.tasks
            if t.status == STATUS_FAILED
            or (t.status == STATUS_COMPLETE and t.exit_code != "0")
        ]

    def get_successful_tasks(self):
        return [
            t
            for t in self.tasks
            if (t.status == STATUS_COMPLETE and t.exit_code == "0")
        ]

    def is_done(self):
        return self.get_incomplete_task_count() == 0


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
    def __init__(self, job_id: str, cluster: Cluster, debug_log_prefix: str) -> None:
        self.job_id = job_id
        self.cluster = cluster
        self.debug_log_prefix = debug_log_prefix
        self.node_counter = 0  # just used to make sure logs are unique

    def add_node(self, preemptable: bool) -> None:
        self.node_counter += 1
        debug_log_path = "{}/{}/{}-{}.txt".format(
            self.debug_log_prefix, self.job_id, get_timestamp(), self.node_counter
        )
        self.cluster.add_node(self.job_id, preemptable, debug_log_path)

    def cancel_nodes(self, state: ClusterState, count: int) -> None:
        pending_node_reqs = [
            x for x in state.node_reqs if x.status == NODE_REQ_SUBMITTED
        ]
        pending_node_reqs.sort(key=lambda x: x.sequence)
        pending_node_reqs = list(reversed(pending_node_reqs))
        if count < len(pending_node_reqs):
            pending_node_reqs = pending_node_reqs[:count]
        for x in pending_node_reqs:
            self.cluster.cancel_add_node(x.operation_id)
