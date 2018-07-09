import re
from .task_store import INCOMPLETE_TASK_STATES, Task, STATUS_FAILED, STATUS_COMPLETE
from .node_req_store import AddNodeReqStore, NodeReq, NODE_REQ_SUBMITTED, NODE_REQ_CLASS_PREEMPTIVE, NODE_REQ_CLASS_NORMAL, NODE_REQ_COMPLETE, REQUESTED_NODE_STATES
from .compute_service import ComputeService
from .node_service import NodeService, MachineSpec
from .job_store import JobStore
from .task_store import TaskStore
from .job_queue import JobQueue
from typing import List, Dict, DefaultDict
from collections import defaultdict
from google.cloud import datastore
import time
import sys
import logging
import os
from .util import get_timestamp
from .datastore_batch import Batch
from abc import abstractmethod

log = logging.getLogger(__name__)

SETUP_IMAGE = "sequenceiq/alpine-curl"  # and image which has curl and sh installed, used to prep the worker node

import json

class ClusterStatus:
    def __init__(self, instances : List[dict]) -> None:
        instances_per_key  = defaultdict(lambda: 0) # type: DefaultDict[str, int]

        running_count = 0
        for instance in instances:
            status = instance['status']
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
        return ", ".join(["{}: {}".format(status, count) for status, count in self.table])

    def is_running(self):
        return self.running_count > 0

class Cluster():
    def __init__(self, project : str, zones : List[str], node_req_store : AddNodeReqStore, job_store : JobStore, task_store : TaskStore, client : datastore.Client, debug_log_prefix: str, credentials=None) -> None:
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

    def get_state(self, job_id):
        return ClusterState(job_id, self.task_store, self.node_req_store, self)

    def add_nodes(self, job_id : str, preemptible : bool, debug_log_url_prefix : str, count : int):
        for i in range(count):
            self.add_node(job_id, preemptible, "{}/{}".format(debug_log_url_prefix, i))

    def add_node(self, job_id : str, preemptible : bool, debug_log_url : str):
        job = self._get_job(job_id)
        pipeline_def = json.loads(job.kube_job_spec)
        operation_id = self.nodes.add_node(pipeline_def, preemptible, debug_log_url)
        req = NodeReq(operation_id = operation_id,
            job_id = job_id,
            status = NODE_REQ_SUBMITTED,
            node_class = NODE_REQ_CLASS_PREEMPTIVE if preemptible else NODE_REQ_CLASS_NORMAL,
            sequence = get_timestamp()
        )
        self.node_req_store.add_node_req(req)
        return operation_id


    def get_cluster_status(self, cluster_name : str) -> ClusterStatus:
        instances = self.compute.get_cluster_instances(self.zones, cluster_name)
        return ClusterStatus(instances)

    def stop_cluster(self, cluster_name : str):
        instances = self.compute.get_cluster_instances(self.zones, cluster_name)
        if len(instances) == 0:
            log.warning("Attempted to delete instances in cluster %s but no instances found!", cluster_name)
        else:
            for instance in instances:
                zone = instance['zone'].split('/')[-1]
                log.info("deleting instance %s associated with cluster %s", instance['name'], cluster_name)
                self.compute.stop(instance['name'], zone)

            for instance in instances:
                zone = instance['zone'].split('/')[-1]
                self.wait_for_instance_status(zone, instance['name'], "TERMINATED")

    def wait_for_instance_status(self, zone : str, instance_name : str, desired_status : str):
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


    def delete_job(self, job_id : str):
        batch = Batch(self.client)

        self.job_store.delete(job_id, batch=batch)

        job_key = self.client.key("Job", job_id)
        entity_job = self.client.get(job_key)

        task_ids = entity_job.get("tasks",[])
        # delete tasks
        for task_id in task_ids:
            self.task_store.delete(task_id, batch=batch)

        # clean up associated node requests
        self.node_req_store.delete_for_job(job_id, batch=batch)

        self.job_store.delete(job_id, batch=batch)

        batch.flush()

    def cancel_add_node(self, operation_id : str):
        self.nodes.cancel_add_node(operation_id )

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
    # def test_api(self):
    #     """Simple api call used to verify the service is enabled"""
    #     result = self.service.projects().operations().list(name="projects/{}/operations".format(self.project)).execute()
    #     assert "operations" in result
    #
    # def test_image(self, docker_image, sample_url, logging_url):
    #     pipeline_def = self.create_pipeline_spec("test-image", docker_image, "bash -c 'echo hello'",
    #                                              "/mnt/kubequeconsume", sample_url, 1, 1, get_random_string(20), 10,
    #                                              False)
    #     operation = self.add_node(pipeline_def, False)
    #     operation_name = operation['name']
    #     while not operation['done']:
    #         operation = self.service.projects().operations().get(name=operation_name).execute()
    #         time.sleep(5)
    #     if "error" in operation:
    #         raise Exception("Got error: {}".format(operation['error']))
    #     log.info("execution completed successfully")

    def is_owner_running(self, owner : str) -> bool:
        if owner == "localhost":
            return False

        m = re.match("projects/([^/]+)/zones/([^/]+)/([^/]+)", owner)
        assert m is not None, "Expected a instance name with zone but got owner={}".format(owner)
        project_id, zone, instance_name = m.groups()
        assert project_id == self.project
        return self.compute.get_instance_status(zone, instance_name) == 'RUNNING'


    def create_pipeline_spec(self, jobid : str, cluster_name : str, consume_exe_url : str, docker_image : str, consume_exe_args : List[str], machine_specs : MachineSpec, monitor_port : int) -> dict:
        mount_point = machine_specs.mount_point

        consume_exe_path = os.path.join(mount_point, "consume")
        consume_data = os.path.join(mount_point, "data")

        return self.nodes.create_pipeline_json(jobid=jobid,
                                          cluster_name=cluster_name,
                                          setup_image=SETUP_IMAGE,
                                          setup_parameters=["sh", "-c",
                                                            "curl -o {consume_exe_path} {consume_exe_url} && chmod a+x {consume_exe_path} && mkdir {consume_data} && chmod a+rwx {consume_data}".format(
                                                                consume_exe_url=consume_exe_url,
                                                                consume_data=consume_data,
                                                                consume_exe_path=consume_exe_path)],
                                          docker_image=docker_image,
                                          docker_command=[consume_exe_path, "consume", "--cacheDir",
                                                          os.path.join(consume_data, "cache"), "--tasksDir",
                                                          os.path.join(consume_data, "tasks")] + consume_exe_args,
                                          machine_specs=machine_specs,
                                          monitor_port=monitor_port)

    def get_cluster_mod(self, job_id):
        return ClusterMod(job_id, self, self.debug_log_prefix)



class ClusterState:
    def __init__(self, job_id : str, task_store : TaskStore, node_req_store : AddNodeReqStore, cluster : Cluster) -> None:
        self.job_id = job_id
        self.cluster = cluster
        self.datastore = datastore
        self.tasks = [] # type: List[Task]
        self.node_reqs = [] # type: List[NodeReq]
        self.node_req_store = node_req_store
        self.task_store = task_store
        #self.add_node_statuses = [] # type: List[AddNodeStatus]

    def update(self):
        # update tasks
        self.tasks = self.task_store.get_tasks(self.job_id)

        # poll all the operations which are not marked as dead
        self.node_reqs = self.node_req_store.get_node_reqs(self.job_id)

        # get all the status of each operation
        for node_req in self.node_reqs:
            if node_req.status != NODE_REQ_COMPLETE:
                op = self.cluster.nodes.get_add_node_status(node_req.operation_id)
                new_status = op.status
                # print("fetched {} and status was {}".format(node_req.operation_id, new_status))
                if new_status != node_req.status:
                    self.node_req_store.update_node_req_status(node_req.operation_id, op.status, op.instance_name)

    def get_summary(self) -> str:
        by_status = defaultdict(lambda: 0)
        for t in self.tasks:
            by_status[t.status] += 1
        statuses = sorted(by_status.keys())
        task_status = ", ".join(["{} ({})".format(status, by_status[status]) for status in statuses])

        by_status = defaultdict(lambda: 0)
        for r in self.node_reqs:
            by_status[r.status] += 1
        statuses = sorted(by_status.keys())
        node_status = ", ".join(["{} ({})".format(status, by_status[status]) for status in statuses])

        return "tasks: {}, nodes: {}".format(task_status, node_status)

    def get_incomplete_task_count(self) -> int:
        return len([t for t in self.tasks if t.status in INCOMPLETE_TASK_STATES])

    def get_requested_node_count(self) -> int:
        return len([o for o in self.node_reqs if o.status in REQUESTED_NODE_STATES])

    def get_preempt_attempt_count(self) -> int:
        return len([o for o in self.node_reqs if o.node_class == NODE_REQ_CLASS_PREEMPTIVE ])

    def get_running_tasks_with_invalid_owner(self) -> List[str]:
        log.warning("get_running_tasks_with_invalid_owner is still a stub")
        return []

    def get_successful_task_count(self):
        return len([t for t in self.tasks if (t.status == STATUS_COMPLETE and t.exit_code == "0") ])

    def get_failed_task_count(self):
        return len([t for t in self.tasks if t.status == STATUS_FAILED or (t.status == STATUS_COMPLETE and t.exit_code != "0") ])

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
    def __init__(self, job_id : str, cluster : Cluster, debug_log_prefix : str) -> None:
        self.job_id = job_id
        self.cluster = cluster
        self.debug_log_prefix = debug_log_prefix

    def add_node(self, preemptable : bool) -> None:
        raise
        self.cluster.add_node(self.job_id, preemptable, self.debug_log_prefix)

    def cancel_nodes(self, state : ClusterState, count : int) -> None:
        pending_node_reqs = [x for x in state.node_reqs if x.status == NODE_REQ_SUBMITTED ]
        pending_node_reqs.sort(key=lambda x: x.sequence)
        pending_node_reqs = list(reversed(pending_node_reqs))
        if count < len(pending_node_reqs):
            pending_node_reqs = pending_node_reqs[:count]
        for x in pending_node_reqs:
            self.cluster.cancel_add_node(x.operation_id)
