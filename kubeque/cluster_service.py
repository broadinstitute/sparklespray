import re
from .task_store import INCOMPLETE_TASK_STATES, Task
from .node_req_store import AddNodeReqStore, NodeReq, NODE_REQ_SUBMITTED, NODE_REQ_CLASS_PREEMPTIVE, NODE_REQ_CLASS_NORMAL, REQUESTED_NODE_STATES
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


class ClusterState:
    def __init__(self, job_id : str, jq : JobQueue, datastore, cluster) -> None:
        self.job_id = job_id
        self.cluster = cluster
        self.datastore = datastore
        self.tasks = [] # type: List[Task]
        self.node_reqs = [] # type: List[NodeReq]
        self.operations = [] # type: List[dict]

    def update(self):
        # update tasks
        self.tasks = self.jq.get_tasks(self.job_id)

        # poll all the operations which are not marked as dead
        node_reqs = self.datastore.get_node_reqs(self.job_id)
        for node_req in node_reqs:
            if node_req.status in REQUESTED_NODE_STATES:
                op = self.cluster.get_node_req(node_req.operation_id)
                if op.status not in REQUESTED_NODE_STATES:
                    self.datastore.update_node_req_status(node_req.operation_id, op.status)

    def get_incomplete_task_count(self) -> int:
        return len([t for t in self.tasks if t.status in INCOMPLETE_TASK_STATES])

    def get_requested_node_count(self) -> int:
        return len([o for o in self.operations if o.status in REQUESTED_NODE_STATES])

    def get_preempt_attempt_count(self) -> int:
        return len([o for o in self.node_reqs if o.node_class == NODE_REQ_CLASS_PREEMPTIVE ])

    def get_running_tasks_with_invalid_owner(self) -> List[str]:
        raise Exception("unimp")
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

        self.prev[immutable_args] = (value, timestamp)

        return value

class Cluster:
    def __init__(self, project : str, zones : List[str], node_req_store : AddNodeReqStore, job_store : JobStore, task_store : TaskStore, client : datastore.Client, credentials=None) -> None:
        self.compute = ComputeService(project, credentials)
        self.nodes = NodeService(project, zones, credentials)
        self.node_req_store = node_req_store
        self.project = project
        self.zones = zones
        self.client = client
        self._get_job = CachingCaller(job_store.get_job)
        self.job_store = job_store
        self.task_store = task_store

    def get_state(self):
        raise Exception("unimp")

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
        for node_req in self.node_req_store.get_node_reqs(job_id):
            self.node_req_store.delete(node_req.operation_id, batch=batch)

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

