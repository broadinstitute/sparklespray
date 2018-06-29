import re
from kubeque.node_req_store import AddNodeReqStore, NodeReq, NODE_REQ_SUBMITTED, NODE_REQ_CLASS_PREEMPTIVE, NODE_REQ_CLASS_NORMAL
from kubeque.compute_service import ComputeService
from kubeque.node_service import NodeService, MachineSpec
from typing import List, Dict, DefaultDict
from collections import defaultdict
import time
import sys
import logging
import os

log = logging.getLogger(__name__)

SETUP_IMAGE = "sequenceiq/alpine-curl"  # and image which has curl and sh installed, used to prep the worker node

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

class Cluster:
    def __init__(self, project : str, zones : List[str], node_req_store : AddNodeReqStore, credentials=None) -> None:
        self.compute = ComputeService(project, credentials)
        self.nodes = NodeService(project, zones, credentials)
        self.node_req_store = node_req_store
        self.project = project
        self.zones = zones

    def add_node(self, job_id : str, preemptible : bool, pipeline_def : dict, debug_log_url : str):
        operation_id = self.nodes.add_node(pipeline_def, preemptible, debug_log_url)
        req = NodeReq(operation_id = operation_id,
            job_id = job_id,
            status = NODE_REQ_SUBMITTED,
            node_class = NODE_REQ_CLASS_PREEMPTIVE if preemptible else NODE_REQ_CLASS_NORMAL
        )
        self.node_req_store.add_node_req(req)
        return operation_id


    def get_cluster_status(self, cluster_name : str) -> ClusterStatus:
        instances = self.compute.get_cluster_instances(self.zones, cluster_name)
        return ClusterStatus(instances)

    def stop_cluster(self, cluster_name):
        instances = self.compute.get_cluster_instances(cluster_name)
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

