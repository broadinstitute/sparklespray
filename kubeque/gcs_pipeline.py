import time
from oauth2client.client import GoogleCredentials
from apiclient.discovery import build
import logging
import random
import string
import sys
from googleapiclient.errors import HttpError
from kubeque.gcp import NODE_REQ_COMPLETE, NODE_REQ_RUNNING, NODE_REQ_SUBMITTED
import re
import attr
import os
from collections import defaultdict

log = logging.getLogger(__name__)

SETUP_IMAGE = "sequenceiq/alpine-curl"  # and image which has curl and sh installed, used to prep the worker node


@attr.s
class MachineSpec(object):
    boot_volume_in_gb = attr.ib()
    mount_point = attr.ib()
    machine_type = attr.ib()


def get_random_string(length):
    return "".join([random.choice(string.ascii_lowercase) for x in range(length)])


# def safe_job_name(job_id):
#    return "job-"+job_id
# def delete_job(jobid):
#    raise Exception("unimp")
# def stop_job(jobid):
#    raise Exception("unimp")

def _normalize_label(label):
    label = label.lower()
    label = re.sub("[^a-z0-9]+", "-", label)
    if re.match("[^a-z].*", label) is not None:
        label = "x-" + label
    return label


def format_table(header, rows):
    with_header = [header]
    for row in rows:
        with_header.append([str(x) for x in row])

    def extract_column(i):
        return [row[i] for row in with_header]

    def max_col_len(i):
        column = extract_column(i)
        return max([len(x) for x in column])

    col_widths = [max_col_len(i) + 2 for i in range(len(header))]

    format_str = "".join(["{{: >{}}}".format(w) for w in col_widths])
    lines = [format_str.format(*header)]
    lines.append("-" * sum(col_widths))
    for row in rows:
        lines.append(format_str.format(*row))

    return "".join([x + "\n" for x in lines])


class ClusterStatus:
    def __init__(self, instances):
        instances_per_key = defaultdict(lambda: 0)
        running_count = 0
        for instance in instances:
            status = instance['status']
            key = (status,)
            instances_per_key[key] += 1
            if status != "STOPPING":
                running_count += 1

        table = []
        for key, count in instances_per_key.items():
            table.append(list(key) + [count])
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


class AddNodeStatus:
    def __init__(self, status):
        self.status = status

    def get_event_summary(self, since=None):
        log = []
        events = self.status['metadata']['events']
        # TODO: Better yet, sort by timestamp
        events = list(reversed(events))
        if since is not None:
            assert isinstance(since, AddNodeStatus)
            events = events[len(since.status['metadata']['events']):]

        for event in events:
            if event['details']['@type'] == "type.googleapis.com/google.genomics.v2alpha1.ContainerStoppedEvent":
                actionId = event['details']['actionId']
                action = self.status['metadata']['pipeline']['actions'][actionId - 1]
                log.append("Completed ({}): {}".format(action['imageUri'], repr(action['commands'])))
                log.append(event['description'])
                log.append("exitStatus: {}, stderr:".format(event['details']['exitStatus']))
                log.append(event['details']['stderr'])
            else:
                # if event['details']['@type'] != 'type.googleapis.com/google.genomics.v2alpha1.ContainerStartedEvent':
                log.append(event['description'])
        return "\n".join(log)

    def is_done(self):
        return self.status['done']


class Cluster:
    def __init__(self, project, zones, credentials=None):
        self.compute = build('compute', 'v1', credentials=credentials)
        self.service = build('genomics', 'v2alpha1', credentials=credentials)
        self.project = project
        self.zones = zones

    def _get_cluster_instances(self, cluster_name):
        instances = []
        for zone in self.zones:
            # print(dict(project=self.project, zone=zone, filter="labels.kubeque-cluster=" + cluster_name))
            i = self.compute.instances().list(project=self.project, zone=zone,
                                              filter="labels.kubeque-cluster=" + cluster_name).execute().get('items',
                                                                                                             [])
            instances.extend(i)
        return instances

    def get_cluster_status(self, cluster_name):
        instances = self._get_cluster_instances(cluster_name)
        return ClusterStatus(instances)

    def stop_cluster(self, cluster_name):
        instances = self._get_cluster_instances(cluster_name)
        if len(instances) == 0:
            log.warning("Attempted to delete instances in cluster %s but no instances found!", cluster_name)
        else:
            for instance in instances:
                zone = instance['zone'].split('/')[-1]
                log.info("deleting instance %s associated with cluster %s", instance['name'], cluster_name)
                self.compute.instances().delete(project=self.project, zone=zone, instance=instance['name']).execute()

            for instance in instances:
                zone = instance['zone'].split('/')[-1]
                self._wait_for_instance_status(self.project, zone, instance['name'], "TERMINATED")

    def _get_instance_status(self, project, zone, instance_name):
        try:
            instance = self.compute.instances().get(project=project, zone=zone, instance=instance_name).execute()
            return instance['status']
        except HttpError as error:
            if error.resp.status == 404:
                return "TERMINATED"
            else:
                raise Exception("Got HttpError but status was: {}".format(error.resp.status))

    def _wait_for_instance_status(self, project, zone, instance_name, desired_status):
        def p(msg):
            sys.stdout.write(msg)
            sys.stdout.flush()

        p("Waiting for {} to become {}...".format(instance_name, desired_status))
        prev_status = None
        while True:
            instance_status = self._get_instance_status(project, zone, instance_name)
            if instance_status != prev_status:
                prev_status = instance_status
                p("(now {})".format(instance_status))
            if instance_status == desired_status:
                break
            time.sleep(5)
            p(".")
        p("\n")

    def get_node_req_status(self, operation_id):
        raise Exception("unimp")
        request = self.service.projects.operations().get(name=operation_id)
        response = request.execute()

        runtimeMetadata = response.get("metadata", {}).get("runtimeMetadata", {})
        if "computeEngine" in runtimeMetadata:
            if response['done']:
                return NODE_REQ_COMPLETE
            else:
                return NODE_REQ_RUNNING
        else:
            log.info("Operation %s is not yet started. Events: %s", operation_id, response["metadata"]["events"])
            return NODE_REQ_SUBMITTED

    def add_node(self, pipeline_def, preemptible, debug_log_url):
        "Returns operation name"
        # make a deep copy
        import json
        pipeline_def = json.loads(json.dumps(pipeline_def))

        # mutate the pipeline as needed
        if preemptible is not None:
            pipeline_def['pipeline']['resources']['virtualMachine']['preemptible'] = preemptible

        cp_action = {'imageUri': 'google/cloud-sdk:alpine',
                     # 'commands': ["gsutil", "rsync", "-r", "/google/logs",
                     #              "gs://broad-achilles-kubeque/test-kube/sleeptest/1/pipeline.log"],
                     'commands': ["gsutil", "cp", "/google/logs/output", debug_log_url],
                     'flags': ["ALWAYS_RUN"]
                     }
        pipeline_def['pipeline']['actions'].append(cp_action)
        # print(json.dumps(pipeline_def, indent=2))

        # Run the pipeline
        operation = self.service.pipelines().run(body=pipeline_def).execute()

        return operation['name']

    def get_add_node_status(self, operation_name):
        request = self.service.projects().operations().get(name=operation_name)
        response = request.execute()
        return AddNodeStatus(response)

    def test_api(self):
        """Simple api call used to verify the service is enabled"""
        result = self.service.projects().operations().list(name="projects/{}/operations".format(self.project)).execute()
        assert "operations" in result

    def test_image(self, docker_image, sample_url, logging_url):
        pipeline_def = self.create_pipeline_spec("test-image", docker_image, "bash -c 'echo hello'",
                                                 "/mnt/kubequeconsume", sample_url, 1, 1, get_random_string(20), 10,
                                                 False)
        operation = self.add_node(pipeline_def, False)
        operation_name = operation['name']
        while not operation['done']:
            operation = self.service.projects().operations().get(name=operation_name).execute()
            time.sleep(5)
        if "error" in operation:
            raise Exception("Got error: {}".format(operation['error']))
        log.info("execution completed successfully")

    def is_owner_running(self, owner):
        if owner == "localhost":
            return False

        m = re.match("projects/([^/]+)/zones/([^/]+)/([^/]+)", owner)
        assert m is not None, "Expected a instance name with zone but got owner={}".format(owner)
        project_id, zone, instance_name = m.groups()
        return self._get_instance_status(project_id, zone, instance_name) == 'RUNNING'

    def determine_machine_type(self, cpu_request, mem_limit):
        raise Exception("unimp")
        log.warning("determine_machine_type is a stub, always returns n1-standard-1")
        return "n1-standard-1"

    def create_pipeline_spec(self, jobid, cluster_name, consume_exe_url, docker_image, consume_exe_args, machine_specs):
        mount_point = machine_specs.mount_point

        consume_exe_path = os.path.join(mount_point, "consume")
        consume_data = os.path.join(mount_point, "data")

        return self._create_pipeline_json(jobid=jobid,
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
                                          machine_specs=machine_specs)

    def _create_pipeline_json(self,
                              jobid,
                              cluster_name,
                              setup_image,
                              setup_parameters,
                              docker_image,
                              docker_command,
                              machine_specs):
        # labels have a few restrictions
        normalized_jobid = _normalize_label(jobid)

        mounts = [
            {
                'disk': 'ephemeralssd',
                'path': machine_specs.mount_point,
                'readOnly': False
            }
        ]

        log.warning("Using pd-standard for local storage instead of local ssd")
        pipeline_def = {
            'pipeline': {
                'actions': [
                    {'imageUri': setup_image,
                     'commands': setup_parameters,
                     'mounts': mounts
                     },
                    {'imageUri': docker_image,
                     'commands': docker_command,
                     'mounts': mounts
                     }
                ],
                'resources': {
                    'projectId': self.project,
                    'zones': self.zones,
                    'virtualMachine': {
                        'machineType': machine_specs.machine_type,
                        'preemptible': False,
                        'disks': [
                            {'name': 'ephemeralssd'}  # TODO: figure out type to specify for local_ssd
                        ],
                        'serviceAccount': {
                            'email': 'default',
                            'scopes': [
                                'https://www.googleapis.com/auth/cloud-platform',
                            ]
                        },
                        'bootDiskSizeGb': machine_specs.boot_volume_in_gb,
                        'labels': {
                            'kubeque-cluster': cluster_name,
                            'sparkles-job': normalized_jobid
                        }
                    }
                }
            },
            'labels': {
                'kubeque-cluster': cluster_name,
                'sparkles-job': normalized_jobid
            }
        }

        return pipeline_def

# next task: support consume rpc calls (use gprc?)
# Write seperate test case for that. (launch consume locally, and try to communicate with it. Maybe make a one-shot mode where job can be read from file instead of pulled from queue?)
