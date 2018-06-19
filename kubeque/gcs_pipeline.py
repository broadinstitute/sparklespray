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

log = logging.getLogger(__name__)

def get_random_string(length):
    return "".join([random.choice(string.ascii_lowercase) for x in range(length)])

def safe_job_name(job_id):
    return "job-"+job_id

from collections  import defaultdict

def delete_job(jobid):
    raise Exception("unimp")

def stop_job(jobid):
    raise Exception("unimp")

def format_table(header, rows):
    with_header = [header]
    for row in rows:
        with_header.append([str(x) for x in row])

    def extract_column(i):
        return [row[i] for row in with_header]

    def max_col_len(i):
        column = extract_column(i)
        return max([len(x) for x in column])

    col_widths = [max_col_len(i)+2 for i in range(len(header))]

    format_str = "".join(["{{: >{}}}".format(w) for w in col_widths])
    lines = [format_str.format(*header)]
    lines.append("-"*sum(col_widths))
    for row in rows:
        lines.append(format_str.format(*row))

    return "".join([x+"\n" for x in lines])

class ClusterStatus:
    def __init__(self, instances):
        instances_per_key = defaultdict(lambda: 0)
        running_count = 0
        for instance in instances:
            status = instance['status']
            key = (status, )
            instances_per_key[key] += 1
            if status != "STOPPING":
                running_count += 1

        table = []
        for key, count in instances_per_key.items():
            table.append( list(key) + [count] )
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


def _normalize_label(label):
    label = label.lower()
    label = re.sub("[^a-z0-9]+", "-", label)
    if re.match("[^a-z].*", label) is not None:
        label = "x-"+label
    return label

class Cluster:
    def __init__(self, project, zones, credentials=None):
        self.compute = build('compute', 'v1', credentials=credentials)
        self.service = build('genomics', 'v2alpha1', credentials=credentials)
        self.project = project
        self.zones = zones

    def _get_cluster_instances(self, cluster_name):
        instances = []
        for zone in self.zones:
            #print(dict(project=self.project, zone=zone, filter="labels.kubeque-cluster=" + cluster_name))
            i = self.compute.instances().list(project=self.project, zone=zone, filter="labels.kubeque-cluster="+cluster_name).execute().get('items', [])
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
        request = self.service.projects.operations().get(name=operation_id)
        response = request.execute()
# sample response
# done: false
# metadata:
#   '@type': type.googleapis.com/google.genomics.v1.OperationMetadata
#   clientId: ''
#   createTime: '2018-03-05T04:07:14Z'
#   events:
#   - description: start
#     startTime: '2018-03-05T04:08:02.203108868Z'
#   - description: pulling-image
#     startTime: '2018-03-05T04:08:02.203198515Z'
#   labels:
#     kubeque-cluster: c-757f3dfbda551dae1580
#     sparkles-job: test-dir-sub
#   projectId: broad-achilles
#   request:
#     '@type': type.googleapis.com/google.genomics.v1alpha2.RunPipelineRequest
#     ...
#   runtimeMetadata:
#     '@type': type.googleapis.com/google.genomics.v1alpha2.RuntimeMetadata
#     computeEngine:
#       diskNames: []
#       instanceName: ggp-15892891052525656519
#       machineType: us-east1-b/n1-standard-2
#       zone: us-east1-b
#   startTime: '2018-03-05T04:07:17Z'
# name: operations/ENOc3qKfLBjHs5q-1Ia5x9wBILbm7f71GCoPcHJvZHVjdGlvblF1ZXVl
        #print("operation", operation_id)
        #print(response)

        runtimeMetadata = response.get("metadata", {}).get("runtimeMetadata", {})
        if "computeEngine" in runtimeMetadata:
            if response['done']:
                return NODE_REQ_COMPLETE
            else:
                return NODE_REQ_RUNNING
        else:
            log.info("Operation %s is not yet started. Events: %s", operation_id, response["metadata"]["events"])
            return NODE_REQ_SUBMITTED

    def add_node(self, pipeline_def, preemptible):
        "Returns operation name"
        # make a deep copy
        import json
        pipeline_def = json.loads(json.dumps(pipeline_def))

        # mutate the pipeline as needed
        if preemptible is not None:
            pipeline_def['pipeline']['resources']['virtualMachine']['preemptible'] = preemptible

        # Run the pipeline
        operation = self.service.pipelines().run(body=pipeline_def).execute()

        return operation['name']

    def get_add_node_status(self, operation_name):
        request = self.service.projects().operations().get(name=operation_name)
        response = request.execute()
        return response

    def test_api(self):
        """Simple api call used to verify the service is enabled"""
        result = self.service.projects().operations().list(name="projects/{}/operations".format(self.project)).execute()
        assert "operations" in result

    def test_image(self, docker_image, sample_url, logging_url):
        pipeline_def = self.create_pipeline_spec("test-image", docker_image, "bash -c 'echo hello'", "/mnt/kubequeconsume", sample_url, 1, 1, get_random_string(20), 10, False)
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

        import re
        m = re.match("projects/([^/]+)/zones/([^/]+)/([^/]+)", owner)
        assert m is not None, "Expected a instance name with zone but got owner={}".format(owner)
        project_id, zone, instance_name = m.groups()
        return self._get_instance_status(project_id, zone, instance_name) == 'RUNNING'

    def determine_machine_type(self, cpu_request, mem_limit):
        log.warning("determine_machine_type is a stub, always returns n1-standard-1")
        return "n1-standard-1"

    def create_pipeline_spec(self,
                             jobid,
                             docker_image,
                             docker_command,
                             data_mount_point,
                             kubequeconsume_url,
                             cpu_request,
                             mem_limit,
                             cluster_name,
                             bootDiskSizeGb):
        assert kubequeconsume_url

        # labels have a few restrictions
        normalized_jobid = _normalize_label(jobid)

        machine_type = self.determine_machine_type(cpu_request, mem_limit)

        log.warning("Using pd-standard for local storage instead of local ssd")
        pipeline_def = {
            'pipeline': {
                'actions' : [
                    {'imageUri': docker_image,
                     'commands': docker_command,
                     'mounts': [
                         {
                             'disk': 'ephemeralssd',
                             'path': data_mount_point,
                             'readOnly': False
                         }
                     ]
                     }
                ],
                'resources': {
                    'projectId': self.project,
                    'zones': self.zones,
                    'virtualMachine': {
                        'machineType': machine_type,
                        'preemptible': False,
                        'disks': [
                            {'name': 'ephemeralssd'} # TODO: figure out type to specify for local_ssd
                        ],
                        'serviceAccount': {
                            'email': 'default',
                            'scopes': [
                                'https://www.googleapis.com/auth/cloud-platform',
                            ]
                        },
                        'bootDiskSizeGb': bootDiskSizeGb,
                        'labels' : {
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


