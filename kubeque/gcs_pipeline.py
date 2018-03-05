import time
from oauth2client.client import GoogleCredentials
from apiclient.discovery import build
import logging
import random
import string
import sys
from googleapiclient.errors import HttpError
from kubeque.gcp import NODE_REQ_COMPLETE, NODE_REQ_RUNNING, NODE_REQ_SUBMITTED

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

import re

def _normalize_label(label):
    label = label.lower()
    label = re.sub("[^a-z0-9]+", "-", label)
    if re.match("[^a-z].*", label) is not None:
        label = "x-"+label
    return label

class Cluster:
    def __init__(self, project, zones, credentials=None):
        self.compute = build('compute', 'v1', credentials=credentials)
        self.service = build('genomics', 'v1alpha2', credentials=credentials)
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
        request = self.service.operations().get(name=operation_id)
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
        if "computeEngine" in response["metadata"]["runtimeMetadata"]:
            if response['done']:
                return NODE_REQ_COMPLETE
            else:
                return NODE_REQ_RUNNING
        else:
            log.info("Operation %s is not yet started. Events: %s", operation_id, response["metadata"]["events"])
            return NODE_REQ_SUBMITTED

    def add_node(self, pipeline_def, preemptible):
        # make a deep copy
        import json
        pipeline_def = json.loads(json.dumps(pipeline_def))

        # mutate the pipeline as needed
        if preemptible is not None:
            pipeline_def['ephemeralPipeline']['resources']['preemptible'] = preemptible
        logging_url = pipeline_def['pipelineArgs']['logging']['gcsPath']

        # Run the pipeline
        operation = self.service.pipelines().run(body=pipeline_def).execute()
        log_prefix = operation['name'].replace("operations/", "")
        log.info("Node's log will be written to: %s/%s", logging_url, log_prefix)

        return operation

    def test_api(self):
        """Simple api call used to verify the service is enabled"""
        result = self.service.pipelines().list(projectId=self.project).execute()
        assert "pipelines" in result

    def test_image(self, docker_image, sample_url, logging_url):
        pipeline_def = self.create_pipeline_spec("test-image", docker_image, "bash -c 'echo hello'", "/mnt/kubequeconsume", logging_url, sample_url, 1, 1, get_random_string(20), 10, False)
        operation = self.add_node(pipeline_def, False)
        operation_name = operation['name']
        while not operation['done']:
            operation = self.service.operations().get(name=operation_name).execute()
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

    def create_pipeline_spec(self,
                             jobid,
                             docker_image,
                             docker_command,
                             data_mount_point,
                             logging_url,
                             kubequeconsume_url,
                             cpu_request,
                             mem_limit,
                             cluster_name,
                             bootDiskSizeGb,
                             preemptible
                             ):
        # labels have a few restrictions
        normalized_jobid = _normalize_label(jobid)

        pipeline_def = {

            'ephemeralPipeline': {
                'projectId': self.project,
                'name': "kubeque-worker",

                # Define the resources needed for this pipeline.
                'resources': {
                    'minimum_cpu_cores': cpu_request,
                    'preemptible': preemptible,
                    'minimum_ram_gb': mem_limit,
                    'zones': self.zones,
                    'bootDiskSizeGb': bootDiskSizeGb,
                    # Create a data disk that is attached to the VM and destroyed when the
                    # pipeline terminates.
                    'disks': [{
                        'name': 'ephemeralssd',
                        'type': "LOCAL_SSD",  # PERSISTENT_SSD, PERSISTENT_HDD
                        # size_gb: if PERSISTENT_* is selected
                        'autoDelete': True,

                        # Within the Docker container, specify a mount point for the disk.
                        # The pipeline input argument below will specify that inputs should be
                        # written to this disk.
                        'mountPoint': data_mount_point,
                    }],
                },

                # Specify the Docker image to use along with the command
                'docker': {
                    'imageName': docker_image,
                    'cmd': docker_command
                },

                # The Pipelines API currently supports full GCS paths, along with patterns (globs),
                # but it doesn't directly support a list of files being passed as a single input
                # parameter ("gs://bucket/foo.bam gs://bucket/bar.bam").
                #
                # We can simply generate a series of inputs (input0, input1, etc.) to support this here.
                #
                # 'inputParameters': [ {
                #   'name': 'inputFile0',
                #   'description': 'Cloud Storage path to an input file',
                #   'localCopy': {
                #     'path': 'workspace/',
                #     'disk': 'datadisk'
                #   }
                # }, {
                #   'name': 'inputFile1',
                #   'description': 'Cloud Storage path to an input file',
                #   'localCopy': {
                #     'path': 'workspace/',
                #     'disk': 'datadisk'
                #   }
                # <etc>
                # } ],

                # The inputFile<n> specified in the pipelineArgs (see below) will specify the
                # Cloud Storage path to copy to /mnt/data/workspace/.

                'inputParameters': [{
                    'name': 'kubequeconsume',
                    'description': 'Executable for localization',
                    'localCopy': {
                        'path': 'kubequeconsume',
                        'disk': 'ephemeralssd'
                    }
                }],

                # By specifying an outputParameter, we instruct the pipelines API to
                # copy /mnt/data/workspace/* to the Cloud Storage location specified in
                # the pipelineArgs (see below).
                #    'outputParameters': [ {
                #      'name': 'outputPath',
                #      'description': 'Cloud Storage path for where to FastQC output',
                #      'localCopy': {
                #        'path': 'workspace/*',
                #        'disk': 'datadisk'
                #      }
                #    } ]
            },

            'pipelineArgs': {
                'projectId': self.project,

                # Pass the user-specified Cloud Storage paths as a map of input files
                # 'inputs': {
                #   'inputFile0': 'gs://bucket/foo.bam',
                #   'inputFile1': 'gs://bucket/bar.bam',
                #   <etc>
                # }

                # Pass the user-specified Cloud Storage destination for pipeline logging
                'logging': {
                    'gcsPath': logging_url
                },
                'labels': {
                    'kubeque-cluster': cluster_name,
                    'sparkles-job': normalized_jobid
                },
                'inputs' : {
                'kubequeconsume': kubequeconsume_url
            }, 'serviceAccount': {
                    'email' : 'default',
                    'scopes': [
                        'https://www.googleapis.com/auth/cloud-platform',
                    ]
                }
            }
        }

        assert kubequeconsume_url

        return pipeline_def


