import time
from oauth2client.client import GoogleCredentials
from apiclient.discovery import build
import logging

log = logging.getLogger(__name__)

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

class Cluster:
    def __init__(self, project, zones, credentials=None):
        self.compute = build('compute', 'v1', credentials=credentials)
        self.service = build('genomics', 'v1alpha2', credentials=credentials)
        self.project = project
        self.zones = zones

    def _get_cluster_instances(self, cluster_name):
        instances = []
        for zone in self.zones:
            i = self.compute.instances().list(project=self.project, zone=zone, filter="labels.kubeque-cluster="+cluster_name).execute().get('items', [])
            instances.extend(i)
        return instances

    def get_cluster_status(self, cluster_name):
        instances = self._get_cluster_instances(cluster_name)
        return ClusterStatus(instances)

    def stop_cluster(self, cluster_name):
        log.info("Deleting instances in cluster %s", cluster_name)
        instances = self._get_cluster_instances(cluster_name)
        for instance in instances:
            log.info("deleting instance %s", instance['id'])
            self.compute.instances().delete(project=self.project, zone=instance['zone'], instance=instance['id'])

    def add_node(self, pipeline_def):
        # Run the pipeline
        logging_url = pipeline_def['pipelineArgs']['logging']['gcsPath']
        operation = self.service.pipelines().run(body=pipeline_def).execute()
        log_prefix = operation['name'].replace("operations/", "")
        log.info("Node's log will be written to: %s/%s", logging_url, log_prefix)

        # Emit the result of the pipeline run submission
        #pp = pprint.PrettyPrinter(indent=2)
        #pp.pprint(operation)

    def create_pipeline_spec(self,
                             docker_image,
                             docker_command,
                             data_mount_point,
                             logging_url,
                             kubequeconsume_url,
                             cpu_request,
                             mem_limit,
                             cluster_name):

        pipeline_def = {

            'ephemeralPipeline': {
                'projectId': self.project,
                'name': "kubeque-worker",

                # Define the resources needed for this pipeline.
                'resources': {
                    'minimum_cpu_cores': cpu_request,
                    'preemptible': False,
                    'minimum_ram_gb': mem_limit,
                    'zones': self.zones,
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
                    'kubeque-cluster': cluster_name
                },
                'inputs' : {
                'kubequeconsume': kubequeconsume_url
            }, 'serviceAccount': {
                    'email' : 'default',
                    'scopes': [
                        'https://www.googleapis.com/auth/cloud-platform',
#                        'https://www.googleapis.com/auth/userinfo.email',
#                        'https://www.googleapis.com/auth/compute',
#                        'https://www.googleapis.com/auth/devstorage.full_control',
#                        'https://www.googleapis.com/auth/datastore',
#                        'https://www.googleapis.com/auth/pubsub',
#                        'https://www.googleapis.com/auth/bigquery'
                    ]
                }
            }
        }

        assert kubequeconsume_url

        return pipeline_def
