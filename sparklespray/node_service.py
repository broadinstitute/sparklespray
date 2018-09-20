import time
from apiclient.discovery import build
import logging
import random
import string
import sys
import re
import attr
import os
from collections import defaultdict
import json
from .compute_service import ComputeService
from .node_req_store import NODE_REQ_COMPLETE, NODE_REQ_RUNNING, NODE_REQ_SUBMITTED, NODE_REQ_STAGING, NODE_REQ_FAILED
from typing import List, DefaultDict, Tuple

# from oauth2client.client import GoogleCredentials
# from google.cloud import datastore
# from kubeque.gcp import NODE_REQ_COMPLETE, NODE_REQ_RUNNING, NODE_REQ_SUBMITTED

from .log import log


@attr.s
class MachineSpec(object):
    boot_volume_in_gb = attr.ib()
    mount_point = attr.ib()
    machine_type = attr.ib()


def get_random_string(length):
    return "".join([random.choice(string.ascii_lowercase) for x in range(length)])


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


class AddNodeStatus:
    def __init__(self, response: dict) -> None:
        self.response = response

    @property
    def instance_name(self):
        events = self.response.get('metadata', {}).get('events')
        for event in events:
            instance = event.get('details', {}).get('instance')
            if instance is not None:
                return instance
        return None

        # print(self.response)
        # return self.response['metadata']['runtimeMetadata']['computeEngine']['instanceName']

    @property
    def status(self):
        from .cluster_service import CONSUMER_ACTION_ID

        if self.response['done']:
            if 'error' in self.response:
                return NODE_REQ_FAILED
            else:
                return NODE_REQ_COMPLETE
        else:
            instance_name = self.instance_name
            if instance_name is None:
                return NODE_REQ_SUBMITTED
            else:
                events = self.response.get('metadata', {}).get('events')
                for event in events:
                    if event['details'].get("actionId") == CONSUMER_ACTION_ID:
                        return NODE_REQ_RUNNING
                return NODE_REQ_STAGING
            #
            # start_events = [x for x in events if x['description'] == 'pulling-image']
            # if len(start_events) > 0:
            #     return NODE_REQ_RUNNING
            # else:
            #     return NODE_REQ_SUBMITTED

    @property
    def error_message(self):
        if "error" not in self.response:
            return None

        return self.response['error']['message']

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
                log.append("Completed ({}): {}".format(
                    action['imageUri'], repr(action['commands'])))
                log.append(event['description'])
                log.append("exitStatus: {}, stderr:".format(
                    event['details']['exitStatus']))
                log.append(event['details']['stderr'])
            else:
                # if event['details']['@type'] != 'type.googleapis.com/google.genomics.v2alpha1.ContainerStartedEvent':
                log.append(event['description'])
        return "\n".join(log)

    def is_done(self) -> bool:
        return self.status['done']


from .compute_service import DirCache


class NodeService:
    def __init__(self, project: str, zones: List[str], credentials=None) -> None:
        self.service = build('genomics', 'v2alpha1',
                             credentials=credentials, cache_discovery=True, cache=DirCache(".sparkles-cache/services"))
        self.zones = zones
        self.project = project

    def test_pipeline_api(self, project_id):
        request = self.service.projects().operations().list(
            name=f"projects/{project_id}/operations", filter="labels.invalid= \"invalid\"")
        response = request.execute()
        assert "operations" in response
        # print(response)

    def get_operation_details(self, operation_name : str) -> dict:
        request = self.service.projects().operations().get(name=operation_name)
        response = request.execute()
        return response

    def get_add_node_status(self, operation_name: str) -> AddNodeStatus:
        response = self.get_operation_details(operation_name)
        with open("response.log", "a") as fd:
            fd.write(json.dumps(response, indent=2)+"\n")
        return AddNodeStatus(response)

    def cancel_add_node(self, operation_name: str):
        request = self.service.projects().operations().cancel(name=operation_name)
        request.execute()

    def add_node(self, pipeline_def: dict, preemptible: bool, debug_log_url: str):
        "Returns operation name"
        # make a deep copy
        pipeline_def = json.loads(json.dumps(pipeline_def))

        # mutate the pipeline as needed
        if preemptible is not None:
            pipeline_def['pipeline']['resources']['virtualMachine']['preemptible'] = preemptible

        if debug_log_url:
            cp_action = {'imageUri': 'google/cloud-sdk:alpine',
                         'commands': ["gsutil", "cp", "/google/logs/output", debug_log_url],
                         'flags': ["ALWAYS_RUN"]
                         }
            pipeline_def['pipeline']['actions'].append(cp_action)
        # print(json.dumps(pipeline_def, indent=2))

        # Run the pipeline
        operation = self.service.pipelines().run(body=pipeline_def).execute()

        return operation['name']

    def test_pipeline_submit_api(self, setup_image, job_image, command, machine_type, boot_volume_in_gb):
        normalized_jobid = "test-pipeline-submit-api"
        pipeline_def = {
            'pipeline': {
                'actions': [
                    {'imageUri': setup_image,
                     'commands': command
                     },
                    {'imageUri': job_image,
                     'commands': command
                     },
                ],
                'resources': {
                    'projectId': self.project,
                    'zones': self.zones,
                    'virtualMachine': {
                        'machineType': machine_type,
                        'preemptible': False,
                        'serviceAccount': {
                            'email': 'default',
                            'scopes': [
                                'https://www.googleapis.com/auth/cloud-platform',
                            ]
                        },
                        'bootDiskSizeGb': boot_volume_in_gb,
                        'labels': {
                            'kubeque-cluster': normalized_jobid,
                            'sparkles-job': normalized_jobid
                        }
                    }
                }
            },
            'labels': {
                'kubeque-cluster': normalized_jobid,
                'sparkles-job': normalized_jobid
            }
        }

        operation_name = self.add_node(pipeline_def, False, None)
        prev_status_text = None
        out = sys.stdout
        while True:
            status = self.get_add_node_status(operation_name)
            status_text = status.status
            if prev_status_text != status_text:
                out.write(f"({status_text})")
                prev_status_text = status_text
            else:
                out.write(".")
            out.flush()
            if status_text == NODE_REQ_COMPLETE:
                break
            time.sleep(2)
        out.write("\n")

    def create_pipeline_json(self,
                             jobid: str,
                             cluster_name: str,
                             setup_image: str,
                             setup_parameters: List[str],
                             docker_image: str,
                             docker_command: List[str],
                             machine_specs: MachineSpec,
                             monitor_port: int) -> dict:
        # labels have a few restrictions
        normalized_jobid = _normalize_label(jobid)

        mounts = [
            {
                'disk': 'ephemeralssd',
                'path': machine_specs.mount_point,
                'readOnly': False
            }
        ]

        pipeline_def = {
            'pipeline': {
                'actions': [
                    {'imageUri': setup_image,
                     'commands': setup_parameters,
                     'mounts': mounts
                     },
                    {'imageUri': docker_image,
                     'commands': docker_command,
                     'mounts': mounts,
                     'portMappings': {str(monitor_port): monitor_port}
                     }
                ],
                'resources': {
                    'projectId': self.project,
                    'zones': self.zones,
                    'virtualMachine': {
                        'machineType': machine_specs.machine_type,
                        'preemptible': False,
                        'disks': [
                            {
                                'name': 'ephemeralssd',
                                'type': 'local-ssd'
                            }
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
