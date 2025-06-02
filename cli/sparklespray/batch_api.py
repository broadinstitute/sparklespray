import google.cloud.batch_v1alpha.types as batch
from pydantic import BaseModel
from typing import List, Dict
import time
from google.cloud.batch_v1alpha.services.batch_service import BatchServiceClient
from google.cloud.compute_v1.services.instances import InstancesClient
import google.cloud.compute_v1.types as compute
from google.api_core.exceptions import NotFound
from .node_req_store import (
    NodeReq,
    NODE_REQ_COMPLETE,
    NODE_REQ_FAILED,
    NODE_REQ_SUBMITTED,
    NODE_REQ_STAGING,
    NODE_REQ_RUNNING,
    NODE_REQ_CLASS_NORMAL,
    NODE_REQ_CLASS_PREEMPTIVE,
)

import json


class Runnable(BaseModel):
    image: str
    command: List[str]


class Disk(BaseModel):
    # Device name that the guest operating system will see.
    name: str
    size_gb: int
    # Disk type as shown in gcloud compute disk-types list. For example, local SSD uses type "local-ssd". Persistent disks and boot disks use "pd-balanced", "pd-extreme", "pd-ssd" or "pd-standard".
    type: str
    # The mount path for the volume, e.g. /mnt/disks/share.
    mount_path: str


class JobSpec(BaseModel):
    task_count: str

    runnables: List[Runnable]  # normal workers

    machine_type: str
    preemptible: bool
    # Each location can be a region or a zone. Only one region or multiple zones in one region is supported now. For example, ["regions/us-central1"] allow VMs in any zones in region us-central1. ["zones/us-central1-a", "zones/us-central1-c"] only allow VMs in zones us-central1-a and us-central1-c.
    locations: List[str]
    # The tags identify valid sources or targets for network firewalls.
    monitor_port: int
    network_tags: List[str]

    # Custom labels to apply to the job and all the Compute Engine resources that both are created by this allocation policy and support labels.
    # labels: Dict[str, str]
    sparkles_job: str
    sparkles_cluster: str
    sparkles_timestamp: str

    boot_disk: Disk
    disks: List[Disk]

    service_account_email: str
    scopes: List[str]
    # project: str
    # location: str


def _create_volumes(disks: List[Disk]):
    return [
        batch.Volume(mount_path=disk.mount_path, device_name=disk.name)
        for disk in disks
    ]


def _create_runnables(runnables: List[Runnable], disks: List[Disk], monitor_port: int):
    return [
        batch.Runnable(
            container=batch.Runnable.Container(
                image_uri=runnable.image,
                commands=runnable.command,
                volumes=[f"{disk.mount_path}:{disk.mount_path}" for disk in disks],
                options=f"-p {monitor_port}:{monitor_port}",
                # "enableImageStreaming": True
            ),
        )
        for runnable in runnables
    ]


def _create_disks(disks: List[Disk]):
    return [
        batch.AllocationPolicy.AttachedDisk(
            device_name=disk.name,
            new_disk=batch.AllocationPolicy.Disk(type=disk.type, size_gb=disk.size_gb),
        )
        for disk in disks
    ]


def _create_parent_id(project, location):
    return f"projects/{project}/locations/{location}"


def create_batch_job_from_job_spec(
    project: str, location: str, self: JobSpec, worker_count: int, max_retry_count: int
):
    # batch api only supports a single group at this time
    # BATCH_TASK_INDEX
    task_groups = [
        batch.TaskGroup(
            task_count=worker_count,
            task_count_per_node="1",
            task_spec=batch.TaskSpec(
                runnables=_create_runnables(
                    self.runnables, self.disks, self.monitor_port
                ),
                volumes=_create_volumes(self.disks),
            ),
            # max_retry_count=max_retry_count,
            # lifecycle_policies=batch.LifecyclePolicy(action=batch.LifecyclePolicy.Action.RETRY_TASK,
            #                                          action_condition=batch.LifecyclePolicy.ActionCondition(exit_codes=[50001]))
        )
    ]

    job = batch.Job(
        task_groups=task_groups,
        allocation_policy=batch.AllocationPolicy(
            location=batch.AllocationPolicy.LocationPolicy(
                allowed_locations=self.locations
            ),
            service_account=batch.ServiceAccount(
                email=self.service_account_email, scopes=self.scopes
            ),
            instances=[
                batch.AllocationPolicy.InstancePolicyOrTemplate(
                    policy=batch.AllocationPolicy.InstancePolicy(
                        machine_type=self.machine_type,
                        provisioning_model=(
                            batch.AllocationPolicy.ProvisioningModel.SPOT
                            if self.preemptible
                            else batch.AllocationPolicy.ProvisioningModel.STANDARD
                        ),
                        boot_disk=batch.AllocationPolicy.Disk(
                            type=self.boot_disk.type,
                            size_gb=self.boot_disk.size_gb,
                            image="batch-cos",
                        ),
                        disks=_create_disks(self.disks),
                    )
                )
            ],
            tags=self.network_tags,
        ),
        logs_policy=batch.LogsPolicy(destination="CLOUD_LOGGING"),
        labels={
            "sparkles-job": self.sparkles_job,
            "sparkles-cluster": self.sparkles_cluster,
            # "sparkles-timestamp": self.sparkles_timestamp
        },
    )

    from .gcp_utils import normalize_label, make_unique_label, validate_label

    return batch.CreateJobRequest(
        parent=f"projects/{project}/locations/{location}",
        job_id=make_unique_label(f"sparkles-{self.sparkles_job}"),
        job=job,
    )


class Event(BaseModel):
    description: str
    timestamp: float


def to_node_reqs(job: batch.Job):
    # convert batch API states to the slightly simpler set that sparkles is using

    # PENDING (1):
    #     The Task is created and waiting for
    #     resources.
    # ASSIGNED (2):
    #     The Task is assigned to at least one VM.
    # RUNNING (3):
    #     The Task is running.
    # FAILED (4):
    #     The Task has failed.
    # SUCCEEDED (5):
    #     The Task has succeeded.
    # UNEXECUTED (6):
    #     The Task has not been executed when the Job
    #     finishes.
    assert len(job.task_groups) == 1
    task_group = job.task_groups[0]
    tasks_accounted_for = 0
    # if task_group.task_count != len(tasks):
    #     breakpoint()

    job_id = job.labels["sparkles-job"]
    cluster_id = job.labels["sparkles-cluster"]

    assert len(job.allocation_policy.instances) == 1
    pm = job.allocation_policy.instances[0].policy.provisioning_model
    if pm == batch.AllocationPolicy.ProvisioningModel.SPOT:
        node_class = NODE_REQ_CLASS_PREEMPTIVE
    else:
        assert pm == batch.AllocationPolicy.ProvisioningModel.STANDARD
        node_class = NODE_REQ_CLASS_NORMAL

    node_reqs = []
    # breakpoint()
    for status_task_group in dict(job.status.task_groups).values():
        for task_state, task_state_count in status_task_group.counts.items():
            if task_state in [batch.TaskStatus.State.PENDING.name]:
                state = NODE_REQ_SUBMITTED
            elif task_state in [batch.TaskStatus.State.ASSIGNED.name]:
                state = NODE_REQ_STAGING
            elif task_state in [batch.TaskStatus.State.RUNNING.name]:
                state = NODE_REQ_RUNNING
            elif task_state in [batch.TaskStatus.State.FAILED.name]:
                state = NODE_REQ_FAILED
            elif task_state in [batch.TaskStatus.State.SUCCEEDED.name]:
                state = NODE_REQ_COMPLETE
            else:
                assert task_state in [
                    batch.TaskStatus.State.UNEXECUTED.name
                ], f"state was {task_state}"
                state = NODE_REQ_FAILED

            for _ in range(task_state_count):
                node_reqs.append(
                    NodeReq(
                        operation_id=job.name,
                        cluster_id=cluster_id,
                        status=state,
                        node_class=node_class,
                        sequence="0",
                        job_id=job_id,
                        instance_name=None,
                    )
                )
                tasks_accounted_for += 1

    while tasks_accounted_for < task_group.task_count:
        node_reqs.append(
            NodeReq(
                operation_id=job.name,
                cluster_id=cluster_id,
                status=NODE_REQ_SUBMITTED,
                node_class=node_class,
                sequence="0",
                job_id=job_id,
                instance_name=None,
            )
        )
        tasks_accounted_for += 1

    #    assert task_group.task_count == tasks_accounted_for, f"Job defined to have {task_group.task_count} workers, but only {tasks_accounted_for} workers reported status"
    return node_reqs


class EventPrinter:
    def __init__(self):
        self.last_event = None

    def print_new(self, events: List[Event]):
        # not sure the sort is needed....
        events = sorted(events, key=lambda x: x.timestamp)
        for event in events:
            if self.last_event is None or self.last_event < event.timestamp:
                self.last_event = event.timestamp
                print(event.description)


TERMINAL_STATES = set(
    [
        batch.JobStatus.State.CANCELLED,
        batch.JobStatus.State.FAILED,
        batch.JobStatus.State.SUCCEEDED,
    ]
)


def is_job_complete(job: batch.Job):
    return job.status.state in TERMINAL_STATES


def is_job_successful(job: batch.Job):
    return job.status.state == batch.JobStatus.State.SUCCEEDED


class ClusterAPI:
    def __init__(
        self,
        batch_service_client: BatchServiceClient,
        compute_engine_client: InstancesClient,
    ):
        self.batch_service = batch_service_client
        self.compute_engine_client = compute_engine_client

    def create_job(
        self,
        project: str,
        location: str,
        job: JobSpec,
        worker_count: int,
        max_retry_count: int,
    ):
        request = create_batch_job_from_job_spec(
            project, location, job, worker_count, max_retry_count
        )
        job_result = self.batch_service.create_job(request)
        print("created", job_result.name)
        return job_result.name

    def delete(self, name):
        self.batch_service.delete_job(batch.DeleteJobRequest(name=name))

    def cancel(self, name):
        self.batch_service.cancel_job(batch.CancelJobRequest(name=name))

    def get_job_status(self, name):
        response = self.batch_service.get_job(batch.GetJobRequest(name=name))
        return response

    def delete_node_reqs(
        self, project: str, location: str, cluster_id: str, only_terminal_reqs=False
    ):
        jobs = self._get_jobs_with_label(
            project, location, "sparkles-cluster", cluster_id
        )
        for job in jobs:
            if only_terminal_reqs and job.status.state not in TERMINAL_STATES:
                continue

            self.delete(job.name)

    def get_node_reqs(self, project: str, location: str, cluster_id: str):
        jobs = self._get_jobs_with_label(
            project, location, "sparkles-cluster", cluster_id
        )
        jobs = list(jobs)
        # if len(jobs) > 0:
        #     breakpoint()
        node_reqs = []
        for job in jobs:
            assert len(job.task_groups) == 1
            # tasks = self._get_tasks_for_job(job)
            node_reqs.extend(to_node_reqs(job))
        return node_reqs

    def _get_jobs_with_label(self, project: str, location: str, key: str, value: str):
        return self.batch_service.list_jobs(
            batch.ListJobsRequest(
                parent=_create_parent_id(project, location),
                filter=f"labels.{key}={json.dumps(value)}",
            )
        )

    def _get_tasks_for_job(self, job: batch.Job):
        tasks = []
        for task_group in job.task_groups:
            these = list(
                self.batch_service.list_tasks(
                    batch.ListTasksRequest(parent=task_group.name)
                )
            )
            breakpoint()
            tasks.extend(these)
        return tasks

    def is_instance_running(self, instance_uri):
        m = re.match("projects/([^/]+)/zones/([^/]+)/([^/]+)", instance_uri)
        assert m
        project, zone, instance = m.groups()

        try:
            self.compute_engine_client.get(
                compute.GetInstanceRequest(
                    instance=instance, project=project, zone=zone
                )
            )
        except NotFound:
            return False
        return True


import re
