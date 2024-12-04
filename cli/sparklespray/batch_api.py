import google.cloud.batch_v1alpha.types as batch
from pydantic import BaseModel
from typing import List, Dict
import time

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
    runnables: List[Runnable]
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
                options=f"-p {monitor_port}:{monitor_port}"
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


def to_request(project: str, location: str, self: JobSpec):
    job = batch.Job(
        task_groups=[
            batch.TaskGroup(
                task_count=self.task_count,
                task_count_per_node="1",
                task_spec=batch.TaskSpec(
                    runnables=_create_runnables(
                        self.runnables, self.disks, self.monitor_port
                    ),
                    volumes=_create_volumes(self.disks),
                ),
            )
        ],
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


# import enum

# class State(enum.Enum):
#     failed = "failed"
#     pending = "pending"
#     running = "running"
#     succeeded = "succeeded"
#     canceled = "canceled"
#     cancel_pending = "cancel_pending"

# TERMINAL_STATES = [State.failed, State.succeeded, State.canceled]


class Event(BaseModel):
    description: str
    timestamp: float


# @dataclass
# class JobStatus:
#     name: str
#     state: State
#     events: List[Event]
#     original_response: batch.Job

#     def is_done(self):
#         return self.state in TERMINAL_STATES


def to_node_reqs(job: batch.Job):
    # make sure that this job only describes a single task
    # breakpoint()
    assert len(job.task_groups) == 1
    assert job.task_groups[0].task_count == 1

    # convert batch API states to the slightly simpler set that sparkles is using

    if job.status.state in [batch.JobStatus.State.QUEUED]:
        state = NODE_REQ_SUBMITTED
    elif job.status.state == batch.JobStatus.State.SCHEDULED:
        state = NODE_REQ_STAGING
    elif job.status.state in [batch.JobStatus.State.RUNNING]:
        state = NODE_REQ_RUNNING
    elif job.status.state in [batch.JobStatus.State.FAILED]:
        state = NODE_REQ_FAILED
    elif job.status.state in [batch.JobStatus.State.SUCCEEDED]:
        state = NODE_REQ_COMPLETE
    elif job.status.state in [batch.JobStatus.State.CANCELLED]:
        state = NODE_REQ_FAILED
    else:
        assert job.status.state in [
            batch.JobStatus.State.DELETION_IN_PROGRESS,
            batch.JobStatus.State.CANCELLATION_IN_PROGRESS,
        ], f"state was {job.status.state}"
        state = NODE_REQ_FAILED

    job_id = job.labels["sparkles-job"]
    cluster_id = job.labels["sparkles-cluster"]

    assert len(job.allocation_policy.instances) == 1
    pm = job.allocation_policy.instances[0].policy.provisioning_model
    if pm == batch.AllocationPolicy.ProvisioningModel.SPOT:
        node_class = NODE_REQ_CLASS_PREEMPTIVE
    else:
        assert pm == batch.AllocationPolicy.ProvisioningModel.STANDARD
        node_class = NODE_REQ_CLASS_NORMAL

    return [
        NodeReq(
            operation_id=job.name,
            cluster_id=cluster_id,
            status=state,
            node_class=node_class,
            sequence="0",
            job_id=job_id,
            instance_name=None,
        )
    ]


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


import json


class ClusterAPI:
    def __init__(self, batch_service_client):
        self.batch_service = batch_service_client

    def create_job(self, project: str, location: str, job: JobSpec):
        request = to_request(project, location, job)
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

    def stop_cluster(self, project: str, location: str, cluster_id: str):
        jobs = self._get_jobs_with_label(
            project, location, "sparkles-cluster", cluster_id
        )
        for job in jobs:
            self.delete(job.name)

    def get_node_reqs(self, project: str, location: str, cluster_id: str):
        jobs = self._get_jobs_with_label(
            project, location, "sparkles-cluster", cluster_id
        )
        node_reqs = []
        for job in jobs:
            node_reqs.extend(to_node_reqs(job))
        return node_reqs

    def _get_jobs_with_label(self, project: str, location: str, key: str, value: str):
        return self.batch_service.list_jobs(
            batch.ListJobsRequest(
                parent=_create_parent_id(project, location),
                filter=f"labels.{key}={json.dumps(value)}",
            )
        )
