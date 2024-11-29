from google.cloud.batch_v1alpha.services.batch_service import BatchServiceClient
import google.cloud.batch_v1alpha.types as batch
from dataclasses import dataclass
from typing import List, Dict


@dataclass
class Runnable:
    image: str
    command: List[str]


@dataclass
class Disk:
    # Device name that the guest operating system will see.
    name: str
    size_gb: int
    # Disk type as shown in gcloud compute disk-types list. For example, local SSD uses type "local-ssd". Persistent disks and boot disks use "pd-balanced", "pd-extreme", "pd-ssd" or "pd-standard".
    type: str
    # The mount path for the volume, e.g. /mnt/disks/share.
    mount_path: str


@dataclass
class JobSpec:
    task_count: str
    runnables: List[Runnable]
    machine_type: str
    preemptible: bool
    # Each location can be a region or a zone. Only one region or multiple zones in one region is supported now. For example, ["regions/us-central1"] allow VMs in any zones in region us-central1. ["zones/us-central1-a", "zones/us-central1-c"] only allow VMs in zones us-central1-a and us-central1-c.
    locations: List[str]
    # The tags identify valid sources or targets for network firewalls.
    network_tags: List[str]
    # Custom labels to apply to the job and all the Compute Engine resources that both are created by this allocation policy and support labels.
    labels: Dict[str, str]

    boot_disk: Disk
    disks: List[Disk]

    project: str
    location: str


def _create_volumes(disks: List[Disk]):
    return [
        batch.Volume(mount_path=disk.mount_path, device_name=disk.name)
        for disk in disks
    ]


def _create_runnables(runnables: List[Runnable], disks: List[Disk]):
    return [
        batch.Runnable(
            container=batch.Runnable.Container(
                image_uri=runnable.image,
                commands=runnable.command,
                volumes=_create_volumes(disks)
                # "enableImageStreaming": True
            )
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

import time
def to_request(self: JobSpec):
    return batch.CreateJobRequest(
        parent=f"projects/{self.project}/locations/{self.location}",
        job_id=f"sparkles-batch-test-{int(time.time())}",
        job=batch.Job(
            task_groups=[
                batch.TaskGroup(
                    task_count=self.task_count,
                    task_count_per_node="1",
                    task_spec=batch.TaskSpec(
                        runnables=_create_runnables(self.runnables, self.disks)
                    ),
                )
            ],
            allocation_policy=batch.AllocationPolicy(
                location=batch.AllocationPolicy.LocationPolicy(
                    allowed_locations=self.locations
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
                labels=self.labels,
                tags=self.network_tags,
            ),
            logs_policy=batch.LogsPolicy(destination="CLOUD_LOGGING"),
        ),
    )

import enum

class State(enum.Enum):
    failed = "failed" 
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    canceled = "canceled"
    cancel_pending = "cancel_pending"

TERMINAL_STATES = [State.failed, State.succeeded, State.canceled]

@dataclass
class Event:
    description: str
    timestamp : float

@dataclass
class JobStatus:
    name: str
    state: State
    events: List[Event]
    original_response: batch.Job

    def is_done(self):
        return self.state in TERMINAL_STATES


def to_job_status(job : batch.Job):
    # convert batch API states to the slightly simpler set that sparkles is using
    if job.status.state in [ batch.JobStatus.State.QUEUED, 
                            batch.JobStatus.State.SCHEDULED ]:
        state = State.pending
    elif job.status.state in [batch.JobStatus.State.RUNNING]:
        state = State.running
    elif job.status.state in [batch.JobStatus.State.FAILED]:
        state = State.failed
    elif job.status.state in [batch.JobStatus.State.SUCCEEDED]:
        state = State.succeeded
    elif job.status.state in [batch.JobStatus.State.CANCELLED]:
        state = State.canceled
    else:
        assert job.status.state in [batch.JobStatus.State.DELETION_IN_PROGRESS,batch.JobStatus.State.CANCELLATION_IN_PROGRESS ], f"state was {job.status.state}"
        state = State.cancel_pending

    return JobStatus(name=job.name, state=state, events=[
        Event(description=e.description, timestamp=e.event_time.timestamp()) for e in job.status.status_events], original_response=job)

class EventPrinter:
    def __init__(self):
        self.last_event = None
    def print_new(self, events:List[Event]):
        # not sure the sort is needed....
        events = sorted(events, key=lambda x: x.timestamp)
        for event in events:
            if self.last_event is None or self.last_event < event.timestamp:
                self.last_event = event.timestamp
                print(event.description)

class Wrapper:
    def __init__(self):
        self.batch_service = BatchServiceClient()

    def create_job(self, job: JobSpec):
        request = to_request(job)
        job_result = self.batch_service.create_job(request)
        print("created", job_result.name)
        return to_job_status(job_result)

    def cancel(self, name):
        self.batch_service.cancel_job(batch.CancelJobRequest(name=name))

    def get_job_status(self, name):
        response = self.batch_service.get_job(batch.GetJobRequest(name=name))
        return to_job_status(response)


if __name__ == "__main__":
    w = Wrapper()
    job = JobSpec(
        task_count="1",
        project="broad-achilles",
        location="us-central1",
        runnables=[Runnable(image="alpine", command=["sleep", "60"])],
        machine_type="n4-standard-2",
        preemptible=True,
        locations=["regions/us-central1"],
        network_tags=[],
        labels={"sparkles-batch": "test"},
        boot_disk=Disk(name="bootdisk", size_gb=40, type="hyperdisk-balanced", mount_path="/"),
        disks=[Disk(name="data", size_gb=50, type="hyperdisk-balanced", mount_path="/data")],
    )

    print("creating job")
    status = w.create_job(job)
    name = status.name
    printer = EventPrinter()
    count = 0 
    while not status.is_done():
        count += 1
        printer.print_new(status.events)
        print("sleeping")
        time.sleep(2)
        # if count == 10:
        #     print("cancelling")
        #     w.cancel(name)
        status = w.get_job_status(name)
    print(status)    
