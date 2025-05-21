import os
from .gcp_utils import validate_label
from datetime import datetime
from .batch_api import JobSpec, Runnable, Disk
from .gcp_utils import normalize_label, make_unique_label, validate_label
from .config import Config
from .model import PersistentDiskMount, DiskMountT
from typing import List
from .log import log
from typing import cast


def get_consume_command(job_spec: JobSpec):
    consume_runnable = [
        x for x in job_spec.runnables if x.command[0].endswith("consume")
    ]
    assert len(consume_runnable) == 1
    return consume_runnable[0].command


def create_job_spec(
    job_id,
    sparklesworker_image,
    work_root_dir,
    docker_image,
    cluster_name,
    project,
    monitor_port,
    service_account_email,
    machine_type: str,
    location: str,
    boot_volume: PersistentDiskMount,
    mounts: List[DiskMountT],
):
    consume_exe_path = os.path.join(work_root_dir, "consume")
    consume_data = os.path.join(work_root_dir, "data")

    validate_label(job_id)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    def create_runnables(shutdown_after):
        consume_command = [
            consume_exe_path,
            "consume",
            "--cacheDir",
            os.path.join(consume_data, "cache"),
            "--tasksDir",
            os.path.join(consume_data, "tasks"),
            "--cluster",
            cluster_name,
            "--projectId",
            project,
            "--port",
            str(monitor_port),
            "--timeout",
            "10",
            "--shutdownAfter",
            "0",
            "--ftShutdownAfter",
            str(shutdown_after),
        ]

        log.debug(f"exec: {' '.join(consume_command)}")

        runnables = [
            Runnable(
                image=sparklesworker_image,
                command=["copyexe", "--dst", consume_exe_path],
            ),
            # Runnable(
            #     image=docker_image,
            #     command=["printenv"],
            # ),
            Runnable(
                image=docker_image,
                command=consume_command,
            ),
        ]
        return runnables

    assert len(mounts) <= 1, "Does not currently support more than one data mount"
    job = JobSpec(
        task_count="1",
        runnables=create_runnables(60 * 10),  # keep a worker around for 10 minutes
        machine_type=machine_type,
        preemptible=True,
        locations=[f"regions/{location}"],
        network_tags=["sparklesworker"],
        monitor_port=monitor_port,
        boot_disk=Disk(
            name="bootdisk",
            size_gb=boot_volume.size_in_gb,
            type=boot_volume.type,
            mount_path="/",
        ),
        disks=[
            Disk(name="data", size_gb=m.size_in_gb, type=m.type, mount_path=m.path)
            for m in cast(List[PersistentDiskMount], mounts)
        ],
        sparkles_job=job_id,
        sparkles_cluster=cluster_name,
        sparkles_timestamp=timestamp,
        service_account_email=service_account_email,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    return job


def create_test_job(
    job_id: str,
    cluster_name: str,
    docker_image: str,
    service_account_email: str,
    config: Config,
):
    validate_label(job_id)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    job = JobSpec(
        task_count="1",
        runnables=[
            Runnable(image=docker_image, command=["echo", "test"]),
        ],
        machine_type="n4-standard-2",
        preemptible=True,
        locations=[f"regions/{config.location}"],
        network_tags=["sparklesworker"],
        monitor_port=config.monitor_port,
        boot_disk=Disk(
            name="bootdisk", size_gb=40, type="hyperdisk-balanced", mount_path="/"
        ),
        disks=[],
        sparkles_job=job_id,
        sparkles_cluster=cluster_name,
        sparkles_timestamp=timestamp,
        service_account_email=service_account_email,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return job
