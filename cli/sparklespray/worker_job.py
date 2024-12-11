import os
from .gcp_utils import validate_label
from datetime import datetime
from .batch_api import JobSpec, Runnable, Disk
from .gcp_utils import normalize_label, make_unique_label, validate_label

def get_consume_command(job_spec : JobSpec):
    consume_runnable = [x for x in job_spec.runnables if x.command[0].endswith("consume")]
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
):

    consume_exe_path = os.path.join(work_root_dir, "consume")
    consume_data = os.path.join(work_root_dir, "data")

    validate_label(job_id)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

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
                str(60*10) # keep worker around for 10 minutes?
            ]

    print(f"exec: {' '.join(consume_command)}")

    runnables = [
        Runnable(
            image=sparklesworker_image, command=["copyexe", "--dst", consume_exe_path]
        ),
        Runnable(
            image=docker_image,
            command=consume_command,
        )
    ]

    job = JobSpec(
        task_count="1",
        runnables=runnables,
        machine_type="n4-standard-2",
        preemptible=True,
        locations=["regions/us-central1"],
        network_tags=["sparklesworker"],
        monitor_port=monitor_port,
        boot_disk=Disk(
            name="bootdisk", size_gb=40, type="hyperdisk-balanced", mount_path="/"
        ),
        disks=[
            Disk(name="data", size_gb=50, type="hyperdisk-balanced", mount_path="/mnt")
        ],
        sparkles_job=job_id,
        sparkles_cluster=cluster_name,
        sparkles_timestamp=timestamp,
        service_account_email=service_account_email,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    return job
