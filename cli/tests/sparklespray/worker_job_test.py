from sparklespray.worker_job import create_job_spec, get_consume_command
from sparklespray.model import PersistentDiskMount


def _job_spec(**kwargs):
    return create_job_spec(
        job_id="test-job",
        sparklesworker_image="sparklesworker:latest",
        work_root_dir="/mnt/disks/mount_1",
        docker_image="ubuntu:latest",
        cluster_name="test-cluster",
        project="test-project",
        monitor_port=6032,
        service_account_email="test@sample.com",
        machine_type="n1-standard-1",
        location="us-central1",
        boot_volume=PersistentDiskMount(
            path="/", size_in_gb=40, type="pd-balanced", mount_options=[]
        ),
        mounts=[],
        **kwargs,
    )


def test_worker_linger_defaults_to_600():
    job = _job_spec()
    command = get_consume_command(job)
    assert "--ftShutdownAfter" in command
    assert command[command.index("--ftShutdownAfter") + 1] == "600"


def test_worker_linger_is_passed_through():
    job = _job_spec(worker_linger=120)
    command = get_consume_command(job)
    assert command[command.index("--ftShutdownAfter") + 1] == "120"
