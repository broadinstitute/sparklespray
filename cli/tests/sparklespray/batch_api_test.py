import google.cloud.batch_v1alpha.types as batch

from sparklespray.batch_api import (
    JobSpec,
    Disk,
    Runnable,
    create_batch_job_from_job_spec,
)


def job_spec_factory(preemptible=True, provision_mode="preemptible"):
    return JobSpec(
        task_count="1",
        runnables=[Runnable(image="mock-image", command=["consume"])],
        machine_type="n1-standard-1",
        preemptible=preemptible,
        locations=["regions/us-central1"],
        monitor_port=1203,
        network_tags=[],
        sparkles_job="x",
        sparkles_cluster="x",
        sparkles_timestamp="",
        boot_disk=Disk(
            name="disk1",
            size_gb=50,
            type="pd-standard",
            mount_path="/mnt/disk1",
        ),
        disks=[],
        service_account_email="invalid@sample.com",
        scopes=[],
        provision_mode=provision_mode,
    )


def _instance_policy(job_spec):
    request = create_batch_job_from_job_spec("proj", "us-central1", job_spec, 1, 1)
    return request.job.allocation_policy.instances[0].policy


def test_flex_provision_mode_sets_flex_start_and_no_reservation():
    policy = _instance_policy(job_spec_factory(provision_mode="flex"))

    assert policy.provisioning_model == batch.AllocationPolicy.ProvisioningModel.FLEX_START
    assert policy.reservation == "NO_RESERVATION"


def test_normal_provision_mode_forces_standard_regardless_of_preemptible():
    policy = _instance_policy(job_spec_factory(preemptible=True, provision_mode="normal"))

    assert policy.provisioning_model == batch.AllocationPolicy.ProvisioningModel.STANDARD
    assert policy.reservation == ""


def test_preemptible_provision_mode_uses_dynamic_preemptible_flag():
    spot_policy = _instance_policy(
        job_spec_factory(preemptible=True, provision_mode="preemptible")
    )
    assert spot_policy.provisioning_model == batch.AllocationPolicy.ProvisioningModel.SPOT

    standard_policy = _instance_policy(
        job_spec_factory(preemptible=False, provision_mode="preemptible")
    )
    assert (
        standard_policy.provisioning_model
        == batch.AllocationPolicy.ProvisioningModel.STANDARD
    )
