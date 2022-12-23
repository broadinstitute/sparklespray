from sparklespray.model import MachineSpec, PersistentDiskMount, ExistingDiskMount
from sparklespray.gcp_utils import create_pipeline_spec
from sparklespray.model import DEFAULT_SSD_SIZE

def _create_pipeline_spec(machine_specs):
    return create_pipeline_spec(
        project="projectid",
        zones=["us-central"],
        jobid="jobid",
        cluster_name="clustername",
        consume_exe_url="https://consume.exe",
        consume_exe_md5="123456",
        docker_image="dockerimage",
        consume_exe_args=["exeargs"],
        machine_specs=machine_specs,
        monitor_port=6000,
    )

def test_pd_standard():
    machine_specs = MachineSpec(
        service_account_email="test@sample.com",
        boot_volume_in_gb=100,
        pd_mount_points=[PersistentDiskMount(
                path="/d1",
                size_in_gb=200,
                type="pd-standard"
        )],
        work_root_dir="/mnt",
        machine_type="machinetype",
    )

    pipeline_spec = _create_pipeline_spec(machine_specs)

    expected_mounts = [
                        {"disk": "disk0", "path": "/d1", "readOnly": False}
                    ]
    excepted_volumes =  [{"volume": "disk0", "sizeGb": 200, "type": "pd-standard"}]
    for action in pipeline_spec['pipeline']['actions']:
        assert action["mounts"] == expected_mounts
    assert "disks" not in pipeline_spec['pipeline']['resources']['virtualMachine']
    assert pipeline_spec['pipeline']['resources']['virtualMachine']["volumes"] == excepted_volumes
    breakpoint()
    print("x")

def test_existing_mount():
    machine_specs = MachineSpec(
        service_account_email="test@sample.com",
        boot_volume_in_gb=100,
        pd_mount_points=[ExistingDiskMount(
                name= "d1",
                path="/d1"
        )],
        work_root_dir="/mnt",
        machine_type="machinetype",
    )

    pipeline_spec = _create_pipeline_spec(machine_specs)

    expected_mounts = [
                        {"disk": "disk0", "path": "/d1", "readOnly": False}
                    ]
    excepted_volumes =  [{"volume": "disk0", "existingDisk": {"disk": "d1"}}]
    for action in pipeline_spec['pipeline']['actions']:
        assert action["mounts"] == expected_mounts
    assert "disks" not in pipeline_spec['pipeline']['resources']['virtualMachine']
    assert pipeline_spec['pipeline']['resources']['virtualMachine']["volumes"] == excepted_volumes
