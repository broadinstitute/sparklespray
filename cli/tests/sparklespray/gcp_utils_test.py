from sparklespray.model import MachineSpec, PersistentDiskMount, ExistingDiskMount
from sparklespray.gcp_utils import create_pipeline_spec


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

import pytest

def test_pd_standard():
    pytest.skip("Lots of refactoring -- need to fix this")
    
    machine_specs = MachineSpec(
        service_account_email="test@sample.com",
        boot_volume_in_gb=100,
        mounts=[
            PersistentDiskMount(path="/d1", size_in_gb=200, type="pd-standard")
        ],
        work_root_dir="/mnt",
        machine_type="machinetype",

    )

    pipeline_spec = _create_pipeline_spec(machine_specs)

    expected_mounts = [{"disk": "disk0", "path": "/d1", "readOnly": False}]
    # excepted_volumes = [{"volume": "disk0", "disk": { "sizeGb": 200, "type": "pd-standard" }}]
    for action in pipeline_spec["pipeline"]["actions"]:
        assert action["mounts"] == expected_mounts

    assert pipeline_spec["pipeline"]["resources"]["virtualMachine"]['disks'] == [{'type': 'pd-standard', 'sizeGb': 200, 'name': 'disk0'}]
    # assert "disks" not in pipeline_spec["pipeline"]["resources"]["virtualMachine"]
    # assert (
    #     pipeline_spec["pipeline"]["resources"]["virtualMachine"]["volumes"]
    #     == excepted_volumes
    # )
    assert "volumes" not in pipeline_spec

# def test_existing_mount():
#     machine_specs = MachineSpec(
#         service_account_email="test@sample.com",
#         boot_volume_in_gb=100,
#         mounts=[ExistingDiskMount(name="d1", path="/d1")],
#         work_root_dir="/mnt",
#         machine_type="machinetype",
#     )

#     pipeline_spec = _create_pipeline_spec(machine_specs)

#     expected_mounts = [{"disk": "disk0", "path": "/d1", "readOnly": False}]
#     excepted_volumes = [{"volume": "disk0", "disk": {"disk": "d1"}}]
#     for action in pipeline_spec["pipeline"]["actions"]:
#         assert action["mounts"] == expected_mounts
#     assert "disks" not in pipeline_spec["pipeline"]["resources"]["virtualMachine"]
#     assert (
#         pipeline_spec["pipeline"]["resources"]["virtualMachine"]["volumes"]
#         == excepted_volumes
#     )
