from typing import List, Dict, Any
from .model import MachineSpec, LOCAL_SSD, PersistentDiskMount, ExistingDiskMount
import re
import os

# and image which has curl and sh installed, used to prep the worker node
SETUP_IMAGE = "sequenceiq/alpine-curl"


def normalize_label(label):
    label = label.lower()
    label = re.sub("[^a-z0-9]+", "-", label)
    if re.match("[^a-z].*", label) is not None:
        label = "x-" + label
    return label


def create_pipeline_json(
    project: str,
    zones: List[str],
    jobid: str,
    cluster_name: str,
    setup_image: str,
    setup_commands: List[List[str]],
    docker_image: str,
    docker_command: List[str],
    machine_specs: MachineSpec,
    monitor_port: int,
) -> dict:
    # labels have a few restrictions
    normalized_jobid = normalize_label(jobid)

    mounts = []
    volumes = []
    for i, pd in enumerate(machine_specs.mounts):
        if isinstance(pd, ExistingDiskMount):
            volumes.append({"volume": f"disk{i}", "existingDisk": {"disk": pd.name}})
        elif isinstance(pd, PersistentDiskMount):
            volumes.append(
                {
                    "volume": f"disk{i}",
                    "persistentDisk": {"sizeGb": pd.size_in_gb, "type": pd.type},
                }
            )
        else:
            raise ValueError("{pd} was neither an ")

        mounts.append(
            {
                "disk": f"disk{i}",
                "path": pd.path,
                "readOnly": False,
            }
        )

    actions: List[Dict[str, Any]] = []

    for setup_command in setup_commands:
        actions.append(
            # set up directories
            {
                "imageUri": setup_image,
                "commands": setup_command,
                "mounts": mounts,
            }
        )

    actions.append(
        # start consumer
        {
            "imageUri": docker_image,
            "commands": docker_command,
            "mounts": mounts,
            "portMappings": {str(monitor_port): monitor_port},
        }
    )

    pipeline_def = {
        "pipeline": {
            "actions": actions,
            "resources": {
                "projectId": project,
                "zones": zones,
                "virtualMachine": {
                    "machineType": machine_specs.machine_type,
                    "preemptible": False,
                    "volumes": volumes,
                    "serviceAccount": {
                        "email": "default",
                        "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
                    },
                    "bootDiskSizeGb": machine_specs.boot_volume_in_gb,
                    "serviceAccount": {
                        "email": machine_specs.service_account_email,
                        "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
                    },
                    "labels": {
                        "kubeque-cluster": cluster_name,
                        "sparkles-job": normalized_jobid,
                    },
                },
            },
        },
        "labels": {
            "kubeque-cluster": cluster_name,
            "sparkles-job": normalized_jobid,
        },
    }

    return pipeline_def


def create_pipeline_spec(
    project: str,
    zones: List[str],
    jobid: str,
    cluster_name: str,
    consume_exe_url: str,
    consume_exe_md5: str,
    docker_image: str,
    consume_exe_args: List[str],
    machine_specs: MachineSpec,
    monitor_port: int,
) -> dict:
    work_root_dir = machine_specs.work_root_dir

    consume_exe_path = os.path.join(work_root_dir, "consume")
    consume_data = os.path.join(work_root_dir, "data")

    exe_dir = os.path.dirname(consume_exe_path)
    checksum_path = os.path.join(exe_dir, "expected-checksums")
    setup_commands = [
        # download executable
        ["mkdir", "-p", exe_dir],
        ["chmod", "a+rwx", exe_dir],
        ["curl", "-o", consume_exe_path, consume_exe_url],
        # verify checksum of downloaded file
        ["sh", "-c", f'echo "{consume_exe_md5}  {consume_exe_path}" > {checksum_path}'],
        ["md5sum", "-c", checksum_path],
        # mark file as executable
        ["chmod", "a+x", consume_exe_path],
        # set up directory that'll be used by consume exe
        ["mkdir", "-p", consume_data],
        ["chmod", "a+rwx", consume_data],
    ]

    return create_pipeline_json(
        project=project,
        zones=zones,
        jobid=jobid,
        cluster_name=cluster_name,
        setup_image=SETUP_IMAGE,
        setup_commands=setup_commands,
        docker_image=docker_image,
        docker_command=[
            consume_exe_path,
            "consume",
            "--cacheDir",
            os.path.join(consume_data, "cache"),
            "--tasksDir",
            os.path.join(consume_data, "tasks"),
        ]
        + consume_exe_args,
        machine_specs=machine_specs,
        monitor_port=monitor_port,
    )
