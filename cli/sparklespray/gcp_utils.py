from typing import List, Dict, Any
from .model import MachineSpec, LOCAL_SSD, PersistentDiskMount, ExistingDiskMount
import re
import os

# and image which has curl and sh installed, used to prep the worker node
SETUP_IMAGE = "sequenceiq/alpine-curl"

max_label_len = 63


def normalize_label(label):
    label = label.lower()
    label = re.sub("[^a-z0-9]+", "-", label)
    if re.match("[^a-z].*", label) is not None:
        label = "x-" + label
    return label


def validate_label(label):
    assert len(label) <= max_label_len
    assert re.match("[a-z][a-z0-9-]*", label) is not None


def make_unique_label(label):
    import string
    import random

    suffix_len = 5
    new_label = f"{label[:max_label_len-suffix_len-1]}-{''.join(random.sample(list(string.ascii_lowercase) + list(string.digits), suffix_len))}"
    validate_label(new_label)
    return new_label


# def create_pipeline_json(
#     project: str,
#     zones: List[str],
#     jobid: str,
#     cluster_name: str,
#     setup_image: str,
#     setup_commands: List[List[str]],
#     docker_image: str,
#     docker_command: List[str],
#     machine_specs: MachineSpec,
#     monitor_port: int,
# ) -> dict:
#     # labels have a few restrictions
#     normalized_jobid = normalize_label(jobid)

#     mounts = []
#     #    volumes = []
#     disks = []
#     for i, pd in enumerate(machine_specs.mounts):
#         if isinstance(pd, ExistingDiskMount):
#             # only allowed with life science API
#             # volumes.append({"volume": f"disk{i}", "disk": {"disk": pd.name}})
#             raise Exception("Mounting existing volumes is not allowed at this time")
#         elif isinstance(pd, PersistentDiskMount):
#             # only allowed with life science API
#             # volumes.append(
#             #     {
#             #         "volume": f"disk{i}",
#             #         "disk": {"sizeGb": pd.size_in_gb, "type": pd.type},
#             #     }
#             # )
#             disks.append({"type": pd.type, "sizeGb": pd.size_in_gb, "name": f"disk{i}"})
#         else:
#             raise ValueError("{pd} was neither an ")

#         mounts.append(
#             {
#                 "disk": f"disk{i}",
#                 "path": pd.path,
#                 "readOnly": False,
#             }
#         )

#     actions: List[Dict[str, Any]] = []

#     for setup_command in setup_commands:
#         actions.append(
#             # set up directories
#             {
#                 "imageUri": setup_image,
#                 "commands": setup_command,
#                 "mounts": mounts,
#             }
#         )

#     actions.append(
#         # start consumer
#         {
#             "imageUri": docker_image,
#             "commands": docker_command,
#             "mounts": mounts,
#             "portMappings": {str(monitor_port): monitor_port},
#         }
#     )

#     pipeline_def = {
#         "pipeline": {
#             "actions": actions,
#             "resources": {
#                 # "projectId": project,
#                 "zones": zones,
#                 "virtualMachine": {
#                     "machineType": machine_specs.machine_type,
#                     "preemptible": False,
#                     # this seems to only be allowed in life science API
#                     # "volumes": volumes,
#                     "disks": disks,
#                     "serviceAccount": {
#                         "email": "default",
#                         "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
#                     },
#                     "bootDiskSizeGb": machine_specs.boot_volume_in_gb,
#                     "serviceAccount": {
#                         "email": machine_specs.service_account_email,
#                         "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
#                     },
#                     "labels": {
#                         "kubeque-cluster": cluster_name,
#                         "sparkles-job": normalized_jobid,
#                     },
#                 },
#             },
#         },
#         "labels": {
#             "kubeque-cluster": cluster_name,
#             "sparkles-job": normalized_jobid,
#         },
#     }

#     return pipeline_def


def get_region(zone):
    # drop the zone suffix to get the name of the region
    # that contains the zone
    # us-east1-b -> us-east1
    m = re.match("^([a-z0-9]+-[a-z0-9]+)-[a-z0-9]+$", zone)
    assert m, f"Zone doesn't look like a valid zone name: {zone}"
    return m.group(1)
