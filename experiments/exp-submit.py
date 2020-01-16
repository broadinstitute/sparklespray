from apiclient.discovery import build
import time

service = build("genomics", "v2alpha1")


def create_pipeline_json(
    project_id,
    zones,
    bucket_name: str,
    bucket_mount_path: str,
    machine_specs_mount_point: str,
    machine_specs_machine_type: str,
    machine_specs_boot_volume_in_gb: int,
    debug_log_url: str,
) -> dict:

    normalized_jobid = "test-submission"

    mounts = [
        {"disk": "ephemeralssd", "path": machine_specs_mount_point, "readOnly": False}
    ]

    return {
        "pipeline": {
            "actions": [
                {
                    "imageUri": "alpine",
                    "commands": ["mkdir", "-p", bucket_mount_path],
                    "mounts": mounts,
                },
                {
                    "imageUri": "chiaen/docker-gcsfuse",
                    # , "--stat-cache-ttl", "10000h", "--stat-cache-type", "10000h", "-o", "ro", "--foreground"],
                    "commands": [
                        "gcsfuse",
                        "--foreground",
                        bucket_name,
                        bucket_mount_path,
                    ],
                    "mounts": mounts,
                    "flags": ["RUN_IN_BACKGROUND", "ENABLE_FUSE"],
                },
                {
                    "imageUri": "alpine",
                    "commands": ["ls", "-l", bucket_mount_path],
                    "mounts": mounts,
                },
                {
                    "imageUri": "google/cloud-sdk:alpine",
                    "commands": ["gsutil", "cp", "/google/logs/output", debug_log_url],
                    "flags": ["ALWAYS_RUN"],
                },
            ],
            "resources": {
                "projectId": project_id,
                "zones": zones,
                "virtualMachine": {
                    "machineType": machine_specs_machine_type,
                    "preemptible": False,
                    "disks": [{"name": "ephemeralssd", "type": "local-ssd"}],
                    "serviceAccount": {
                        "email": "default",
                        "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
                    },
                    "bootDiskSizeGb": machine_specs_boot_volume_in_gb,
                    "labels": {"sparkles-job": normalized_jobid},
                },
            },
        },
        "labels": {"sparkles-job": normalized_jobid},
    }


pipeline_def = create_pipeline_json(
    project_id="broad-achilles",
    zones=["us-central1-b"],
    bucket_name="genomics-public-data",
    bucket_mount_path="/mnt/genomics-public-data",
    machine_specs_mount_point="/mnt/",
    machine_specs_machine_type="n1-standard-2",
    machine_specs_boot_volume_in_gb=20,
    debug_log_url="gs://broad-achilles-kubeque/test-gcsfuse",
)


def get_operation_details(operation_name: str) -> dict:
    request = service.projects().operations().get(name=operation_name)
    response = request.execute()
    return response


operation = service.pipelines().run(body=pipeline_def).execute()

operation_name = operation["name"]
prev_status_text = None
while True:
    response = get_operation_details(operation_name)
    print(response)
    print("")
    # status_text = status.status
    # if prev_status_text != status_text:
    #     out.write(f"({status_text})")
    #     prev_status_text = status_text
    # else:
    #     out.write(".")
    # out.flush()
    # if status_text == NODE_REQ_COMPLETE:
    #     break
    if response["done"]:
        break
    time.sleep(5)
