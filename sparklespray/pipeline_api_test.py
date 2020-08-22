from apiclient.discovery import build
import time

service = build(
    "genomics",
    "v2alpha1",
    #    credentials=credentials,
    # cache_discovery=True,
    # cache=DirCache(".sparkles-cache/services"),
)

mounts = [{"disk": "ephemeralssd", "path": "/mnt/"}]

pipeline_def = {
    "pipeline": {
        "actions": [
            {
                "imageUri": "us.gcr.io/broad-achilles/sparkles-helper:4.0.0",
                "commands": [
                    "bash",
                    "-c",
                    "mkdir -p /mnt/data/buckets/broad-achilles-kubeque && gcsfuse --foreground -o ro "
                    "--stat-cache-ttl 24h --type-cache-ttl 24h --file-mode 755 --implicit-dirs broad-achilles-kubeque /mnt/data/buckets/broad-achilles-kubeque & "
                    "sleep 10 && echo container1 && cat /mnt/data/buckets/broad-achilles-kubeque/test/cas/4eb38fa0e9778adcfa6bed3fc52cd0ce97ecc5d80ca06924a079ef22df93adea",
                ],
                "flags": ["ENABLE_FUSE", "RUN_IN_BACKGROUND"],
                "mounts": mounts,
            },
            {
                "imageUri": "us.gcr.io/broad-achilles/sparkles-helper:4.0.0",
                "commands": [
                    "bash",
                    "-c",
                    "sleep 12 && echo container2 && cat /mnt/data/buckets/broad-achilles-kubeque/test/cas/4eb38fa0e9778adcfa6bed3fc52cd0ce97ecc5d80ca06924a079ef22df93adea",
                ],
                "mounts": mounts,
            },
            {
                "imageUri": "google/cloud-sdk:alpine",
                "commands": [
                    "gsutil",
                    "cp",
                    "/google/logs/output",
                    "gs://broad-achilles-kubeque/test-kube/node-logs/test/test-job.txt",
                ],
                "flags": ["ALWAYS_RUN"],
            },
        ],
        "resources": {
            "projectId": "broad-achilles",
            "zones": ["us-central1-b"],
            "virtualMachine": {
                "machineType": "n1-standard-2",
                "preemptible": True,
                "disks": [
                    {"name": "ephemeralssd", "sizeGb": 375, "type": "local-ssd",}
                ],
                "serviceAccount": {
                    "email": "default",
                    "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
                },
                "bootDiskSizeGb": 26,
                "labels": {"sparkles-cluster": "test-job", "sparkles-job": "test-job",},
            },
        },
    },
    "labels": {"sparkles-cluster": "test-job", "sparkles-job": "test-job",},
}


def get_op(operation_name):
    request = service.projects().operations().get(name=operation_name)
    return request.execute()


operation = service.pipelines().run(body=pipeline_def).execute()
operation_name = operation["name"]
print(operation_name)

import json

last_events_len = 0
while True:
    op = get_op(operation_name)
    events = op["metadata"]["events"]
    if len(events) > last_events_len:
        new_events = events[: (len(events) - last_events_len)]
        new_events.reverse()
        for event in new_events:
            print(json.dumps(event, indent=2))
        last_events_len = len(events)
    # print(op)
    if op["done"]:
        break
    print("sleeping...")
    time.sleep(5)
