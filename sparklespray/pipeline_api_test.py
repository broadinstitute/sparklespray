from apiclient.discovery import build
import time
import json
from google.cloud.storage.client import Client

project = "broad-achilles"
zone = "us-central1-b"
location = "us-central1"
log_dest = "gs://broad-achilles-kubeque/test-kube/node-logs/test/test-job.txt"


service = build("lifesciences", "v2beta",)
# resp = service.projects().locations().list(name=f"projects/{project}").execute()
# print(resp)

mounts = [{"disk": "ephemeralssd", "path": "/mnt/data"}]

pipeline_def = {
    "pipeline": {
        "actions": [
            {
                "imageUri": "us.gcr.io/broad-achilles/sparkles-helper:4.0.0",
                "commands": [
                    "bash",
                    "-c",
                    "mkdir -p /mnt/data/buckets/genomics-public-data && "
                    "gcsfuse --foreground -o ro --implicit-dirs genomics-public-data /mnt/data/buckets/genomics-public-data & "
                    "sleep 10 && "
                    "echo container1 && "
                    "ls -l /mnt/data/buckets/genomics-public-data/1000-genomes/other/sample_info/sample_info.schema && "
                    "md5sum /mnt/data/buckets/genomics-public-data/1000-genomes/other/sample_info/sample_info.schema && "
                    "echo sleeping && sleep 10000",
                ],
                "enableFuse": True,
                "runInBackground": True,
                "mounts": mounts,
            },
            {
                "imageUri": "us.gcr.io/broad-achilles/sparkles-helper:4.0.0",
                "commands": [
                    "bash",
                    "-c",
                    "sleep 12 && "
                    "echo container2 && "
                    "md5sum /mnt/data/buckets/genomics-public-data/1000-genomes/other/sample_info/sample_info.schema",
                ],
                "mounts": mounts,
            },
            {
                "imageUri": "google/cloud-sdk:alpine",
                "commands": ["gsutil", "cp", "/google/logs/output", log_dest,],
                "alwaysRun": True,
            },
        ],
        "resources": {
            "zones": [zone],
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
    request = service.projects().locations().operations().get(name=operation_name)
    return request.execute()


operation = (
    service.projects()
    .locations()
    .pipelines()
    .run(parent=f"projects/{project}/locations/{location}", body=pipeline_def)
    .execute()
)
operation_name = operation["name"]
print(operation_name)

# poll operation status
last_events_len = 0
while True:
    op = get_op(operation_name)
    events = op["metadata"].get("events", [])
    if len(events) > last_events_len:
        new_events = events[: (len(events) - last_events_len)]
        new_events.reverse()
        for event in new_events:
            print(json.dumps(event, indent=2))
        last_events_len = len(events)
    #    print(op)
    if op.get("done", False):
        break
    print("sleeping...")
    time.sleep(5)


client = Client()


def get_as_str(path):
    import re

    m = re.match("^gs://([^/]+)/(.*)$", path)
    assert m != None, "invalid remote path: {}".format(path)
    bucket_name = m.group(1)
    path = m.group(2)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    return blob.download_as_string().decode("utf8")


print("------ output ------")
print(get_as_str(log_dest))
