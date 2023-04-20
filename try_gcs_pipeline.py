import json
import time
from kubeque.gcs_pipeline import Cluster, MachineSpec

c = Cluster("broad-achilles", ["us-central1-b"])
jobid = "test-job"
docker_image = "sequenceiq/alpine-curl"
mount_point = "/mnt/kubeque-data"
kubequeconsume_url = "invalidurl"
cpu_request = 1
mem_limit = 4
cluster_name = "test-cluster"
bootDiskSizeGb = 10
consume_exe_url = (
    "https://broad-achilles-kubeque.storage.googleapis.com/kubetest/dist/mock-consume"
)
consume_exe_args = ["test", "arg"]
machine_spec = MachineSpec(
    boot_volume_in_gb=bootDiskSizeGb,
    mount_point=mount_point,
    machine_type="n1-standard-1",
)
spec = c.create_pipeline_spec(
    jobid=jobid,
    cluster_name=cluster_name,
    consume_exe_url=consume_exe_url,
    docker_image=docker_image,
    consume_exe_args=consume_exe_args,
    machine_specs=machine_spec,
)
print(spec)
op_name = c.add_node(spec, False)
print(op_name)
prev_status = None
while True:
    status = c.get_add_node_status(op_name)
    print("Status:")
    print(status.get_event_summary(prev_status))
    prev_status = status
    # print(json.dumps(status, indent=2))
    if status.is_done():
        break
    print(status.status)
    time.sleep(5)

    def get_event_summary(self, since=None):
        log = []
        events = self.status["metadata"]["events"]
        # TODO: Better yet, sort by timestamp
        events = list(reversed(events))
        if since is not None:
            assert isinstance(since, AddNodeStatus)
            events = events[len(since.status["metadata"]["events"]) :]

        for event in events:
            if (
                event["details"]["@type"]
                == "type.googleapis.com/google.genomics.v2alpha1.ContainerStoppedEvent"
            ):
                actionId = event["details"]["actionId"]
                action = self.status["metadata"]["pipeline"]["actions"][actionId - 1]
                log.append(
                    "Completed ({}): {}".format(
                        action["imageUri"], repr(action["commands"])
                    )
                )
                log.append(event["description"])
                log.append(
                    "exitStatus: {}, stderr:".format(event["details"]["exitStatus"])
                )
                log.append(event["details"]["stderr"])
            else:
                # if event['details']['@type'] != 'type.googleapis.com/google.genomics.v2alpha1.ContainerStartedEvent':
                log.append(event["description"])
        return "\n".join(log)
