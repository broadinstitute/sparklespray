import json
import time
from kubeque.gcs_pipeline import Cluster

c = Cluster("broad-achilles", ["us-central1-b"])
jobid = 'test-job'
docker_image = 'appropriate/curl'
docker_command = ['-fsSL', 'https://www.google.com/']
data_mount_point = '/mnt/kubeque-data'
kubequeconsume_url = 'invalidurl'
cpu_request = 1
mem_limit = 4
cluster_name = 'test-cluster'
bootDiskSizeGb = 10
spec = c.create_pipeline_spec(jobid, docker_image, docker_command, data_mount_point, kubequeconsume_url, cpu_request, mem_limit, cluster_name, bootDiskSizeGb)
print(spec)
op_name = c.add_node(spec, False)
print(op_name)
while True:
    status = c.get_add_node_status(op_name)
    print("Status:")
    print(json.dumps(status, indent=2))
    if status['done']:
        break
    time.sleep(5)
