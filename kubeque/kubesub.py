# functions for doing generating kubernetes datastructures and submissions.

import os
import json
import tempfile
import subprocess

def execute_command(cmd):
    subprocess.check_call(cmd)

def add_secret_mount(config, secret_name, mount_path):
  if "volumes" not in config['spec']:
    config['spec']['volumes'] = []
  config['spec']['volumes'].append({
    "name": "secret-volume",
    "secret": {
      "secretName": secret_name
    }
  })

  for c in config['spec']['containers']:
    if "volumeMounts" not in c:
      c["volumeMounts"] = []
    c["volumeMounts"].append({
      "name": "secret-volume",
      "readOnly": True,
      "mountPath": mount_path        
    })

def submit_job(name, parallelism, image, command, environment_vars=[], secrets=[], cpu_request="1.0", mem_limit="100M"):
    assert isinstance(command, list)
    config = {"apiVersion": "batch/v1",
     "kind": "Job",
     "metadata": {"name": name},
     "spec": {
      "parallelism": parallelism,
#      "completions": 1,
      "template": {
        "metadata": {
          "name": name
        },
        "spec": {
          "imagePullSecrets": [{"name": "kubeque-registry-key"}],
          "containers": [{
             "name": name,
             "image": image,
             "command": command,
             "resources": {
               "requests": { "cpu": cpu_request, "memory": mem_limit },
               "limits": { "memory": mem_limit },
             },
             "env": [
               {"name": "KUBE_POD_NAME",
                "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}
             ]
            }],
          "restartPolicy": "OnFailure"
        }
    }}}

    for secret_name, mount_path in secrets:
      add_secret_mount(config, secret_name, mount_path)

    for name, value in environment_vars:
      add_environment_var(name, value)

    with tempfile.NamedTemporaryFile("wt") as t:
      json.dump(config, t)
      t.flush()
      cmd = ["kubectl", "create", "-f", t.name]
      execute_command(cmd)

def start_cluster(cluster_name, machine_type, num_nodes):
  cmd = ["gcloud", "container", "clusters", "create", 
    cluster_name,
    "--machine-type", machine_type,
    "--num-nodes", str(num_nodes),
    "--scopes", "datastore,storage-full"
  ]
  execute_command(cmd)

def stop_cluster(cluster_name):
  cmd = ["gcloud", "container", "clusters", "delete", 
    cluster_name]
  execute_command(cmd)

def delete_job(jobid):
  cmd = ["kubectl", "delete", "jobs/{}".format(jobid)]
  execute_command(cmd)

def stop_job(jobid):
  cmd = ["kubectl", "scale", "--replicas=0", "jobs/{}".format(jobid)]
  execute_command(cmd)
    