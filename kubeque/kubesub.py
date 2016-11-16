# functions for doing generating kubernetes datastructures and submissions.

import os
import json
import tempfile

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

def submit_job(name, parallelism, image, command, environment_vars=[], secrets=[]):
    print("submit_job")
    assert isinstance(command, list)
    config = {"apiVersion": "batch/v1",
     "kind": "Job",
     "metadata": {"name": name},
     "spec": {
      #"completions": 1,
      "parallelism": parallelism,
      "template": {
        "metadata": {
          "name": name
        },
        "spec": {
          "imagePullSecrets": [{"name": "kubeque-registry-key"}],
          "containers": [{
             "name": name,
             "image": image,
             "command": command
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
      cmd = "kubectl create -f {}".format(t.name)
      print("executing", cmd)
      ret = os.system(cmd)
    assert ret == 0
