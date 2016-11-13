# functions for doing generating kubernetes datastructures and submissions.

import os
import json
import tempfile

def submit_job(name, parallelism, image, command):
    print("submit_job")
    assert isinstance(command, list)
    config = {"apiVersion": "batch/v1",
     "kind": "Job",
     "metadata": {"name": name},
     "spec": {
      "completions": 1,
      "parallelism": 1,
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

    with tempfile.NamedTemporaryFile("wt") as t:
      json.dump(config, t)
      t.flush()
      cmd = "kubectl create -f {}".format(t.name)
      print("executing", cmd)
      ret = os.system(cmd)
    assert ret == 0
