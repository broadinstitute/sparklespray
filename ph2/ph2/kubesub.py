import os
import json
import tempfile

def submit_job(name, parallelism, image, command):
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
             "env": [
               {"name": "AWS_ACCESS_KEY_ID",
                "valueFrom": { "secretKeyRef": {"name": "kubeque-aws", "key": "keyid"}}},
               {"name": "AWS_SECRET_ACCESS_KEY",
               "valueFrom": { "secretKeyRef": {"name": "kubeque-aws", "key": "secretkey"}}}],
             "command": command
            }],
          "restartPolicy": "OnFailure"
        }
    }}}

    with tempfile.NamedTemporaryFile("wt") as t:
      json.dump(config, t)
      t.flush()
      ret = os.system("kubectl create -f {}".format(t.name))
    assert ret == 0
