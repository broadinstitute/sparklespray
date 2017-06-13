# functions for doing generating kubernetes datastructures and submissions.

import json
import tempfile
import subprocess


def execute_command(cmd, ignore_error=False):
  if ignore_error:
    subprocess.call(cmd)    
  else:
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

def create_kube_job_spec(name, parallelism, image, command, environment_vars=[], secrets=[], cpu_request="1.0", mem_limit="100M"):
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
                          "volumes": [
                              {"name": "host-var-volume",
                               "hostPath": {
                                   "path": "/var"
                               }}
                          ],
                          "containers": [{
                              "name": name,
                              "image": image,
                              "volumeMounts": [{
                                  "mountPath": "/host-var",
                                  "name": "host-var-volume"
                              }],
                              "command": command,
                              "resources": {
                                  "requests": {"cpu": cpu_request, "memory": mem_limit},
                                  "limits": {"memory": mem_limit},
                              },
                              "env": [
                                  {"name": "KUBE_POD_NAME",
                                   "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}
                              ]
                          }],
                          "restartPolicy": "Never"
                      }
                  }}}

    for secret_name, mount_path in secrets:
        add_secret_mount(config, secret_name, mount_path)

    for name, value in environment_vars:
        add_environment_var(name, value)

    return json.dumps(config)

def submit_job_spec(job_spec):
    with tempfile.NamedTemporaryFile("wt") as t:
      t.write(job_spec)
      t.flush()
      cmd = ["kubectl", "create", "-f", t.name]
      execute_command(cmd)

def _gcloud_cmd(args, config_name="kubeque"):
    return ["gcloud", "--configuration="+config_name] + list(args)

def _full_cluster_name(project_id, zone, cluster_name):
    return "gke_{}_{}_{}".format(project_id, zone, cluster_name)

def setup_cluster_creds(project_id, zone, cluster_name, configuration):
    # setup credentials for kubectl
    execute_command(_gcloud_cmd(["container", "clusters", "get-credentials",
                                 cluster_name
                                 ]))
    full_cluster_name = _full_cluster_name(project_id, zone, cluster_name)
    # rewrite entry getting credentials
    import yaml
    import os
    kube_config_path = os.path.expanduser("~/.kube/config")
    found_config = False
    with open(kube_config_path, "rt") as fd:
        kube_config = yaml.load(fd)
        for u in kube_config['users']:
            if u['name'] == full_cluster_name:
                assert u["user"]["auth-provider"]["config"][
                           "cmd-args"] == "config config-helper --format=json"
                u["user"]["auth-provider"]["config"][
                    "cmd-args"] = "config --configuration={} config-helper --format=json".format(configuration)
                found_config = True
    assert found_config
    with open(kube_config_path, "wt") as fd:
        fd.write(yaml.dump(kube_config))


def start_cluster(cluster_name, machine_type, num_nodes):
  execute_command(_gcloud_cmd(["container", "clusters", "create",
    cluster_name,
    "--machine-type", machine_type,
    "--num-nodes", str(num_nodes),
    "--scopes", "datastore,storage-full,https://www.googleapis.com/auth/pubsub"
  ]))


def stop_cluster(cluster_name):
  execute_command(_gcloud_cmd(["container", "clusters", "delete",
    cluster_name]))

def delete_job(jobid):
  cmd = ["kubectl", "delete", "jobs/{}".format(jobid)]
  execute_command(cmd, ignore_error=True)

def stop_job(jobid):
  cmd = ["kubectl", "scale", "--replicas=0", "jobs/{}".format(jobid)]
  execute_command(cmd)

def add_node_pool(cluster_name, node_pool_name, machine_type, num_nodes,
                  min_nodes, max_nodes, is_autoscale, is_preemptable):

    cmd = _gcloud_cmd(["alpha", "container", "node-pools", "create",
           node_pool_name,
           "--cluster", cluster_name,
           "--machine-type", machine_type,
           "--num-nodes", str(num_nodes),
           "--scopes", "datastore,storage-full,https://www.googleapis.com/auth/pubsub"
           ])
    if is_preemptable:
        cmd += ["--preemptible"]
    if is_autoscale:
        assert min_nodes is not None
        assert max_nodes is not None
        cmd += ["--enable-autoscaling", "--max-nodes="+str(max_nodes), "--min-nodes="+str(min_nodes)]
    execute_command(cmd)

def rm_node_pool(cluster_name, node_pool_name):
    cmd = _gcloud_cmd(["container", "node-pools", "delete",
           node_pool_name,
           "--cluster", cluster_name,
        ])
    execute_command(cmd)

def peek(project_id, zone, cluster_name, pod_name, lines):
    import pykube
    import os

    kube_config = os.path.expanduser("~/.kube/config")
    full_cluster_name = _full_cluster_name(project_id, zone, cluster_name)
    config = pykube.KubeConfig.from_file(kube_config)
    config.set_current_context(full_cluster_name)
    print("user={}".format(config.user))
    api = pykube.HTTPClient(config)
    pods = list(pykube.Pod.objects(api).filter( field_selector={ "metadata.name": pod_name } ))
    if len(pods) > 1:
        print("{} pods had the name {}".format(len(pods), pod_name))
    elif len(pods) == 0:
        print("Could not find pod named {}".format(pod_name))
    else:
        pod = pods[0]
        print(pod.logs(tail_lines=lines))

