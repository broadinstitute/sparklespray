# functions for doing generating kubernetes datastructures and submissions.

import json
import tempfile
import subprocess


def execute_command(cmd, ignore_error=False, capture_stdout=False):
  if capture_stdout:
      assert not ignore_error
      return subprocess.check_output(cmd)
  else:
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

def resize_node_pool(cluster_name, node_pool_name, count, min, max, autoscale):
    cmd = _gcloud_cmd(['alpha', 'container', 'clusters', 'update', cluster_name])

    change_autoscale = False
    if autoscale == True:
        cmd.append('--enable-autoscaling')
        assert min is not None
        cmd.append("--min-nodes={}".format(min))
        assert max is not None
        cmd.append("--max-nodes={}".format(max))
        change_autoscale = True
    elif autoscale == False:
        cmd.append('--disable-autoscaling')
        assert min is None
        assert max is None
        change_autoscale = True

    def append_node_pool(cmd):
        if node_pool_name is not None:
            cmd.append("--node-pool={}".format(node_pool_name))

    append_node_pool(cmd)
    if change_autoscale:
        execute_command(cmd)

    if count is not None:
        cmd = _gcloud_cmd([ 'alpha', 'container', 'clusters', 'resize', cluster_name, "--size={}".format(count)])
        append_node_pool(cmd)
        execute_command(cmd)

def cluster_status(cluster_name):
    cmd = _gcloud_cmd(['alpha', 'container', 'clusters', 'describe', cluster_name])
    stdout = execute_command(cmd, capture_stdout=True)
    import yaml
    import io
    desc = yaml.load(io.StringIO(stdout.decode("utf8")))
    nodePools = desc['nodePools']

    import google.auth

    credentials, project = google.auth.default()

    from google.auth.transport.requests import AuthorizedSession
    authed_session = AuthorizedSession(credentials)

    for np in nodePools:
        for url in np['instanceGroupUrls']:
            response = authed_session.get(url)
            response_json = response.json()
            np['targetSize'] = response_json['targetSize']

        if not ('autoscaling' in np) or 'enabled' not in np['autoscaling']:
            np['autoscaling'] = dict(enabled=False)

    nodePoolRows = [
        dict(nodePool=np['name'],
             targetSize=np['targetSize'],
             autoscaling=np['autoscaling']['enabled'],
             max=np['autoscaling'].get('maxNodeCount',''),
             min=np['autoscaling'].get('minNodeCount',''),
             machineType=np['config']['machineType'],
             status=np['status'])
        for np in nodePools
    ]
    _printTable(nodePoolRows, ["nodePool", "targetSize", "autoscaling", "min", "max", "machineType", "status"])

def _printTable(rows, header):
    def get_col_width(h):
        widths = [ len(str(row[h])) for row in rows ]
        return max(widths)

    colwidths = [ max([get_col_width(h), len(h)]) for h in header ]
    format_string = " ".join(["{{:{}}}".format(w+3) for w in colwidths])
    header_div = " ".join(["-"*(w+3) for w in colwidths])

    print(format_string.format(*header))
    print(header_div)
    for row in rows:
        print(format_string.format(*[str(row[h]) for h in header]))

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

