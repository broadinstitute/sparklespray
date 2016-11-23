# Reaper: Reconciles claimed tasked against information about pods.  If it finds the pod has failed, it 
# updates the status of tasks which were claimed by the pod at the time of failure.

import os
import json
from kubeque.gcp import create_gcs_job_queue, IO
import kubeque.main 
import logging
import argparse
import subprocess
log = logging.getLogger(__name__)
import pykube

def monitor(kube_config, namespace, poll_interval):
    api = pykube.HTTPClient(pykube.KubeConfig.from_file(kube_config))
    pods = pykube.Pod.objects(api).filter(namespace=namespace)

    jq = create_gcs_job_queue()

    while True:
        pods_by_id = dict([(x.uid, x) for x in pods])

        # find the names of all owners that currently have claimed a task
        id_and_owner_names = jq.find_owners_in_use()

        log.info("fetched %d pods from kube and %d pods with claimed task from job queue", len(pods_by_id), len(id_and_owner_names))

        for task_id, owner_name in id_and_owner_names:
            if owner_name not in pods_by_id:
                log.warn("%s missing from pods.  Assuming dead", owner_name)
                dead_owners.add(owner_name)
                continue
            
            pod = pods_by_id[owner_name]
            if pod.phase == "Failed":
                # TODO: handle reason == OOM, vs out of disk, etc.  However, I don't know what are all the possible values
                # so for now, just record failure reason and don't attempt to recover.  Eventually, we'd like 
                # to recognize unreachable nodes and not mark them as failures, but reset them to be ready to run. 
                jq.update_failed(task_id, pod.reason)
                log.warn("recording %s failed due to %s", owner_name, pod.reason)
            elif pod.phase != "Running":
                log.warn("%s reported as '%s', but job queue reports in use", owner_name, pod.phase)

        time.sleep(poll_interval)

def main(argv=None):
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--pollinterval", type=int, default=60)
    parser.add_argument("--config", default=os.path.expanduser("~/.kube/config"))

    args = parser.parse_args(argv)

    monitor(args.config, args.namespace, args.pollinterval)
