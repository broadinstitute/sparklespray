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
import time

def monitor(kube_config, namespace, poll_interval, project_id, cluster_name, dry_run):
    api = pykube.HTTPClient(pykube.KubeConfig.from_file(kube_config))
    def get_pods():
        return [x.obj for x in pykube.Pod.objects(api).filter(namespace=namespace)]
    jq = create_gcs_job_queue(project_id, cluster_name)

    def mark_task_failed(task_id, reason):
        if dry_run:
            log.warning("Would mark task %s as failed with %s", task_id, reason)
        else:
            jq.task_completed(task_id, False, reason)

    running = True
    while running:
        if poll_interval == 0:
            running = False

        pods = get_pods()
        print(pods)
        pods_by_id = dict([(x["metadata"]["name"], x) for x in pods])

        log.info("pods_by_id: %s", pods_by_id)

        # find the names of all owners that currently have claimed a task
        log.info("fetching claimed_tasks")
        id_and_owner_names = jq.get_claimed_task_ids()
        log.info("claimed_tasks: %s", id_and_owner_names)

        log.info("fetched %d pods from kube and %d pods with claimed task from job queue", len(pods_by_id), len(id_and_owner_names))

        for task_id, owner_name in id_and_owner_names:
            if owner_name not in pods_by_id:
                log.warning("%s missing from pods.  Assuming dead.  Marking %s as failed", owner_name, task_id)
                mark_task_failed(task_id, "MissingPod")
                continue

            pod = pods_by_id[owner_name]
            if pod["status"]["containerStatuses"][0]["state"] == "terminated":
                reason = pod["status"]["containerStatuses"][0]["reason"]
                log.info("terminated reason=%s", reason)
            if pod["status"]["phase"] == "Failed":
                # TODO: handle reason == OOM, vs out of disk, etc.  However, I don't know what are all the possible values
                # so for now, just record failure reason and don't attempt to recover.  Eventually, we'd like 
                # to recognize unreachable nodes and not mark them as failures, but reset them to be ready to run. 
                #reason = pod["status"]["containerStatuses"][0]["reason"]
                reason = "MaybeOOM"
                mark_task_failed(task_id, reason)
                log.warning("recording %s failed due to %s", owner_name, reason)
            elif pod["status"]["phase"] != "Running":
                phase = pod["status"]["phase"]
                log.warning("%s reported as '%s', but job queue reports in use", owner_name, phase)

        log.info("Sleeping...")
        time.sleep(poll_interval)

def main(argv=None):
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--pollinterval", type=int, default=60, help="delay in seconds between each poll.  Set to zero to only poll once")
    parser.add_argument("--config", default=os.path.expanduser("~/.kube/config"))
    parser.add_argument("--dryrun", action="store_true")
    parser.add_argument("project_id")
    parser.add_argument("cluster_name")

    args = parser.parse_args(argv)

    monitor(args.config, args.namespace, args.pollinterval, args.project_id, args.cluster_name, args.dryrun)
