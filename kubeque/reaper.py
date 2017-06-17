# Reaper: Reconciles claimed tasked against information about pods.  If it finds the pod has failed, it 
# updates the status of tasks which were claimed by the pod at the time of failure.

import os
import json
import logging
log = logging.getLogger(__name__)
import pykube

import collections
from kubeque.gcp import STATUS_CLAIMED

POD_STATUS_OOM = "OOM"
POD_STATUS_DEAD = "dead"
POD_STATUS_ACTIVE = "active"
POD_STATUS_OTHER = "other"
PodStatus = collections.namedtuple("PodStatus", "name status details")
ClaimedTask = collections.namedtuple("ClaimedTask", "task_id pod_name")
import time
from kubeque.agedset import AgedSet
def reconcile(claimed_tasks, pods, fail_task, warn, dead_set, min_dead_age=30):
    timestamp = time.time()

#    print("claimed_tasks: ", claimed_tasks)
#    print("pods: ", [(p.name, p.status) for p in pods])
    pod_by_name = dict([(p.name, p) for p in pods])

    # make sure there's a key for each non-dead pod
    tasks_per_pod = collections.defaultdict(lambda: [])
    dead_pods = []
    for pod in pods:
        if pod.status in (POD_STATUS_ACTIVE, POD_STATUS_OTHER):
            tasks_per_pod[pod.name] = []

        if pod.status == POD_STATUS_DEAD:
            dead_pods.append(pod.name)

    for task in claimed_tasks:
        tasks_per_pod[task.pod_name].append(task.task_id)

    # find pods with multiple tasks, or no tasks
    for pod_name, tasks in tasks_per_pod.items():
        p = pod_by_name.get(pod_name)
        if len(tasks) > 1:
            warn("Multiple tasks {} were owned by pod {}".format(tasks, pod_name), p)
        elif len(tasks) == 0:
            if p.status == POD_STATUS_ACTIVE:
                warn("Pod {} is active, but no tasks owned by pod".format(pod_name), p)
            elif p.status == POD_STATUS_OTHER:
                warn("Pod {} is not dead, and no tasks owned by pod".format(pod_name), p)

    # find tasks without active pods
    active_pods = set([p.name for p in pods if p.status == POD_STATUS_ACTIVE])
    pods_with_tasks = set(tasks_per_pod.keys())
    missing_pods = pods_with_tasks.difference(active_pods)

    # require a missing pod be missing for a mininum amount of time to let the any outstanding updates to the tasks
    # eventually become visible.  Don't want to mark a complete task as failed in error.   Alternatively, we could
    # change "fail" to skip update if state is already done.   That would work for all cases except pods becoming
    # transiently unavailible.  This approach is slower but perhaps more robust.  Consider revisiting.
    dead_set.update(missing_pods, timestamp)

#    oom_pods = [pod.name for pod in pods if pod.status == POD_STATUS_OOM]
#    for missing_pod in set(dead_set.find_older_than(timestamp-min_dead_age)) | set(oom_pods):
    for missing_pod in dead_set.find_older_than(timestamp - min_dead_age):
        pod = pod_by_name.get(missing_pod)
        pod_details = None
        if pod is not None:
            pod_details = pod.details

        if pod is None:
            reason = "missing"
        elif pod.status == POD_STATUS_DEAD:
            reason = "pod dead"
        elif pod.status == POD_STATUS_OOM:
            reason = "OOM"
        else:
            reason = "pod not active"

        for task_id_with_missing_pod in tasks_per_pod[missing_pod]:
            fail_task(task_id_with_missing_pod, reason, pod_details)

class Reaper:
    def __init__(self, job_id, job_queue, kube_config="~/.kube/config", namespace="default"):
        kube_config_path = os.path.expanduser(kube_config)
        self.namespace = namespace
        self.api = pykube.HTTPClient(pykube.KubeConfig.from_file(kube_config_path))
        self.job_queue = job_queue
        self.job_id = job_id
        self.dead_set = AgedSet()

    def get_pods(self):
        def pod_status(pod):
            if pod["status"]["phase"] == "Running":
                return POD_STATUS_ACTIVE

            if pod["status"]["phase"] == "Pending":
#                if pod["status"]["conditions"]["reason"]
                return POD_STATUS_DEAD # Not exactly dead.  Maybe rename this "INACTIVE"

            if "containerStatuses" in pod["status"]:
                container_statuses = pod["status"]["containerStatuses"]
                assert len(container_statuses) == 1
                container_state = container_statuses[0]["state"]
                if "terminated" in container_state:
                    if container_state["terminated"]["reason"] == "OOMKilled":
                        return POD_STATUS_OOM

            if pod["status"]["phase"] in ("Succeeded", "Failed"):
                return POD_STATUS_DEAD

            return POD_STATUS_OTHER

        pod_dicts = [x.obj for x in pykube.Pod.objects(self.api).filter(namespace=self.namespace, selector="kubequeJob="+self.job_id)]
#        for pod_dict in pod_dicts:
#            print(json.dumps(pod_dict, indent=4))

        pods = [PodStatus(x["metadata"]["name"], pod_status(x), x) for x in pod_dicts]
        return pods

    def get_claimed_tasks(self):
        tasks = self.job_queue.get_tasks(self.job_id, status=STATUS_CLAIMED)
        return [ClaimedTask(t.task_id, t.owner) for t in tasks]

    def poll(self):
        pods = self.get_pods()
        claimed_tasks = self.get_claimed_tasks()
        reconcile(claimed_tasks, pods, self.fail_task, self.warn, self.dead_set)

    def fail_task(self, task_id, reason, pod_details):
        log.warning("Marking task %s as failed: %s", task_id, reason)
        self.job_queue.task_completed(task_id, False, failure_reason=reason)

    def warn(self, msg, pod_details):
        log.warning("%s", msg)



# def _monitor(kube_config, namespace, poll_interval, project_id, cluster_name, dry_run):
#     api = pykube.HTTPClient(pykube.KubeConfig.from_file(kube_config))
#     def get_pods():
#         return [x.obj for x in pykube.Pod.objects(api).filter(namespace=namespace)]
#     jq = create_gcs_job_queue(project_id, cluster_name)
#
#     def mark_task_failed(task_id, reason):
#         if dry_run:
#             log.warning("Would mark task %s as failed with %s", task_id, reason)
#         else:
#             jq.task_completed(task_id, False, reason)
#
#     running = True
#     while running:
#         if poll_interval == 0:
#             running = False
#
#         pods = get_pods()
#         print(pods)
#         pods_by_id = dict([(x["metadata"]["name"], x) for x in pods])
#
#         log.info("pods_by_id: %s", pods_by_id)
#
#         # find the names of all owners that currently have claimed a task
#         log.info("fetching claimed_tasks")
#         id_and_owner_names = jq.get_claimed_task_ids()
#         log.info("claimed_tasks: %s", id_and_owner_names)
#
#         log.info("fetched %d pods from kube and %d pods with claimed task from job queue", len(pods_by_id), len(id_and_owner_names))
#
#         for task_id, owner_name in id_and_owner_names:
#             if owner_name not in pods_by_id:
#                 log.warning("%s missing from pods.  Assuming dead.  Marking %s as failed", owner_name, task_id)
#                 mark_task_failed(task_id, "MissingPod")
#                 continue
#
#             pod = pods_by_id[owner_name]
#             if pod["status"]["containerStatuses"][0]["state"] == "terminated":
#                 reason = pod["status"]["containerStatuses"][0]["reason"]
#                 log.info("terminated reason=%s", reason)
#             if pod["status"]["phase"] == "Failed":
#                 # TODO: handle reason == OOM, vs out of disk, etc.  However, I don't know what are all the possible values
#                 # so for now, just record failure reason and don't attempt to recover.  Eventually, we'd like
#                 # to recognize unreachable nodes and not mark them as failures, but reset them to be ready to run.
#                 #reason = pod["status"]["containerStatuses"][0]["reason"]
#                 reason = "MaybeOOM"
#                 mark_task_failed(task_id, reason)
#                 log.warning("recording %s failed due to %s", owner_name, reason)
#             elif pod["status"]["phase"] != "Running":
#                 phase = pod["status"]["phase"]
#                 log.warning("%s reported as '%s', but job queue reports in use", owner_name, phase)
#
#         log.info("Sleeping...")
#         time.sleep(poll_interval)
#
# def main(argv=None):
#     logging.basicConfig(level=logging.INFO)
#
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--namespace", default="default")
#     parser.add_argument("--pollinterval", type=int, default=60, help="delay in seconds between each poll.  Set to zero to only poll once")
#     parser.add_argument("--config", default=os.path.expanduser("~/.kube/config"))
#     parser.add_argument("--dryrun", action="store_true")
#     parser.add_argument("project_id")
#     parser.add_argument("cluster_name")
#
#     args = parser.parse_args(argv)
#
#     monitor(args.config, args.namespace, args.pollinterval, args.project_id, args.cluster_name, args.dryrun)
#
#
