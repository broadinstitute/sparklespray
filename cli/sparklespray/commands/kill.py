from ..job_queue import JobQueue
from ..log import log
from ..task_store import (
    STATUS_PENDING,
    STATUS_KILLED,
)
from .. import txtui
from .shared import _get_jobids_from_pattern
from ..cluster_service import Cluster


def kill_cmd(jq: JobQueue, cluster, args):
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    if len(jobids) == 0:
        log.warning("No jobs found matching pattern")
    for jobid in jobids:
        # TODO: stop just marks the job as it shouldn't run any more.  tasks will still be claimed.
        log.info("Marking %s as killed", jobid)
        ok, job = jq.kill_job(jobid)
        assert ok
        if not args.keepcluster:
            cluster.stop_cluster(job.cluster)
            _reconcile_claimed_tasks(jq, tasks)

        # if there are any sit sitting at pending, mark them as killed
        tasks = jq.task_storage.get_tasks(jobid, status=STATUS_PENDING)
        txtui.user_print("Marking {} pending tasks as killed".format(len(tasks)))
        for task in tasks:
            jq.reset_task(task.task_id, status=STATUS_KILLED)




def _reconcile_claimed_tasks(jq: JobQueue, cluster: Cluster):
    "For all 'claimed' tasks associated with cluster, confirm that the owner is still running"
    raise NotImplemented


#     tasks = jq.get_tasks_for_cluster(job.cluster, STATUS_CLAIMED)
#     for task in tasks:
#         _update_if_owner_missing(cluster, jq, task)


# def _update_if_owner_missing(cluster : Cluster, jq : JobQueue, task : Task):
#     if task.status != STATUS_CLAIMED:
#         return
#     if not cluster.is_owner_running(task.owner):
#         job = jq.get_job(task.job_id)
#         if job.status == JOB_STATUS_KILLED:
#             new_status = STATUS_KILLED
#         else:
#             new_status = STATUS_PENDING
#         log.info(
#             "Task %s is owned by %s which does not appear to be running, resetting status from 'claimed' to '%s'",
#             task.task_id,
#             task.owner,
#             new_status,
#         )
#         jq.reset_task(task.task_id, status=new_status)


def add_kill_cmd(subparser):
    parser = subparser.add_parser("kill", help="Terminate the specified job")
    parser.set_defaults(func=kill_cmd)
    parser.add_argument(
        "--keepcluster",
        action="store_true",
        help="If set will also terminate the nodes that the job is using to run. (This could impact other running jobs that use the same docker image)",
    )
    parser.add_argument("jobid_pattern")
