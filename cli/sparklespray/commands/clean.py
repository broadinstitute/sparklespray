from ..task_store import (
    STATUS_CLAIMED,
    STATUS_PENDING,
)
from ..job_queue import JobQueue, Job
from ..cluster_service import Cluster
from ..resize_cluster import GetPreempted
from ..log import log
from .shared import _get_jobids_from_pattern


def clean(
    cluster: Cluster,
    jq: JobQueue,
    job_id: str,
    force: bool = False,
    force_pending: bool = False,
    only_nodes: bool = False,
):
    cluster.cleanup_node_reqs(job_id)
    if only_nodes:
        return

    if not force:
        # Check to not remove tasks that are claimed and still running
        tasks = cluster.task_store.get_tasks(job_id, status=STATUS_CLAIMED)
        if len(tasks) > 0:
            # if some tasks are still marked 'claimed' verify that the owner is still running
            reset_task_ids = _update_claimed_are_still_running(jq, cluster, job_id)

            still_running = []
            for task in tasks:
                if task.task_id not in reset_task_ids:
                    still_running.append(task.task_id)

            log.info(
                "reset_task_ids=%s, still_running=%s", reset_task_ids, still_running
            )
            if len(still_running) > 0:
                log.warning(
                    "job %s is still running (%d tasks), cannot remove",
                    job_id,
                    len(still_running),
                )
                return False

        if not force_pending:
            # Check to not remove tasks that are pending (waiting to be claimed)
            job = cluster.job_store.get_job(job_id)
            assert isinstance(job, Job)
            tasks = cluster.task_store.get_tasks(job_id, status=STATUS_PENDING)
            nb_tasks_pending = len(tasks)
            if nb_tasks_pending > 0 and cluster.has_active_node_requests(job.cluster):
                log.warning(
                    "Job {} has {} tasks that are still pending and active requests for VMs. If you want to force cleaning them,"
                    " please use 'sparkles kill {}'".format(
                        job_id, nb_tasks_pending, job_id
                    )
                )
                return False

    cluster.delete_job(job_id)
    return True


def clean_cmd(cluster: Cluster, jq, args):
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    for jobid in jobids:
        log.info("Deleting %s", jobid)
        clean(
            cluster,
            jq,
            jobid,
            args.force,
            args.force_pending,
            only_nodes=args.only_nodes,
        )


def _update_claimed_are_still_running(jq, cluster, job_id):
    get_preempted = GetPreempted(min_bad_time=0)
    state = cluster.get_state(job_id)
    state.update()
    task_ids = get_preempted(state)
    if len(task_ids) > 0:
        log.info(
            "Resetting tasks which appear to have been preempted: %s",
            ", ".join(task_ids),
        )
        for task_id in task_ids:
            jq.reset_task(task_id)
    return task_ids


def add_clean_cmd(subparser):
    parser = subparser.add_parser(
        "clean",
        help="Remove jobs which are not currently running from the database of jobs",
    )
    parser.set_defaults(func=clean_cmd)
    parser.add_argument(
        "--force",
        "-f",
        help="If set, will delete job regardless of whether it is running or not",
        action="store_true",
    )
    parser.add_argument(
        "jobid_pattern",
        nargs="?",
        help="If specified will only attempt to remove jobs that match this pattern",
    )
    parser.add_argument(
        "--force_pending",
        "-p",
        help="If set, will delete pending jobs",
        action="store_true",
    )
    parser.add_argument(
        "--only-nodes",
        action="store_true",
        help="If set, will not delete the job, only completed node requests.",
    )
