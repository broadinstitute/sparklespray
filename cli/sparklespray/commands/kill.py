from ..job_queue import JobQueue
from ..log import log
from ..task_store import STATUS_PENDING, STATUS_KILLED, STATUS_CLAIMED
from .. import txtui
from .shared import _get_jobids_from_pattern
from ..cluster_service import Cluster, create_cluster


def kill_cmd(jq: JobQueue, args, config, datastore_client, cluster_api):
    """
    Terminate a running Sparklespray job and optionally its cluster.

    This function handles the process of killing a job, including:
    - Marking the job as killed in the job store
    - Optionally stopping the compute cluster associated with the job
    - Resetting claimed tasks to allow them to be properly terminated
    - Marking any pending tasks as killed to prevent further execution

    Args:
        jq: JobQueue instance for accessing job and task information
        args: Command line arguments containing jobid_pattern and keepcluster flag
        config: Configuration object with project settings
        datastore_client: Google Cloud Datastore client
        cluster_api: API for interacting with compute clusters

    Returns:
        None
    """
    job_ids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    if len(job_ids) == 0:
        log.warning("No jobs found matching pattern")
    for job_id in job_ids:
        # TODO: stop just marks the job as it shouldn't run any more.  tasks will still be claimed.
        cluster = create_cluster(config, jq, datastore_client, cluster_api, job_id)
        log.info("Marking %s as killed", job_id)
        ok, job = jq.kill_job(job_id)
        assert ok
        if not args.keepcluster:
            cluster.stop_cluster()
            jq.reset(job_id, None, statuses_to_clear=[STATUS_CLAIMED])

        # if there are any sit sitting at pending, mark them as killed
        tasks = jq.task_storage.get_tasks(job_id, status=STATUS_PENDING)
        txtui.user_print("Marking {} pending tasks as killed".format(len(tasks)))
        for task in tasks:
            jq.reset_task(task.task_id, status=STATUS_KILLED)


def add_kill_cmd(subparser):
    parser = subparser.add_parser("kill", help="Terminate the specified job")
    parser.set_defaults(func=kill_cmd)
    parser.add_argument(
        "--keepcluster",
        action="store_true",
        help="If set will also terminate the nodes that the job is using to run. (This could impact other running jobs that use the same docker image)",
    )
    parser.add_argument("jobid_pattern")
