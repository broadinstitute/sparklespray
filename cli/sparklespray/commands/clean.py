from ..task_store import (
    STATUS_CLAIMED,
    STATUS_PENDING,
)
from ..job_queue import JobQueue, Job
from ..cluster_service import Cluster, create_cluster
from ..log import log
from .shared import _get_jobids_from_pattern


def clean(
    cluster: Cluster,
    jq: JobQueue,
    job_id: str,
    force: bool = False,
):
    if force:
        cluster.stop_cluster()
    else:
        # Check to not remove tasks that are claimed and still running
        still_running = jq.task_storage.get_tasks(job_id, status=STATUS_CLAIMED)
        if len(still_running) > 0:
            log.warning(
                "job %s is still running (%d tasks), cannot remove. Job must be stopped before it can be removed",
                job_id,
                len(still_running),
            )
            return False

    jq.delete_job(job_id)

    return True


def clean_cmd(config, datastore_client, cluster_api, jq, args):
    job_ids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    for job_id in job_ids:
        log.info("Deleting %s", job_id)

        cluster = create_cluster(config, jq, datastore_client, cluster_api, job_id)

        clean(
            cluster,
            jq,
            job_id,
            args.force,
        )


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
