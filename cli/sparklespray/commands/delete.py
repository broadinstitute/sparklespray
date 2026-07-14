from ..job_queue import JobQueue
from ..cluster_service import Cluster, create_cluster
from ..log import log
from .shared import _get_jobids_from_pattern


def delete(
    cluster: Cluster,
    jq: JobQueue,
    job_id: str,
    stop_cluster: bool = True,
):
    if stop_cluster:
        cluster.stop_cluster()
    # reap terminal Batch jobs before deleting the job record, since resolving the
    # cluster id requires the job to still exist in the datastore
    cluster.delete_complete_requests()
    jq.delete_job(job_id)

    return True


def delete_cmd(config, datastore_client, cluster_api, jq, args):
    job_ids = _get_jobids_from_pattern(jq, args.jobid_pattern)
    for job_id in job_ids:
        log.info("Deleting %s", job_id)

        cluster = create_cluster(config, jq, datastore_client, cluster_api, job_id)

        delete(
            cluster,
            jq,
            job_id,
        )


def add_delete_cmd(subparser):
    parser = subparser.add_parser(
        "delete",
        help="Remove job from the database of jobs",
    )
    parser.set_defaults(func=delete_cmd)
    parser.add_argument(
        "jobid_pattern",
        nargs="?",
        help="If specified will only attempt to remove jobs that match this pattern",
    )
