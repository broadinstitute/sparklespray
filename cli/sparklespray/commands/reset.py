from ..task_store import (
    STATUS_FAILED,
    STATUS_CLAIMED,
    STATUS_PENDING,
    STATUS_KILLED,
    STATUS_COMPLETE,
)
from ..job_queue import JobQueue
from ..cluster_service import Cluster
from ..io_helper import IO
from ..log import log
from .shared import _get_jobids_from_pattern
from ..cluster_service import create_cluster
from ..reset import reset_orphaned_tasks

def add_reset_cmd(subparser):
    parser = subparser.add_parser(
        "reset",
        help="Mark any 'claimed', 'killed' or 'failed' or jobs with non-zero exit jobs as ready for execution again.  Useful largely only during debugging issues with job submission. Potentially also useful for retrying after transient failures.",
    )
    parser.set_defaults(func=reset_cmd)
    parser.add_argument("jobid_pattern")
    parser.add_argument("--orphaned", action="store_true", help="If set, will mark any tasks which are claimed but the owner isn't live as 'pending'")
    parser.add_argument(
        "--all",
        action="store_true",
        help="If set, will mark all tasks as 'pending', not just 'claimed', 'killed' or 'failed' tasks. The first parameter can be either a job ID or an individual task ID",
    )


def reset_cmd(jq: JobQueue, args, config, datastore_client, cluster_api):
    jobid_pattern = args.jobid_pattern
    if "." in jobid_pattern:
        task = jq.task_storage.get_task(jobid_pattern)
        print(f"reseting task from {task.status} -> STATUS_PENDING")
        jq._reset_task(task, STATUS_PENDING)
    else:
        job_ids = _get_jobids_from_pattern(jq, jobid_pattern)
        if len(job_ids) == 0:
            raise Exception(f'No jobs matched pattern "{jobid_pattern}"')
        for jobid in job_ids:
            cluster = create_cluster(config, jq, datastore_client, cluster_api, jobid)
            cluster.stop_cluster()

            if args.orphaned:
                assert not args.all, "--all and --orphaned cannot be used together"
                orphaned_state = reset_orphaned_tasks(jobid, cluster, jq)
                updated = orphaned_state.orphaned_count
            else:
                if args.all:
                    assert not args.orphaned, "--all and --orphaned cannot be used together"

                    statuses_to_clear = [
                        STATUS_CLAIMED,
                        STATUS_FAILED,
                        STATUS_COMPLETE,
                        STATUS_KILLED,
                    ]
                else:
                    statuses_to_clear = [STATUS_CLAIMED, STATUS_FAILED, STATUS_KILLED]
                log.info(
                    "reseting %s by changing tasks with statuses (%s) -> %s",
                    jobid,
                    ",".join(statuses_to_clear),
                    STATUS_PENDING,
                )
                updated = jq.reset(jobid, None, statuses_to_clear=statuses_to_clear)
            log.info("updated %d tasks", updated)
