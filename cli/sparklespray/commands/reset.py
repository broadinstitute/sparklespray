from ..task_store import (
    STATUS_FAILED,
    STATUS_CLAIMED,
    STATUS_PENDING,
    STATUS_KILLED,
    STATUS_COMPLETE,
)
from ..job_queue import JobQueue, Job
from ..cluster_service import Cluster
from ..io_helper import IO
from ..resize_cluster import GetPreempted
from ..config import get_config_path, load_config, create_services, Config, BadConfig
from ..log import log
from .shared import _get_jobids_from_pattern


def add_reset_cmd(subparser):
    parser = subparser.add_parser(
        "reset",
        help="Mark any 'claimed', 'killed' or 'failed' or jobs with non-zero exit jobs as ready for execution again.  Useful largely only during debugging issues with job submission. Potentially also useful for retrying after transient failures.",
    )
    parser.set_defaults(func=reset_cmd)
    parser.add_argument("jobid_pattern")
    parser.add_argument(
        "--all",
        action="store_true",
        help="If set, will mark all tasks as 'pending', not just 'claimed', 'killed' or 'failed' tasks. The first parameter can be either a job ID or an individual task ID",
    )

def reset_cmd(jq: JobQueue, io: IO, cluster: Cluster, args):
    jobid_pattern = args.jobid_pattern
    if "." in jobid_pattern:
        task = jq.task_storage.get_task(jobid_pattern)
        print(f"reseting task from {task.status} -> STATUS_PENDING")
        jq._reset_task(task, STATUS_PENDING)
    else:
        for jobid in _get_jobids_from_pattern(jq, jobid_pattern):
            if args.all:
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

            cluster.cleanup_node_reqs(jobid)
            log.info("Cleaned up old node requests")
