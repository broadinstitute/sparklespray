from ..task_store import (
    STATUS_CLAIMED,
    STATUS_COMPLETE,
)
from ..job_queue import JobQueue
from ..cluster_service import Cluster
from ..io_helper import IO
from .shared import _get_jobids_from_pattern
import json
from .. import txtui
from ..print_failures import print_failures

from .shared import _summarize_task_statuses

from ..cluster_service import create_cluster


def status_cmd(jq: JobQueue, io: IO, args, config, datastore_client, cluster_api):
    """
    Display the status of one or more Sparklespray jobs.
    
    This function retrieves and displays job status information, including:
    - Overall job completion status
    - Optional detailed statistics about task execution times
    - Optional memory usage statistics from completed tasks
    - Optional information about failed tasks
    
    Args:
        jq: JobQueue instance for accessing job and task information
        io: IO helper for interacting with cloud storage
        args: Command line arguments containing jobid_pattern, stats and failed flags
        config: Configuration object with project settings
        datastore_client: Google Cloud Datastore client
        cluster_api: API for interacting with compute clusters
        
    Returns:
        None
    """
    jobids = _get_jobids_from_pattern(jq, args.jobid_pattern)

    for jobid in jobids:
        cluster = create_cluster(config, jq, datastore_client, cluster_api, jobid)

        tasks = cluster.task_store.get_tasks(jobid)
        status, complete = _summarize_task_statuses(tasks)
        txtui.user_print(f"{jobid}: {status}")
        if args.stats:
            task_times = []

            command_result_urls = []
            claim_count_per_task = []
            for task in tasks:
                command_result_urls.append(task.command_result_url)

                claimed_time = None
                task_time = None
                claimed_count = 0
                for entry in task.history:
                    if entry.status == STATUS_CLAIMED:
                        claimed_time = entry.timestamp
                        claimed_count += 1
                    elif entry.status == STATUS_COMPLETE:
                        task_time = entry.timestamp - claimed_time
                claim_count_per_task.append(claimed_count)

                if task_time is not None:
                    task_times.append(task_time)

            claim_count_per_task.sort()
            n = len(claim_count_per_task)
            txtui.user_print(
                "Number of times a task was claimed quantiles: {}, {}, {}, {}, {}, mean: {:1f}".format(
                    claim_count_per_task[0],
                    claim_count_per_task[int(n / 4)],
                    claim_count_per_task[int(n / 2)],
                    claim_count_per_task[int(n * 3 / 4)],
                    claim_count_per_task[n - 1],
                    sum(claim_count_per_task) / n,
                )
            )

            task_times.sort()
            n = len(task_times)
            txtui.user_print(
                "task count: {}, execution time quantiles (in minutes): {:.1f}, {:.1f}, {:.1f}, {:.1f}, {:.1f}, mean: {:.1f}".format(
                    n,
                    task_times[0] / 60,
                    task_times[int(n / 4)] / 60,
                    task_times[int(n / 2)] / 60,
                    task_times[int(n * 3 / 4)] / 60,
                    task_times[n - 1] / 60,
                    sum(task_times) / n / 60,
                )
            )

            txtui.user_print("Getting memory stats...")
            results = io.bulk_get_as_str(command_result_urls).values()
            results = [json.loads(body) for body in results if body is not None]
            max_memory_size = [x["resource_usage"]["max_memory_size"] for x in results]
            max_memory_size.sort()
            n = len(max_memory_size)
            txtui.user_print(
                "max memory quantiles: {}, {}, {}, {}, {}, mean: {}".format(
                    max_memory_size[0],
                    max_memory_size[int(n / 4)],
                    max_memory_size[int(n / 2)],
                    max_memory_size[int(n * 3 / 4)],
                    max_memory_size[n - 1],
                    sum(max_memory_size) / n,
                )
            )
        if args.failed:
            print_failures(jq, io, jobid, False)


def add_status_cmd(subparser):
    parser = subparser.add_parser(
        "status", help="Print the status for the tasks which make up the specified job"
    )
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--failed", action="store_true")
    parser.set_defaults(func=status_cmd)
    parser.add_argument("jobid_pattern", nargs="?")
