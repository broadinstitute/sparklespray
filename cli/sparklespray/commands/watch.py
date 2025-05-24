import time
from ..task_store import STATUS_COMPLETE
from ..config import Config
from ..io_helper import IO
from ..job_queue import JobQueue
from ..cluster_service import Cluster
from ..batch_api import JobSpec
from ..log import log
from ..watch import run_tasks, PrintStatus, CompletionMonitor, StreamLogs, ResizeCluster
from .shared import _resolve_jobid


class TimeoutException(Exception):
    """Exception raised when an operation times out."""

    pass


def add_watch_cmd(subparser):
    parser = subparser.add_parser("watch", help="Monitor the job")
    parser.set_defaults(func=watch_cmd)
    parser.set_defaults(loglive=True)
    parser.add_argument("jobid")
    parser.add_argument("--nodes", "-n", type=int, help="The target number of workers")
    parser.add_argument(
        "--verify",
        action="store_true",
        help="If set, before watching will confirm all finished jobs wrote their output. Any jobs whose output is missing will be reset",
    )
    parser.add_argument(
        "--loglive",
        help="Stream output (on by default)",
        action="store_true",
        dest="loglive",
    )
    parser.add_argument(
        "--no-loglive",
        help="tail the first running task we can find",
        action="store_false",
        dest="loglive",
    )


from ..batch_api import ClusterAPI
from ..cluster_service import create_cluster


def watch_cmd(
    jq: JobQueue,
    io: IO,
    config: Config,
    args,
    cluster_api: ClusterAPI,
    datastore_client,
):
    job_id = _resolve_jobid(jq, args.jobid)
    if args.verify:
        check_completion(jq, io, job_id)

    max_preemptable_attempts_scale = config.max_preemptable_attempts_scale

    cluster = create_cluster(
        config=config,
        jq=jq,
        datastore_client=datastore_client,
        cluster_api=cluster_api,
        job_id=job_id,
    )

    successful = watch(
        io,
        jq,
        cluster,
        target_nodes=args.nodes,
        loglive=args.loglive,
        max_preemptable_attempts_scale=max_preemptable_attempts_scale,
    )

    if successful:
        return 0
    else:
        return 1


def watch(
    io: IO,
    jq: JobQueue,
    cluster: Cluster,
    target_nodes=None,
    max_preemptable_attempts_scale=None,
    initial_poll_delay=1.0,
    max_poll_delay=30.0,
    loglive=None,
) -> bool:
    """
    Monitor and manage a running Sparklespray job cluster.

    This function sets up monitoring tasks to track job progress, stream logs,
    and manage cluster resources. It will continue running until all tasks
    complete or the user interrupts with Ctrl+C.

    Args:
        io: IO helper for interacting with cloud storage
        jq: JobQueue for accessing job and task information
        cluster: Cluster object managing compute resources
        target_nodes: Number of worker nodes to maintain (defaults to job's target_node_count)
        max_preemptable_attempts_scale: Scale factor for determining max preemptable nodes
            (multiplied by target_nodes if provided)
        initial_poll_delay: Starting delay between status polls in seconds
        max_poll_delay: Maximum delay between status polls in seconds
        loglive: Whether to stream logs from running tasks (defaults to True)

    Returns:
        20 if interrupted by keyboard interrupt, None otherwise

    Raises:
        Exception: If no tasks appear for the job after multiple checks
    """
    job_id = cluster.job_id
    job = jq.get_job_must(job_id)
    assert job is not None

    if target_nodes is None:
        target_nodes = job.target_node_count

    if max_preemptable_attempts_scale is None:
        max_preemptable_attempts = job.max_preemptable_attempts
    else:
        max_preemptable_attempts = target_nodes * max_preemptable_attempts_scale

    log.info(
        "targeting %s nodes. First %s nodes will be preemptive (from job: target_node_count=%s, max_preemptable_attempts=%s)",
        target_nodes,
        max_preemptable_attempts,
        job.target_node_count,
        job.max_preemptable_attempts,
    )

    _wait_until_tasks_exist(cluster, job_id)

    if loglive is None:
        loglive = True

    tasks = [
        CompletionMonitor(),
        StreamLogs(loglive, cluster, io),
        PrintStatus(initial_poll_delay, max_poll_delay),
    ]
    if target_nodes == 0:
        log.warning(
            "target_nodes = 0, so no VMs will be powered on. Worker will need to be started manually for anything to run"
        )
        job_spec = JobSpec.model_validate_json(job.kube_job_spec)
        from ..worker_job import get_consume_command

        log.warning(f'execute: {" ".join(get_consume_command(job_spec))}')
    else:
        tasks.append(
            ResizeCluster(
                cluster,
                target_nodes,
                max_preemptable_attempts,
            )
        )

    try:
        run_tasks(job_id, job.cluster, tasks, cluster)

        tasks = cluster.task_store.get_tasks(job_id=job_id)
        for task in tasks:
            if task.status != STATUS_COMPLETE or str(task.exit_code) != "0":
                return False

        return True
    except KeyboardInterrupt:
        print("Interrupted -- Exiting, but your job will continue to run unaffected.")
        return False


def _wait_until_tasks_exist(cluster: Cluster, job_id: str):
    tasks = cluster.task_store.get_tasks(job_id=job_id)

    check_attempts = 0
    while len(tasks) == 0:
        log.warning(
            "Did not see any tasks for %s, sleeping and will check again...", job_id
        )
        time.sleep(5)
        check_attempts += 1
        if check_attempts > 20:
            raise TimeoutException(
                "Even after checking many times, no tasks ever appeared. Aborting"
            )


def check_completion(jq: JobQueue, io: IO, job_id: str):
    successful_count = 0
    completed_count = 0

    tasks = jq.task_storage.get_tasks(job_id)
    for task in tasks:
        if task.status == STATUS_COMPLETE:
            if (completed_count % 100) == 0:
                print(
                    "Verified {} out of {} completed tasks successfully wrote output".format(
                        successful_count, len(tasks)
                    )
                )
            completed_count += 1
            if io.exists(task.command_result_url):
                successful_count += 1
            else:
                print(
                    "task {} missing {}, resetting".format(
                        task.task_id, task.command_result_url
                    )
                )
                jq.reset_task(task.task_id)
