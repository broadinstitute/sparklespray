import time
from ..task_store import STATUS_COMPLETE
from ..config import Config
from ..io_helper import IO
from ..job_queue import JobQueue
from ..cluster_service import Cluster

from ..log import log
from ..watch import run_tasks, PrintStatus, CompletionMonitor, StreamLogs, ResizeCluster
from .shared import _resolve_jobid


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


def watch_cmd(jq: JobQueue, io: IO, cluster: Cluster, config: Config, args):
    jobid = _resolve_jobid(jq, args.jobid)
    if args.verify:
        check_completion(jq, io, jobid)

    max_preemptable_attempts_scale = config.max_preemptable_attempts_scale

    watch(
        io,
        jq,
        jobid,
        cluster,
        target_nodes=args.nodes,
        loglive=args.loglive,
        max_preemptable_attempts_scale=max_preemptable_attempts_scale,
    )


def watch(
    io: IO,
    jq: JobQueue,
    job_id: str,
    cluster: Cluster,
    target_nodes=None,
    max_preemptable_attempts_scale=None,
    initial_poll_delay=1.0,
    max_poll_delay=30.0,
    loglive=None,
):
    job = jq.get_job(job_id)
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
        ResizeCluster(
            target_nodes, max_preemptable_attempts, cluster.get_cluster_mod(job_id)
        ),
        StreamLogs(loglive, cluster, io),
        PrintStatus(initial_poll_delay, max_poll_delay),
    ]

    try:
        run_tasks(job_id, job.cluster, tasks, cluster)
    except KeyboardInterrupt:
        print("Interrupted -- Exiting, but your job will continue to run unaffected.")
        return 20


def _wait_until_tasks_exist(cluster: Cluster, job_id: str):
    state = cluster.get_state(job_id)
    state.update()

    check_attempts = 0
    while len(state.get_tasks()) == 0:
        log.warning(
            "Did not see any tasks for %s, sleeping and will check again...", job_id
        )
        time.sleep(5)
        state.update()
        check_attempts += 1
        if check_attempts > 20:
            raise Exception(
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
