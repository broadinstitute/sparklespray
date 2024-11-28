import time
import sys
import logging
import contextlib
import json
from ..logclient import LogMonitor, CommunicationError
from typing import Callable, Optional
from ..cluster_service import ClusterState
from ..txtui import print_log_content
import datetime
import subprocess
import os
from ..task_store import STATUS_COMPLETE
import collections
from ..config import Config
import tempfile
import json
from ..task_store import Task, STATUS_FAILED
from ..cluster_service import NodeReq
from .shared import _summarize_task_statuses
from typing import Dict
from collections import defaultdict
from ..node_req_store import NODE_REQ_CLASS_NORMAL, NODE_REQ_CLASS_PREEMPTIVE
from ..resize_cluster import ResizeCluster, GetPreempted
from ..io_helper import IO
from ..job_queue import JobQueue
from ..cluster_service import Cluster
from .. import txtui
from ..job_store import Job

from ..log import log
from ..txtui import user_print
from ..startup_failure_tracker import StartupFailureTracker
from ..task_store import _is_terminal_status, Task
from ..node_req_store import REQUESTED_NODE_STATES, NodeReq


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
    from ..main import _resolve_jobid

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


def _watch(
    job_id: str,
    state: ClusterState,
    initial_poll_delay: float,
    max_poll_delay: float,
    loglive: bool,
    cluster: Cluster,
    other_tasks: List[PeriodicTask],
    flush_stdout_from_complete_task: Optional[Callable[[str, int], None]] = None,
):
    """
    Periodically update the following independent threads:
    6. What is the estimated completion rate?
    7. Stream output from one of the running processes.
    """

    tasks = [
        PrintStatus(initial_poll_delay, max_poll_delay),
        CompletionMonitor(),
    ] + other_tasks
    # if loglive:
    #     tasks.append(StreamLogs())

    run_tasks(job_id, state.cluster_id, tasks, cluster)


class FlushStdout:
    def __init__(self, jq, io):
        self.flush_stdout_calls = 0
        self.jq = jq
        self.io = io

    def __call__(self, task_id, offset):
        flush_stdout_from_complete_task(self.jq, self.io, task_id, offset)
        self.flush_stdout_calls += 1


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

    flush_stdout = FlushStdout(jq, io)

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
    state = cluster.get_state(job_id)
    state.update()

    _wait_until_tasks_exist(state, job_id)

    if loglive is None:
        loglive = True

    resize_cluster = ResizeCluster(
        target_nodes, max_preemptable_attempts, cluster.get_cluster_mod(job_id)
    )

    try:
        _watch(
            job_id,
            state,
            initial_poll_delay,
            max_poll_delay,
            loglive,
            cluster,
            [resize_cluster],
            flush_stdout_from_complete_task=flush_stdout,
        )
    except KeyboardInterrupt:
        print("Interrupted -- Exiting, but your job will continue to run unaffected.")
        return 20

    _print_final_summary(state, flush_stdout, loglive)


def _wait_until_tasks_exist(state, job_id):
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


def _print_final_summary(state, flush_stdout: FlushStdout, loglive: bool):
    failures = state.get_failed_task_count()
    successes = state.get_successful_task_count()
    txtui.user_print(
        f"Job finished. {successes} tasks completed successfully, {failures} tasks failed"
    )

    if failures > 0:
        log.warning(
            "At least one task failed. Dumping stdout from one of the failures."
        )
        failed_tasks = state.get_failed_tasks()
        flush_stdout(failed_tasks[0].task_id, 0)

    elif (
        flush_stdout.flush_stdout_calls == 0 and loglive
    ):  # if we want the logs and yet we've never written out a single log, pick one at random and write it out
        log.info("Dumping arbitrary successful task stdout")
        successful_tasks = state.get_successful_tasks()
        flush_stdout(successful_tasks[0].task_id, 0)

    return failures == 0


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
