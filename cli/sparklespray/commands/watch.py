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

class RetryError(Exception):
    pass


class TooManyNodeFailures(Exception):
    pass


from ..resize_cluster import ResizeCluster, GetPreempted
from ..io_helper import IO
from ..job_queue import JobQueue
from ..cluster_service import Cluster
from .. import txtui
from ..job_store import Job

from ..log import log
from ..txtui import user_print
from ..startup_failure_tracker import StartupFailureTracker


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


@contextlib.contextmanager
def _exception_guard(deferred_msg, reset=None):
    try:
        yield
    except OSError as ex:
        # consider these as non-fatal
        msg = deferred_msg()
        log.exception(msg)
        log.warning("Ignoring exception and continuing...")
        if reset is not None:
            reset()
    except RetryError as ex:
        msg = deferred_msg()
        log.exception(msg)
        log.warning("Ignoring exception and continuing...")
        if reset is not None:
            reset()


def print_error_lines(lines):
    from termcolor import colored, cprint

    for line in lines:
        print(colored(line, "red"))


def flush_stdout_from_complete_task(jq, io, task_id, offset):
    task = jq.task_storage.get_task(task_id)
    spec = json.loads(io.get_as_str(task.args))

    attempts = 0
    while True:
        rest_of_stdout = io.get_as_str(spec["stdout_url"], start=offset, must=False)
        if rest_of_stdout is not None:
            break
        log.warning(
            "Log %s doesn't appear to exist yet. Will try again...", spec["stdout_url"]
        )
        time.sleep(5)
        attempts += 1
        if attempts > 10:
            log.warning("Log file never did appear. Giving up trying to tail log")
            return

    print_log_content(datetime.datetime.now(), rest_of_stdout)




def _watch(
    job_id: str,
    state: ClusterState,
    initial_poll_delay: float,
    max_poll_delay: float,
    loglive: bool,
    cluster: Cluster,
    poll_cluster: Callable[[], None],
    flush_stdout_from_complete_task: Optional[Callable[[str, int], None]] = None,
):
    log_monitor = None
    prev_summary = None

    startup_failure_tracker = StartupFailureTracker(state.get_completed_node_names())

    # how many completions do we need to see before we can estimate the rate of completion?
    min_completion_timestamps = 10
    # how many completions do we want to use to estimate our rate at most
    max_completion_timestamps = 50
    # a log of the timestamp of each completion in order it occurred
    completion_timestamps = []
    prev_completion_count = None

    poll_delay = initial_poll_delay
    while True:
        with _exception_guard(
            lambda: "summarizing status of job {} threw exception".format(job_id)
        ):
            state.update()

        if state.is_done():
            break

        completion_count = state.get_failed_task_count() + state.get_successful_task_count()
        if prev_completion_count is None:
            prev_completion_count = completion_count
        else:
            now = time.time()
            for _ in range(completion_count - prev_completion_count):
                completion_timestamps.append(now)
            # if we have too many samples, drop the oldest
            while len(completion_timestamps) > max_completion_timestamps:
                del completion_timestamps[0]
            prev_completion_count = completion_count

        completion_rate = None
        if len(completion_timestamps) > min_completion_timestamps:
            completion_window = time.time() - completion_timestamps[0]
            if completion_window > 0:
                # only compute the completion rate if the time over which we measured is non-zero
                # otherwise we get a division by zero
                completion_rate = len(completion_timestamps)/completion_window

        summary = state.get_summary(completion_rate=completion_rate)
        if prev_summary != summary:
            user_print(summary)
            prev_summary = summary

            poll_delay = initial_poll_delay
        else:
            # if the status hasn't changed since last time then slow down polling
            poll_delay = min(poll_delay * 1.5, max_poll_delay)

        startup_failure_tracker.update(
            time.time(), state.tasks, state.get_completed_node_names()
        )
        if startup_failure_tracker.is_too_many_failures(time.time()):
            raise TooManyNodeFailures()

        poll_cluster()

        # check each of the failed nodes to see if we have no tasks

        if log_monitor is None:
            if loglive:
                running = list(state.get_running_tasks())
                if len(running) > 0:
                    task = running[0]
                    if task.monitor_address is not None:
                        log.info(
                            "Obtained monitor address for task %s: %s",
                            task.task_id,
                            task.monitor_address,
                        )
                        log_monitor = LogMonitor(
                            cluster.client, task.monitor_address, task.task_id
                        )
                        print_log_content(
                            None,
                            "[starting tail of log {}]".format(log_monitor.task_id),
                            from_sparkles=True,
                        )
        else:
            if state.is_task_running(log_monitor.task_id):
                with _exception_guard(lambda: "polling log file threw exception"):
                    try:
                        log_monitor.poll()
                    except CommunicationError as ex:
                        log.warning(
                            "Got error polling log. shutting down log watch: {}".format(
                                ex
                            )
                        )
                        log_monitor.close()
                        log_monitor = None

            else:
                print_log_content(
                    None,
                    "[{} is no longer running, tail of log stopping]".format(
                        log_monitor.task_id
                    ),
                    from_sparkles=True,
                )
                if flush_stdout_from_complete_task:
                    flush_stdout_from_complete_task(
                        log_monitor.task_id, log_monitor.offset
                    )
                log_monitor = None

        time.sleep(poll_delay)


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
    resize_cluster = ResizeCluster(
        target_nodes, max_preemptable_attempts=max_preemptable_attempts
    )
    get_preempted = GetPreempted()

    state = cluster.get_state(job_id)
    state.update()

    _wait_until_tasks_exist(state, job_id)

    if loglive is None:
        loglive = True

    def poll_cluster():
        with _exception_guard(lambda: "restarting preempted nodes threw exception"):
            task_ids = get_preempted(state)
            if len(task_ids) > 0:
                log.warning(
                    "Resetting tasks which appear to have been preempted: %s",
                    ", ".join(task_ids),
                )
                for task_id in task_ids:
                    jq.reset_task(task_id)

        with _exception_guard(lambda: "rescaling cluster threw exception"):
            resize_cluster(state, cluster.get_cluster_mod(job_id))

    try:
        _watch(
            job_id,
            state,
            initial_poll_delay,
            max_poll_delay,
            loglive,
            cluster,
            poll_cluster,
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
            raise Exception("Even after checking many times, no tasks ever appeared. Aborting")

def _print_final_summary(state, flush_stdout : FlushStdout, loglive : bool):
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
