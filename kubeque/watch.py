import time
import sys
import logging
import contextlib
import json
from .logclient import LogMonitor

from google.gax.errors import RetryError

from .resize_cluster import ResizeCluster, GetPreempted
from .io import IO
from .job_queue import JobQueue
from .cluster_service import Cluster

log = logging.getLogger(__name__)

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

def dump_stdout_if_single_task(jq, io, jobid):
    tasks = jq.get_tasks(jobid)
    if len(tasks) != 1:
        return
    task = list(tasks)[0]
    spec = json.loads(io.get_as_str(task.args))
    stdout_lines = io.get_as_str(spec['stdout_url']).split("\n")
    stdout_lines = stdout_lines[-100:]
    print_error_lines(stdout_lines)


def watch(io : IO, jq : JobQueue, job_id :str, cluster: Cluster, target_nodes=None, initial_poll_delay=1.0, max_poll_delay=30.0):
    job = jq.get_job(job_id)
    loglive=None

    log_monitor = None

    resize_cluster = ResizeCluster(target_node_count=job.target_node_count,
                                   max_preemptable_attempts=job.max_preemptable_attempts)
    get_preempted = GetPreempted()

    poll_delay = initial_poll_delay
    prev_summary = None
    state = cluster.get_state(job_id)
    state.update()

    task_count = len(state.get_tasks())
    log.info("loglive=%s, tasks=%s", loglive, task_count)

    if loglive is None and task_count == 1:
        loglive = True
        log.info("Only one task, so tailing log")

    if loglive and task_count != 1:
        log.warning("Could not tail logs because there are %d tasks, and we can only watch one task at a time", len(job.tasks))
        loglive = False


    try:
        while True:
            with _exception_guard(lambda: "summarizing status of job {} threw exception".format(job_id)):
                state.update()

            if state.is_done():
                break

            summary = state.get_summary()
            if prev_summary != summary:
                log.info("%s", summary)
                prev_summary = summary

                poll_delay = initial_poll_delay
            else:
                # if the status hasn't changed since last time then slow down polling
                poll_delay = min(poll_delay * 1.5, max_poll_delay)

            with _exception_guard(lambda: "restarting preempted nodes threw exception"):
                task_ids = get_preempted(state)
                if len(task_ids) > 0:
                    log.info("Resetting tasks which appear to have been preempted: %s", ", ".join(task_ids))
                    for task_id in task_ids:
                        jq.reset_task(task_id)

            with _exception_guard(lambda: "rescaling cluster threw exception"):
                resize_cluster(state, cluster.get_cluster_mod(job_id))

            if log_monitor is None:
                if loglive:
                    task = list(state.get_tasks())[0]
                    if task.monitor_address is not None:
                        log.info("Obtained monitor address for task %s: %s", task.task_id, task.monitor_address)
                        log_monitor = LogMonitor(cluster.client, task.monitor_address, task.task_id)
            else:
                with _exception_guard(lambda: "polling log file threw exception"):
                    log_monitor.poll()

            time.sleep(poll_delay)

        failures = state.get_failed_task_count()
        successes = state.get_successful_task_count()
        log.info("Job finished. %d tasks completed successfully, %d tasks failed", successes, failures)
        if failures > 0 and len(job.tasks) == 1:
            log.warning("Job failed, and there was only one task, so dumping the tail of the output from that task")
            dump_stdout_if_single_task(jq, io, job_id)


        return failures == 0

    except KeyboardInterrupt:
        print("Interrupted -- Exiting, but your job will continue to run unaffected.")
        sys.exit(1)
