import time
import sys
import logging
import contextlib
import json
from .logclient import LogMonitor

from google.gax.errors import RetryError

from .resize_cluster import ResizeCluster
from .io import IO
from .job_queue import JobQueue

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


class ReuseRecentValue:
    def __init__(self, fn, max_age_in_seconds):
        self.fn = fn
        self.max_age_in_seconds = max_age_in_seconds
        self.last_call_timestamp = None
        self.last_value = None
    
    def get(self):
        if self.last_call_timestamp is None or (time.time() - self.last_call_timestamp) < self.max_age_in_seconds:
            self.last_value = self.fn()
            self.last_call_timestamp = time.time()
        return self.last_value

# class TasksStatusWrapper:
#     def __init__(self, get_tasks):
#         self.get_tasks = get_tasks
#
#     def get_status(self):
#         return TasksStatus(self.get_tasks())
#
# class ReportTaskStatus:
#     def __init__(self, tsw):
#         self.tsw = tsw
#         self.prev_status = None
#
#     def __call__(self):
#         status = self.tsw.get_status().summary
#         if status != self.prev_status:
#             log.info("Tasks: %s", status)
#         self.prev_status = status


# class ReportClusterStatus:
#     def __init__(self, get_cluster_status, max_age_in_seconds=60):
#         self.last_check = None
#         self.prev_status = None
#         self.max_age_in_seconds = max_age_in_seconds
#         self.get_cluster_status = get_cluster_status
#         self.last_good_state_time = None
#
#     def __call__(self, cluster):
#         if self.last_check is None or (time.time() - self.last_check) > self.max_age_in_seconds:
#             cluster_status = self.get_cluster_status()
#             if self.prev_status != cluster_status:
#                 log.info("Nodes: %s", cluster_status.as_string())
#
#             if cluster_status.is_running():
#                 last_good_state_time = time.time()
#             else:
#                 if time.time() - last_good_state_time > self.min_check_time and not saturate:
#                     log.error("Tasks haven't completed, but cluster is now offline. Aborting!")
#                     node_reqs = jq.get_node_reqs(jobid)
#                     for i, node_req in enumerate(node_reqs):
#                         log.info("Dumping node request %d", i)
#
#                         status = cluster.get_add_node_status(node_req.operation_id)
#                         print(json.dumps(status.status, indent=2))
#                     raise Exception("Cluster prematurely stopped")
#             self.prev_status = cluster_status
#             self.last_check = time.time()

def dump_stdout_if_single_task(jq, io, jobid):
    tasks = jq.get_tasks(jobid)
    if len(tasks) != 1:
        return
    task = list(tasks)[0]
    spec = json.loads(io.get_as_str(task.args))
    stdout_lines = io.get_as_str(spec['stdout_url']).split("\n")
    stdout_lines = stdout_lines[-100:]
    print_error_lines(stdout_lines)


def watch(io : IO, jq : JobQueue, job_id :str, poll_delay=10.0, loglive=False):
    job = jq.get_job(job_id)

    log_monitor = None
    if loglive:
        if len(job.tasks) != 1:
            log.warning("Could not tail logs because there are %d tasks, and we can only watch one task at a time", len(job.tasks))
        else:
            task_id = job.tasks[0]
            task = jq.storage.get_task(task_id)
            log_monitor = LogMonitor(jq.storage.client, task.monitor_address, task_id)

    resize_cluster = ResizeCluster()

    prev_summary = None
    try:
        while True:
            with _exception_guard(lambda: "summarizing status of job {} threw exception".format(jobid)):
                state = cluster.get_state()

            if state.is_done():
                break

            summary = state.get_summary()
            if prev_summary != summary:
                log.info("%s", summary)
                prev_summary = summary

            # with _exception_guard(lambda: "restarting preempted nodes threw exception"):
            #     resub_preempted(state, cluster_mod)

            with _exception_guard(lambda: "rescaling cluster threw exception"):
                resize_cluster(state, cluster)

            if log_monitor is not None:
                with _exception_guard(lambda: "polling log file threw exception"):
                    log_monitor.poll()

            time.sleep(poll_delay)

        failures = state.get_failed_task_count()
        if failures > 0 and len(job.tasks) == 1:
            log.warning("Job failed, and there was only one task, so dumping the tail of the output from that task")
            dump_stdout_if_single_task(jq, io, jobid)

        return failures == 0

    except KeyboardInterrupt:
        print("Interrupted -- Exiting, but your job will continue to run unaffected.")
        sys.exit(1)