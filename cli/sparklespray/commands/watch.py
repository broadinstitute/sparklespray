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


from dataclasses import dataclass
class ClusterStateQuery:
    def __init__(self, now : float, _get_tasks, _get_nodes):
        self.now = now
        self._get_tasks = _get_tasks
        self._get_nodes = _get_nodes

    def get_time(self):
        return self.now

    def get_tasks(self):
        return self._get_tasks()

    def get_nodes(self):
        return self._get_nodes()

@dataclass
class NextPoll:
    "Return from PeriodicTask.poll() to indicate when this task should be run again"
    delay : float

class StopPolling:
    "Return from PeriodicTask.poll to indicate that all tasks should stop"

class PeriodicTask:
    "This is an abstract class for tasks which should do some work (via calling poll()), pause for some duration and then poll() will be called again."

    def poll(self, state : ClusterStateQuery):
        "Function to be implemented with behavior for this task. Task should return None, if this task should stop, or NextPoll(delay) if it should be called again after `delay` seconds."
        return None
    
    def finish(self, state : ClusterStateQuery):
        "Function called after job is done and no more calls to poll() will be made"

import functools
@functools.total_ordering
class ScheduledTask:
    def __init__(self, timestamp, task):
        self.timestamp = timestamp
        self.task = task
    
    def __lt__(self, other):
        return self.timestamp < other.timestamp
    def __eq__(self, other):
        return self.timestamp == other.timestamp

from typing import List
import heapq

class RateLimitedCall:
    def __init__(self, callback, min_delay):
        self.callback = callback
        self.prev_value = None
        self.prev_call_timestamp = None
        self.min_delay = min_delay

    def __call__(self):
        now = time.time()
        if (self.prev_call_timestamp is None) or ((now -self.prev_call_timestamp) > self.min_delay):
            self.prev_call_timestamp = now
            #print("calling", self.callback)
            self.prev_value = self.callback()
        return self.prev_value


from ..task_store import TaskStore
def run_tasks(job_id: str, cluster_id: str, tasks: List[PeriodicTask], cluster: Cluster):
    now = time.time()
    timeline = []

    # schedule all tasks to run immediately
    for task in tasks:
        heapq.heappush(timeline, ScheduledTask(now, task))

    get_tasks = RateLimitedCall(lambda: cluster.task_store.get_tasks(job_id), 1)
    get_nodes = RateLimitedCall(lambda: cluster.node_req_store.get_node_reqs(cluster_id), 1)

    while True:
        now = time.time()

        # get the next task whose time is soonest
        scheduled_task = heapq.heappop(timeline)
        
        # if it's not yet time for this task, sleep until then
        if scheduled_task.timestamp > now:
            time.sleep(scheduled_task.timestamp - now)

        # now, run the task and find out if we need to run it again later
        #print("calling", scheduled_task.task)
        next_action = scheduled_task.task.poll(ClusterStateQuery(now, get_tasks, get_nodes))
        #print("next action", next_action)

        if next_action is not None:
            if isinstance(next_action, StopPolling):
                break
            else:
                assert isinstance(next_action, NextPoll)
                # add it to the schedule at the appropriate time
                heapq.heappush(timeline, ScheduledTask(now+next_action.delay, scheduled_task.task))

    # now that we're done polling, call finish() on all tasks
    final_state = ClusterStateQuery(time.time(), get_tasks, get_nodes)
    for task in tasks:
        task.finish(final_state)

class CompletionMonitor(PeriodicTask):
    def __init__(self):
        self.cur_delay = self.initial_delay = 0.5
        self.max_delay = 20

    def next_poll_delay(self):
        self.cur_delay = min(self.max_delay, self.cur_delay * 2)
        return NextPoll(self.cur_delay)

    def poll(self, state):
        # Is everything done? If so, terminate loop.
        tasks = state.get_tasks()
        if _count_incomplete_tasks(tasks) == 0:
            return StopPolling()
        return self.next_poll_delay()

from ..task_store import _is_terminal_status, Task
from ..node_req_store import REQUESTED_NODE_STATES, NodeReq

class RestartPreemptedTasks:
    "Have any nodes unexpectedly shutdown? If so, change the status of the tasks they owned to pending."

def _count_incomplete_tasks(tasks:List[Task]):
    return sum([1  for task in tasks if not _is_terminal_status(task.status) ])

def _count_requested_nodes(node_reqs:List[NodeReq]):
    return sum([1 for o in node_reqs if o.status in REQUESTED_NODE_STATES])

def _count_preempt_attempt(node_reqs:List[NodeReq]) -> int:
    return sum(
            [1 for o in node_reqs if o.node_class == NODE_REQ_CLASS_PREEMPTIVE]
        )


class ResizeCluster(PeriodicTask):
    # adjust cluster size
    # Given a (target size, a restart-preempt budget, current number of outstanding operations, current number of pending tasks)
    # decide whether to add more add_node operations or remove add_node operations.
    def __init__(
        self,
        target_node_count: int,
        max_preemptable_attempts: int,
        cluster_mod,
        seconds_between_modifications: int = 60,
    ) -> None:
        self.target_node_count = target_node_count
        self.max_preemptable_attempts = max_preemptable_attempts
        self.seconds_between_modifications = seconds_between_modifications
        self.cluster_mod = cluster_mod

    def poll(self, state: ClusterStateQuery):
        modified = False

        # cap our target by the number of tasks which have not finished
        target_node_count = min(
            self.target_node_count, _count_incomplete_tasks(state.get_tasks())
        )

        requested_nodes = _count_requested_nodes(state.get_nodes()) # state.get_requested_node_count()
        print("target_node_count > requested_nodes", target_node_count , requested_nodes)
        if target_node_count > requested_nodes:
            # Is our target higher than what we have now? Then add that many nodes
            remaining_preempt_attempts = (
                self.max_preemptable_attempts - _count_preempt_attempt(state.get_nodes())
            )
            nodes_to_add = target_node_count - requested_nodes
            if nodes_to_add > 0:
                print(
                    "Currently targeting having {} nodes running, but we've only requested {} nodes. Adding {}, remaining_preempt_attempts={}".format(
                        target_node_count,
                        requested_nodes,
                        nodes_to_add,
                        remaining_preempt_attempts,
                    )
                )
                for _ in range(nodes_to_add):
                    preemptable = remaining_preempt_attempts > 0
                    if preemptable:
                        remaining_preempt_attempts -= 1
                    self.cluster_mod.add_node(preemptable=preemptable)
                    modified = True

        elif target_node_count < requested_nodes:
            # We have requested too many. Start cancelling
            needs_cancel = requested_nodes - target_node_count
            if needs_cancel > 0:
                self.cluster_mod.cancel_nodes(state, needs_cancel)
                modified = True

        if modified:
            self.last_modification = state.get_time()

        return NextPoll(self.seconds_between_modifications)

class StreamLogs:
    # Stream output from one of the running processes.
    def __init__(self, cluster):
        self.log_monitor = None
        self.cluster = cluster

    def start_logging(self, state):
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
                    self.cluster.client, task.monitor_address, task.task_id
                )
                print_log_content(
                    None,
                    "[starting tail of log {}]".format(log_monitor.task_id),
                    from_sparkles=True,
                )

    def do_next_read(self):
        with _exception_guard(lambda: "polling log file threw exception"):
            try:
                self.log_monitor.poll()
            except CommunicationError as ex:
                log.warning(
                    "Got error polling log. shutting down log watch: {}".format(
                        ex
                    )
                )
                self.log_monitor.close()
                self.log_monitor = None

    def do_last_read(self):
        print_log_content(
            None,
            "[{} is no longer running, tail of log stopping]".format(
                self.log_monitor.task_id
            ),
            from_sparkles=True,
        )

        self.flush_stdout_from_complete_task(
                self.log_monitor.task_id, self.log_monitor.offset
            )
        self.log_monitor = None


    def flush_stdout_from_complete_task(self,task_id, offset):
        task = self.jq.task_storage.get_task(task_id)
        spec = json.loads(self.io.get_as_str(task.args))

        attempts = 0
        while True:
            rest_of_stdout = self.io.get_as_str(spec["stdout_url"], start=offset, must=False)
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


    def poll(self, state):
        if self.log_monitor is None:
            self.start_logging()
        else:
            if state.is_task_running(self.log_monitor.task_id):
                self.do_next_read()
            else:
                self.do_last_read()
        return NextPoll(1)

    def finish(self):
        if self.log_monitor is not None:
            self.do_last_read()

from ..task_store import Task, STATUS_FAILED
from ..cluster_service import NodeReq
from .shared import _summarize_task_statuses
from typing import Dict
from collections import defaultdict
from ..node_req_store import NODE_REQ_CLASS_NORMAL, NODE_REQ_CLASS_PREEMPTIVE

def format_summary(tasks: List[Task], node_reqs: List[NodeReq]):
    # compute status of tasks
    by_status: Dict[str, int] = defaultdict(lambda: 0)
    for t in tasks:
        if t.status == STATUS_COMPLETE:
            label = "{}(code={})".format(t.status, t.exit_code)
        elif t.status == STATUS_FAILED:
            label = "{}({})".format(t.status, t.failure_reason)
        else:
            label = t.status
        by_status[label] += 1
    statuses = sorted(by_status.keys())
    task_status = ", ".join(
        ["{} ({})".format(status, by_status[status]) for status in statuses]
    )

    # compute status of workers
    by_status = defaultdict(lambda: 0)
    to_desc = {
        NODE_REQ_CLASS_NORMAL: "non-preempt",
        NODE_REQ_CLASS_PREEMPTIVE: "preemptible",
    }
    for r in node_reqs:
        label = "{}(type={})".format(r.status, to_desc[r.node_class])
        by_status[label] += 1
    statuses = sorted(by_status.keys())
    node_status = ", ".join(
        [f"{status} ({by_status[status]})" for status in statuses]
    )

    msg = f"tasks: {task_status}, worker nodes: {node_status}"
    # if completion_rate:
    #     incomplete_task_count = self.get_incomplete_task_count()
    #     remaining_estimate_in_seconds = float(incomplete_task_count)/completion_rate
    #     msg += f", eta: {(remaining_estimate_in_seconds/60):.1f} minutes to complete remaining {incomplete_task_count} tasks"
    return msg


class PrintStatus(PeriodicTask):
    def __init__(self, initial_poll_delay, max_poll_delay):
        self.poll_delay = self.initial_poll_delay = initial_poll_delay
        self.max_poll_delay = max_poll_delay
        self.prev_summary = None

    def poll(self, state : ClusterStateQuery):
        summary = format_summary(state.get_tasks(), state.get_nodes())
        if self.prev_summary != summary:
            user_print(summary)
            self.prev_summary = summary

            self.poll_delay = self.initial_poll_delay
        else:
            # if the status hasn't changed since last time then slow down polling
            self.poll_delay = min(self.poll_delay * 1.5, self.max_poll_delay)
        return NextPoll(self.poll_delay)


class EstimateRateOfCompletion:
    def __init__(self):
        # how many completions do we need to see before we can estimate the rate of completion?
        self.min_completion_timestamps = 10
        # how many completions do we want to use to estimate our rate at most
        self.max_completion_timestamps = 50
        # a log of the timestamp of each completion in order it occurred
        self.completion_timestamps = []
        self.prev_completion_count = None

    def poll(self):
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


class StartupFailureMonitor(PeriodicTask):
    "Are nodes failing to start?"
    def __init__(self, completed_node_names):
        self.previously_completed = set(completed_node_names)
        self.prev_completed_node_names_count = None
        self.event_log = []

    def update(self, timestamp, tasks, completed_node_names):
        # if len(self.event_log) == 0:
        #     self.event_log.append(StartEvent(timestamp))

        # fast path: if no names have been added, just return because nothing to do
        if self.prev_completed_node_names_count == len(completed_node_names):
            return

        # record the count for next time
        self.prev_completed_node_names_count = len(completed_node_names)

        # find the number of tasks per node
        task_count_by_node_name = collections.defaultdict(lambda: 0)
        for task in tasks:
            if task.owner is None:
                continue
            node_name = task.owner.split("/")[-1]
            task_count_by_node_name[node_name] += 1

        # find nodes which have newly completed
        newly_completed = []
        for node_name in completed_node_names:
            if node_name not in self.previously_completed:
                newly_completed.append(node_name)

        # for each completion, check to see how many tasks were run
        nodes_finished_without_running_anything = 0
        nodes_finished_processing_at_least_one = 0
        for node_name in newly_completed:
            tasks_started = task_count_by_node_name[node_name]
            log.info(
                "Node %s completed after executing %d tasks", node_name, tasks_started
            )
            if tasks_started == 0:
                nodes_finished_without_running_anything += 1
            else:
                nodes_finished_processing_at_least_one += 1

        if (
            nodes_finished_without_running_anything
            + nodes_finished_processing_at_least_one
        ) > 0:
            self.event_log.append(
                NodesCompleted(
                    timestamp,
                    nodes_finished_processing_at_least_one,
                    nodes_finished_without_running_anything,
                )
            )

        # remember which nodes we've checked
        self.previously_completed.update(newly_completed)

        # keep track of the last 15 minutes of history
        oldest_timestamp = timestamp - (15 * 60)
        self.event_log = [x for x in self.event_log if x.timestamp > oldest_timestamp]

    def is_too_many_failures(self, timestamp):
        good = 0
        bad = 0
        max_bad = 10
        grace_period_elapsed = False

        for event in self.event_log:
            good += event.processed_count
            bad += event.zero_processed_count

            if good == 0 and bad >= max_bad:
                # this means no nodes were started that successfully started processing data
                # but at least 10 nodes started which failed without any processing

                # now, it's possible that nodes were started but the queue was drained before they
                # came online. If that's the case, give ourselves a grace period of 30 seconds.
                # if the queue is empty then we won't be starting any more nodes, and we'll realize
                # we're done before that period elapses.
                if (timestamp - event.timestamp) > 30:
                    grace_period_elapsed = True

        return grace_period_elapsed and (good == 0 and bad >= max_bad)



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

    tasks = [PrintStatus(initial_poll_delay, max_poll_delay), CompletionMonitor()] + other_tasks
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
        target_nodes,
        max_preemptable_attempts,
        cluster.get_cluster_mod(job_id))
    
    #reset_preempted = ResetPreempted()
    # get_preempted = GetPreempted()
    #         task_ids = get_preempted(state)
    #         if len(task_ids) > 0:
    #             log.warning(
    #                 "Resetting tasks which appear to have been preempted: %s",
    #                 ", ".join(task_ids),
    #             )
    #             for task_id in task_ids:
    #                 jq.reset_task(task_id)

    #     with _exception_guard(lambda: "rescaling cluster threw exception"):
    #         resize_cluster(state, cluster.get_cluster_mod(job_id))

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
