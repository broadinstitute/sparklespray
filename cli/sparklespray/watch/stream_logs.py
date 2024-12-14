import time
import json
from ..livelog.logclient import LogMonitor, CommunicationError
from ..txtui import print_log_content
import datetime
import json

from ..log import log
from .runner_types import NextPoll, ClusterStateQuery
from .shared import _exception_guard, _only_running_tasks
from ..cluster_service import Cluster
from .. import txtui
from .shared import (
    _only_failed_tasks,
    _only_running_tasks,
    _only_successful_tasks,
    _is_task_running,
)

from time import monotonic

class CooldownCounter:
    def __init__(self, initial_delay):
        self.initial_delay = initial_delay
        self.current_delay = initial_delay
        self.next_expiration = monotonic()
    
    def reset(self):
        self.current_delay = self.initial_delay

    def failure(self):
        self.next_expiration = monotonic() + self.current_delay
        self.current_delay = self.current_delay * 2
 
    def time_till_cool(self):
        return max(0.0, self.next_expiration - monotonic())
    
    
from collections import defaultdict

class StreamLogs:
    # Stream output from one of the running processes.
    def __init__(self, stream_logs: bool, cluster: Cluster, io):
        self.log_monitor = None
        self.cluster = cluster
        self.stream_logs = stream_logs
        self.complete_tasks_printed = 0
        self.io = io
        self.task_cooldown_counters = defaultdict(lambda: CooldownCounter(5))

    def start_logging(self, state: ClusterStateQuery):
        running = _only_running_tasks(state.get_tasks())
        if len(running) > 0:
            task = running[0]
            
            if self.task_cooldown_counters[task.task_id].time_till_cool() > 0:
                return

            if task.monitor_address is not None:
                log.info(
                    "Obtained monitor address for task %s: %s",
                    task.task_id,
                    task.monitor_address,
                )
                self.log_monitor = LogMonitor(
                    self.cluster.client, task.monitor_address, task.task_id
                )
                print_log_content(
                    None,
                    "[starting tail of log {}]".format(self.log_monitor.task_id),
                    from_sparkles=True,
                )

    def _abort_logging(self):
        # log.warning(f"Request for status from task {self.log_monitor.task_id} failed. This could be because the node was preempted. If so, the task will be restarted elsewhere.")
        task_id = self.log_monitor.task_id
        self.log_monitor.close()
        self.log_monitor = None
        # keep track of the fact that this task is behaving badly and backoff
        counter = self.task_cooldown_counters[task_id]
        print_log_content(
            None,
            f"Retreiving live log and status from {task_id} failed, but this could be a transient issue. Will continue monitoring, and won't try communicating with this task for at least {counter.current_delay} seconds.",
            is_important=False,
        )

        counter.failure()

    def do_next_read(self):
        assert self.log_monitor is not None

        with _exception_guard(lambda: "polling log file threw exception"):
            try:
                # print_log_content(
                #     None,
                #     "calling poll",
                #     from_sparkles=True,
                # )

                self.log_monitor.poll()
                # print_log_content(
                #     None,
                #     "calling poll complete",
                #     from_sparkles=True,
                # )
            except TimeoutError as ex:
                log.info(f"Got error polling log. shutting down log watch due to exception: {ex}")
                self._abort_logging()

    def do_last_read(self):
        assert self.log_monitor is not None

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

    def flush_stdout_from_complete_task(self, task_id, offset):
        task = self.cluster.task_store.get_task(task_id)
        spec = json.loads(self.io.get_as_str(task.args))

        attempts = 0
        while True:
            rest_of_stdout = self.io.get_as_str(
                spec["stdout_url"], start=offset, must=False
            )
            if rest_of_stdout is not None:
                break
            log.warning(
                "Log %s doesn't appear to exist yet. Will try again...",
                spec["stdout_url"],
            )
            time.sleep(5)
            attempts += 1
            if attempts > 10:
                log.warning("Log file never did appear. Giving up trying to tail log")
                return

        print_log_content(datetime.datetime.now(), rest_of_stdout)
        self.complete_tasks_printed += 1

    def poll(self, state: ClusterStateQuery):
        if self.stream_logs:
            if self.log_monitor is None:
                self.start_logging(state)
            else:
                if _is_task_running(self.log_monitor.task_id, state.get_tasks()):
                    self.do_next_read()
                else:
                    self.do_last_read()
            return NextPoll(1)
        else:
            return None

    def finish(self, state):
        if self.log_monitor is not None:
            self.do_last_read()

        self._print_final_summary(state)

    def _print_final_summary(self, state: ClusterStateQuery):
        tasks = state.get_tasks()
        failed_tasks = _only_failed_tasks(tasks)
        successful_tasks = _only_successful_tasks(tasks)

        assert len(tasks) == len(failed_tasks) + len(
            successful_tasks
        ), "Everything should be either failed or completed by this point"

        txtui.user_print(
            f"Job finished. {len(successful_tasks)} tasks completed successfully, {len(failed_tasks)} tasks failed"
        )

        if len(failed_tasks) > 0:
            log.warning(
                "At least one task failed. Dumping stdout from one of the failures."
            )
            self.flush_stdout_from_complete_task(failed_tasks[0].task_id, 0)

        # if we want the logs and yet we've never written out a single log, pick one at random and write it out
        elif self.complete_tasks_printed == 0 and self.stream_logs:
            log.info("Dumping arbitrary successful task stdout")
            self.flush_stdout_from_complete_task(successful_tasks[0].task_id, 0)


# class FlushStdout:
#     def __init__(self, jq, io):
#         self.flush_stdout_calls = 0
#         self.jq = jq
#         self.io = io

#     def __call__(self, task_id, offset):
#         flush_stdout_from_complete_task(self.jq, self.io, task_id, offset)
#         self.flush_stdout_calls += 1
